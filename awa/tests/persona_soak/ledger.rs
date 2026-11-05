//! The client-side ledger: every job a persona enqueued, what the workers saw, and
//! what the system reported back. The database may prune terminal rows long before
//! the run ends, so the ledger is the oracle's memory of outcomes.

use super::config::Persona;
use std::collections::HashMap;
use std::sync::Mutex;

const SHARDS: usize = 64;

/// What a job is allowed to end as.
pub mod expect {
    pub const COMPLETED: u8 = 1;
    /// Failed, which with the DLQ enabled means a DLQ row.
    pub const FAILED: u8 = 2;
    pub const CANCELLED: u8 = 4;
    pub const ANY_TERMINAL: u8 = COMPLETED | FAILED | CANCELLED;
}

/// Facts about a job, accumulated as the run proceeds.
pub mod flag {
    /// An operator cancel, drain or purge may legitimately have removed the job.
    pub const OPERATOR_REMOVED: u32 = 1 << 0;
    pub const OPERATOR_CANCEL_OK: u32 = 1 << 1;
    pub const OPERATOR_MOVED: u32 = 1 << 2;
    pub const DLQ_RETRIED: u32 = 1 << 3;
    pub const HANDLER_CANCELLED: u32 = 1 << 4;
    pub const RESCUED: u32 = 1 << 5;
    pub const EXHAUSTED: u32 = 1 << 6;
    pub const COMPLETED_EVENT: u32 = 1 << 7;
    pub const RETURNED_COMPLETED: u32 = 1 << 8;
    /// The job is served by a firehose queue where lifecycle events are not collected.
    pub const NO_EVENTS: u32 = 1 << 9;
    /// First seen at execution time (cron-created); not enqueued by a producer.
    pub const UNREGISTERED: u32 = 1 << 10;
    /// A DLQ row for this job was purged by an operator.
    pub const DLQ_PURGED: u32 = 1 << 11;
    pub const CHAOS_VICTIM: u32 = 1 << 12;
}

#[derive(Debug, Clone)]
pub struct JobRecord {
    pub persona: Persona,
    pub queue: u32,
    pub enqueued_ms: u32,
    pub expect: u8,
    pub flags: u32,
    pub executions: u16,
    pub in_flight: u8,
    pub overlapped: bool,
    pub max_attempt: i16,
    pub first_start_ms: u32,
    pub last_start_ms: u32,
    pub completed_events: u8,
    pub exhausted_events: u8,
    pub cancel_events: u8,
    pub completed_event_ms: u32,
    /// When an operator cancel on this job committed, 0 if none.
    pub operator_cancel_ms: u32,
}

impl JobRecord {
    pub fn new(persona: Persona, queue: u32, enqueued_ms: u32, expect: u8, flags: u32) -> Self {
        Self {
            persona,
            queue,
            enqueued_ms,
            expect,
            flags,
            executions: 0,
            in_flight: 0,
            overlapped: false,
            max_attempt: 0,
            first_start_ms: 0,
            last_start_ms: 0,
            completed_events: 0,
            exhausted_events: 0,
            cancel_events: 0,
            completed_event_ms: 0,
            operator_cancel_ms: 0,
        }
    }

    pub fn has(&self, flag: u32) -> bool {
        self.flags & flag != 0
    }
}

pub struct Ledger {
    shards: Vec<Mutex<HashMap<i64, JobRecord>>>,
    /// `(cron name, fire time)` -> job ids executed for it; more than one is a fencing failure.
    cron_fires: Mutex<HashMap<(String, String), Vec<i64>>>,
}

impl Default for Ledger {
    fn default() -> Self {
        Self::new()
    }
}

impl Ledger {
    pub fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            cron_fires: Mutex::new(HashMap::new()),
        }
    }

    fn shard(&self, id: i64) -> &Mutex<HashMap<i64, JobRecord>> {
        &self.shards[(id as u64 % SHARDS as u64) as usize]
    }

    pub fn register(&self, id: i64, record: JobRecord) {
        self.shard(id).lock().expect("ledger").insert(id, record);
    }

    /// Applies `update` to the job's record, creating an unregistered record when the
    /// job was not enqueued by a producer (cron fires, operator moves).
    pub fn with<R>(
        &self,
        id: i64,
        default: impl FnOnce() -> JobRecord,
        update: impl FnOnce(&mut JobRecord) -> R,
    ) -> R {
        let mut shard = self.shard(id).lock().expect("ledger");
        let record = shard.entry(id).or_insert_with(|| {
            let mut record = default();
            record.flags |= flag::UNREGISTERED;
            record
        });
        update(record)
    }

    pub fn mark(&self, id: i64, flags: u32) {
        if let Some(record) = self.shard(id).lock().expect("ledger").get_mut(&id) {
            record.flags |= flags;
        }
    }

    pub fn mark_many(&self, ids: &[i64], flags: u32) {
        for id in ids {
            self.mark(*id, flags);
        }
    }

    /// Adds `flags` to every record for which `predicate` holds.
    pub fn mark_where(&self, predicate: impl Fn(&JobRecord) -> bool, flags: u32) -> usize {
        let mut marked = 0;
        for shard in &self.shards {
            for record in shard.lock().expect("ledger").values_mut() {
                if predicate(record) {
                    record.flags |= flags;
                    marked += 1;
                }
            }
        }
        marked
    }

    pub fn get(&self, id: i64) -> Option<JobRecord> {
        self.shard(id).lock().expect("ledger").get(&id).cloned()
    }

    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().expect("ledger").len())
            .sum()
    }

    /// Calls `visit` for every record. Shards are locked one at a time.
    pub fn for_each(&self, mut visit: impl FnMut(i64, &JobRecord)) {
        for shard in &self.shards {
            let shard = shard.lock().expect("ledger");
            for (id, record) in shard.iter() {
                visit(*id, record);
            }
        }
    }

    pub fn note_cron_fire(&self, name: String, fire_time: String, job_id: i64) -> bool {
        let mut fires = self.cron_fires.lock().expect("cron fires");
        let ids = fires.entry((name, fire_time)).or_default();
        if !ids.contains(&job_id) {
            ids.push(job_id);
        }
        ids.len() == 1
    }

    pub fn cron_duplicates(&self) -> Vec<((String, String), Vec<i64>)> {
        self.cron_fires
            .lock()
            .expect("cron fires")
            .iter()
            .filter(|(_, ids)| ids.len() > 1)
            .map(|(key, ids)| (key.clone(), ids.clone()))
            .collect()
    }

    pub fn cron_fire_count(&self) -> usize {
        self.cron_fires.lock().expect("cron fires").len()
    }
}
