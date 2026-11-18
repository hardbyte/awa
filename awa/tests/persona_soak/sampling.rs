//! Sampler hygiene: what each oracle and sampler query ran under, and the samples a
//! timeout skipped. A skipped sample is counted and reported, never dropped silently.

use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Why a query gave up without an answer, when it was one of the bounds the samplers set
/// on purpose rather than a failure of the check.
pub fn skip_reason(error: &sqlx::Error) -> Option<&'static str> {
    match error {
        sqlx::Error::Database(database) => match database.code().as_deref() {
            Some("57014") => Some("statement_timeout"),
            Some("55P03") => Some("lock_timeout"),
            _ => None,
        },
        sqlx::Error::PoolTimedOut => Some("pool_timeout"),
        _ => None,
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SamplerStats {
    pub ok: u64,
    pub skipped: BTreeMap<String, u64>,
    pub last_skip: Option<String>,
}

#[derive(Default)]
pub struct SamplerLog {
    samplers: Mutex<BTreeMap<String, SamplerStats>>,
    /// Pool name to the `statement_timeout` and `lock_timeout` its sessions run under.
    pools: Mutex<BTreeMap<String, String>>,
}

impl SamplerLog {
    pub fn ok(&self, sampler: &str) {
        self.samplers
            .lock()
            .expect("sampler log")
            .entry(sampler.to_string())
            .or_default()
            .ok += 1;
    }

    /// Records a skipped sample; returns how many `reason` skips the sampler has had.
    pub fn skipped(&self, sampler: &str, reason: &str, detail: &str) -> u64 {
        let mut samplers = self.samplers.lock().expect("sampler log");
        let stats = samplers.entry(sampler.to_string()).or_default();
        stats.last_skip = Some(detail.chars().take(300).collect());
        let count = stats.skipped.entry(reason.to_string()).or_default();
        *count += 1;
        *count
    }

    pub fn register_pool(&self, pool: &str, settings: String) {
        self.pools
            .lock()
            .expect("sampler pools")
            .insert(pool.to_string(), settings);
    }

    pub fn stats(&self) -> BTreeMap<String, SamplerStats> {
        self.samplers.lock().expect("sampler log").clone()
    }

    pub fn pools(&self) -> BTreeMap<String, String> {
        self.pools.lock().expect("sampler pools").clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_are_counted_by_reason() {
        let log = SamplerLog::default();
        log.ok("stall");
        assert_eq!(log.skipped("stall", "statement_timeout", "canceled"), 1);
        assert_eq!(
            log.skipped("stall", "statement_timeout", "canceled again"),
            2
        );
        assert_eq!(log.skipped("stall", "lock_timeout", "lock"), 1);
        let stats = log.stats();
        assert_eq!(stats["stall"].ok, 1);
        assert_eq!(stats["stall"].skipped["statement_timeout"], 2);
        assert_eq!(stats["stall"].last_skip.as_deref(), Some("lock"));
        assert_eq!(
            skip_reason(&sqlx::Error::PoolTimedOut),
            Some("pool_timeout")
        );
        assert_eq!(skip_reason(&sqlx::Error::RowNotFound), None);
    }
}
