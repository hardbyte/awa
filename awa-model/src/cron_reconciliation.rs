//! Owner-scoped periodic declarations and reconciliation. All decisions use
//! database time and the same transaction serializer as runtime evidence writes.
use crate::{
    cron::{self, CronJobRow, PeriodicJob},
    AwaError,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use uuid::Uuid;

/// Explicit opt-in: all periodic jobs registered on this client form the
/// owner's complete desired set, including an explicitly configured empty set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodicReconciliation {
    pub owner_id: String,
    pub revision: String,
    pub grace_ms: i64,
}
impl PeriodicReconciliation {
    pub fn authoritative(
        owner: impl Into<String>,
        revision: impl Into<String>,
        grace: Duration,
    ) -> Result<Self, AwaError> {
        let value = Self {
            owner_id: owner.into(),
            revision: revision.into(),
            grace_ms: i64::try_from(grace.as_millis())
                .map_err(|_| AwaError::Validation("cron grace period is too large".into()))?,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<(), AwaError> {
        if self.owner_id.trim().is_empty()
            || self.owner_id.len() > 200
            || self.owner_id.contains('\0')
            || self.revision.trim().is_empty()
            || self.revision.len() > 200
            || self.revision.contains('\0')
            || self.grace_ms < 0
        {
            return Err(AwaError::Validation("cron owner and revision must be non-empty, at most 200 bytes, without NUL; grace must be nonnegative".into()));
        }
        Ok(())
    }
}

/// Immutable diagnostic evidence used by a plan. Revisions are opaque labels.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct CronDeclaration {
    pub instance_id: Uuid,
    pub revision: String,
    pub desired_hash: String,
    pub grace_ms: i64,
    pub expires_at: DateTime<Utc>,
}
/// A dry run and an applied decision share this response contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronReconciliationPlan {
    pub owner_id: String,
    pub desired_hash: Option<String>,
    pub declarations: Vec<CronDeclaration>,
    pub blocking_instances: Vec<Uuid>,
    pub blockers: Vec<String>,
    pub additions: Vec<String>,
    pub updates: Vec<String>,
    pub retirements: Vec<String>,
    pub conflicts: Vec<String>,
    pub agreed_since: Option<DateTime<Utc>>,
    pub grace_remaining_ms: i64,
    pub evaluated_at: DateTime<Utc>,
    pub applied: bool,
}

pub fn canonical_manifest(jobs: &[PeriodicJob]) -> Result<(serde_json::Value, String), AwaError> {
    let mut names = BTreeSet::new();
    let mut ordered = jobs.to_vec();
    ordered.sort_by(|a, b| a.name.cmp(&b.name));
    for job in &ordered {
        if job.name.trim().is_empty() || job.name.len() > 200 || !names.insert(&job.name) {
            return Err(AwaError::Validation(format!(
                "cron schedule name must be non-empty, unique, and at most 200 bytes: {}",
                job.name
            )));
        }
    }
    // Consumers may enable serde_json/preserve_order through feature unification.
    // Normalize recursively so that cannot change the fleet protocol hash.
    let mut value = serde_json::to_value(ordered)?;
    value.sort_all_objects();
    let hash = blake3::hash(&serde_json::to_vec(&value)?)
        .to_hex()
        .to_string();
    Ok((value, hash))
}

pub async fn lock(conn: &mut PgConnection) -> Result<(), AwaError> {
    sqlx::query("SELECT awa.cron_protocol_lock()")
        .execute(conn)
        .await?;
    Ok(())
}

/// Publish within the runtime snapshot transaction. The snapshot's statement
/// trigger already owns the protocol lock; standalone callers take it here too.
pub async fn publish(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    instance: Uuid,
    config: &PeriodicReconciliation,
    jobs: &[PeriodicJob],
) -> Result<(), AwaError> {
    let conn = &mut **tx;
    config.validate()?;
    let (manifest, hash) = canonical_manifest(jobs)?;
    lock(conn).await?;
    sqlx::query("INSERT INTO awa.cron_owners(owner_id) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(&config.owner_id)
        .execute(&mut *conn)
        .await?;
    let known: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM awa.cron_manifests WHERE owner_id=$1 AND desired_hash=$2)",
    )
    .bind(&config.owner_id)
    .bind(&hash)
    .fetch_one(&mut *conn)
    .await?;
    if !known {
        sqlx::query("INSERT INTO awa.cron_manifests(owner_id,desired_hash,manifest) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING").bind(&config.owner_id).bind(&hash).bind(&manifest).execute(&mut *conn).await?;
    }
    sqlx::query("INSERT INTO awa.cron_declarations(instance_id, owner_id, revision, desired_hash, grace_ms) VALUES ($1,$2,$3,$4,$5) ON CONFLICT(instance_id) DO UPDATE SET owner_id=EXCLUDED.owner_id, revision=EXCLUDED.revision, desired_hash=EXCLUDED.desired_hash, grace_ms=EXCLUDED.grace_ms, last_seen_at=clock_timestamp()")
        .bind(instance).bind(&config.owner_id).bind(&config.revision).bind(&hash).bind(config.grace_ms).execute(&mut *conn).await?;
    let synced: Option<String> =
        sqlx::query_scalar("SELECT synced_hash FROM awa.cron_owners WHERE owner_id=$1")
            .bind(&config.owner_id)
            .fetch_one(&mut *conn)
            .await?;
    if synced.as_ref() != Some(&hash) {
        sqlx::query("SELECT set_config('awa.cron_owner', $1, true)")
            .bind(&config.owner_id)
            .execute(&mut *conn)
            .await?;
        // One batch per changed manifest, never one statement per schedule per
        // heartbeat. The INSERT itself establishes ownership atomically.
        sqlx::query(r#"
            INSERT INTO awa.cron_jobs(name,cron_expr,timezone,kind,queue,args,priority,max_attempts,tags,metadata,missed_fire_policy,owner_id)
            SELECT name,cron_expr,timezone,kind,queue,args,priority,max_attempts,tags,metadata,missed_fire_policy,$2
            FROM jsonb_to_recordset($1) AS j(name TEXT,cron_expr TEXT,timezone TEXT,kind TEXT,queue TEXT,args JSONB,priority SMALLINT,max_attempts SMALLINT,tags TEXT[],metadata JSONB,missed_fire_policy TEXT)
            ORDER BY name
            ON CONFLICT(name) DO UPDATE SET cron_expr=EXCLUDED.cron_expr,timezone=EXCLUDED.timezone,kind=EXCLUDED.kind,queue=EXCLUDED.queue,args=EXCLUDED.args,priority=EXCLUDED.priority,max_attempts=EXCLUDED.max_attempts,tags=EXCLUDED.tags,metadata=EXCLUDED.metadata,missed_fire_policy=EXCLUDED.missed_fire_policy,updated_at=clock_timestamp()
            WHERE awa.cron_jobs.owner_id=EXCLUDED.owner_id AND awa.cron_jobs.retired_at IS NULL
              AND ROW(awa.cron_jobs.cron_expr,awa.cron_jobs.timezone,awa.cron_jobs.kind,awa.cron_jobs.queue,awa.cron_jobs.args,awa.cron_jobs.priority,awa.cron_jobs.max_attempts,awa.cron_jobs.tags,awa.cron_jobs.metadata,awa.cron_jobs.missed_fire_policy)
                  IS DISTINCT FROM ROW(EXCLUDED.cron_expr,EXCLUDED.timezone,EXCLUDED.kind,EXCLUDED.queue,EXCLUDED.args,EXCLUDED.priority,EXCLUDED.max_attempts,EXCLUDED.tags,EXCLUDED.metadata,EXCLUDED.missed_fire_policy)
        "#).bind(&manifest).bind(&config.owner_id).execute(&mut *conn).await?;
        sqlx::query("UPDATE awa.cron_owners SET synced_hash=$2 WHERE owner_id=$1")
            .bind(&config.owner_id)
            .bind(&hash)
            .execute(&mut *conn)
            .await?;
        sqlx::query("SELECT set_config('awa.cron_owner', '', true)")
            .execute(&mut *conn)
            .await?;
    }
    evaluate_locked(conn, &config.owner_id, true, false).await?;
    Ok(())
}

/// Fail startup on an ownership conflict even while the owner's live manifests
/// disagree. Background publication retains conflicting evidence for diagnostics.
pub async fn check_registration(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    config: &PeriodicReconciliation,
    jobs: &[PeriodicJob],
) -> Result<(), AwaError> {
    let conn = &mut **tx;
    lock(conn).await?;
    let names: Vec<_> = jobs.iter().map(|j| j.name.as_str()).collect();
    let conflicts: Vec<String> = sqlx::query_scalar("SELECT name FROM awa.cron_jobs WHERE name=ANY($1) AND (owner_id IS DISTINCT FROM $2 OR retired_at IS NOT NULL) ORDER BY name").bind(&names).bind(&config.owner_id).fetch_all(&mut *conn).await?;
    if !conflicts.is_empty() {
        return Err(AwaError::Validation(format!("cron schedules {} are owned by another manager or retired; explicit adoption/transfer/restore required", conflicts.join(", "))));
    }
    Ok(())
}

fn definition(row: &CronJobRow) -> Result<PeriodicJob, AwaError> {
    Ok(PeriodicJob {
        name: row.name.clone(),
        cron_expr: row.cron_expr.clone(),
        timezone: row.timezone.clone(),
        kind: row.kind.clone(),
        queue: row.queue.clone(),
        args: row.args.clone(),
        priority: row.priority,
        max_attempts: row.max_attempts,
        tags: row.tags.clone(),
        metadata: row.metadata.clone(),
        missed_fire_policy: cron::CronMissedFirePolicy::parse(&row.missed_fire_policy)?,
    })
}

#[derive(Default, sqlx::FromRow)]
struct OwnerAgreement {
    agreed_hash: Option<String>,
    agreed_since: Option<DateTime<Utc>>,
    evidence_until: Option<DateTime<Utc>>,
}

async fn evaluate_locked(
    conn: &mut PgConnection,
    owner: &str,
    persist: bool,
    apply: bool,
) -> Result<CronReconciliationPlan, AwaError> {
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *conn)
        .await?;
    let state: Option<OwnerAgreement> = sqlx::query_as(if persist { "SELECT agreed_hash,agreed_since,evidence_until FROM awa.cron_owners WHERE owner_id=$1 FOR UPDATE" } else { "SELECT agreed_hash,agreed_since,evidence_until FROM awa.cron_owners WHERE owner_id=$1" }).bind(owner).fetch_optional(&mut *conn).await?;
    let blocking_instances: Vec<Uuid> = sqlx::query_scalar("SELECT instance_id FROM awa.runtime_instances WHERE last_seen_at + make_interval(secs => GREATEST(30.0, snapshot_interval_ms * 0.003)) > $1 AND cron_protocol IS DISTINCT FROM awa.cron_protocol_version() ORDER BY instance_id").bind(now).fetch_all(&mut *conn).await?;
    let declarations: Vec<CronDeclaration> = sqlx::query_as("SELECT d.instance_id,d.revision,d.desired_hash,d.grace_ms, LEAST(d.last_seen_at,r.last_seen_at) + make_interval(secs => GREATEST(30.0,r.snapshot_interval_ms * 0.003)) AS expires_at FROM awa.cron_declarations d JOIN awa.runtime_instances r USING(instance_id) WHERE d.owner_id=$1 AND LEAST(d.last_seen_at,r.last_seen_at) + make_interval(secs => GREATEST(30.0,r.snapshot_interval_ms * 0.003)) > $2 AND NOT r.shutting_down ORDER BY d.instance_id")
        .bind(owner).bind(now).fetch_all(&mut *conn).await?;
    let hashes: BTreeSet<_> = declarations
        .iter()
        .map(|d| d.desired_hash.clone())
        .collect();
    let mut p = CronReconciliationPlan {
        owner_id: owner.into(),
        desired_hash: None,
        declarations,
        blocking_instances,
        blockers: vec![],
        additions: vec![],
        updates: vec![],
        retirements: vec![],
        conflicts: vec![],
        agreed_since: None,
        grace_remaining_ms: 0,
        evaluated_at: now,
        applied: false,
    };
    if !p.blocking_instances.is_empty() {
        p.blockers.push("unsupported_runtime_protocol".into());
    }
    if p.declarations.is_empty() {
        p.blockers.push("zero_live_declarations".into());
    }
    if hashes.len() > 1 {
        p.blockers.push("mixed_desired_manifests".into());
    }
    if hashes.len() == 1 {
        p.desired_hash = hashes.first().cloned();
        if !persist || apply {
            let manifest: serde_json::Value = sqlx::query_scalar(
                "SELECT manifest FROM awa.cron_manifests WHERE owner_id=$1 AND desired_hash=$2",
            )
            .bind(owner)
            .bind(&p.desired_hash)
            .fetch_one(&mut *conn)
            .await?;
            let jobs: Vec<PeriodicJob> = serde_json::from_value(manifest)?;
            describe_changes(conn, owner, &jobs, &mut p).await?;
        }
    }
    if !p.conflicts.is_empty() {
        p.blockers.push("ownership_or_retirement_conflict".into());
    }
    if p.blockers.is_empty() {
        let grace = p.declarations.iter().map(|d| d.grace_ms).max().unwrap_or(0);
        let OwnerAgreement {
            agreed_hash: old_hash,
            agreed_since: since,
            evidence_until: until,
        } = state.unwrap_or_default();
        let since = if old_hash == p.desired_hash && until.is_some_and(|t| t > now) {
            since.unwrap_or(now)
        } else {
            now
        };
        p.agreed_since = Some(since);
        p.grace_remaining_ms = grace
            .saturating_sub((now - since).num_milliseconds())
            .max(0);
        if persist {
            sqlx::query("UPDATE awa.cron_owners SET agreed_hash=$2,agreed_since=$3,evidence_until=$4 WHERE owner_id=$1")
                .bind(owner).bind(&p.desired_hash).bind(since).bind(p.declarations.iter().map(|d|d.expires_at).min()).execute(&mut *conn).await?;
        }
        if p.grace_remaining_ms > 0 {
            p.blockers.push("grace_period".into());
        }
    } else if persist {
        sqlx::query("UPDATE awa.cron_owners SET agreed_hash=NULL,agreed_since=NULL,evidence_until=NULL WHERE owner_id=$1").bind(owner).execute(&mut *conn).await?;
    }
    if apply && p.blockers.is_empty() {
        sqlx::query("SELECT set_config('awa.cron_admin','on',true)")
            .execute(&mut *conn)
            .await?;
        let revisions: BTreeSet<_> = p.declarations.iter().map(|d| d.revision.as_str()).collect();
        sqlx::query("UPDATE awa.cron_jobs SET retired_at=$2,retired_by=$3,retired_revision=$4,updated_at=$2 WHERE name=ANY($1) AND owner_id=$3 AND retired_at IS NULL")
            .bind(&p.retirements).bind(now).bind(owner).bind(revisions.into_iter().collect::<Vec<_>>().join(", ")).execute(&mut *conn).await?;
        sqlx::query("SELECT set_config('awa.cron_admin','',true)")
            .execute(&mut *conn)
            .await?;
        p.applied = true;
    }
    Ok(p)
}

async fn describe_changes(
    conn: &mut PgConnection,
    owner: &str,
    jobs: &[PeriodicJob],
    p: &mut CronReconciliationPlan,
) -> Result<(), AwaError> {
    let names: Vec<_> = jobs.iter().map(|j| j.name.as_str()).collect();
    let rows = sqlx::query_as::<_, CronJobRow>(
        "SELECT * FROM awa.cron_jobs WHERE owner_id=$1 OR name=ANY($2) ORDER BY name",
    )
    .bind(owner)
    .bind(&names)
    .fetch_all(&mut *conn)
    .await?;
    let by_name: BTreeMap<_, _> = rows.iter().map(|r| (r.name.as_str(), r)).collect();
    let names: BTreeSet<_> = jobs.iter().map(|j| j.name.as_str()).collect();
    for job in jobs {
        match by_name.get(job.name.as_str()) {
            None => p.additions.push(job.name.clone()),
            Some(row) if row.owner_id.as_deref() != Some(owner) => p.conflicts.push(format!(
                "{}: owned by {}; explicit adoption/transfer required",
                job.name,
                row.owner_id.as_deref().unwrap_or("<unowned>")
            )),
            Some(row) if row.retired_at.is_some() => p
                .conflicts
                .push(format!("{}: retired; explicit restore required", job.name)),
            Some(row) => {
                if serde_json::to_value(definition(row)?)? != serde_json::to_value(job)? {
                    p.updates.push(job.name.clone());
                }
            }
        }
    }
    p.retirements = rows
        .iter()
        .filter(|r| {
            r.owner_id.as_deref() == Some(owner)
                && r.retired_at.is_none()
                && !names.contains(r.name.as_str())
        })
        .map(|r| r.name.clone())
        .collect();
    Ok(())
}

/// Preview a proposed complete manifest without publishing evidence or changing
/// schedule definitions. The live declaration gate remains visible in the plan.
pub async fn preview(
    pool: &PgPool,
    owner: &str,
    jobs: &[PeriodicJob],
) -> Result<CronReconciliationPlan, AwaError> {
    PeriodicReconciliation::authoritative(owner, "preview", Duration::ZERO)?;
    let (_, hash) = canonical_manifest(jobs)?;
    let mut tx = pool.begin().await?;
    lock(&mut tx).await?;
    let mut p = evaluate_locked(&mut tx, owner, false, false).await?;
    if p.desired_hash.as_ref() != Some(&hash) {
        p.blockers.push("manifest_not_published".into());
    }
    p.desired_hash = Some(hash);
    p.additions.clear();
    p.updates.clear();
    p.retirements.clear();
    p.conflicts.clear();
    describe_changes(&mut tx, owner, jobs, &mut p).await?;
    if !p.conflicts.is_empty()
        && !p
            .blockers
            .iter()
            .any(|b| b == "ownership_or_retirement_conflict")
    {
        p.blockers.push("ownership_or_retirement_conflict".into());
    }
    tx.commit().await?;
    Ok(p)
}

pub async fn plan(pool: &PgPool, owner: &str) -> Result<CronReconciliationPlan, AwaError> {
    evaluate(pool, owner, false).await
}
pub async fn reconcile(pool: &PgPool, owner: &str) -> Result<CronReconciliationPlan, AwaError> {
    evaluate(pool, owner, true).await
}
async fn evaluate(
    pool: &PgPool,
    owner: &str,
    apply: bool,
) -> Result<CronReconciliationPlan, AwaError> {
    let mut tx = pool.begin().await?;
    lock(&mut tx).await?;
    let p = evaluate_locked(&mut tx, owner, apply, apply).await?;
    tx.commit().await?;
    Ok(p)
}
pub async fn owners(pool: &PgPool) -> Result<Vec<String>, AwaError> {
    Ok(
        sqlx::query_scalar("SELECT owner_id FROM awa.cron_owners ORDER BY owner_id")
            .fetch_all(pool)
            .await?,
    )
}

/// An explicit operator action; dry-run is the default at CLI/API boundaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum CronOwnerAction {
    Adopt {
        name: String,
        owner_id: String,
        expected_owner: Option<String>,
    },
    Retire {
        name: String,
    },
    RetireOwner {
        owner_id: String,
    },
    Restore {
        name: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronActionPlan {
    pub action: CronOwnerAction,
    pub schedules: Vec<String>,
    pub applied: bool,
}
pub async fn operate(
    pool: &PgPool,
    action: CronOwnerAction,
    actor: &str,
    apply: bool,
) -> Result<CronActionPlan, AwaError> {
    if actor.trim().is_empty() {
        return Err(AwaError::Validation(
            "cron operator actor must be non-empty".into(),
        ));
    }
    let mut tx = pool.begin().await?;
    lock(&mut tx).await?;
    let rows = sqlx::query_as::<_, CronJobRow>(if apply {
        "SELECT * FROM awa.cron_jobs ORDER BY name FOR UPDATE"
    } else {
        "SELECT * FROM awa.cron_jobs ORDER BY name"
    })
    .fetch_all(&mut *tx)
    .await?;
    let selected: Vec<_> = rows
        .iter()
        .filter(|r| match &action {
            CronOwnerAction::RetireOwner { owner_id } => r.owner_id.as_ref() == Some(owner_id),
            CronOwnerAction::Adopt { name, .. }
            | CronOwnerAction::Retire { name }
            | CronOwnerAction::Restore { name } => &r.name == name,
        })
        .collect();
    if selected.is_empty() && !matches!(&action, CronOwnerAction::RetireOwner { .. }) {
        return Err(AwaError::Validation("cron schedule not found".into()));
    }
    if let CronOwnerAction::Adopt {
        owner_id,
        expected_owner,
        ..
    } = &action
    {
        PeriodicReconciliation::authoritative(owner_id, "operator", Duration::ZERO)?;
        if selected[0].owner_id != *expected_owner {
            return Err(AwaError::Validation(format!(
                "cron ownership conflict: expected {expected_owner:?}, found {:?}",
                selected[0].owner_id
            )));
        }
    }
    if apply {
        sqlx::query("SELECT set_config('awa.cron_admin','on',true)")
            .execute(&mut *tx)
            .await?;
        let selected_names: Vec<_> = selected.iter().map(|r| r.name.as_str()).collect();
        match &action {
            CronOwnerAction::Adopt { owner_id, .. } => {
                sqlx::query(
                    "INSERT INTO awa.cron_owners(owner_id) VALUES ($1) ON CONFLICT DO NOTHING",
                )
                .bind(owner_id)
                .execute(&mut *tx)
                .await?;
                sqlx::query("UPDATE awa.cron_jobs SET owner_id=$2,updated_at=clock_timestamp() WHERE name=ANY($1)").bind(&selected_names).bind(owner_id).execute(&mut *tx).await?;
            }
            CronOwnerAction::Restore { .. } => {
                sqlx::query("UPDATE awa.cron_jobs SET retired_at=NULL,retired_by=NULL,retired_revision=NULL,last_enqueued_at=clock_timestamp(),updated_at=clock_timestamp() WHERE name=ANY($1) AND retired_at IS NOT NULL").bind(&selected_names).execute(&mut *tx).await?;
            }
            _ => {
                sqlx::query("UPDATE awa.cron_jobs SET retired_at=clock_timestamp(),retired_by=$2,retired_revision='operator',updated_at=clock_timestamp() WHERE name=ANY($1) AND retired_at IS NULL").bind(&selected_names).bind(actor).execute(&mut *tx).await?;
            }
        }
        // Adoption, transfer and restoration begin a new agreement window.
        sqlx::query(
            "UPDATE awa.cron_owners SET synced_hash=NULL,agreed_since=NULL,evidence_until=NULL",
        )
        .execute(&mut *tx)
        .await?;
    }
    let p = CronActionPlan {
        action,
        schedules: selected.iter().map(|r| r.name.clone()).collect(),
        applied: apply,
    };
    tx.commit().await?;
    Ok(p)
}
