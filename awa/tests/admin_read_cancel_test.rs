//! Cancel a real public read after PostgreSQL has entered its transaction,
//! before SQLx receives ReadyForQuery. No sleeps or custom BEGIN statements.

mod ci_timing;

use awa::model::admin::{self, ListJobsFilter};
use awa::model::{insert_many, migrations, InsertOpts, InsertParams, QueueStorage};
use awa::{AwaError, JobState, QueueStorageConfig};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::PgPool;
use std::io;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Notify};
use tokio::task::{JoinHandle, JoinSet};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn bound() -> Duration {
    ci_timing::scaled_timeout(Duration::from_secs(5))
}

#[derive(Default)]
struct BeginGate {
    armed: AtomicBool,
    held: watch::Sender<Option<i32>>,
    release: Notify,
}

impl BeginGate {
    async fn held_pid(&self) -> TestResult<i32> {
        let mut observed = self.held.subscribe();
        loop {
            if let Some(pid) = *observed.borrow_and_update() {
                return Ok(pid);
            }
            observed.changed().await?;
        }
    }
}

struct Proxy {
    port: u16,
    gate: Arc<BeginGate>,
    task: JoinHandle<()>,
}

impl Proxy {
    async fn start(options: &PgConnectOptions) -> TestResult<Self> {
        // The proxy is local and test-only; credentials and backend secrets are
        // forwarded unchanged and never logged. SSL is disabled only on it.
        assert!(
            options.get_socket().is_none(),
            "this TCP fixture needs a TCP DATABASE_URL"
        );
        let upstream = (options.get_host().to_string(), options.get_port());
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let port = listener.local_addr()?.port();
        let gate = Arc::new(BeginGate::default());
        let connections_gate = gate.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((client, _)) = accepted else { break };
                        let upstream = upstream.clone();
                        let gate = connections_gate.clone();
                        connections.spawn(async move { relay(client, upstream, gate).await });
                    }
                    Some(_finished) = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Ok(Self { port, gate, task })
    }

    async fn stop(self) {
        self.gate.release.notify_one();
        self.task.abort();
        let _ = self.task.await;
        // Dropping the listener's JoinSet aborts every relay and closes both
        // socket halves, including a relay still holding ReadyForQuery.
    }
}

async fn frame<R: AsyncRead + Unpin>(stream: &mut R) -> io::Result<Option<(u8, Vec<u8>)>> {
    let tag = match stream.read_u8().await {
        Ok(tag) => tag,
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    };
    let length = stream.read_u32().await?;
    if !(4..=1024 * 1024).contains(&length) {
        return Err(io::Error::other("invalid test proxy frame length"));
    }
    let mut payload = vec![0; (length - 4) as usize];
    stream.read_exact(&mut payload).await?;
    Ok(Some((tag, payload)))
}

async fn forward<W: AsyncWrite + Unpin>(stream: &mut W, tag: u8, payload: &[u8]) -> io::Result<()> {
    stream.write_u8(tag).await?;
    stream.write_u32((payload.len() + 4) as u32).await?;
    stream.write_all(payload).await
}

async fn relay(
    mut client: TcpStream,
    upstream: (String, u16),
    gate: Arc<BeginGate>,
) -> io::Result<()> {
    let mut server = TcpStream::connect((upstream.0.as_str(), upstream.1)).await?;
    // Startup packets precede the normal tagged PostgreSQL messages.
    let length = client.read_u32().await?;
    if !(8..=64 * 1024).contains(&length) {
        return Err(io::Error::other("invalid startup packet"));
    }
    let mut startup = vec![0; (length - 4) as usize];
    client.read_exact(&mut startup).await?;
    server.write_u32(length).await?;
    server.write_all(&startup).await?;

    let (mut client_read, mut client_write) = client.into_split();
    let (mut server_read, mut server_write) = server.into_split();
    let awaiting_begin = Arc::new(AtomicBool::new(false));
    let frontend_gate = gate.clone();
    let frontend_begin = awaiting_begin.clone();
    let frontend = async move {
        while let Some((tag, payload)) = frame(&mut client_read).await? {
            if tag == b'Q' {
                let sql = String::from_utf8_lossy(&payload);
                let is_begin = sql
                    .trim_end_matches('\0')
                    .split_whitespace()
                    .next()
                    .is_some_and(|first| first.eq_ignore_ascii_case("BEGIN"));
                if is_begin && frontend_gate.armed.swap(false, Ordering::SeqCst) {
                    frontend_begin.store(true, Ordering::SeqCst);
                }
            }
            forward(&mut server_write, tag, &payload).await?;
        }
        Ok::<_, io::Error>(())
    };
    let backend = async move {
        let mut pid = 0;
        let mut begin_completed = false;
        while let Some((tag, payload)) = frame(&mut server_read).await? {
            if tag == b'K' && payload.len() == 8 {
                pid = i32::from_be_bytes(payload[..4].try_into().expect("four PID bytes"));
            }
            if tag == b'C'
                && payload.as_slice() == b"BEGIN\0"
                && awaiting_begin.load(Ordering::SeqCst)
            {
                begin_completed = true;
            }
            if tag == b'Z' && begin_completed && awaiting_begin.swap(false, Ordering::SeqCst) {
                if payload.as_slice() != b"T" || pid <= 0 {
                    return Err(io::Error::other("BEGIN did not enter a server transaction"));
                }
                // CommandComplete was forwarded, but the real transaction
                // status frame stays here until the read future is dropped.
                gate.held.send_replace(Some(pid));
                gate.release.notified().await;
                begin_completed = false;
            }
            forward(&mut client_write, tag, &payload).await?;
        }
        Ok::<_, io::Error>(())
    };
    tokio::select! { result = frontend => result, result = backend => result }
}

struct Fixture {
    admin: PgPool,
    observer: PgPool,
    reader: PgPool,
    store: Arc<QueueStorage>,
    database: String,
    job_id: i64,
    proxy: Proxy,
}

impl Fixture {
    async fn new() -> TestResult<Self> {
        let url = awa_testing::setup::database_url();
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        let database = format!("awa_read_cancel_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE DATABASE {database}"))
            .execute(&admin)
            .await?;
        let options = PgConnectOptions::from_str(&url)?.database(&database);
        let observer = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options.clone())
            .await?;
        migrations::run(&observer).await?;
        awa_testing::setup::activate_queue_storage(&observer).await;
        let store = Arc::new(QueueStorage::new(QueueStorageConfig::default())?);
        let rows = insert_many(
            &observer,
            &[InsertParams {
                kind: "admin_read_cancel".to_string(),
                args: serde_json::json!({}),
                opts: InsertOpts {
                    queue: "admin_read_cancel".to_string(),
                    ..Default::default()
                },
            }],
        )
        .await?;
        let proxy = Proxy::start(&options).await?;
        let reader = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(bound())
            .connect_with(
                options
                    .host("127.0.0.1")
                    .port(proxy.port)
                    .ssl_mode(PgSslMode::Disable),
            )
            .await?;
        Ok(Self {
            admin,
            observer,
            reader,
            store,
            database,
            job_id: rows[0].id,
            proxy,
        })
    }

    async fn close(self) -> TestResult {
        self.proxy.gate.release.notify_one();
        // Stop relays even if a broken connection cannot be returned normally.
        let closed = tokio::time::timeout(bound(), self.reader.close()).await;
        self.proxy.stop().await;
        if closed.is_err() {
            tokio::time::timeout(bound(), self.reader.close()).await?;
        }
        self.observer.close().await;
        tokio::time::timeout(
            bound(),
            sqlx::query(&format!("DROP DATABASE {} WITH (FORCE)", self.database))
                .execute(&self.admin),
        )
        .await??;
        self.admin.close().await;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum Read {
    Job,
    Counts,
    Queue,
    Kinds,
    Timeseries,
    Jobs,
}

async fn public_read(which: Read, store: &QueueStorage, pool: &PgPool, job_id: i64) -> TestResult {
    match which {
        Read::Job => {
            let job = store.load_job(pool, job_id).await?.expect("job exists");
            assert_eq!((job.id, job.state), (job_id, JobState::Available));
        }
        Read::Counts => assert_eq!(
            admin::state_counts(pool).await?.get(&JobState::Available),
            Some(&1)
        ),
        Read::Queue => assert_eq!(
            admin::queue_overview(pool, "admin_read_cancel")
                .await?
                .unwrap()
                .available,
            1
        ),
        Read::Kinds => assert_eq!(
            admin::job_kind_overviews(pool)
                .await?
                .iter()
                .find(|kind| kind.kind == "admin_read_cancel")
                .unwrap()
                .job_count,
            1
        ),
        Read::Timeseries => assert_eq!(
            admin::state_timeseries(pool, 60)
                .await?
                .iter()
                .filter(|bucket| bucket.state == JobState::Available)
                .map(|bucket| bucket.count)
                .sum::<i64>(),
            1
        ),
        Read::Jobs => assert_eq!(
            admin::list_jobs(pool, &ListJobsFilter::default())
                .await?
                .iter()
                .map(|job| job.id)
                .collect::<Vec<_>>(),
            vec![job_id]
        ),
    }
    Ok(())
}

async fn backend_is_clean(observer: &PgPool, pid: i32) -> TestResult {
    tokio::time::timeout(bound(), async {
        loop {
            let state: Option<(String, bool)> = sqlx::query_as(
                "SELECT state, xact_start IS NULL FROM pg_stat_activity WHERE pid = $1",
            )
            .bind(pid)
            .fetch_optional(observer)
            .await?;
            if state
                .as_ref()
                .is_none_or(|(state, no_tx)| state == "idle" && *no_tx)
            {
                return Ok::<_, sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| {
        io::Error::other(format!(
            "backend {pid} retained a transaction after read cancellation"
        ))
    })??;
    Ok(())
}

async fn cancelled_begin(which: Read) -> TestResult {
    let fixture = Fixture::new().await?;
    let reader = fixture.reader.clone();
    let observer = fixture.observer.clone();
    let store = fixture.store.clone();
    let gate = fixture.proxy.gate.clone();
    let job_id = fixture.job_id;
    let tested = tokio::spawn(async move {
        gate.armed.store(true, Ordering::SeqCst);
        let mut read = Box::pin(public_read(which, &store, &reader, job_id));
        let pid = tokio::select! {
            held = tokio::time::timeout(bound(), gate.held_pid()) => held??,
            result = &mut read => return Err(io::Error::other(format!("{which:?} completed before real BEGIN gate: {result:?}")).into()),
        };
        let state: (String, bool) = sqlx::query_as(
            "SELECT state, xact_start IS NOT NULL FROM pg_stat_activity WHERE pid = $1")
            .bind(pid).fetch_one(&observer).await?;
        assert_eq!(state, ("idle in transaction".to_string(), true), "real BEGIN reached server");
        drop(read);
        gate.release.notify_one();
        backend_is_clean(&observer, pid).await?;
        tokio::time::timeout(bound(), public_read(which, &store, &reader, job_id)).await??;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }).await;
    let cleanup = fixture.close().await;
    tested??;
    cleanup
}

#[tokio::test]
async fn cancelled_load_job_begin_does_not_return_open_transaction() -> TestResult {
    cancelled_begin(Read::Job).await
}

#[tokio::test]
async fn cancelled_state_counts_begin_does_not_return_open_transaction() -> TestResult {
    cancelled_begin(Read::Counts).await
}

#[tokio::test]
async fn cancelled_queue_overview_begin_does_not_return_open_transaction() -> TestResult {
    cancelled_begin(Read::Queue).await
}

#[tokio::test]
async fn cancelled_kind_overviews_begin_does_not_return_open_transaction() -> TestResult {
    cancelled_begin(Read::Kinds).await
}

#[tokio::test]
async fn cancelled_timeseries_begin_does_not_return_open_transaction() -> TestResult {
    cancelled_begin(Read::Timeseries).await
}

#[tokio::test]
async fn cancelled_job_list_begin_does_not_return_open_transaction() -> TestResult {
    cancelled_begin(Read::Jobs).await
}

#[tokio::test]
async fn completed_begin_and_query_error_preserve_healthy_connection() -> TestResult {
    let fixture = Fixture::new().await?;
    let reader = fixture.reader.clone();
    let observer = fixture.observer.clone();
    let store = fixture.store.clone();
    let job_id = fixture.job_id;
    let tested = tokio::spawn(async move {
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&reader)
            .await?;
        public_read(Read::Job, &store, &reader, job_id).await?;
        public_read(Read::Counts, &store, &reader, job_id).await?;
        sqlx::query("ALTER TABLE awa.deferred_jobs RENAME TO cancel_test_deferred")
            .execute(&observer)
            .await?;
        let error = store
            .load_job(&reader, job_id)
            .await
            .expect_err("real post-BEGIN query error");
        assert!(
            matches!(error, AwaError::Database(sqlx::Error::Database(ref db))
            if db.code().as_deref() == Some("42P01")),
            "expected undefined table, got {error:?}"
        );
        backend_is_clean(&observer, pid).await?;
        sqlx::query("ALTER TABLE awa.cancel_test_deferred RENAME TO deferred_jobs")
            .execute(&observer)
            .await?;
        public_read(Read::Job, &store, &reader, job_id).await?;
        let reused: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&reader)
            .await?;
        assert_eq!(
            reused, pid,
            "completed BEGIN and ordinary query errors retain healthy reuse"
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await;
    let cleanup = fixture.close().await;
    tested??;
    cleanup
}
