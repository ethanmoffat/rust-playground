use std::sync::atomic::{AtomicBool, AtomicUsize};

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

struct TestClient {
    id: u64,
    healthy: bool,
}

struct TestManager {
    counters: Arc<Counters>,
    fail_create: AtomicBool,
    create_delay_millis: AtomicU64,
    recycle_delay: Duration,
}

impl Manager for TestManager {
    type Type = Session<TestClient>;
    type Error = SqlError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        tokio::time::sleep(Duration::from_millis(self.create_delay_millis.load(Ordering::Relaxed))).await;
        if self.fail_create.load(Ordering::Relaxed) {
            return Err(SqlError::ConnectTimeout);
        }
        let id = self.counters.created.load(Ordering::Relaxed);
        Ok(Session::new(
            TestClient { id, healthy: true },
            Arc::clone(&self.counters),
        ))
    }

    async fn recycle(&self, session: &mut Self::Type, _: &Metrics) -> RecycleResult<Self::Error> {
        tokio::time::sleep(self.recycle_delay).await;
        if session.client.healthy {
            Ok(())
        } else {
            Err(RecycleError::Backend(SqlError::DirtySession))
        }
    }
}

fn test_pool(max_size: usize, create_delay: Duration, recycle_delay: Duration) -> managed::Pool<TestManager> {
    match managed::Pool::builder(TestManager {
        counters: Arc::new(Counters::default()),
        fail_create: AtomicBool::new(false),
        create_delay_millis: AtomicU64::new(create_delay.as_millis() as u64),
        recycle_delay,
    })
    .max_size(max_size)
    .runtime(Runtime::Tokio1)
    .create_timeout(Some(Duration::from_secs(1)))
    .recycle_timeout(Some(Duration::from_secs(1)))
    .build()
    {
        Ok(pool) => pool,
        Err(error) => panic!("test pool setup failed: {error}"),
    }
}

async fn checkout(pool: &managed::Pool<TestManager>) -> Result<Lease<TestManager>, SqlError> {
    acquire_lease(pool, &pool.manager().counters, Duration::from_millis(100)).await
}

fn config() -> ConnectionConfig {
    ConnectionConfig::new("localhost", 1433, "application", "private-user", "private-password")
}

fn pool_config() -> PoolConfig {
    PoolConfig {
        max_size: 1,
        acquire_timeout: Duration::from_millis(100),
    }
}

#[test]
fn configuration_identity_and_secret_redaction() -> TestResult {
    let configuration = config();
    let pool = SqlPool::new(configuration.clone(), pool_config())?;
    assert!(pool.matches_config(&configuration));
    assert!(!pool.matches_config(&ConnectionConfig::new(
        "localhost",
        1433,
        "application",
        "other",
        "private-password"
    )));
    assert!(!pool.matches_config(&ConnectionConfig::new(
        "localhost",
        1433,
        "application",
        "private-user",
        "other"
    )));
    assert!(!pool.matches_config(&ConnectionConfig::new(
        "other",
        1433,
        "application",
        "private-user",
        "private-password"
    )));
    assert!(!pool.matches_config(&ConnectionConfig::new(
        "localhost",
        1434,
        "application",
        "private-user",
        "private-password"
    )));
    assert!(!pool.matches_config(&ConnectionConfig::new(
        "localhost",
        1433,
        "other",
        "private-user",
        "private-password"
    )));
    assert!(!pool.matches_config(&configuration.clone().trust_server_certificate(true)));
    assert!(
        !pool.matches_config(
            &configuration
                .clone()
                .with_timeouts(Duration::from_secs(1), Duration::from_secs(30))
        )
    );
    assert!(
        !pool.matches_config(
            &configuration
                .clone()
                .with_timeouts(Duration::from_secs(15), Duration::from_secs(1))
        )
    );
    let error = configuration.database_error(
        "login",
        tiberius::error::Error::Protocol("private-user private-password".into()),
    );
    for rendered in [
        format!("{configuration:?}"),
        format!("{pool:?}"),
        format!("{error:?}"),
        error.to_string(),
    ] {
        assert!(!rendered.contains("private-user"));
        assert!(!rendered.contains("private-password"));
    }
    assert!(std::error::Error::source(&error).is_none());
    Ok(())
}

#[test]
fn invalid_configuration_is_rejected_without_io() {
    for maximum in [0, tokio::sync::Semaphore::MAX_PERMITS + 1] {
        assert!(matches!(
            SqlPool::new(
                config(),
                PoolConfig {
                    max_size: maximum,
                    ..pool_config()
                }
            ),
            Err(SqlError::InvalidConfig(_))
        ));
    }
    assert!(matches!(
        SqlPool::new(
            config(),
            PoolConfig {
                acquire_timeout: Duration::ZERO,
                ..pool_config()
            }
        ),
        Err(SqlError::InvalidConfig(_))
    ));
    for configuration in [
        config().with_timeouts(Duration::ZERO, Duration::from_secs(1)),
        config().with_timeouts(Duration::from_secs(1), Duration::ZERO),
        ConnectionConfig::new("", 1433, "application", "", ""),
        ConnectionConfig::new("localhost", 1433, "", "", ""),
    ] {
        assert!(matches!(
            SqlPool::new(configuration, pool_config()),
            Err(SqlError::InvalidConfig(_))
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn clean_checkout_reuses_and_mutable_access_dirties_again() -> TestResult {
    let pool = test_pool(1, Duration::ZERO, Duration::ZERO);
    let mut first = checkout(&pool).await?;
    let id = first.object().client.id;
    first.reusable = true;
    drop(first);
    assert_eq!(pool.status().available, 1);
    let mut second = checkout(&pool).await?;
    assert_eq!(second.object().client.id, id);
    second.reusable = true;
    let _ = second.object_mut();
    assert!(!second.reusable);
    drop(second);
    assert_eq!(pool.status().size, 0);
    let third = checkout(&pool).await?;
    assert_ne!(third.object().client.id, id);
    drop(third);
    let counters = &pool.manager().counters;
    assert_eq!(counters.created.load(Ordering::Relaxed), 2);
    assert_eq!(counters.acquired.load(Ordering::Relaxed), 3);
    assert_eq!(counters.reused.load(Ordering::Relaxed), 1);
    assert_eq!(counters.discarded.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dirty_drop_releases_capacity() -> TestResult {
    let pool = test_pool(1, Duration::ZERO, Duration::ZERO);
    drop(checkout(&pool).await?);
    assert_eq!(pool.status().size, 0);
    drop(checkout(&pool).await?);
    assert_eq!(pool.manager().counters.created.load(Ordering::Relaxed), 2);
    assert_eq!(pool.manager().counters.discarded.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn task_abort_detaches_dirty_checkout_and_recovers_permit() -> TestResult {
    let pool = test_pool(1, Duration::ZERO, Duration::ZERO);
    let clone = pool.clone();
    let (ready, receiver) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let lease = checkout(&clone).await?;
        let _ = ready.send(());
        std::future::pending::<()>().await;
        drop(lease);
        Ok::<_, SqlError>(())
    });
    receiver.await?;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(pool.status().size, 0);
    let replacement = checkout(&pool).await?;
    assert_eq!(replacement.object().client.id, 1);
    drop(replacement);
    assert_eq!(pool.manager().counters.discarded.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn exhaustion_has_total_timeout_and_recovers() -> TestResult {
    let pool = test_pool(1, Duration::ZERO, Duration::ZERO);
    let held = checkout(&pool).await?;
    let started = Instant::now();
    assert!(matches!(checkout(&pool).await, Err(SqlError::AcquireTimeout)));
    assert_eq!(started.elapsed(), Duration::from_millis(100));
    assert_eq!(pool.status().size, 1);
    assert_eq!(pool.manager().counters.acquire_failures.load(Ordering::Relaxed), 1);
    drop(held);
    drop(checkout(&pool).await?);
    assert!(pool.manager().counters.acquire_wait_nanos.load(Ordering::Relaxed) >= 100_000_000);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn shared_clones_keep_one_connection_budget_under_concurrency() -> TestResult {
    let pool = test_pool(2, Duration::ZERO, Duration::ZERO);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let clone = pool.clone();
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        tasks.push(tokio::spawn(async move {
            let mut lease = checkout(&clone).await?;
            let concurrent = active.fetch_add(1, Ordering::Relaxed) + 1;
            peak.fetch_max(concurrent, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(1)).await;
            active.fetch_sub(1, Ordering::Relaxed);
            lease.reusable = true;
            Ok::<_, SqlError>(())
        }));
    }
    for task in tasks {
        task.await??;
    }
    assert_eq!(peak.load(Ordering::Relaxed), 2);
    assert_eq!(pool.status().size, 2);
    assert_eq!(pool.status().available, 2);
    assert_eq!(pool.manager().counters.created.load(Ordering::Relaxed), 2);
    assert_eq!(pool.manager().counters.acquired.load(Ordering::Relaxed), 12);
    pool.close();
    assert_eq!(pool.manager().counters.discarded.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn creation_failure_and_timeout_do_not_leak_slots() -> TestResult {
    let pool = test_pool(1, Duration::ZERO, Duration::ZERO);
    pool.manager().fail_create.store(true, Ordering::Relaxed);
    assert!(matches!(checkout(&pool).await, Err(SqlError::ConnectTimeout)));
    assert_eq!(pool.status().size, 0);
    pool.manager().fail_create.store(false, Ordering::Relaxed);
    drop(checkout(&pool).await?);
    let slow = test_pool(1, Duration::from_secs(2), Duration::ZERO);
    assert!(matches!(checkout(&slow).await, Err(SqlError::AcquireTimeout)));
    assert_eq!(slow.status().size, 0);
    assert_eq!(slow.manager().counters.created.load(Ordering::Relaxed), 0);
    assert!(matches!(
        acquire_lease(&slow, &slow.manager().counters, Duration::from_secs(3)).await,
        Err(SqlError::AcquireTimeout)
    ));
    assert_eq!(slow.status().size, 0);
    assert_eq!(slow.manager().counters.acquire_failures.load(Ordering::Relaxed), 2);
    slow.manager().create_delay_millis.store(0, Ordering::Relaxed);
    drop(checkout(&slow).await?);
    assert_eq!(slow.manager().counters.created.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn unhealthy_recycle_is_discarded_before_replacement() -> TestResult {
    let pool = test_pool(1, Duration::ZERO, Duration::ZERO);
    let mut lease = checkout(&pool).await?;
    lease.object_mut().client.healthy = false;
    lease.reusable = true;
    drop(lease);
    let replacement = checkout(&pool).await?;
    assert_eq!(replacement.object().client.id, 1);
    assert_eq!(pool.manager().counters.discarded.load(Ordering::Relaxed), 1);
    assert_eq!(pool.manager().counters.reused.load(Ordering::Relaxed), 0);
    drop(replacement);
    assert_eq!(pool.status().size, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn acquisition_cancellation_during_recycle_discards_and_recovers() -> TestResult {
    let pool = test_pool(1, Duration::ZERO, Duration::from_secs(1));
    let mut lease = checkout(&pool).await?;
    lease.reusable = true;
    drop(lease);
    let clone = pool.clone();
    let task = tokio::spawn(async move { checkout(&clone).await });
    tokio::task::yield_now().await;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(pool.status().size, 0);
    assert_eq!(pool.manager().counters.discarded.load(Ordering::Relaxed), 1);
    assert_eq!(pool.manager().counters.acquire_failures.load(Ordering::Relaxed), 1);
    drop(checkout(&pool).await?);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn total_acquisition_deadline_cancels_recycle_and_recovers() -> TestResult {
    let pool = test_pool(1, Duration::ZERO, Duration::from_secs(2));
    let mut lease = checkout(&pool).await?;
    lease.reusable = true;
    drop(lease);
    assert!(matches!(checkout(&pool).await, Err(SqlError::AcquireTimeout)));
    assert_eq!(pool.status().size, 0);
    assert_eq!(pool.manager().counters.discarded.load(Ordering::Relaxed), 1);
    drop(checkout(&pool).await?);
    assert_eq!(pool.manager().counters.created.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn acquisition_cancellation_during_creation_recovers_permit() -> TestResult {
    let pool = test_pool(1, Duration::from_secs(2), Duration::ZERO);
    let clone = pool.clone();
    let task = tokio::spawn(async move { checkout(&clone).await });
    tokio::task::yield_now().await;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(pool.status().size, 0);
    assert_eq!(pool.manager().counters.created.load(Ordering::Relaxed), 0);
    assert_eq!(pool.manager().counters.acquire_failures.load(Ordering::Relaxed), 1);
    pool.manager().create_delay_millis.store(0, Ordering::Relaxed);
    drop(checkout(&pool).await?);
    assert_eq!(pool.manager().counters.created.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn waiting_cancellation_releases_waiter_and_records_failure() -> TestResult {
    let pool = test_pool(1, Duration::ZERO, Duration::ZERO);
    let held = checkout(&pool).await?;
    let clone = pool.clone();
    let task = tokio::spawn(async move { checkout(&clone).await });
    tokio::task::yield_now().await;
    assert_eq!(pool.status().waiting, 1);
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(pool.status().waiting, 0);
    assert_eq!(pool.manager().counters.acquire_failures.load(Ordering::Relaxed), 1);
    drop(held);
    drop(checkout(&pool).await?);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn close_wakes_waiters_and_in_flight_clean_return_closes() -> TestResult {
    let pool = test_pool(1, Duration::ZERO, Duration::ZERO);
    let mut held = checkout(&pool).await?;
    let clone = pool.clone();
    let waiter = tokio::spawn(async move { checkout(&clone).await.map(|_| ()) });
    tokio::task::yield_now().await;
    pool.close();
    pool.close();
    assert!(matches!(waiter.await?, Err(SqlError::PoolClosed)));
    assert!(matches!(checkout(&pool.clone()).await, Err(SqlError::PoolClosed)));
    held.reusable = true;
    drop(held);
    assert_eq!(pool.status().size, 0);
    assert_eq!(pool.status().max_size, 0);
    assert_eq!(pool.manager().counters.discarded.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn close_during_creation_discards_and_rejects_checkout() -> TestResult {
    let pool = test_pool(1, Duration::from_millis(50), Duration::ZERO);
    let clone = pool.clone();
    let task = tokio::spawn(async move { checkout(&clone).await.map(|_| ()) });
    tokio::task::yield_now().await;
    pool.close();
    assert!(matches!(task.await?, Err(SqlError::PoolClosed)));
    assert_eq!(pool.status().size, 0);
    assert_eq!(pool.manager().counters.created.load(Ordering::Relaxed), 1);
    assert_eq!(pool.manager().counters.discarded.load(Ordering::Relaxed), 1);
    assert_eq!(pool.manager().counters.acquired.load(Ordering::Relaxed), 0);
    assert_eq!(pool.manager().counters.acquire_failures.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn total_connection_deadline_includes_stalled_login() -> TestResult {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let configuration = ConnectionConfig::new(
        "127.0.0.1",
        listener.local_addr()?.port(),
        "isolated",
        "user",
        "password",
    )
    .with_timeouts(Duration::from_millis(40), Duration::from_secs(1));
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        std::future::pending::<()>().await;
        drop(socket);
        Ok::<_, std::io::Error>(())
    });
    let result = SqlConnection::connect(&configuration).await;
    server.abort();
    let _ = server.await;
    assert!(matches!(result, Err(SqlError::ConnectTimeout)));
    Ok(())
}

fn live_config() -> Result<ConnectionConfig, Box<dyn std::error::Error + Send + Sync>> {
    use std::env;
    let standard = env::var("ARROWCOL_SQLSERVER_TEST").as_deref() == Ok("1");
    if !standard && env::var("ARROWCOL_SQL_TEST_ISOLATED").as_deref() != Ok("1") {
        return Err("set ARROWCOL_SQLSERVER_TEST=1 only for an isolated test server".into());
    }
    let password =
        match env::var_os("ARROWCOL_APP_PASSWORD_FILE").or_else(|| env::var_os("ARROWCOL_SQLSERVER_PASSWORD_FILE")) {
            Some(path) => {
                let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../..")
                    .join(path);
                std::fs::read_to_string(path)?.trim_end_matches(['\r', '\n']).to_owned()
            }
            None => env::var("ARROWCOL_SQL_TEST_PASSWORD")?,
        };
    Ok(ConnectionConfig::new(
        env::var("ARROWCOL_SQLSERVER_HOST").or_else(|_| env::var("ARROWCOL_SQL_TEST_HOST"))?,
        env::var("ARROWCOL_SQLSERVER_PORT")
            .or_else(|_| env::var("ARROWCOL_SQL_TEST_PORT"))?
            .parse()?,
        env::var("ARROWCOL_SQL_TEST_DATABASE").unwrap_or_else(|_| "arrowcol".to_owned()),
        env::var("ARROWCOL_SQL_TEST_USERNAME").unwrap_or_else(|_| "arrowcol_app".to_owned()),
        password,
    )
    .trust_server_certificate(env::var("ARROWCOL_SQL_TEST_TRUST_CERT").map_or(standard, |value| value == "1")))
}

async fn spid(client: &mut SqlConnection) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let row = client
        .simple_query("SELECT @@SPID")
        .await?
        .into_row()
        .await?
        .ok_or("missing session id")?;
    Ok(row.try_get::<i16, _>(0)?.ok_or("null session id")?.into())
}

#[tokio::test]
#[ignore = "requires explicit isolated SQL Server environment; see README"]
async fn live_spid_reuse_isolation_reset_and_active_transaction_rejection() -> TestResult {
    let pool = SqlPool::new(
        live_config()?,
        PoolConfig {
            max_size: 1,
            acquire_timeout: Duration::from_secs(5),
        },
    )?;
    let mut first = pool.acquire().await?;
    let first_id = spid(&mut first).await?;
    first
        .simple_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION; COMMIT TRANSACTION")
        .await?
        .into_results()
        .await?;
    first.finish().await?;
    drop(first);
    let mut second = pool.acquire().await?;
    assert_eq!(spid(&mut second).await?, first_id);
    let row = second
        .simple_query("SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID")
        .await?
        .into_row()
        .await?
        .ok_or("missing isolation row")?;
    assert_eq!(row.try_get::<i16, _>(0)?, Some(2));
    second.begin_transaction().await?;
    assert!(matches!(second.finish().await, Err(SqlError::DirtySession)));
    drop(second);
    let mut third = pool.acquire().await?;
    third.finish().await?;
    drop(third);
    assert_eq!(pool.stats().created, 2);
    assert_eq!(pool.stats().reused, 1);
    assert_eq!(pool.stats().discarded, 1);
    pool.close();
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit isolated SQL Server environment; see README"]
async fn live_cancelled_transaction_and_command_timeout_discard() -> TestResult {
    let configuration = live_config()?.with_timeouts(Duration::from_secs(5), Duration::from_millis(100));
    let pool = SqlPool::new(
        configuration,
        PoolConfig {
            max_size: 1,
            acquire_timeout: Duration::from_secs(5),
        },
    )?;
    let clone = pool.clone();
    let (ready, receiver) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut client = clone.acquire().await?;
        let configuration = client.config.clone();
        client
            .begin_transaction()
            .await
            .map_err(|error| configuration.database_error("test begin", error))?;
        let _ = ready.send(());
        client
            .simple_query("WAITFOR DELAY '00:00:03'")
            .await
            .map_err(|error| configuration.database_error("test wait", error))?
            .into_results()
            .await
            .map_err(|error| configuration.database_error("test wait", error))?;
        Ok::<_, SqlError>(())
    });
    receiver.await?;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(pool.stats().discarded, 1);
    let mut timed_out = pool.acquire().await?;
    assert!(timed_out.simple_query("WAITFOR DELAY '00:00:03'").await.is_err());
    drop(timed_out);
    assert_eq!(pool.stats().discarded, 2);
    let mut recovered = pool.acquire().await?;
    recovered.finish().await?;
    drop(recovered);
    assert_eq!(pool.stats().size, 1);
    pool.close();
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit isolated SQL Server environment; see README"]
async fn live_unpooled_finish_does_not_reset_or_inspect_session() -> TestResult {
    let mut client = SqlConnection::connect(&live_config()?).await?;
    client.begin_transaction().await?;
    client.finish().await?;
    let row = client
        .simple_query("SELECT @@TRANCOUNT")
        .await?
        .into_row()
        .await?
        .ok_or("missing transaction count")?;
    assert_eq!(row.try_get::<i32, _>(0)?, Some(1));
    client.rollback_transaction().await?;
    Ok(())
}
