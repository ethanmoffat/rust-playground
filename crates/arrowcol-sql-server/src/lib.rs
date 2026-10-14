//! Bounded, opt-in SQL Server pooling with explicit successful-operation return.
//!
//! A checkout is dirty until [`SqlConnection::finish`] succeeds. Dropping a dirty
//! checkout detaches and closes its client instead of returning it to Deadpool.
//! Callers must consume every operation result and only call `finish` after a
//! successful operation (including any commit or rollback).

use std::{
    fmt,
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use deadpool::{
    Runtime,
    managed::{self, Manager, Metrics, RecycleError, RecycleResult},
};
use tiberius::{AuthMethod, Config, EncryptionLevel};
use tokio::{net::TcpStream, time::Instant};
use tokio_util::compat::TokioAsyncWriteCompatExt;

/// The existing Tiberius client and Tokio TCP transport.
pub type SqlClient = tiberius::Client<tokio_util::compat::Compat<TcpStream>>;

/// Connection identity and bounds. Credentials are private and never debugged.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectionConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
    trust_server_certificate: bool,
    connect_timeout: Duration,
    command_timeout: Duration,
}

impl ConnectionConfig {
    /// Creates a SQL-authenticated, encryption-required connection configuration.
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            database: database.into(),
            username: username.into(),
            password: password.into(),
            trust_server_certificate: false,
            connect_timeout: Duration::from_secs(15),
            command_timeout: Duration::from_secs(30),
        }
    }

    /// Controls certificate verification without disabling encryption.
    #[must_use]
    pub const fn trust_server_certificate(mut self, trust: bool) -> Self {
        self.trust_server_certificate = trust;
        self
    }

    /// Sets the total TCP/login bound and the driver command/reset bound.
    #[must_use]
    pub const fn with_timeouts(mut self, connect: Duration, command: Duration) -> Self {
        self.connect_timeout = connect;
        self.command_timeout = command;
        self
    }

    fn validate(&self) -> Result<(), SqlError> {
        if self.host.is_empty() || self.database.is_empty() {
            return Err(SqlError::InvalidConfig("host and database must not be empty"));
        }
        if self.connect_timeout.is_zero() || self.command_timeout.is_zero() {
            return Err(SqlError::InvalidConfig(
                "connection and command timeouts must be positive",
            ));
        }
        Ok(())
    }

    fn database_error(&self, phase: &'static str, error: tiberius::error::Error) -> SqlError {
        let mut message = error.to_string();
        // SQL Server login diagnostics can echo the login name. Do not retain a
        // raw driver source, which would bypass redaction through Error::source.
        for secret in [&self.password, &self.username] {
            if !secret.is_empty() {
                message = message.replace(secret, "[redacted]");
            }
        }
        SqlError::Database { phase, message }
    }
}

impl fmt::Debug for ConnectionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionConfig")
            .field("trust_server_certificate", &self.trust_server_certificate)
            .field("connect_timeout", &self.connect_timeout)
            .field("command_timeout", &self.command_timeout)
            .finish_non_exhaustive()
    }
}

/// Bounds for one role's pool. Pool clones share these bounds.
#[derive(Clone, Copy, Debug)]
pub struct PoolConfig {
    /// Maximum physical connections, including checked-out connections.
    pub max_size: usize,
    /// Total checkout bound, including waiting, health checking, and creation.
    pub acquire_timeout: Duration,
}

/// Safe-to-display connection/pool errors. Driver diagnostics redact credentials.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    /// Invalid configuration, rejected before network activity.
    #[error("invalid SQL Server pool configuration: {0}")]
    InvalidConfig(&'static str),
    /// TCP setup failure; no connection configuration is included.
    #[error("SQL Server TCP setup failed: {0}")]
    Tcp(#[source] std::io::Error),
    /// Total TCP and login deadline expired.
    #[error("SQL Server TCP/login timed out")]
    ConnectTimeout,
    /// Redacted driver diagnostics, including login or session-validation errors.
    #[error("SQL Server {phase} failed: {message}")]
    Database {
        /// Fixed operation name.
        phase: &'static str,
        /// Credential-redacted driver diagnostic.
        message: String,
    },
    /// Session validation/reset exceeded its total deadline.
    #[error("SQL Server session validation/reset timed out")]
    FinishTimeout,
    /// A transaction remains active or the verification response is invalid.
    #[error("SQL Server session is not clean (active transaction or invalid verification response)")]
    DirtySession,
    /// Waiting, creation, or recycling exceeded the total checkout deadline.
    #[error("SQL Server pool acquisition timed out")]
    AcquireTimeout,
    /// The pool has explicitly been closed.
    #[error("SQL Server pool is closed")]
    PoolClosed,
    /// An internal Deadpool setup failure.
    #[error("SQL Server pool setup failed")]
    PoolSetup,
}

/// Read-only snapshot. Cumulative counters use relaxed atomics, not a transaction.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PoolStats {
    /// Successful physical connections created.
    pub created: u64,
    /// Successful checkouts, including newly created connections.
    pub acquired: u64,
    /// Successful checkouts of previously returned connections.
    pub reused: u64,
    /// Physical connections dropped, including unhealthy/dirty and closed pools.
    pub discarded: u64,
    /// Failed or cancelled acquisition attempts.
    pub acquire_failures: u64,
    /// Cumulative checkout latency, including failed/cancelled attempts.
    pub acquire_wait_seconds: f64,
    /// Current physical connection count from Deadpool.
    pub size: usize,
    /// Current available connection count from Deadpool.
    pub available: usize,
    /// Current pool maximum; zero after close.
    pub max_size: usize,
}

#[derive(Default)]
struct Counters {
    created: AtomicU64,
    acquired: AtomicU64,
    reused: AtomicU64,
    discarded: AtomicU64,
    acquire_failures: AtomicU64,
    acquire_wait_nanos: AtomicU64,
}

struct Session<T> {
    client: T,
    counters: Arc<Counters>,
}

impl<T> Session<T> {
    fn new(client: T, counters: Arc<Counters>) -> Self {
        counters.created.fetch_add(1, Ordering::Relaxed);
        Self { client, counters }
    }
}

impl<T> Drop for Session<T> {
    fn drop(&mut self) {
        self.counters.discarded.fetch_add(1, Ordering::Relaxed);
    }
}

struct SqlManager {
    config: ConnectionConfig,
    counters: Arc<Counters>,
}

impl Manager for SqlManager {
    type Type = Session<Box<SqlClient>>;
    type Error = SqlError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let client = connect_client(&self.config).await?;
        Ok(Session::new(Box::new(client), Arc::clone(&self.counters)))
    }

    async fn recycle(&self, session: &mut Self::Type, _: &Metrics) -> RecycleResult<Self::Error> {
        verify_session(&mut session.client, &self.config)
            .await
            .map_err(RecycleError::Backend)
    }
}

/// Deadpool-managed pool. Construct separate instances for separate role budgets.
#[derive(Clone)]
pub struct SqlPool {
    pool: managed::Pool<SqlManager>,
    acquire_timeout: Duration,
}

impl SqlPool {
    /// Constructs a lazy bounded pool; this does not establish a connection.
    ///
    /// # Errors
    /// Rejects zero bounds, oversized pool limits, and invalid connection bounds.
    pub fn new(config: ConnectionConfig, pool_config: PoolConfig) -> Result<Self, SqlError> {
        config.validate()?;
        if pool_config.max_size == 0 || pool_config.max_size > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(SqlError::InvalidConfig(
                "pool maximum must be positive and within semaphore limits",
            ));
        }
        if pool_config.acquire_timeout.is_zero() {
            return Err(SqlError::InvalidConfig("acquisition timeout must be positive"));
        }
        let connect_timeout = config.connect_timeout;
        let command_timeout = config.command_timeout;
        let manager = SqlManager {
            config,
            counters: Arc::new(Counters::default()),
        };
        let pool = managed::Pool::builder(manager)
            .max_size(pool_config.max_size)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(pool_config.acquire_timeout))
            .create_timeout(Some(connect_timeout))
            .recycle_timeout(Some(command_timeout))
            .build()
            .map_err(|_| SqlError::PoolSetup)?;
        Ok(Self {
            pool,
            acquire_timeout: pool_config.acquire_timeout,
        })
    }

    /// Exclusively checks out a dirty-by-default client.
    ///
    /// # Errors
    /// Returns creation failures, a closed pool, or the total acquisition timeout.
    /// A failed idle health check is discarded by Deadpool before replacement.
    pub async fn acquire(&self) -> Result<SqlConnection, SqlError> {
        let lease = acquire_lease(&self.pool, &self.pool.manager().counters, self.acquire_timeout).await?;
        Ok(SqlConnection {
            inner: ConnectionInner::Pooled(lease),
            config: self.pool.manager().config.clone(),
        })
    }

    /// Checks exact identity, credentials, certificate policy, and timeout bounds.
    #[must_use]
    pub fn matches_config(&self, config: &ConnectionConfig) -> bool {
        &self.pool.manager().config == config
    }

    /// Closes all idle clients and rejects waiting/future checkouts.
    ///
    /// In-flight guards are not interrupted; they close when dropped. Call this
    /// after role work has drained. Closing any clone closes every clone.
    pub fn close(&self) {
        self.pool.close();
    }

    /// Returns cumulative counters and the current Deadpool status.
    #[must_use]
    pub fn stats(&self) -> PoolStats {
        let counters = &self.pool.manager().counters;
        let status = self.pool.status();
        PoolStats {
            created: counters.created.load(Ordering::Relaxed),
            acquired: counters.acquired.load(Ordering::Relaxed),
            reused: counters.reused.load(Ordering::Relaxed),
            discarded: counters.discarded.load(Ordering::Relaxed),
            acquire_failures: counters.acquire_failures.load(Ordering::Relaxed),
            acquire_wait_seconds: counters.acquire_wait_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
            size: status.size,
            available: status.available,
            max_size: status.max_size,
        }
    }
}

impl fmt::Debug for SqlPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlPool").field("stats", &self.stats()).finish()
    }
}

struct Lease<M: Manager> {
    object: Option<managed::Object<M>>,
    reusable: bool,
}

impl<M: Manager> Lease<M> {
    fn object(&self) -> &M::Type {
        match &self.object {
            Some(object) => object,
            None => unreachable!("a lease only loses its object during drop"),
        }
    }

    fn object_mut(&mut self) -> &mut M::Type {
        self.reusable = false;
        match &mut self.object {
            Some(object) => object,
            None => unreachable!("a lease only loses its object during drop"),
        }
    }
}

impl<M: Manager> Drop for Lease<M> {
    fn drop(&mut self) {
        if !self.reusable
            && let Some(object) = self.object.take()
        {
            drop(managed::Object::take(object));
        }
    }
}

struct Acquisition<'a> {
    counters: &'a Counters,
    started: Instant,
    succeeded: bool,
}

impl Drop for Acquisition<'_> {
    fn drop(&mut self) {
        let nanos = self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        let _ = self
            .counters
            .acquire_wait_nanos
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |previous| {
                Some(previous.saturating_add(nanos))
            });
        if !self.succeeded {
            self.counters.acquire_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn acquire_lease<M: Manager<Error = SqlError>>(
    pool: &managed::Pool<M>,
    counters: &Counters,
    timeout: Duration,
) -> Result<Lease<M>, SqlError> {
    let mut acquisition = Acquisition {
        counters,
        started: Instant::now(),
        succeeded: false,
    };
    let object = tokio::time::timeout(timeout, pool.get())
        .await
        .map_err(|_| SqlError::AcquireTimeout)?
        .map_err(|error| match error {
            managed::PoolError::Backend(error) => error,
            managed::PoolError::Closed => SqlError::PoolClosed,
            managed::PoolError::Timeout(_) => SqlError::AcquireTimeout,
            _ => SqlError::PoolSetup,
        })?;
    // Deadpool can finish an already-started create/recycle after close. Do not
    // hand such a client to a new operation after shutdown has been observed.
    if pool.is_closed() {
        drop(managed::Object::take(object));
        return Err(SqlError::PoolClosed);
    }
    counters.acquired.fetch_add(1, Ordering::Relaxed);
    if managed::Object::metrics(&object).recycle_count > 0 {
        counters.reused.fetch_add(1, Ordering::Relaxed);
    }
    acquisition.succeeded = true;
    Ok(Lease {
        object: Some(object),
        reusable: false,
    })
}

enum ConnectionInner {
    Pooled(Lease<SqlManager>),
    Unpooled(Box<SqlClient>),
}

/// Operation-scoped client guard. No asynchronous cleanup is attempted on drop.
///
/// Mutable access always dirties the guard, even after an earlier `finish`.
pub struct SqlConnection {
    inner: ConnectionInner,
    config: ConnectionConfig,
}

impl SqlConnection {
    /// Establishes a fresh unpooled client using the same TCP/login bounds.
    ///
    /// # Errors
    /// Returns invalid configuration, TCP/login failures, or connection timeout.
    pub async fn connect(config: &ConnectionConfig) -> Result<Self, SqlError> {
        config.validate()?;
        Ok(Self {
            inner: ConnectionInner::Unpooled(Box::new(connect_client(config).await?)),
            config: config.clone(),
        })
    }

    /// Verifies no active transaction and resets isolation to read committed.
    ///
    /// Every verification result is fully consumed within the command bound.
    /// Only successful completion permits pooled return. Errors and cancellation
    /// leave the guard dirty. This never rolls back or retries application work.
    /// Unpooled clients are always closed on drop, so finishing them is a no-op.
    ///
    /// # Caller contract
    /// Consume **all** operation results before calling this, and call it only
    /// after successful operation/transaction completion. Tiberius does not
    /// expose pending-result state; its next request flushes abandoned packets
    /// without interpreting their server errors. This method cannot recover an
    /// operation whose stream was abandoned or whose outcome is uncertain.
    ///
    /// # Errors
    /// Returns transaction-state, validation/reset, or total command-timeout errors.
    pub async fn finish(&mut self) -> Result<(), SqlError> {
        let client = match &mut self.inner {
            ConnectionInner::Pooled(lease) => &mut lease.object_mut().client,
            ConnectionInner::Unpooled(_) => return Ok(()),
        };
        verify_session(client, &self.config).await?;
        if let ConnectionInner::Pooled(lease) = &mut self.inner {
            lease.reusable = true;
        }
        Ok(())
    }
}

impl Deref for SqlConnection {
    type Target = SqlClient;

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            ConnectionInner::Pooled(lease) => &lease.object().client,
            ConnectionInner::Unpooled(client) => client,
        }
    }
}

impl DerefMut for SqlConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.inner {
            ConnectionInner::Pooled(lease) => &mut lease.object_mut().client,
            ConnectionInner::Unpooled(client) => client,
        }
    }
}

async fn connect_client(config: &ConnectionConfig) -> Result<SqlClient, SqlError> {
    tokio::time::timeout(config.connect_timeout, async {
        let mut driver_config = Config::new();
        driver_config.host(&config.host);
        driver_config.port(config.port);
        driver_config.database(&config.database);
        driver_config.authentication(AuthMethod::sql_server(&config.username, &config.password));
        driver_config.encryption(EncryptionLevel::Required);
        driver_config.handshake_timeout(Some(config.connect_timeout));
        driver_config.command_timeout(Some(config.command_timeout));
        if config.trust_server_certificate {
            driver_config.trust_cert();
        }
        let tcp = TcpStream::connect(driver_config.get_addr())
            .await
            .map_err(SqlError::Tcp)?;
        tcp.set_nodelay(true).map_err(SqlError::Tcp)?;
        SqlClient::connect(driver_config, tcp.compat_write())
            .await
            .map_err(|error| match error {
                tiberius::error::Error::Io {
                    kind: std::io::ErrorKind::TimedOut,
                    ..
                } => SqlError::ConnectTimeout,
                error => config.database_error("login", error),
            })
    })
    .await
    .map_err(|_| SqlError::ConnectTimeout)?
}

async fn verify_session(client: &mut SqlClient, config: &ConnectionConfig) -> Result<(), SqlError> {
    const VERIFY: &str = "IF @@TRANCOUNT = 0 AND XACT_STATE() = 0 \
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED; \
        SELECT @@TRANCOUNT AS transaction_count, XACT_STATE() AS transaction_state";
    tokio::time::timeout(config.command_timeout, async {
        let results = client
            .simple_query(VERIFY)
            .await
            .map_err(|error| config.database_error("session validation/reset", error))?
            .into_results()
            .await
            .map_err(|error| config.database_error("session validation/reset", error))?;
        if results.len() != 1 || results[0].len() != 1 {
            return Err(SqlError::DirtySession);
        }
        let row = &results[0][0];
        if row.try_get::<i32, _>(0).ok().flatten() != Some(0) || row.try_get::<i32, _>(1).ok().flatten() != Some(0) {
            return Err(SqlError::DirtySession);
        }
        Ok(())
    })
    .await
    .map_err(|_| SqlError::FinishTimeout)?
}

#[cfg(test)]
mod tests;
