//! Web UI and query proxy for Kusto clusters.
//!
//! The server hosts the browser application and is the only component that reaches a cluster: the
//! browser calls same-origin `/api/*` routes and the server forwards them to configured clusters,
//! addressed by connection id so the browser can never choose an arbitrary upstream.

pub mod config;
pub mod kusto;
pub mod server;

use std::sync::Arc;

use anyhow::{Context, Result};
pub use config::{Cli, Settings};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Initializes logging; `RUST_LOG` overrides the default `info` filter.
///
/// # Errors
///
/// Returns an error when `RUST_LOG` is invalid or logging was already initialized.
pub fn initialize_tracing(json: bool) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .context("parse RUST_LOG")?;
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    let initialized = if json {
        builder.json().try_init()
    } else {
        builder.try_init()
    };
    initialized.map_err(|error| anyhow::anyhow!("initialize tracing: {error}"))
}

/// Validates the configuration and serves until SIGINT or SIGTERM.
///
/// # Errors
///
/// Returns an error for invalid configuration or when the listener cannot be bound.
pub async fn run(cli: Cli) -> Result<()> {
    let settings = cli.settings()?;
    let connections = settings
        .connections
        .iter()
        .map(|connection| format!("{}={}", connection.name, config::display_uri(&connection.uri)))
        .collect::<Vec<_>>()
        .join("; ");
    if cli.validate_and_exit {
        info!(bind = %settings.bind, %connections, static_dir = %settings.static_dir.display(), "configuration is valid");
        return Ok(());
    }
    let client = kusto::KustoClient::new(settings.max_response_bytes).context("create HTTP client")?;
    let listener = TcpListener::bind(settings.bind)
        .await
        .with_context(|| format!("bind {}", settings.bind))?;
    info!(bind = %settings.bind, %connections, version = env!("CARGO_PKG_VERSION"), "kusto-explorer listening");
    let app = server::router(Arc::new(server::AppState::new(settings, client)));
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve HTTP")?;
    info!("kusto-explorer stopped");
    Ok(())
}

async fn shutdown_signal() {
    let interrupt = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "failed to listen for SIGINT");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {},
        () = terminate => {},
    }
    info!("shutdown signal received");
}
