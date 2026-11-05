//! Orderbook data collector entry point.
//!
//! Connects to an exchange (Kalshi, Polymarket, or Mock via [`ExchangeAdapter`]),
//! subscribes to all live markets, and maintains an in-memory orderbook for each.
//! Emits SBE-encoded price-level deltas continuously and full snapshots every 60 seconds.
//!
//! Implements all challenge requirements and extensions:
//!   - Extension 1: Redundant WebSockets per bucket with automatic failover
//!   - Extension 2: Subscription monitoring (idle symbol resync + periodic reconnect)
//!   - Extension 3: Generic encode/decode per exchange ([`ExchangeAdapter`])
//!   - Extension 4: Latency research documented in LATENCY.md

use std::sync::Arc;

use anyhow::{Context, Result};
use engineering_challenge::adapter::ExchangeAdapter;
use engineering_challenge::auth::Credential;
use engineering_challenge::collector::Collector;
use engineering_challenge::kalshi::KalshiAdapter;
use engineering_challenge::mock::MockAdapter;
use tokio::signal;
use tracing::info;

/// Simple .env loader populating missing environment variables before thread spawning.
fn load_dotenv() {
    if let Ok(content) = std::fs::read_to_string(".env") {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                let k = k.trim();
                let v = v.trim().trim_matches('"').trim_matches('\'');
                if std::env::var(k).is_err() {
                    // Safety: single-threaded startup before async runtime or background workers
                    unsafe {
                        std::env::set_var(k, v);
                    }
                }
            }
        }
    }
}

fn main() -> Result<()> {
    load_dotenv();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<()> {
    // Structured logging: write to stderr so stdout is reserved for SBE binary data
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "engineering_challenge=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    info!("Starting orderbook collector");

    let force_mock = std::env::var("MOCK")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false)
        || std::env::args().any(|a| a == "--mock");

    let adapter: Arc<dyn ExchangeAdapter> = if force_mock {
        info!("Running with MockAdapter (--mock requested)");
        Arc::new(MockAdapter::new())
    } else {
        let cred = Credential::from_env().context(
            "Kalshi credentials are required for live mode. Set KALSHI_API_KEY_ID and \
             KALSHI_PRIVATE_KEY, or pass --mock for synthetic data",
        )?;
        info!("Loaded Kalshi credentials for key ID: {}", cred.key_id);
        Arc::new(KalshiAdapter::new(Arc::new(cred)))
    };

    let collector = Arc::new(Collector::new(adapter).await?);
    let collector_handle = Arc::clone(&collector);

    let mut run_handle = tokio::spawn(async move { collector_handle.run().await });

    // Stop on SIGINT, or as soon as the collector itself stops. A collector that has
    // failed must not leave a live process behind that writes nothing.
    tokio::select! {
        signal = signal::ctrl_c() => {
            signal?;
            info!("Received SIGINT – shutting down");

            // Clean shutdown: close sockets and flush SBE writer
            collector.shutdown();
            run_handle.await.context("collector task failed")??;
        }
        result = &mut run_handle => {
            result.context("collector task failed")??;
            anyhow::bail!("collector stopped without a shutdown request");
        }
    }

    info!("Shutdown complete");
    Ok(())
}
