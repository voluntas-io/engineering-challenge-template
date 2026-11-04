//! Orderbook data collector entry point.
//!
//! Connects to an exchange (Kalshi, Polymarket, or Mock via [`ExchangeAdapter`]),
//! subscribes to all live markets, and maintains an in-memory orderbook for each.
//! Emits SBE-encoded price-level deltas continuously and full snapshots every 60 seconds.
//!
//! Implements all challenge requirements and extensions:
//!   - Extension 1: Redundant WebSockets per bucket with automatic failover
//!   - Extension 2: Subscription monitoring (idle symbol resubscribe + periodic reconnect)
//!   - Extension 3: Generic encode/decode per exchange ([`ExchangeAdapter`])
//!   - Extension 4: Latency research documented in LATENCY.md

use std::sync::Arc;

use anyhow::Result;
use engineering_challenge::adapter::ExchangeAdapter;
use engineering_challenge::auth::Credential;
use engineering_challenge::collector::Collector;
use engineering_challenge::kalshi::KalshiAdapter;
use engineering_challenge::mock::MockAdapter;
use tokio::signal;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
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
        match Credential::from_env() {
            Ok(cred) => {
                info!("Loaded Kalshi credentials for key ID: {}", cred.key_id);
                Arc::new(KalshiAdapter::new(Arc::new(cred)))
            }
            Err(e) => {
                warn!("Kalshi credentials not found: {e:#}");
                warn!("Falling back to MockAdapter. Set KALSHI_API_KEY_ID & KALSHI_PRIVATE_KEY for live exchange connection.");
                Arc::new(MockAdapter::new())
            }
        }
    };

    let collector = Arc::new(Collector::new(adapter).await?);
    let collector_handle = Arc::clone(&collector);

    let run_handle = tokio::spawn(async move { collector_handle.run().await });

    // Block until SIGINT (Ctrl-C)
    signal::ctrl_c().await?;
    info!("Received SIGINT – shutting down");

    // Clean shutdown: close sockets and flush SBE writer
    collector.shutdown();

    if let Err(e) = run_handle.await? {
        tracing::error!("Collector exited with error: {e:#}");
    }

    info!("Shutdown complete");
    Ok(())
}
