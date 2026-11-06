//! Kalshi exchange adapter.

mod auth;
pub(crate) mod codec;
mod discovery;
mod session;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use reqwest::Client;

pub use auth::KalshiAuth;
pub use session::{KalshiExchange, KalshiSessionFactory};

use crate::{
    config::{ExchangeKind, KalshiSettings, NetworkSettings},
    exchange::{DiscoveryResult, Exchange, ExchangeCapabilities, capabilities_for},
    model::MarketInfo,
    session::SessionFactory,
};

#[async_trait]
impl Exchange for KalshiExchange {
    fn name(&self) -> &'static str {
        "kalshi"
    }

    fn environment(&self) -> &'static str {
        match self.settings().environment {
            crate::config::KalshiEnvironment::Demo => "demo",
            crate::config::KalshiEnvironment::Production => "production",
        }
    }

    fn capabilities(&self) -> ExchangeCapabilities {
        capabilities_for(ExchangeKind::Kalshi)
    }

    async fn discover_markets(&self, max_markets: usize) -> anyhow::Result<Vec<MarketInfo>> {
        Ok(self.fetch_markets(max_markets).await?.markets)
    }

    async fn discover_markets_with_stats(
        &self,
        max_markets: usize,
    ) -> anyhow::Result<DiscoveryResult> {
        self.fetch_markets(max_markets).await
    }

    fn session_factory(self: Arc<Self>) -> Arc<dyn SessionFactory> {
        self
    }
}

impl KalshiExchange {
    /// Construct the adapter. `require_auth` is false for public discovery and
    /// true for live collection.
    pub fn new(
        settings: KalshiSettings,
        network: NetworkSettings,
        require_auth: bool,
    ) -> anyhow::Result<Self> {
        let auth = match KalshiAuth::from_settings(&settings) {
            Ok(auth) => Some(Arc::new(auth)),
            Err(error) if require_auth => return Err(error),
            Err(_) => None,
        };
        let client = Client::builder()
            .user_agent(concat!(
                "voluntas-collector/",
                env!("CARGO_PKG_VERSION"),
                " (+https://github.com/voluntas-io/engineering-challenge)"
            ))
            .timeout(Duration::from_secs(settings.request_timeout_secs))
            .build()
            .map_err(|error| anyhow::anyhow!("building Kalshi HTTP client: {error}"))?;
        Ok(Self {
            settings,
            auth,
            client,
            network,
        })
    }

    #[must_use]
    pub fn settings(&self) -> &KalshiSettings {
        &self.settings
    }

    pub(crate) fn auth(&self) -> Option<&Arc<KalshiAuth>> {
        self.auth.as_ref()
    }

    pub(crate) fn client(&self) -> &Client {
        &self.client
    }
}
