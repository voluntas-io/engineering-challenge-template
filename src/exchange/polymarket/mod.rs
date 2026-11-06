//! Polymarket public CLOB adapter.

mod codec;
mod discovery;
mod session;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use reqwest::Client;

pub use session::{PolymarketExchange, PolymarketSessionFactory};

use crate::{
    config::{ExchangeKind, NetworkSettings, PolymarketSettings},
    exchange::{DiscoveryResult, Exchange, ExchangeCapabilities, capabilities_for},
    model::MarketInfo,
    session::SessionFactory,
};

#[async_trait]
impl Exchange for PolymarketExchange {
    fn name(&self) -> &'static str {
        "polymarket"
    }

    fn environment(&self) -> &'static str {
        "public"
    }

    fn capabilities(&self) -> ExchangeCapabilities {
        capabilities_for(ExchangeKind::Polymarket)
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

impl PolymarketExchange {
    pub fn new(settings: PolymarketSettings, network: NetworkSettings) -> anyhow::Result<Self> {
        let client = Client::builder()
            .user_agent(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
                 AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 \
                 VoluntasCollector/0.3",
            )
            .timeout(Duration::from_secs(settings.request_timeout_secs))
            .build()
            .map_err(|error| anyhow::anyhow!("building Polymarket HTTP client: {error}"))?;
        Ok(Self {
            settings,
            client,
            network,
        })
    }

    #[must_use]
    pub fn settings(&self) -> &PolymarketSettings {
        &self.settings
    }

    pub(crate) fn client(&self) -> &Client {
        &self.client
    }
}
