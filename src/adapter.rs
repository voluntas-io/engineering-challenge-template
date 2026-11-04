//! Generic Exchange Adapter trait (Extension 3).
//!
//! Abstracts the wire protocol (URL, authentication headers, subscription encoding,
//! message decoding) so the collector engine remains completely exchange-agnostic.

use anyhow::Result;
use crate::Side;

/// Canonical inbound message after exchange-specific JSON is decoded.
#[derive(Debug)]
pub enum InboundMessage {
    /// Subscription acknowledged by the server with a subscription ID.
    Subscribed { sid: u64 },
    /// Full orderbook snapshot for a symbol.
    Snapshot {
        ticker: String,
        clock: u64,
        /// Normalized bids: (price, qty) with 1_000_000 = $1.00 / 1 share.
        bids: Vec<(i64, i64)>,
        /// Normalized asks: (price, qty) with 1_000_000 = $1.00 / 1 share.
        asks: Vec<(i64, i64)>,
    },
    /// Incremental price level update.
    Delta {
        ticker: String,
        clock: u64,
        price: i64,
        qty_delta: i64,
        side: Side,
        exchange_ts_ms: Option<i64>,
    },
    /// Exchange error notification.
    Error {
        code: Option<i64>,
        message: String,
    },
}

/// Generic interface implemented by each exchange (Kalshi, Polymarket, Mock).
pub trait ExchangeAdapter: Send + Sync {
    /// Human-readable exchange name (e.g. "kalshi").
    fn name(&self) -> &str;

    /// WebSocket endpoint URL.
    fn ws_url(&self) -> &str;

    /// Extra HTTP headers required during the WebSocket handshake (e.g. auth).
    fn auth_headers(&self) -> Vec<(&'static str, String)>;

    /// Encode command to subscribe to a list of market tickers.
    fn encode_subscribe(&self, id: u64, tickers: &[String]) -> Result<String>;

    /// Encode command to add markets to an existing subscription.
    fn encode_add_markets(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String>;

    /// Encode command to remove markets from an existing subscription.
    fn encode_remove_markets(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String>;

    /// Encode command to request a fresh snapshot for markets on an existing subscription.
    fn encode_get_snapshot(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String>;

    /// Decode an incoming raw text message from the WebSocket into canonical format.
    fn decode_message(&self, text: &str) -> Result<Option<InboundMessage>>;
}
