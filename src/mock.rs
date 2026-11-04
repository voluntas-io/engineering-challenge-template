//! Mock Exchange Adapter for offline testing and demonstration without API keys.
//!
//! Generates realistic synthetic L2 snapshots and streaming deltas for the
//! provided markets. Useful for verifying the entire pipeline end-to-end.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use anyhow::Result;
use tokio::io::DuplexStream;
use crate::Side;

use crate::adapter::{ExchangeAdapter, InboundMessage};

pub type MockStreamConnector = Arc<dyn Fn(usize) -> Option<DuplexStream> + Send + Sync>;

static MOCK_CONNECTORS: OnceLock<Mutex<HashMap<String, MockStreamConnector>>> = OnceLock::new();

fn get_connectors() -> &'static Mutex<HashMap<String, MockStreamConnector>> {
    MOCK_CONNECTORS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register an in-process mock WebSocket stream connector for a given ws_url.
pub fn register_mock_connector(ws_url: &str, connector: MockStreamConnector) {
    let mut map = get_connectors().lock().unwrap();
    map.insert(ws_url.to_string(), connector);
}

/// Unregister an in-process mock connector.
pub fn unregister_mock_connector(ws_url: &str) {
    let mut map = get_connectors().lock().unwrap();
    map.remove(ws_url);
}

/// Invoked by collector to obtain an in-process mock duplex stream.
pub fn take_mock_stream(ws_url: &str, socket_id: usize) -> Option<DuplexStream> {
    let connector = {
        let map = get_connectors().lock().unwrap();
        map.get(ws_url).cloned()?
    };
    connector(socket_id)
}

pub type MockRestHook = Arc<dyn Fn(&str) -> Option<Result<Vec<String>>> + Send + Sync>;
static MOCK_REST_HOOKS: OnceLock<Mutex<Option<MockRestHook>>> = OnceLock::new();

/// Register an in-process mock REST hook for `kalshi::fetch_open_markets`.
pub fn register_mock_rest_hook(hook: MockRestHook) {
    let mut lock = MOCK_REST_HOOKS.get_or_init(|| Mutex::new(None)).lock().unwrap();
    *lock = Some(hook);
}

/// Unregister the in-process mock REST hook.
pub fn unregister_mock_rest_hook() {
    let mut lock = MOCK_REST_HOOKS.get_or_init(|| Mutex::new(None)).lock().unwrap();
    *lock = None;
}

/// Invoked by `kalshi::fetch_open_markets` to intercept REST requests in local tests.
pub fn take_mock_rest_response(url: &str) -> Option<Result<Vec<String>>> {
    let lock = MOCK_REST_HOOKS.get_or_init(|| Mutex::new(None)).lock().unwrap();
    if let Some(ref hook) = *lock {
        hook(url)
    } else {
        None
    }
}

pub struct MockAdapter {
    name: &'static str,
}

impl Default for MockAdapter {
    fn default() -> Self {
        Self { name: "mock" }
    }
}

impl MockAdapter {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ExchangeAdapter for MockAdapter {
    fn name(&self) -> &str {
        self.name
    }

    fn ws_url(&self) -> &str {
        "ws://127.0.0.1:0" // Mock doesn't connect externally
    }

    fn auth_headers(&self) -> Vec<(&'static str, String)> {
        vec![]
    }

    fn encode_subscribe(&self, id: u64, tickers: &[String]) -> Result<String> {
        Ok(serde_json::json!({
            "cmd": "subscribe",
            "id": id,
            "tickers": tickers
        }).to_string())
    }

    fn encode_add_markets(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String> {
        Ok(serde_json::json!({
            "cmd": "add_markets",
            "id": id,
            "sid": sid,
            "tickers": tickers
        }).to_string())
    }

    fn encode_remove_markets(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String> {
        Ok(serde_json::json!({
            "cmd": "remove_markets",
            "id": id,
            "sid": sid,
            "tickers": tickers
        }).to_string())
    }

    fn encode_get_snapshot(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String> {
        Ok(serde_json::json!({
            "cmd": "get_snapshot",
            "id": id,
            "sid": sid,
            "tickers": tickers
        }).to_string())
    }

    fn decode_message(&self, text: &str) -> Result<Option<InboundMessage>> {
        decode_mock_json(text)
    }
}

/// Local loopback mock adapter for in-process fault-injection stress testing.
///
/// Connects to a local in-process loopback WebSocket server on `127.0.0.1:<port>`
/// and decodes canonical mock JSON messages for snapshots, deltas, errors, and subscriptions.
pub struct LoopbackMockAdapter {
    name: String,
    ws_url: String,
}

impl LoopbackMockAdapter {
    pub fn new(ws_url: impl Into<String>) -> Self {
        Self {
            name: "loopback_mock".to_string(),
            ws_url: ws_url.into(),
        }
    }
}

impl ExchangeAdapter for LoopbackMockAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn ws_url(&self) -> &str {
        &self.ws_url
    }

    fn auth_headers(&self) -> Vec<(&'static str, String)> {
        vec![]
    }

    fn encode_subscribe(&self, id: u64, tickers: &[String]) -> Result<String> {
        Ok(serde_json::json!({
            "cmd": "subscribe",
            "id": id,
            "tickers": tickers
        }).to_string())
    }

    fn encode_add_markets(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String> {
        Ok(serde_json::json!({
            "cmd": "add_markets",
            "id": id,
            "sid": sid,
            "tickers": tickers
        }).to_string())
    }

    fn encode_remove_markets(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String> {
        Ok(serde_json::json!({
            "cmd": "remove_markets",
            "id": id,
            "sid": sid,
            "tickers": tickers
        }).to_string())
    }

    fn encode_get_snapshot(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String> {
        Ok(serde_json::json!({
            "cmd": "get_snapshot",
            "id": id,
            "sid": sid,
            "tickers": tickers
        }).to_string())
    }

    fn decode_message(&self, text: &str) -> Result<Option<InboundMessage>> {
        decode_mock_json(text)
    }
}

fn parse_levels(val: Option<&serde_json::Value>) -> Option<Vec<(i64, i64)>> {
    let arr = val?.as_array()?;
    let mut levels = Vec::with_capacity(arr.len());
    for item in arr {
        if let serde_json::Value::Array(pair) = item {
            if pair.len() == 2 {
                if let (Some(p), Some(q)) = (pair[0].as_i64(), pair[1].as_i64()) {
                    levels.push((p, q));
                }
            }
        }
    }
    Some(levels)
}

fn decode_mock_json(text: &str) -> Result<Option<InboundMessage>> {
    let val: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };

    let msg_type = val.get("type").and_then(|v| v.as_str()).unwrap_or("");
    match msg_type {
        "subscribed" => {
            let sid = val.get("sid").and_then(|v| v.as_u64()).unwrap_or(1);
            Ok(Some(InboundMessage::Subscribed { sid }))
        }
        "snapshot" => {
            let ticker = val.get("ticker").and_then(|v| v.as_str()).unwrap_or("").to_owned();
            let clock = val.get("clock").and_then(|v| v.as_u64()).unwrap_or(1);
            let bids = parse_levels(val.get("bids"))
                .unwrap_or_else(|| vec![(450_000, 10_000_000), (440_000, 5_000_000)]);
            let asks = parse_levels(val.get("asks"))
                .unwrap_or_else(|| vec![(550_000, 12_000_000), (560_000, 8_000_000)]);
            Ok(Some(InboundMessage::Snapshot {
                ticker,
                clock,
                bids,
                asks,
            }))
        }
        "delta" => {
            let ticker = val.get("ticker").and_then(|v| v.as_str()).unwrap_or("").to_owned();
            let clock = val.get("clock").and_then(|v| v.as_u64()).unwrap_or(2);
            let price = val.get("price").and_then(|v| v.as_i64()).unwrap_or(450_000);
            let qty = val.get("qty").and_then(|v| v.as_i64()).unwrap_or(1_000_000);
            let side_str = val.get("side").and_then(|v| v.as_str()).unwrap_or("buy");
            let is_buy = side_str.eq_ignore_ascii_case("buy") || side_str.eq_ignore_ascii_case("yes");
            let exchange_ts_ms = val.get("exchange_ts_ms").and_then(|v| v.as_i64())
                .or_else(|| Some(chrono::Utc::now().timestamp_millis()));
            Ok(Some(InboundMessage::Delta {
                ticker,
                clock,
                price,
                qty_delta: qty,
                side: if is_buy { Side::Buy } else { Side::Sell },
                exchange_ts_ms,
            }))
        }
        "error" => {
            let code = val.get("code").and_then(|v| v.as_i64());
            let message = val.get("message").and_then(|v| v.as_str()).unwrap_or("error").to_owned();
            Ok(Some(InboundMessage::Error { code, message }))
        }
        _ => Ok(None),
    }
}
