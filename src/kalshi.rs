//! Kalshi-specific types: REST market listing and WebSocket message serde.
//!
//! This module is the **exchange adapter** for Kalshi. It is the only place
//! in the codebase that knows about Kalshi's JSON wire format. The rest of the
//! collector works through the [`ExchangeAdapter`] trait (Extension 3).
//!
//! ## Kalshi orderbook model
//!
//! Kalshi is a binary prediction market. Every contract has a YES and NO side.
//! The orderbook only carries **bids** (the buy side):
//!   - `yes_dollars_fp` — YES bids (each at price P means buyer will pay P per contract)
//!   - `no_dollars_fp`  — NO bids  (each at price Q means buyer will pay Q per contract)
//!
//! To reconstruct a conventional two-sided book we treat:
//!   - YES bids  → **Buy (bid)** side of the YES/USDC book
//!   - NO bids   → **Sell (ask)** side, because a NO bid at Q implies a YES ask at (1−Q)
//!
//! Prices arrive as fixed-point dollar strings (up to 4 d.p.).
//! We convert to the SBE wire format where `1_000_000 = $1.00 USDC`.
//!
//! ## WebSocket protocol
//!
//! Connection: `wss://external-api-ws.kalshi.com/trade-api/ws/v2`
//! Authentication via signed headers at upgrade time.
//!
//! Subscribe to `orderbook_delta` channel; server sends:
//!   1. `orderbook_snapshot` — full book state for each subscribed market
//!   2. `orderbook_delta`    — incremental price-level changes

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::Side;

use crate::adapter::{DecodedFrame, ExchangeAdapter, InboundMessage, SequencePoint};
use crate::auth::Credential;

// ── REST base URL ─────────────────────────────────────────────────────────────

pub const REST_BASE: &str = "https://external-api.kalshi.com/trade-api/v2";
pub const WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
pub const WS_PATH: &str = "/trade-api/ws/v2";

/// Get the active Kalshi REST base URL, respecting the `KALSHI_REST_URL` override.
pub fn rest_base_url() -> String {
    std::env::var("KALSHI_REST_URL").unwrap_or_else(|_| REST_BASE.to_string())
}

/// Kalshi Exchange Adapter implementing [`ExchangeAdapter`].
pub struct KalshiAdapter {
    cred: Arc<Credential>,
}

impl KalshiAdapter {
    pub fn new(cred: Arc<Credential>) -> Self {
        Self { cred }
    }

    pub fn parse_dollars_fp(s: &str) -> Option<i64> {
        parse_dollars_fp(s)
    }

    pub fn parse_qty_fp(s: &str) -> Option<i64> {
        parse_qty_fp(s)
    }
}

impl ExchangeAdapter for KalshiAdapter {
    fn name(&self) -> &str {
        "kalshi"
    }

    fn ws_url(&self) -> &str {
        WS_URL
    }

    fn auth_headers(&self) -> Vec<(&'static str, String)> {
        let headers = self.cred.sign_headers("GET", WS_PATH);
        let pairs = headers.into_header_pairs();
        pairs.to_vec()
    }

    fn encode_subscribe(&self, id: u64, tickers: &[String]) -> Result<String> {
        WsCommand::subscribe_orderbook(id, tickers.to_vec()).to_json()
    }

    fn encode_add_markets(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String> {
        WsCommand::add_markets(id, sid, tickers.to_vec()).to_json()
    }

    fn encode_remove_markets(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String> {
        WsCommand::delete_markets(id, sid, tickers.to_vec()).to_json()
    }

    fn encode_get_snapshot(&self, id: u64, sid: u64, tickers: &[String]) -> Result<String> {
        WsCommand::get_snapshot(id, sid, tickers.to_vec()).to_json()
    }

    fn decode_message(&self, text: &str) -> Result<Option<InboundMessage>> {
        Ok(self.decode_frame(text)?.message)
    }

    fn decode_frame(&self, text: &str) -> Result<DecodedFrame> {
        let env: WsEnvelope = match serde_json::from_str(text) {
            Ok(env) => env,
            Err(e) => {
                debug!("Kalshi WS parse error: {e} in {text:.200}");
                return Ok(DecodedFrame {
                    message: None,
                    sequence: None,
                });
            }
        };

        let is_book_message = matches!(
            env.msg_type.as_str(),
            "orderbook_snapshot" | "orderbook_delta"
        );
        let sequence = match (is_book_message, env.sid, env.seq) {
            (true, Some(sid), Some(seq)) => Some(SequencePoint { sid, seq }),
            _ => None,
        };

        Ok(DecodedFrame {
            message: decode_envelope(env),
            sequence,
        })
    }
}

/// Convert a parsed Kalshi envelope into the canonical inbound message.
fn decode_envelope(env: WsEnvelope) -> Option<InboundMessage> {
    if env.msg_type == "subscribed" {
        let sid = env.sid.or_else(|| {
            env.msg
                .as_ref()
                .and_then(|m| m.get("sid"))
                .and_then(|s| s.as_u64())
        });
        return sid.map(|sid| InboundMessage::Subscribed { sid });
    }

    if env.msg_type == "error" {
        return Some(InboundMessage::Error {
            code: env.code,
            message: env.error.unwrap_or_else(|| "unknown error".into()),
        });
    }

    let msg_val = env.msg.as_ref()?;

    match env.msg_type.as_str() {
        "orderbook_snapshot" => {
            let Some(snap) = decode_snapshot(msg_val) else {
                warn_malformed("orderbook_snapshot", msg_val);
                return None;
            };
            let asks = snap
                .no_bids
                .iter()
                .map(|(p, q)| (1_000_000 - p, *q))
                .collect();
            Some(InboundMessage::Snapshot {
                ticker: snap.market_ticker,
                clock: snap.clock,
                bids: snap.yes_bids,
                asks,
            })
        }
        "orderbook_delta" => {
            let Some(delta) = decode_delta(msg_val) else {
                warn_malformed("orderbook_delta", msg_val);
                return None;
            };
            let (side, price) = match delta.side {
                DeltaSide::Yes => (Side::Buy, delta.price),
                DeltaSide::No => (Side::Sell, 1_000_000 - delta.price),
            };
            Some(InboundMessage::Delta {
                ticker: delta.market_ticker,
                clock: delta.clock,
                price,
                qty_delta: delta.delta,
                side,
                exchange_ts_ms: delta.ts_ms,
            })
        }
        _ => None,
    }
}

/// Make dropped book messages visible without flooding the log.
fn warn_malformed(kind: &str, msg: &serde_json::Value) {
    static DROPPED: AtomicU64 = AtomicU64::new(0);
    let dropped = DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
    if dropped <= 5 || dropped.is_multiple_of(10_000) {
        let sample: String = msg.to_string().chars().take(200).collect();
        warn!("Dropped undecodable {kind} message (total {dropped}): {sample}");
    }
}

// ── REST: list open markets ───────────────────────────────────────────────────

/// A minimal market record from `GET /markets`.
#[derive(Debug, Deserialize)]
pub struct Market {
    pub ticker: String,
    pub status: String,
}

#[derive(Debug, Deserialize)]
struct GetMarketsResponse {
    markets: Vec<Market>,
    #[serde(default)]
    cursor: Option<String>,
}

/// Fetch open, non-MVE markets from the Kalshi REST API.
///
/// Paginates using the cursor. If `MAX_MARKETS` env var is set, stops after
/// collecting that many markets. No authentication is required for this endpoint.
pub async fn fetch_open_markets(client: &reqwest::Client) -> Result<Vec<String>> {
    let max_markets = std::env::var("MAX_MARKETS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());

    let base = rest_base_url();
    let rest_base = base.trim_end_matches('/');

    if let Some(res) = crate::mock::take_mock_rest_response(&format!("{rest_base}/markets")) {
        return res;
    }

    let mut tickers = Vec::new();
    let mut cursor: Option<String> = None;
    let limit_str = max_markets
        .map(|m| m.clamp(1, 1000).to_string())
        .unwrap_or_else(|| "1000".to_string());

    loop {
        let mut req = client.get(format!("{rest_base}/markets")).query(&[
            ("status", "open"),
            ("limit", limit_str.as_str()),
            ("mve_filter", "exclude"),
        ]);

        if let Some(ref c) = cursor {
            req = req.query(&[("cursor", c.as_str())]);
        }

        let resp: GetMarketsResponse = req
            .send()
            .await
            .context("GET /markets request failed")?
            .error_for_status()
            .context("GET /markets returned error status")?
            .json()
            .await
            .context("parsing GET /markets response")?;

        for m in resp.markets {
            if m.status == "open" || m.status == "active" {
                tickers.push(m.ticker);
                if let Some(max) = max_markets
                    && tickers.len() >= max
                {
                    return Ok(tickers);
                }
            }
        }

        match resp.cursor {
            Some(c) if !c.is_empty() => cursor = Some(c),
            _ => break,
        }
    }

    Ok(tickers)
}

// ── WebSocket: outbound messages ──────────────────────────────────────────────

/// Commands we send to the Kalshi WebSocket.
/// Wire format: `{"cmd": "subscribe", "id": N, "params": {...}}`
#[derive(Debug, Serialize)]
pub struct WsCommand {
    pub cmd: &'static str,
    pub id: u64,
    pub params: WsParams,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum WsParams {
    Subscribe(SubscribeParams),
    UpdateSubscription(UpdateSubscriptionParams),
}

#[derive(Debug, Serialize)]
pub struct SubscribeParams {
    pub channels: Vec<String>,
    pub market_tickers: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct UpdateSubscriptionParams {
    pub sids: Vec<u64>,
    pub action: String, // "add_markets" | "delete_markets" | "get_snapshot"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_tickers: Option<Vec<String>>,
}

impl WsCommand {
    /// Subscribe to the `orderbook_delta` channel for the given tickers.
    pub fn subscribe_orderbook(id: u64, tickers: Vec<String>) -> Self {
        Self {
            cmd: "subscribe",
            id,
            params: WsParams::Subscribe(SubscribeParams {
                channels: vec!["orderbook_delta".into()],
                market_tickers: tickers,
            }),
        }
    }

    /// Add markets to an existing subscription.
    pub fn add_markets(id: u64, sid: u64, tickers: Vec<String>) -> Self {
        Self {
            cmd: "update_subscription",
            id,
            params: WsParams::UpdateSubscription(UpdateSubscriptionParams {
                sids: vec![sid],
                action: "add_markets".into(),
                market_tickers: Some(tickers),
            }),
        }
    }

    /// Remove markets from an existing subscription.
    pub fn delete_markets(id: u64, sid: u64, tickers: Vec<String>) -> Self {
        Self {
            cmd: "update_subscription",
            id,
            params: WsParams::UpdateSubscription(UpdateSubscriptionParams {
                sids: vec![sid],
                action: "delete_markets".into(),
                market_tickers: Some(tickers),
            }),
        }
    }

    /// Request a fresh snapshot for markets on an existing subscription.
    pub fn get_snapshot(id: u64, sid: u64, tickers: Vec<String>) -> Self {
        Self {
            cmd: "update_subscription",
            id,
            params: WsParams::UpdateSubscription(UpdateSubscriptionParams {
                sids: vec![sid],
                action: "get_snapshot".into(),
                market_tickers: Some(tickers),
            }),
        }
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).context("serializing WS command")
    }
}

// ── WebSocket: inbound messages ───────────────────────────────────────────────

/// Top-level envelope for all Kalshi WebSocket messages.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct WsEnvelope {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub id: Option<u64>,
    pub sid: Option<u64>,
    pub seq: Option<u64>,
    pub msg: Option<serde_json::Value>,
    // Error fields
    pub code: Option<i64>,
    pub error: Option<String>,
}

/// Decoded snapshot from the exchange.
#[derive(Debug)]
pub struct RawSnapshot {
    pub market_ticker: String,
    pub clock: u64,
    /// (price_fp, qty_fp) with price in `$0.0001` units stored as i64 * 1_000_000
    pub yes_bids: Vec<(i64, i64)>,
    pub no_bids: Vec<(i64, i64)>,
}

/// Decoded delta from the exchange.
#[derive(Debug)]
pub struct RawDelta {
    pub market_ticker: String,
    pub clock: u64,
    pub ts_ms: Option<i64>,
    pub price: i64,
    pub delta: i64,
    pub side: DeltaSide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaSide {
    Yes,
    No,
}

// ── Parsing helpers ───────────────────────────────────────────────────────────

/// Parse a Kalshi fixed-point dollar string like `"0.4200"` → `420_000` (i64 * 1_000_000).
/// Correctly parses negative values like `"-0.50"` → `-500_000` and `"-1.50"` → `-1_500_000`.
/// Returns `None` on parse failure.
pub fn parse_dollars_fp(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (negative, s_val) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };

    if s_val.is_empty() {
        return None;
    }

    let (int_part, frac_part) = match s_val.find('.') {
        Some(dot) => (&s_val[..dot], &s_val[dot + 1..]),
        None => (s_val, ""),
    };

    let int_val: i64 = if int_part.is_empty() {
        if frac_part.is_empty() {
            return None;
        }
        0
    } else {
        int_part.parse().ok()?
    };

    let mut frac_val: i64 = 0;
    let mut multiplier: i64 = 100_000;
    let mut count = 0;
    for b in frac_part.bytes() {
        if !b.is_ascii_digit() {
            return None;
        }
        if count < 6 {
            frac_val += (b - b'0') as i64 * multiplier;
            multiplier /= 10;
            count += 1;
        }
    }

    let abs_val = int_val.checked_mul(1_000_000)?.checked_add(frac_val)?;
    Some(if negative { -abs_val } else { abs_val })
}

/// Parse a contract count fixed-point string like `"13.00"` → `13_000_000`.
pub fn parse_qty_fp(s: &str) -> Option<i64> {
    parse_dollars_fp(s) // same scale: 1_000_000 = 1 share/contract
}

/// Parse a price-level array `[[price_str, qty_str], ...]`.
fn parse_level_array(value: &serde_json::Value) -> Vec<(i64, i64)> {
    let arr = match value.as_array() {
        Some(a) => a,
        None => return vec![],
    };
    arr.iter()
        .filter_map(|entry| {
            let pair = entry.as_array()?;
            if pair.len() < 2 {
                return None;
            }
            let price = parse_dollars_fp(pair[0].as_str()?)?;
            let qty = parse_qty_fp(pair[1].as_str()?)?;
            Some((price, qty))
        })
        .collect()
}

/// Attempt to decode a snapshot from a raw envelope `msg` JSON object.
pub fn decode_snapshot(msg: &serde_json::Value) -> Option<RawSnapshot> {
    let ticker = msg.get("market_ticker")?.as_str()?.to_owned();
    let clock = msg.get("clock")?.as_u64().unwrap_or(0);
    let yes_bids = msg
        .get("yes_dollars_fp")
        .map(parse_level_array)
        .unwrap_or_default();
    let no_bids = msg
        .get("no_dollars_fp")
        .map(parse_level_array)
        .unwrap_or_default();
    Some(RawSnapshot {
        market_ticker: ticker,
        clock,
        yes_bids,
        no_bids,
    })
}

/// Attempt to decode a delta from a raw envelope `msg` JSON object.
pub fn decode_delta(msg: &serde_json::Value) -> Option<RawDelta> {
    let ticker = msg.get("market_ticker")?.as_str()?.to_owned();
    let clock = msg.get("clock")?.as_u64().unwrap_or(0);
    let ts_ms = msg.get("ts_ms").and_then(|v| v.as_i64());
    // Kalshi documents `price_dollars` and `delta_fp`. The shorter names stay as
    // a fallback so a feed that differs from the documentation is still decoded.
    let price = msg.get("price_dollars").or_else(|| msg.get("price"))?;
    let price = parse_dollars_fp(price.as_str()?)?;
    let delta = msg.get("delta_fp").or_else(|| msg.get("delta"))?;
    let delta = parse_qty_fp(delta.as_str()?)?;
    let side_str = msg.get("side")?.as_str()?;
    let side = match side_str {
        "yes" => DeltaSide::Yes,
        "no" => DeltaSide::No,
        _ => {
            warn!("unknown delta side `{side_str}`");
            return None;
        }
    };
    Some(RawDelta {
        market_ticker: ticker,
        clock,
        ts_ms,
        price,
        delta,
        side,
    })
}

// ── Conversion from raw Kalshi types → canonical orderbook updates ─────────────

/// Convert a [`RawDelta`] into a canonical price/side pair the caller can use.
#[allow(dead_code)]
pub fn delta_to_update(delta: &RawDelta) -> (Side, i64, i64) {
    let (side, price) = match delta.side {
        DeltaSide::Yes => (Side::Buy, delta.price),
        DeltaSide::No => (Side::Sell, 1_000_000 - delta.price),
    };
    (side, price, delta.delta)
}
