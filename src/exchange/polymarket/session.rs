//! Polymarket CLOB websocket session.
//!
//! The public market channel uses application-level `PING`/`PONG` frames and
//! absolute level updates. There is no sequence number, so protocol faults are
//! handled by dropping the socket and resubscribing, which yields fresh books.

use std::collections::BTreeSet;

use anyhow::Context;
use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;

use crate::{
    config::{NetworkSettings, PolymarketSettings},
    exchange::polymarket::codec::decode_polymarket_message,
    journal::{DecodeOutcome, FrameKind, RawFrame},
    model::SourceId,
    session::{MarketDataSession, SessionFactory, SessionFailure, SessionFrame},
    time::now_us,
    ws::{self, ConnectOptions, WsPayload, WsStream},
};

pub struct PolymarketExchange {
    pub(crate) settings: PolymarketSettings,
    pub(crate) client: Client,
    pub(crate) network: NetworkSettings,
}

pub type PolymarketSessionFactory = PolymarketExchange;

#[async_trait]
impl SessionFactory for PolymarketExchange {
    async fn create_session(&self, source: SourceId) -> anyhow::Result<Box<dyn MarketDataSession>> {
        let connect_options = self
            .network
            .connect_options()
            .for_ordinal(source_ordinal(source))
            .with_context(|| format!("deriving connect options for source {source}"))?;
        Ok(Box::new(PolymarketSession::new(
            self.settings.clone(),
            connect_options,
        )))
    }
}

/// Unique per-connection ordinal used to derive distinct pinned source ports.
/// Mirrors the Kalshi adapter so pinned ports behave identically per venue.
fn source_ordinal(source: SourceId) -> u32 {
    u32::from(source.shard) * 256 + u32::from(source.replica)
}

pub struct PolymarketSession {
    settings: PolymarketSettings,
    connect_options: ConnectOptions,
    stream: Option<WsStream>,
    desired: BTreeSet<String>,
    subscribed_assets: BTreeSet<String>,
    initial_sent: bool,
    /// Set before writing PONG so a cancelled write is retried instead of
    /// silently dropping the server's keepalive response.
    pong_pending: bool,
}

impl PolymarketSession {
    fn new(settings: PolymarketSettings, connect_options: ConnectOptions) -> Self {
        Self {
            settings,
            connect_options,
            stream: None,
            desired: BTreeSet::new(),
            subscribed_assets: BTreeSet::new(),
            initial_sent: false,
            pong_pending: false,
        }
    }

    fn stream_mut(&mut self) -> Result<&mut WsStream, SessionFailure> {
        self.stream.as_mut().ok_or_else(|| {
            SessionFailure::Reconnect(anyhow::anyhow!("Polymarket session is not connected"))
        })
    }

    async fn send_json(&mut self, value: serde_json::Value) -> Result<(), SessionFailure> {
        let text = serde_json::to_string(&value)
            .context("serializing Polymarket command")
            .map_err(SessionFailure::Reconnect)?;
        ws::send_text(self.stream_mut()?, text)
            .await
            .map_err(SessionFailure::Reconnect)
    }

    async fn send_initial(&mut self) -> Result<(), SessionFailure> {
        let assets: Vec<String> = self.desired.iter().cloned().collect();
        if assets.is_empty() {
            return Ok(());
        }
        // Polymarket expects one market subscription frame for the initial
        // dump. Sending several type=market frames on one socket makes the
        // server emit a non-JSON control response, so keep this frame whole.
        self.send_json(json!({
            "assets_ids": assets,
            "type": "market",
            "initial_dump": true,
            "level": 2,
            "custom_feature_enabled": true
        }))
        .await?;
        self.subscribed_assets = self.desired.clone();
        self.initial_sent = true;
        Ok(())
    }

    async fn flush_pending_pong(&mut self) -> Result<(), SessionFailure> {
        if !self.pong_pending {
            return Ok(());
        }
        ws::send_text(self.stream_mut()?, "PONG".to_owned())
            .await
            .map_err(SessionFailure::Reconnect)?;
        self.pong_pending = false;
        Ok(())
    }
}

#[async_trait]
impl MarketDataSession for PolymarketSession {
    async fn connect(&mut self) -> Result<(), SessionFailure> {
        self.close().await;
        let stream = ws::connect(&self.settings.ws_url, &[], &self.connect_options)
            .await
            .map_err(|error| {
                SessionFailure::Reconnect(error.context("connecting to Polymarket websocket"))
            })?;
        self.stream = Some(stream);
        self.subscribed_assets.clear();
        self.initial_sent = false;
        self.pong_pending = false;
        Ok(())
    }

    async fn apply_subscriptions(
        &mut self,
        desired: &BTreeSet<String>,
    ) -> Result<(), SessionFailure> {
        self.desired = desired.clone();
        if !self.initial_sent {
            return self.send_initial().await;
        }
        let additions: Vec<String> = desired
            .difference(&self.subscribed_assets)
            .cloned()
            .collect();
        let deletions: Vec<String> = self
            .subscribed_assets
            .difference(desired)
            .cloned()
            .collect();
        for chunk in additions.chunks(self.settings.batch_size.max(1)) {
            self.send_json(json!({
                "operation": "subscribe",
                "assets_ids": chunk
            }))
            .await?;
            self.subscribed_assets.extend(chunk.iter().cloned());
        }
        for chunk in deletions.chunks(self.settings.batch_size.max(1)) {
            self.send_json(json!({
                "operation": "unsubscribe",
                "assets_ids": chunk
            }))
            .await?;
            for asset in chunk {
                self.subscribed_assets.remove(asset);
            }
        }
        Ok(())
    }

    async fn next_frame(&mut self) -> Result<SessionFrame, SessionFailure> {
        self.flush_pending_pong().await?;
        let payload = ws::next_payload(self.stream_mut()?)
            .await
            .map_err(SessionFailure::Reconnect)?;
        let received_at_us = now_us().unwrap_or(0);
        let (text, kind) = match payload {
            WsPayload::Idle => return Ok(SessionFrame::idle(0)),
            WsPayload::Text(text) => (text, FrameKind::Text),
            WsPayload::Binary(bytes) => match String::from_utf8(bytes.clone()) {
                Ok(text) => (text, FrameKind::Binary),
                Err(error) => {
                    return Err(SessionFailure::Decode {
                        error: anyhow::Error::new(error)
                            .context("Polymarket binary frame was not UTF-8 JSON"),
                        frame: Some(RawFrame {
                            kind: FrameKind::Binary,
                            payload: bytes,
                        }),
                        received_at_us,
                    });
                }
            },
        };
        let bytes = text.len();
        let control = text.trim();
        if control.is_empty() {
            return Ok(SessionFrame::data(
                Vec::new(),
                bytes,
                RawFrame {
                    kind,
                    payload: text.into_bytes(),
                },
                DecodeOutcome::ignored("empty_frame"),
                received_at_us,
            ));
        }
        if control.eq_ignore_ascii_case("PING") {
            self.pong_pending = true;
            self.flush_pending_pong().await?;
            return Ok(SessionFrame::data(
                Vec::new(),
                bytes,
                RawFrame {
                    kind,
                    payload: text.into_bytes(),
                },
                DecodeOutcome::ignored("control_frame"),
                received_at_us,
            ));
        }
        if control.eq_ignore_ascii_case("PONG") {
            return Ok(SessionFrame::data(
                Vec::new(),
                bytes,
                RawFrame {
                    kind,
                    payload: text.into_bytes(),
                },
                DecodeOutcome::ignored("control_frame"),
                received_at_us,
            ));
        }
        match decode_polymarket_message(&text, received_at_us) {
            Ok(events) => {
                let decode = if events.is_empty() {
                    DecodeOutcome::ignored("no_events")
                } else {
                    DecodeOutcome::decoded(events.len())
                };
                Ok(SessionFrame::data(
                    events,
                    bytes,
                    RawFrame {
                        kind,
                        payload: text.into_bytes(),
                    },
                    decode,
                    received_at_us,
                ))
            }
            Err(error) => {
                let preview: String = text.chars().take(160).collect();
                Err(SessionFailure::Decode {
                    error: error.context(format!(
                        "decoding Polymarket frame ({} bytes, preview={preview:?})",
                        text.len()
                    )),
                    frame: Some(RawFrame {
                        kind,
                        payload: text.into_bytes(),
                    }),
                    received_at_us,
                })
            }
        }
    }

    async fn send_keepalive(&mut self) -> Result<(), SessionFailure> {
        ws::send_text(self.stream_mut()?, "PING".to_owned())
            .await
            .map_err(SessionFailure::Reconnect)
    }

    async fn request_snapshot(&mut self, symbols: &[String]) -> Result<(), SessionFailure> {
        if symbols.is_empty() {
            return Ok(());
        }
        // The market channel has no snapshot command. Cycling the subscription
        // for these assets on the live connection makes the venue treat them
        // as new subscriptions and emit a fresh book for each. The core only
        // accepts the recovery once that book arrives as an exchange
        // snapshot; a venue that sends nothing leaves the probe budget to
        // escalate to a full reconnect.
        let chunk_size = self.settings.batch_size.max(1);
        for chunk in symbols.chunks(chunk_size) {
            self.send_json(json!({
                "operation": "unsubscribe",
                "assets_ids": chunk
            }))
            .await?;
        }
        for asset in symbols {
            self.subscribed_assets.remove(asset);
        }
        for chunk in symbols.chunks(chunk_size) {
            self.send_json(json!({
                "operation": "subscribe",
                "assets_ids": chunk
            }))
            .await?;
            self.subscribed_assets.extend(chunk.iter().cloned());
        }
        Ok(())
    }

    async fn close(&mut self) {
        if let Some(stream) = self.stream.take() {
            let _ = ws::close(stream).await;
        }
        self.subscribed_assets.clear();
        self.initial_sent = false;
        self.pong_pending = false;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

    use super::*;
    use crate::model::ExchangeEvent;

    type ServerWs = WebSocketStream<tokio::net::TcpStream>;

    async fn read_json(ws: &mut ServerWs) -> Value {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .expect("server read timed out")
                .expect("server stream ended")
                .expect("server websocket error");
            match message {
                Message::Text(text) => {
                    return serde_json::from_str(&text).expect("client sent non-JSON");
                }
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("unexpected client frame: {other:?}"),
            }
        }
    }

    async fn send_json(ws: &mut ServerWs, value: Value) {
        ws.send(Message::Text(value.to_string().into()))
            .await
            .unwrap();
    }

    async fn read_text(ws: &mut ServerWs) -> String {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .expect("server read timed out")
                .expect("server stream ended")
                .expect("server websocket error");
            match message {
                Message::Text(text) => return text.to_string(),
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("unexpected client frame: {other:?}"),
            }
        }
    }

    fn book_frame(asset: &str, price: &str, size: &str) -> Value {
        json!({
            "event_type": "book",
            "asset_id": asset,
            "market": "mock",
            "bids": [{"price": price, "size": size}],
            "asks": [],
            "timestamp": "1700000000000",
            "hash": "mock",
        })
    }

    async fn next_snapshot(session: &mut PolymarketSession, asset: &str) -> (i64, i64) {
        for _ in 0..40 {
            let frame = tokio::time::timeout(Duration::from_secs(5), session.next_frame())
                .await
                .expect("client frame timed out")
                .expect("client frame failed");
            for event in frame.events {
                if let ExchangeEvent::Snapshot {
                    native_symbol,
                    book,
                    ..
                } = event
                    && native_symbol == asset
                {
                    let level = book.bids.first().expect("bid level");
                    return (level.price.raw(), level.quantity.raw());
                }
            }
        }
        panic!("no snapshot for {asset}");
    }

    #[tokio::test]
    async fn request_snapshot_cycles_the_subscription_on_the_live_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

            // Initial subscription: one market frame with an initial dump.
            let initial = read_json(&mut ws).await;
            assert_eq!(initial["type"], "market");
            assert_eq!(initial["initial_dump"], true);
            assert_eq!(initial["assets_ids"], json!(["a"]));
            send_json(&mut ws, book_frame("a", "0.4", "10")).await;

            // The probe must cycle the subscription on this same connection
            // instead of dropping it.
            let unsubscribe = read_json(&mut ws).await;
            assert_eq!(unsubscribe["operation"], "unsubscribe");
            assert_eq!(unsubscribe["assets_ids"], json!(["a"]));
            let subscribe = read_json(&mut ws).await;
            assert_eq!(subscribe["operation"], "subscribe");
            assert_eq!(subscribe["assets_ids"], json!(["a"]));
            send_json(&mut ws, book_frame("a", "0.5", "12")).await;

            // Hold the connection open until the client closes it.
            while let Some(message) = ws.next().await {
                if message.is_err() {
                    break;
                }
            }
        });

        let settings = PolymarketSettings {
            ws_url: format!("ws://{address}"),
            ..PolymarketSettings::default()
        };
        let mut session = PolymarketSession::new(settings, ConnectOptions::default());
        session.connect().await.unwrap();
        session
            .apply_subscriptions(&BTreeSet::from(["a".to_owned()]))
            .await
            .unwrap();
        assert_eq!(
            next_snapshot(&mut session, "a").await,
            (400_000, 10_000_000)
        );

        session.request_snapshot(&["a".to_owned()]).await.unwrap();
        assert_eq!(
            next_snapshot(&mut session, "a").await,
            (500_000, 12_000_000)
        );

        session.close().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn application_keepalive_pong_is_not_decoded_as_market_json() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let initial = read_json(&mut ws).await;
            assert_eq!(initial["type"], "market");
            assert_eq!(initial["assets_ids"], json!(["a"]));

            // The direct market-channel contract uses text PING/PONG frames,
            // not JSON event envelopes. A PONG must not terminate the feed.
            assert_eq!(read_text(&mut ws).await, "PING");
            ws.send(Message::Text("PONG".to_owned().into()))
                .await
                .unwrap();
            send_json(&mut ws, book_frame("a", "0.4", "10")).await;
            while let Some(message) = ws.next().await {
                if message.is_err() {
                    break;
                }
            }
        });

        let settings = PolymarketSettings {
            ws_url: format!("ws://{address}"),
            ..PolymarketSettings::default()
        };
        let mut session = PolymarketSession::new(settings, ConnectOptions::default());
        session.connect().await.unwrap();
        session
            .apply_subscriptions(&BTreeSet::from(["a".to_owned()]))
            .await
            .unwrap();
        session.send_keepalive().await.unwrap();

        let pong = tokio::time::timeout(Duration::from_secs(5), session.next_frame())
            .await
            .expect("waiting for PONG timed out")
            .expect("PONG handling failed");
        assert!(
            pong.events.is_empty(),
            "PONG is a control frame, not market data"
        );

        assert_eq!(
            next_snapshot(&mut session, "a").await,
            (400_000, 10_000_000)
        );
        session.close().await;
        server.await.unwrap();
    }
}
