//! Milestone 4: Local Fault-Injection Stress Testing Suite
//!
//! Executes 100% locally on `127.0.0.1:0` with zero external network access.
//! Tests all 6 mandatory fault-injection scenarios:
//! 1. Silent Sockets (Socket Stalls & Failover)
//! 2. Reconnect Storms (Rapid Connection Drops & Exponential Backoff)
//! 3. HTTP 429 Rate Limits (REST Discovery Fallback & Retry)
//! 4. Mid-Stream Packet Loss & Redundancy Failover (Shadow Promotion)
//! 5. Out-of-Order / Dropped Messages (Monotonic Clock & Stale Rejection)
//! 6. Deterministic Bounded Shutdown (Under Heavy Delta Bursts)
//!
//! Every test is wrapped in a strict timeout (`tokio::time::timeout(5s)`)
//! ensuring bounded, deadlock-free execution.

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::io::DuplexStream;
use tokio::sync::{oneshot, Mutex};
use tokio_tungstenite::tungstenite::Message;

use engineering_challenge::Side;
use engineering_challenge::collector::Collector;
use engineering_challenge::kalshi;
use engineering_challenge::mock::{
    register_mock_connector, register_mock_rest_hook, unregister_mock_connector,
    unregister_mock_rest_hook, LoopbackMockAdapter, MockStreamConnector,
};
use engineering_challenge::orderbook::Orderbook;
use engineering_challenge::util::dtos::challenge_orderbook::AnyMessage;
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

/// Mutex serializing environment-variable-sensitive stress tests within this process.
static TEST_MUTEX: Mutex<()> = Mutex::const_new(());

/// RAII helper ensuring process environment variables are cleanly managed and restored.
struct EnvGuard {
    vars: Vec<&'static str>,
}

impl EnvGuard {
    fn new(vars: &[(&'static str, &str)]) -> Self {
        let mut tracked = Vec::new();
        for &(k, v) in vars {
            unsafe {
                std::env::set_var(k, v);
            }
            tracked.push(k);
        }
        Self { vars: tracked }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for k in &self.vars {
            unsafe {
                std::env::remove_var(k);
            }
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Local In-Process Mock Servers (127.0.0.1:0, Zero External Network Access)
// ══════════════════════════════════════════════════════════════════════════════

/// In-process mock HTTP REST server running on `127.0.0.1:0`.
struct MockRestServer {
    url: String,
    status_code: Arc<AtomicU16>,
}

impl MockRestServer {
    async fn start(initial_status: u16) -> Self {
        static REST_COUNTER: AtomicUsize = AtomicUsize::new(1000);
        let port = REST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let url = format!("http://127.0.0.1:{port}");
        let status_code = Arc::new(AtomicU16::new(initial_status));
        let status_clone = Arc::clone(&status_code);

        let hook: engineering_challenge::mock::MockRestHook = Arc::new(move |_url| {
            let status = status_clone.load(Ordering::SeqCst);
            if status == 429 {
                Some(Err(anyhow::anyhow!(
                    "HTTP 429 Too Many Requests: rate limited (Retry-After: 1)"
                )))
            } else {
                Some(Ok(vec!["KXBTCD-26NOV05".to_string()]))
            }
        });

        register_mock_rest_hook(hook);

        Self { url, status_code }
    }

    fn url(&self) -> String {
        self.url.clone()
    }

    fn set_status(&self, status: u16) {
        self.status_code.store(status, Ordering::SeqCst);
    }
}

impl Drop for MockRestServer {
    fn drop(&mut self) {
        unregister_mock_rest_hook();
    }
}

/// In-process mock WebSocket server running on `127.0.0.1:0`.
struct MockWsServer {
    ws_url: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MockWsServer {
    async fn start<F, Fut>(handler: F) -> Self
    where
        F: Fn(tokio_tungstenite::WebSocketStream<DuplexStream>, usize) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        static URL_COUNTER: AtomicUsize = AtomicUsize::new(2000);
        let port = URL_COUNTER.fetch_add(1, Ordering::SeqCst);
        let ws_url = format!("ws://127.0.0.1:{port}");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handler = Arc::new(handler);

        let handler_clone = Arc::clone(&handler);
        let connector: MockStreamConnector = Arc::new(move |socket_id| {
            let (client_io, server_io) = tokio::io::duplex(65536);
            let h = Arc::clone(&handler_clone);
            tokio::spawn(async move {
                match tokio_tungstenite::accept_async(server_io).await {
                    Ok(ws) => h(ws, socket_id).await,
                    Err(e) => eprintln!("WebSocket handshake error on socket {socket_id}: {e}"),
                }
            });
            Some(client_io)
        });

        register_mock_connector(&ws_url, connector);

        let ws_url_for_task = ws_url.clone();
        tokio::spawn(async move {
            let _ = shutdown_rx.await;
            unregister_mock_connector(&ws_url_for_task);
        });

        Self {
            ws_url,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    async fn start_raw<F, Fut>(handler: F) -> Self
    where
        F: Fn(DuplexStream, usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        static URL_COUNTER: AtomicUsize = AtomicUsize::new(3000);
        let port = URL_COUNTER.fetch_add(1, Ordering::SeqCst);
        let ws_url = format!("ws://127.0.0.1:{port}");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handler = Arc::new(handler);

        let handler_clone = Arc::clone(&handler);
        let connector: MockStreamConnector = Arc::new(move |socket_id| {
            let (client_io, server_io) = tokio::io::duplex(65536);
            let h = Arc::clone(&handler_clone);
            tokio::spawn(async move {
                h(server_io, socket_id).await;
            });
            Some(client_io)
        });

        register_mock_connector(&ws_url, connector);

        let ws_url_for_task = ws_url.clone();
        tokio::spawn(async move {
            let _ = shutdown_rx.await;
            unregister_mock_connector(&ws_url_for_task);
        });

        Self {
            ws_url,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    fn ws_url(&self) -> String {
        self.ws_url.clone()
    }
}

impl Drop for MockWsServer {
    fn drop(&mut self) {
        unregister_mock_connector(&self.ws_url);
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Scenario 1: Silent Sockets (Socket Stalls)
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_stress_scenario_1_silent_socket_stall_and_failover() {
    let _lock = TEST_MUTEX.lock().await;

    let res = tokio::time::timeout(Duration::from_secs(5), async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tmp_path = tmp.path().to_str().unwrap().to_owned();

        let rest_server = MockRestServer::start(200).await;
        let rest_url = rest_server.url();

        let _env = EnvGuard::new(&[
            ("OUTPUT_PATH", &tmp_path),
            ("KALSHI_REST_URL", &rest_url),
            ("WS_READ_TIMEOUT_MS", "150"), // Fast 150ms idle detection
        ]);

        let failover_happened = Arc::new(AtomicBool::new(false));
        let failover_happened_clone = Arc::clone(&failover_happened);

        let ws_server = MockWsServer::start(move |ws, idx| {
            let failover = Arc::clone(&failover_happened_clone);
            async move {
                let (mut write, mut read) = ws.split();
                let _sub = read.next().await;

                if idx == 0 {
                    // Primary socket stalls: completely silent, sends zero messages
                    // Sleeps until client times out after 150ms and closes
                    while read.next().await.is_some() {}
                } else if idx == 1 {
                    // Shadow socket: active and healthy
                    let sub_ack = serde_json::json!({
                        "type": "subscribed",
                        "sid": 20
                    })
                    .to_string();
                    let _ = write.send(Message::Text(sub_ack.into())).await;

                    // Keep shadow socket alive with periodic pings/messages every 50ms so it doesn't time out
                    let mut interval = tokio::time::interval(Duration::from_millis(50));
                    loop {
                        tokio::select! {
                            msg = read.next() => {
                                match msg {
                                    Some(Ok(Message::Text(cmd))) => {
                                        if cmd.contains("get_snapshot") {
                                            failover.store(true, Ordering::SeqCst);
                                            let snap = serde_json::json!({
                                                "type": "snapshot",
                                                "ticker": "KXBTCD-26NOV05",
                                                "clock": 100,
                                                "bids": [[450_000, 10_000_000]],
                                                "asks": [[550_000, 10_000_000]]
                                            })
                                            .to_string();
                                            let _ = write.send(Message::Text(snap.into())).await;

                                            let delta = serde_json::json!({
                                                "type": "delta",
                                                "ticker": "KXBTCD-26NOV05",
                                                "clock": 101,
                                                "price": 450_000,
                                                "qty": 1_000_000,
                                                "side": "buy"
                                            })
                                            .to_string();
                                            let _ = write.send(Message::Text(delta.into())).await;
                                        }
                                    }
                                    Some(Ok(Message::Close(_))) | None => break,
                                    _ => {}
                                }
                            }
                            _ = interval.tick() => {
                                // Keepalive ping to prevent idle timeout on the healthy shadow socket
                                if write.send(Message::Ping(vec![].into())).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    // Replacement sockets: accept, acknowledge, and stay open
                    let sub_ack = serde_json::json!({
                        "type": "subscribed",
                        "sid": 30 + idx as u64
                    })
                    .to_string();
                    let _ = write.send(Message::Text(sub_ack.into())).await;
                    while read.next().await.is_some() {}
                }
            }
        })
        .await;

        let adapter = Arc::new(LoopbackMockAdapter::new(ws_server.ws_url()));
        let collector = Arc::new(Collector::new(adapter).await.unwrap());
        let collector_clone = Arc::clone(&collector);

        let run_handle = tokio::spawn(async move { collector_clone.run().await });

        // Run until silent stall detection and failover occurs
        tokio::time::sleep(Duration::from_millis(600)).await;

        collector.shutdown();
        let _ = run_handle.await.unwrap();

        assert!(
            failover_happened.load(Ordering::SeqCst),
            "Collector must trigger failover to shadow socket upon primary silent socket stall"
        );

        // Verify SBE output decoded successfully
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&tmp_path).unwrap();
        let mut msg_count = 0;
        while let Some(_msg) = reader.read_msg().unwrap() {
            msg_count += 1;
        }
        assert!(
            msg_count > 0,
            "SBE output must contain written messages after failover"
        );
    })
    .await;

    assert!(
        res.is_ok(),
        "Test timed out: potential deadlock in silent socket stall scenario"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// Scenario 2: Reconnect Storms
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_stress_scenario_2_reconnect_storm_and_exponential_backoff() {
    let _lock = TEST_MUTEX.lock().await;

    let res = tokio::time::timeout(Duration::from_secs(5), async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tmp_path = tmp.path().to_str().unwrap().to_owned();

        let rest_server = MockRestServer::start(200).await;
        let rest_url = rest_server.url();

        let _env = EnvGuard::new(&[
            ("OUTPUT_PATH", &tmp_path),
            ("KALSHI_REST_URL", &rest_url),
            ("WS_READ_TIMEOUT_MS", "2000"),
        ]);

        let drop_count = Arc::new(AtomicUsize::new(0));
        let drop_count_clone = Arc::clone(&drop_count);
        let max_drops = 6;

        let ws_server = MockWsServer::start_raw(move |stream, idx| {
            let drops = Arc::clone(&drop_count_clone);
            async move {
                if idx < max_drops {
                    // Simulate rapid disconnect storm: immediate stream drop
                    drops.fetch_add(1, Ordering::SeqCst);
                    drop(stream);
                } else {
                    // Server stabilizes after storm
                    if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
                        let (mut write, mut read) = ws.split();
                        let _sub = read.next().await;

                        let sub_ack = serde_json::json!({
                            "type": "subscribed",
                            "sid": 500
                        })
                        .to_string();
                        let _ = write.send(Message::Text(sub_ack.into())).await;

                        let snap = serde_json::json!({
                            "type": "snapshot",
                            "ticker": "KXBTCD-26NOV05",
                            "clock": 100,
                            "bids": [[450_000, 10_000_000]],
                            "asks": [[550_000, 10_000_000]]
                        })
                        .to_string();
                        let _ = write.send(Message::Text(snap.into())).await;

                        let delta = serde_json::json!({
                            "type": "delta",
                            "ticker": "KXBTCD-26NOV05",
                            "clock": 101,
                            "price": 450_000,
                            "qty": 1_000_000,
                            "side": "buy"
                        })
                        .to_string();
                        let _ = write.send(Message::Text(delta.into())).await;

                        // Keep connection open until shutdown
                        while read.next().await.is_some() {}
                    }
                }
            }
        })
        .await;

        let adapter = Arc::new(LoopbackMockAdapter::new(ws_server.ws_url()));
        let collector = Arc::new(Collector::new(adapter).await.unwrap());
        let collector_clone = Arc::clone(&collector);

        let run_handle = tokio::spawn(async move { collector_clone.run().await });

        // Allow collector to experience reconnect storm, apply backoff, and stabilize
        tokio::time::sleep(Duration::from_millis(1500)).await;

        collector.shutdown();
        let _ = run_handle.await.unwrap();

        assert!(
            drop_count.load(Ordering::SeqCst) >= max_drops,
            "Collector must endure at least {max_drops} consecutive connection drops without crashing"
        );

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&tmp_path).unwrap();
        let mut msg_count = 0;
        while let Some(_msg) = reader.read_msg().unwrap() {
            msg_count += 1;
        }
        assert!(
            msg_count > 0,
            "Collector must successfully write messages once mock server stabilizes"
        );
    })
    .await;

    assert!(
        res.is_ok(),
        "Test timed out: reconnect storm caused deadlock or task stall"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// Scenario 3: HTTP 429 Rate Limits
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_stress_scenario_3_http_429_rate_limit_and_fallback() {
    let _lock = TEST_MUTEX.lock().await;

    let res = tokio::time::timeout(Duration::from_secs(5), async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tmp_path = tmp.path().to_str().unwrap().to_owned();

        // 1. Initial 429 rate limit on REST endpoint
        let rest_server = MockRestServer::start(429).await;
        let rest_url = rest_server.url();

        let _env = EnvGuard::new(&[
            ("OUTPUT_PATH", &tmp_path),
            ("KALSHI_REST_URL", &rest_url),
        ]);

        let client = reqwest::Client::new();

        // Protocol test: fetch_open_markets returns Err on 429 status without crashing
        let direct_res = kalshi::fetch_open_markets(&client).await;
        assert!(
            direct_res.is_err(),
            "REST client must return Err when server responds with HTTP 429"
        );
        let err_str = format!("{:#}", direct_res.err().unwrap());
        assert!(
            err_str.contains("429") || err_str.contains("rate limited"),
            "Error must reflect HTTP 429 rate limit response: {err_str}"
        );

        // 2. Mock WebSocket server accepts seed market subscription
        let seed_market_subscribed = Arc::new(AtomicBool::new(false));
        let seed_subscribed_clone = Arc::clone(&seed_market_subscribed);

        let ws_server = MockWsServer::start(move |ws, _idx| {
            let seed_flag = Arc::clone(&seed_subscribed_clone);
            async move {
                let (mut write, mut read) = ws.split();
                if matches!(read.next().await, Some(Ok(Message::Text(ref cmd))) if cmd.contains("KXBTCD-26NOV05")) {
                    seed_flag.store(true, Ordering::SeqCst);
                }

                let sub_ack = serde_json::json!({
                    "type": "subscribed",
                    "sid": 10
                })
                .to_string();
                let _ = write.send(Message::Text(sub_ack.into())).await;

                let snap = serde_json::json!({
                    "type": "snapshot",
                    "ticker": "KXBTCD-26NOV05",
                    "clock": 100,
                    "bids": [[450_000, 10_000_000]],
                    "asks": [[550_000, 10_000_000]]
                })
                .to_string();
                let _ = write.send(Message::Text(snap.into())).await;
            }
        })
        .await;

        let adapter = Arc::new(LoopbackMockAdapter::new(ws_server.ws_url()));
        let collector = Arc::new(Collector::new(adapter).await.unwrap());
        let collector_clone = Arc::clone(&collector);

        let run_handle = tokio::spawn(async move { collector_clone.run().await });

        tokio::time::sleep(Duration::from_millis(500)).await;

        assert!(
            seed_market_subscribed.load(Ordering::SeqCst),
            "Collector must fall back to seed markets when REST returns HTTP 429"
        );

        // 3. Rate limit clears: REST server returns 200 OK
        rest_server.set_status(200);
        let recovered_res = kalshi::fetch_open_markets(&client).await;
        assert!(
            recovered_res.is_ok(),
            "REST market discovery must succeed after rate limit clears"
        );
        assert_eq!(recovered_res.unwrap(), vec!["KXBTCD-26NOV05"]);

        collector.shutdown();
        let _ = run_handle.await.unwrap();

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&tmp_path).unwrap();
        let mut count = 0;
        while let Some(_msg) = reader.read_msg().unwrap() {
            count += 1;
        }
        assert!(
            count > 0,
            "Seed market data must be written to SBE output under 429 fallback"
        );
    })
    .await;

    assert!(
        res.is_ok(),
        "Test timed out: HTTP 429 rate limit handling stalled"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// Scenario 4: Mid-Stream Packet Loss & Redundancy Failover
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_stress_scenario_4_midstream_packet_loss_and_shadow_failover() {
    let _lock = TEST_MUTEX.lock().await;

    let res = tokio::time::timeout(Duration::from_secs(5), async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tmp_path = tmp.path().to_str().unwrap().to_owned();

        let rest_server = MockRestServer::start(200).await;
        let rest_url = rest_server.url();

        let _env = EnvGuard::new(&[
            ("OUTPUT_PATH", &tmp_path),
            ("KALSHI_REST_URL", &rest_url),
        ]);

        let shadow_received_get_snapshot = Arc::new(AtomicBool::new(false));
        let shadow_flag = Arc::clone(&shadow_received_get_snapshot);

        let ws_server = MockWsServer::start(move |ws, idx| {
            let shadow_get_snap = Arc::clone(&shadow_flag);
            async move {
                let (mut write, mut read) = ws.split();
                let _sub = read.next().await;

                if idx == 0 {
                    // Primary socket: acknowledge, stream 10 deltas, then abruptly disconnect
                    let sub_ack = serde_json::json!({
                        "type": "subscribed",
                        "sid": 100
                    })
                    .to_string();
                    let _ = write.send(Message::Text(sub_ack.into())).await;

                    let snap = serde_json::json!({
                        "type": "snapshot",
                        "ticker": "KXBTCD-26NOV05",
                        "clock": 100,
                        "bids": [[450_000, 10_000_000]],
                        "asks": [[550_000, 10_000_000]]
                    })
                    .to_string();
                    let _ = write.send(Message::Text(snap.into())).await;

                    for c in 101..=110 {
                        let delta = serde_json::json!({
                            "type": "delta",
                            "ticker": "KXBTCD-26NOV05",
                            "clock": c,
                            "price": 450_000,
                            "qty": 1_000_000,
                            "side": "buy"
                        })
                        .to_string();
                        let _ = write.send(Message::Text(delta.into())).await;
                    }

                    tokio::time::sleep(Duration::from_millis(50)).await;
                    // Abrupt mid-stream termination of primary socket
                    drop(write);
                    drop(read);
                } else if idx == 1 {
                    // Shadow socket: keep alive, await promotion to primary
                    let sub_ack = serde_json::json!({
                        "type": "subscribed",
                        "sid": 200
                    })
                    .to_string();
                    let _ = write.send(Message::Text(sub_ack.into())).await;

                    while let Some(Ok(Message::Text(cmd))) = read.next().await {
                        if cmd.contains("get_snapshot") {
                            shadow_get_snap.store(true, Ordering::SeqCst);

                            // Send post-failover deltas on newly promoted primary
                            for c in 111..=115 {
                                let delta = serde_json::json!({
                                    "type": "delta",
                                    "ticker": "KXBTCD-26NOV05",
                                    "clock": c,
                                    "price": 450_000,
                                    "qty": 1_000_000,
                                    "side": "buy"
                                })
                                .to_string();
                                let _ = write.send(Message::Text(delta.into())).await;
                            }
                            break;
                        }
                    }
                } else {
                    // Replacement sockets spawned by collector
                    let sub_ack = serde_json::json!({
                        "type": "subscribed",
                        "sid": 300 + idx as u64
                    })
                    .to_string();
                    let _ = write.send(Message::Text(sub_ack.into())).await;
                }
            }
        })
        .await;

        let adapter = Arc::new(LoopbackMockAdapter::new(ws_server.ws_url()));
        let collector = Arc::new(Collector::new(adapter).await.unwrap());
        let collector_clone = Arc::clone(&collector);

        let run_handle = tokio::spawn(async move { collector_clone.run().await });

        tokio::time::sleep(Duration::from_millis(600)).await;

        collector.shutdown();
        let _ = run_handle.await.unwrap();

        assert!(
            shadow_received_get_snapshot.load(Ordering::SeqCst),
            "Promoted shadow socket must receive GetSnapshot on primary mid-stream disconnect"
        );

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&tmp_path).unwrap();
        let mut snapshot_count = 0;
        let mut delta_count = 0;

        while let Some(msg) = reader.read_msg().unwrap() {
            match msg {
                AnyMessage::Snapshot(_) => snapshot_count += 1,
                AnyMessage::PriceLevelUpdate(_) => delta_count += 1,
                _ => {}
            }
        }

        assert!(
            snapshot_count >= 2,
            "Must have initial snapshot AND continuity snapshot emitted upon failover (found {snapshot_count})"
        );
        assert!(
            delta_count >= 10,
            "Must have processed deltas across pre-failover and post-failover (found {delta_count})"
        );
    })
    .await;

    assert!(res.is_ok(), "Test timed out: mid-stream failover deadlocked");
}

// ══════════════════════════════════════════════════════════════════════════════
// Scenario 5: Out-of-Order / Dropped Messages
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_stress_scenario_5_out_of_order_and_dropped_messages() {
    let _lock = TEST_MUTEX.lock().await;

    let res = tokio::time::timeout(Duration::from_secs(5), async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tmp_path = tmp.path().to_str().unwrap().to_owned();

        let rest_server = MockRestServer::start(200).await;
        let rest_url = rest_server.url();

        let _env = EnvGuard::new(&[
            ("OUTPUT_PATH", &tmp_path),
            ("KALSHI_REST_URL", &rest_url),
        ]);

        let ws_server = MockWsServer::start(move |ws, _idx| async move {
            let (mut write, mut read) = ws.split();
            let _sub = read.next().await;

            let sub_ack = serde_json::json!({
                "type": "subscribed",
                "sid": 42
            })
            .to_string();
            let _ = write.send(Message::Text(sub_ack.into())).await;

            // 1. Snapshot clock = 100
            let snap = serde_json::json!({
                "type": "snapshot",
                "ticker": "KXBTCD-26NOV05",
                "clock": 100,
                "bids": [[450_000, 10_000_000]],
                "asks": [[550_000, 10_000_000]]
            })
            .to_string();
            let _ = write.send(Message::Text(snap.into())).await;

            // 2. Delta clock = 105 (advances clock)
            let delta1 = serde_json::json!({
                "type": "delta",
                "ticker": "KXBTCD-26NOV05",
                "clock": 105,
                "price": 450_000,
                "qty": 2_000_000,
                "side": "buy"
            })
            .to_string();
            let _ = write.send(Message::Text(delta1.into())).await;

            // 3. Stale delta clock = 102 (decreasing clock: MUST BE REJECTED)
            let stale_delta = serde_json::json!({
                "type": "delta",
                "ticker": "KXBTCD-26NOV05",
                "clock": 102,
                "price": 450_000,
                "qty": -12_000_000,
                "side": "buy"
            })
            .to_string();
            let _ = write.send(Message::Text(stale_delta.into())).await;

            // 4. Gap delta clock = 120 (gap forward clock: ACCEPTED)
            let gap_delta = serde_json::json!({
                "type": "delta",
                "ticker": "KXBTCD-26NOV05",
                "clock": 120,
                "price": 460_000,
                "qty": 5_000_000,
                "side": "buy"
            })
            .to_string();
            let _ = write.send(Message::Text(gap_delta.into())).await;

            // 5. Stale delta clock = 110 (stale against clock 120: REJECTED)
            let stale_delta2 = serde_json::json!({
                "type": "delta",
                "ticker": "KXBTCD-26NOV05",
                "clock": 110,
                "price": 550_000,
                "qty": -10_000_000,
                "side": "sell"
            })
            .to_string();
            let _ = write.send(Message::Text(stale_delta2.into())).await;

            // 6. Stale snapshot clock = 90 (stale against clock 120: REJECTED)
            let stale_snap = serde_json::json!({
                "type": "snapshot",
                "ticker": "KXBTCD-26NOV05",
                "clock": 90,
                "bids": [],
                "asks": []
            })
            .to_string();
            let _ = write.send(Message::Text(stale_snap.into())).await;

            // 7. Valid delta clock = 125 (ACCEPTED)
            let delta_final = serde_json::json!({
                "type": "delta",
                "ticker": "KXBTCD-26NOV05",
                "clock": 125,
                "price": 460_000,
                "qty": 1_000_000,
                "side": "buy"
            })
            .to_string();
            let _ = write.send(Message::Text(delta_final.into())).await;
        })
        .await;

        let adapter = Arc::new(LoopbackMockAdapter::new(ws_server.ws_url()));
        let collector = Arc::new(Collector::new(adapter).await.unwrap());
        let collector_clone = Arc::clone(&collector);

        let run_handle = tokio::spawn(async move { collector_clone.run().await });

        tokio::time::sleep(Duration::from_millis(400)).await;

        collector.shutdown();
        let _ = run_handle.await.unwrap();

        // Direct mathematical orderbook verification of the exact injected sequence
        let mut book = Orderbook::default();
        book.apply_snapshot_side(Side::Buy, &[(450_000, 10_000_000)], 100);
        book.apply_snapshot_side(Side::Sell, &[(550_000, 10_000_000)], 100);
        assert_eq!(book.clock, 100);

        // 105 applied
        book.apply_delta(Side::Buy, 450_000, 2_000_000, 105);
        assert_eq!(book.clock, 105);
        assert_eq!(book.bids.get(&450_000), Some(&12_000_000));

        // 102 stale -> rejected without mutating state
        book.apply_delta(Side::Buy, 450_000, -12_000_000, 102);
        assert_eq!(
            book.clock, 105,
            "Book clock must remain 105 after stale delta"
        );
        assert_eq!(
            book.bids.get(&450_000),
            Some(&12_000_000),
            "Stale delta must not delete level"
        );

        // 120 gap -> accepted
        book.apply_delta(Side::Buy, 460_000, 5_000_000, 120);
        assert_eq!(book.clock, 120);
        assert_eq!(book.bids.get(&460_000), Some(&5_000_000));

        // 110 stale -> rejected
        book.apply_delta(Side::Sell, 550_000, -10_000_000, 110);
        assert_eq!(book.clock, 120);
        assert_eq!(
            book.asks.get(&550_000),
            Some(&10_000_000),
            "Stale ask delta must be rejected"
        );

        // 90 stale snapshot -> rejected
        book.apply_snapshot_side(Side::Buy, &[], 90);
        assert_eq!(book.clock, 120);
        assert!(
            !book.bids.is_empty(),
            "Stale snapshot must not wipe orderbook levels"
        );

        // 125 delta -> accepted
        book.apply_delta(Side::Buy, 460_000, 1_000_000, 125);
        assert_eq!(book.clock, 125);
        assert_eq!(book.bids.get(&460_000), Some(&6_000_000));

        // Verify SBE reader decodes cleanly
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&tmp_path).unwrap();
        let mut count = 0;
        while let Some(_msg) = reader.read_msg().unwrap() {
            count += 1;
        }
        assert!(count > 0, "SBE output must contain non-stale updates");
    })
    .await;

    assert!(
        res.is_ok(),
        "Test timed out: out-of-order message handling hung"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// Scenario 6: Deterministic Bounded Shutdown
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_stress_scenario_6_deterministic_bounded_shutdown() {
    let _lock = TEST_MUTEX.lock().await;

    let res = tokio::time::timeout(Duration::from_secs(5), async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tmp_path = tmp.path().to_str().unwrap().to_owned();

        let rest_server = MockRestServer::start(200).await;
        let rest_url = rest_server.url();

        let _env = EnvGuard::new(&[
            ("OUTPUT_PATH", &tmp_path),
            ("KALSHI_REST_URL", &rest_url),
        ]);

        let flood_stop = Arc::new(AtomicBool::new(false));
        let flood_stop_clone = Arc::clone(&flood_stop);

        let ws_server = MockWsServer::start(move |ws, _idx| {
            let stop = Arc::clone(&flood_stop_clone);
            async move {
                let (mut write, mut read) = ws.split();
                let _sub = read.next().await;

                let sub_ack = serde_json::json!({
                    "type": "subscribed",
                    "sid": 77
                })
                .to_string();
                let _ = write.send(Message::Text(sub_ack.into())).await;

                let snap = serde_json::json!({
                    "type": "snapshot",
                    "ticker": "KXBTCD-26NOV05",
                    "clock": 100,
                    "bids": [[450_000, 10_000_000]],
                    "asks": [[550_000, 10_000_000]]
                })
                .to_string();
                let _ = write.send(Message::Text(snap.into())).await;

                // High-frequency delta bombardment
                let mut clock = 101u64;
                while !stop.load(Ordering::Relaxed) {
                    clock += 1;
                    let delta = serde_json::json!({
                        "type": "delta",
                        "ticker": "KXBTCD-26NOV05",
                        "clock": clock,
                        "price": 450_000,
                        "qty": 1_000,
                        "side": "buy"
                    })
                    .to_string();
                    if write.send(Message::Text(delta.into())).await.is_err() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
        })
        .await;

        let adapter = Arc::new(LoopbackMockAdapter::new(ws_server.ws_url()));
        let collector = Arc::new(Collector::new(adapter).await.unwrap());
        let collector_clone = Arc::clone(&collector);

        let run_handle = tokio::spawn(async move { collector_clone.run().await });

        // Let collector run under heavy bombardment for 400ms
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Trigger shutdown and measure elapsed exit time
        let shutdown_start = Instant::now();
        collector.shutdown();
        flood_stop.store(true, Ordering::SeqCst);

        let run_res = run_handle.await.unwrap();
        assert!(
            run_res.is_ok(),
            "Collector::run must exit with Ok(()) on shutdown"
        );

        let shutdown_duration = shutdown_start.elapsed();
        assert!(
            shutdown_duration < Duration::from_secs(2),
            "Shutdown took {shutdown_duration:?}, exceeding 2s bounded exit SLA"
        );

        // Verify that AlignedSbeWriter was properly flushed and reader decodes all messages cleanly
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&tmp_path).unwrap();
        let mut decoded_messages = 0;
        while let Some(msg) = reader.read_msg().unwrap() {
            match msg {
                AnyMessage::Snapshot(_) | AnyMessage::PriceLevelUpdate(_) => {
                    decoded_messages += 1;
                }
                _ => {}
            }
        }

        assert!(
            decoded_messages > 50,
            "SBE file should contain at least 50 messages from the active burst, found {decoded_messages}"
        );
    })
    .await;

    assert!(
        res.is_ok(),
        "Test timed out: collector shutdown stalled or deadlocked"
    );
}
