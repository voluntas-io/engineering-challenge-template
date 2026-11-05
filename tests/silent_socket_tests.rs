//! A connection that stops sending frames must be replaced even while the
//! collector keeps sending its own pings. This lives in its own test binary
//! because it sets process-wide environment variables.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use engineering_challenge::collector::Collector;
use engineering_challenge::mock::{
    LoopbackMockAdapter, MockRestHook, MockStreamConnector, register_mock_connector,
    register_mock_rest_hook,
};
use futures_util::StreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_connection_is_replaced_while_pings_are_being_sent() {
    let output = tempfile::NamedTempFile::new().unwrap();
    unsafe {
        std::env::set_var("OUTPUT_PATH", output.path());
        std::env::set_var("KALSHI_REST_URL", "http://127.0.0.1:9");
        std::env::set_var("WS_PING_INTERVAL_MS", "50");
        std::env::set_var("WS_READ_TIMEOUT_MS", "300");
    }

    let hook: MockRestHook = Arc::new(|_| Some(Ok(vec!["TEST".to_owned()])));
    register_mock_rest_hook(hook);

    let connections = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&connections);
    let url = "ws://127.0.0.1:44555".to_owned();
    let connector: MockStreamConnector = Arc::new(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        let (client, server) = tokio::io::duplex(1 << 20);
        tokio::spawn(async move {
            let ws = tokio_tungstenite::accept_async(server).await.unwrap();
            let (write, mut read) = ws.split();
            // Read the subscribe, then go fully silent: no reads, no pongs, no data.
            let _subscribe = read.next().await;
            tokio::time::sleep(Duration::from_secs(60)).await;
            drop((write, read));
        });
        Some(client)
    });
    register_mock_connector(&url, connector);

    let adapter = Arc::new(LoopbackMockAdapter::new(url));
    let collector = Arc::new(Collector::new(adapter).await.unwrap());
    let runner = Arc::clone(&collector);
    let handle = tokio::spawn(async move { runner.run().await });

    tokio::time::sleep(Duration::from_millis(1_000)).await;
    collector.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let total = connections.load(Ordering::SeqCst);
    assert!(
        total >= 4,
        "both silent sockets should be replaced within 1 s, saw {total} connections"
    );
}
