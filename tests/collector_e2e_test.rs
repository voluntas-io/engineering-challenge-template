//! End-to-end integration test for the full Collector pipeline.
//!
//! Spawns the collector with the MockAdapter, lets it stream snapshots and
//! deltas to a temporary file via AlignedSbeWriter, shuts it down gracefully,
//! and verifies that AlignedSbeReader successfully decodes all messages.

use std::sync::Arc;
use std::time::Duration;

use engineering_challenge::collector::Collector;
use engineering_challenge::mock::MockAdapter;
use engineering_challenge::util::dtos::challenge_orderbook::AnyMessage;
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

#[tokio::test]
async fn test_collector_pipeline_e2e() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let tmp_path = tmp.path().to_str().unwrap().to_owned();

    unsafe {
        std::env::set_var("OUTPUT_PATH", &tmp_path);
    }

    let adapter = Arc::new(MockAdapter::new());
    let collector = Arc::new(Collector::new(adapter).await.unwrap());
    let collector_clone = Arc::clone(&collector);

    let run_handle = tokio::spawn(async move {
        collector_clone.run().await
    });

    // Let it run for 1.5 seconds to receive synthetic snapshots & deltas
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Trigger graceful shutdown
    collector.shutdown();
    let _ = run_handle.await.unwrap();

    // Verify written SBE file using AlignedSbeReader
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

    println!("Decoded {snapshot_count} snapshots and {delta_count} deltas");
    assert!(snapshot_count > 0, "should have written at least one snapshot");
    assert!(delta_count > 0, "should have written at least one delta");
}
