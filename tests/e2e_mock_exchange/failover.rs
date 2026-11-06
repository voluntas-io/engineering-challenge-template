use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_socket_loss_fails_over_and_emits_anchoring_snapshot() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("socket-loss.sbe");
    let mock = MockExchange::start(vec![market("cond-a", "asset-a")], true).await;
    mock.set_book("asset-a", &[(400_000, 10_000_000)], &[(600_000, 5_000_000)]);
    let mut settings = test_settings(&mock, &output);
    let metrics_address = unused_local_address();
    settings.metrics.listen = Some(metrics_address);
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_connections(2).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    // Connection 0 is the active source. Connection 1 is gated so it cannot
    // win the initial snapshot race, then receives a snapshot after batch 1.
    sleep(Duration::from_millis(200)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 11_000_000));
    sleep(Duration::from_millis(50)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 12_000_000));
    sleep(Duration::from_millis(100)).await;
    mock.release_initial(1);
    sleep(Duration::from_millis(200)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 13_000_000));
    sleep(Duration::from_millis(50)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 14_000_000));
    sleep(Duration::from_millis(200)).await;

    let truth_at_loss = mock.book("asset-a");
    let previous_next_id = mock.next_id();
    mock.close_connection(0);
    // The surviving replica must reconnect with a new connection epoch,
    // resubscribe, and deliver a fresh exchange snapshot. The coordinator may
    // only promote it after acknowledging the pending recovery generation.
    mock.wait_for_new_connection(previous_next_id).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    wait_for_metric_at_least(metrics_address, "collector_failover_snapshots_total", 1.0).await;
    wait_for_no_resyncing_symbols(metrics_address).await;
    wait_for_metric_at_least(metrics_address, "collector_symbols_anchored", 1.0).await;
    sleep(Duration::from_millis(100)).await;

    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 15_000_000));
    sleep(Duration::from_millis(50)).await;
    assert!(mock.publish_delta("asset-a", Side::Sell, 600_000, 4_000_000));
    sleep(Duration::from_millis(300)).await;
    done.notify_one();
    let report = finish_collector(handle).await;
    let capture = read_capture(&output);
    let updates = mock.delivered_updates_for("asset-a");

    assert_eq!(updates.len(), 6);
    assert_deltas_match_updates(&capture, &updates, SYMBOL_A);
    assert_per_symbol_anchor(&capture, SYMBOL_A);
    assert_delta_receive_times_monotonic(&capture, SYMBOL_A);
    assert_eq!(
        recovery_snapshot(&capture, SYMBOL_A),
        &truth_at_loss,
        "failover snapshot must contain the shadow's current truth"
    );
    assert_eq!(reconstructed_book(&capture, SYMBOL_A), mock.book("asset-a"));
    let summary = verify_capture(&output);
    assert!(summary.valid, "verify rejected clean failover: {summary:?}");
    assert_eq!(report.core.failover_snapshots, 1);
    assert_eq!(report.core.source_switches, 1);
    assert_eq!(report.core.discontinuity_incidents, 1);
    assert_eq!(report.core.unquantifiable_gaps, 1);
    assert_eq!(report.core.deltas_emitted, 6);
    assert!(
        report.sources.iter().any(|source| source.reconnects >= 1),
        "the recovery path must reconnect at least one source"
    );

    let delta_positions: Vec<usize> = capture
        .iter()
        .enumerate()
        .filter_map(|(index, message)| {
            matches!(message, CapturedMessage::Delta { symbol, .. } if symbol == SYMBOL_A)
                .then_some(index)
        })
        .collect();
    assert_eq!(delta_positions.len(), 6);
    let replacement_snapshots = capture[delta_positions[3] + 1..delta_positions[4]]
        .iter()
        .filter(|message| {
            matches!(message, CapturedMessage::Snapshot { symbol, .. } if symbol == SYMBOL_A)
        })
        .count();
    assert_eq!(
        replacement_snapshots, 1,
        "exactly one fresh snapshot may replace the lost active source"
    );
    assert!(
        mock.subscription_log().iter().any(|frame| {
            frame.connection >= previous_next_id
                && frame.operation == "initial_dump"
                && frame.assets > 0
        }),
        "the reconnected source must resubscribe before recovering"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_outage_recovery_exposes_the_uncovered_interval() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("outage.sbe");
    let mock = MockExchange::start(vec![market("cond-a", "asset-a")], false).await;
    mock.set_book("asset-a", &[(400_000, 10_000_000)], &[(600_000, 5_000_000)]);
    let mut settings = test_settings(&mock, &output);
    let metrics_address = unused_local_address();
    settings.metrics.listen = Some(metrics_address);
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_connections(2).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    sleep(Duration::from_millis(200)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 11_000_000));
    sleep(Duration::from_millis(50)).await;
    assert!(mock.publish_delta("asset-a", Side::Sell, 600_000, 4_000_000));
    sleep(Duration::from_millis(200)).await;
    let pre_outage = mock.book("asset-a");

    let previous_next_id = mock.next_id();
    mock.set_suppress_deltas(true);
    mock.set_reject_connections(true);
    mock.close_all();
    sleep(Duration::from_millis(200)).await;

    // These updates occur during the uncovered interval. They change the
    // authoritative book but are not delivered.
    assert!(!mock.publish_delta("asset-a", Side::Buy, 400_000, 20_000_000));
    assert!(!mock.publish_delta("asset-a", Side::Sell, 600_000, 1_000_000));
    let truth_after_outage = mock.book("asset-a");
    assert_ne!(pre_outage, truth_after_outage);

    sleep(Duration::from_millis(700)).await;
    let recovery_started_us = now_us().expect("wall clock");
    mock.set_reject_connections(false);
    mock.set_suppress_deltas(false);
    mock.wait_for_new_connections(previous_next_id, 2).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    // Recovery is only complete after a fresh exchange snapshot has been
    // accepted against the acknowledged recovery fence and continuity is Live.
    wait_for_metric_at_least(metrics_address, "collector_failover_snapshots_total", 1.0).await;
    wait_for_no_resyncing_symbols(metrics_address).await;
    wait_for_metric_at_least(metrics_address, "collector_symbols_anchored", 1.0).await;
    sleep(Duration::from_millis(100)).await;

    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 21_000_000));
    sleep(Duration::from_millis(300)).await;
    done.notify_one();
    let report = finish_collector(handle).await;
    let capture = read_capture(&output);
    let delivered = mock.delivered_updates_for("asset-a");
    let all = mock.all_updates_for("asset-a");

    assert_eq!(delivered.len(), 3);
    assert_eq!(all.len(), 5);
    assert_deltas_match_updates(&capture, &delivered, SYMBOL_A);
    assert_per_symbol_anchor(&capture, SYMBOL_A);
    snapshot_at_or_after_matching(&capture, SYMBOL_A, recovery_started_us, &truth_after_outage);
    assert_eq!(reconstructed_book(&capture, SYMBOL_A), mock.book("asset-a"));

    // The missed interval is not silently represented as complete: verify
    // reports the pre-outage deltas disagreeing with the recovery snapshot.
    let summary = verify_capture(&output);
    assert!(
        !summary.valid,
        "a capture with an uncovered interval must not verify as complete"
    );
    assert!(summary.snapshot_mismatches >= 1);
    assert_eq!(summary.mismatch_samples[0]["symbol"], "polymarket:cond-a:0");
    assert!(report.core.failover_snapshots >= 1);
    assert_eq!(report.core.deltas_emitted, 3);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_subscription_is_detected_and_repaired() {
    let directory = tempfile::tempdir().expect("tempdir");
    let output = directory.path().join("silent.sbe");
    let mock = MockExchange::start(vec![market("cond-a", "asset-a")], false).await;
    mock.set_book("asset-a", &[(400_000, 10_000_000)], &[(600_000, 5_000_000)]);
    let mut settings = test_settings(&mock, &output);
    settings.collector.stale_probe_after_secs = 10;
    settings.collector.probe_interval_secs = 1;
    settings.collector.max_probes = 1;
    let done = Arc::new(Notify::new());
    let handle = spawn_collector(settings, Arc::clone(&done));

    mock.wait_for_connections(2).await;
    mock.wait_for_subscriptions("asset-a", 2).await;
    sleep(Duration::from_millis(200)).await;
    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 11_000_000));
    assert!(mock.publish_delta("asset-a", Side::Sell, 600_000, 4_000_000));
    sleep(Duration::from_millis(200)).await;
    let quiet_truth = mock.book("asset-a");

    let connections_before = mock.next_id();
    mock.set_suppress_deltas(true);
    let quiet_started = tokio::time::Instant::now();
    // The stale probe must repair the quiet symbol by cycling its
    // subscription on the live connection. No new websocket may be opened.
    wait_until_for(
        Duration::from_secs(20),
        "a quiet-symbol resubscribe",
        &mut || {
            mock.subscription_log().iter().any(|frame| {
                frame.operation == "unsubscribe" && frame.connection < connections_before
            })
        },
    )
    .await;
    assert!(
        quiet_started.elapsed() >= Duration::from_secs(9),
        "resubscribe happened before the stale-probe threshold"
    );
    assert_eq!(
        mock.next_id(),
        connections_before,
        "the repair must stay on the existing connection"
    );
    mock.set_suppress_deltas(false);
    sleep(Duration::from_millis(300)).await;

    assert!(mock.publish_delta("asset-a", Side::Buy, 400_000, 12_000_000));
    sleep(Duration::from_millis(300)).await;
    done.notify_one();
    let report = finish_collector(handle).await;
    let capture = read_capture(&output);
    let updates = mock.delivered_updates_for("asset-a");

    assert_deltas_match_updates(&capture, &updates, SYMBOL_A);
    assert_per_symbol_anchor(&capture, SYMBOL_A);
    assert_delta_receive_times_monotonic(&capture, SYMBOL_A);
    assert_eq!(
        recovery_snapshot(&capture, SYMBOL_A),
        &quiet_truth,
        "repair snapshot must contain the unchanged truth"
    );
    let summary = verify_capture(&output);
    assert!(
        summary.valid,
        "silent repair should leave a self-consistent capture: {summary:?}"
    );
    assert!(report.core.stale_probes >= 1);
    // The repair stays on the live connection: the probe cycles the
    // subscription, so no source switch, failover, or reconnect is involved.
    assert_eq!(report.core.source_switches, 0);
    assert_eq!(report.core.failover_snapshots, 0);
    assert!(report.sources.iter().all(|source| source.reconnects == 0));
    let log = mock.subscription_log();
    assert!(
        log.iter()
            .any(|frame| frame.operation == "subscribe" && !frame.initial),
        "the resubscribe must re-add the asset"
    );
}
