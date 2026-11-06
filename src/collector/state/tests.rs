use super::*;
use crate::config::ExchangeKind;
use crate::metrics::{CatalogHealth, MetricsRegistry, ReadinessPolicy, RuntimeStatus, WriterStats};
use crate::model::{
    ContinuityState, Control, DiscontinuityReason, EventMeta, ExchangeEvent, Level, MarketInfo,
    ResetReason, SourceStatus,
};
use crate::time::now_us;

const NOW: i64 = 1_700_000_000_000_000;
const SOURCE_A: SourceId = SourceId::new(0, 0);
const SOURCE_B: SourceId = SourceId::new(0, 1);

fn settings_for(exchange: ExchangeKind) -> Settings {
    let mut settings = Settings::for_exchange(exchange);
    settings.collector.shards = 1;
    settings.collector.replicas = 2;
    settings.collector.snapshot_interval_secs = 60;
    settings.collector.stale_probe_after_secs = 120;
    settings.collector.probe_interval_secs = 30;
    settings.collector.max_probes = 3;
    settings
}

fn core() -> CollectorCore {
    CollectorCore::new(
        CoreConfig::from_settings(
            &settings_for(ExchangeKind::Polymarket),
            1,
            ExchangeCapabilities::new(false),
        )
        .unwrap(),
    )
    .unwrap()
}

fn clocked_core() -> CollectorCore {
    CollectorCore::new(
        CoreConfig::from_settings(
            &settings_for(ExchangeKind::Kalshi),
            1,
            ExchangeCapabilities::new(true),
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn capabilities_are_not_derived_from_the_venue_name() {
    let unclocked_kalshi = CoreConfig::from_settings(
        &settings_for(ExchangeKind::Kalshi),
        1,
        ExchangeCapabilities::new(false),
    )
    .unwrap();
    assert!(!unclocked_kalshi.capabilities.has_sequence_clock);

    let clocked_polymarket = CoreConfig::from_settings(
        &settings_for(ExchangeKind::Polymarket),
        1,
        ExchangeCapabilities::new(true),
    )
    .unwrap();
    assert!(clocked_polymarket.capabilities.has_sequence_clock);
}

fn market(native: &str) -> MarketInfo {
    MarketInfo {
        exchange: "test".to_owned(),
        native_symbol: native.to_owned(),
        symbol: native.to_owned(),
        title: Some(native.to_owned()),
        outcome: None,
        exchange_market_id: None,
        close_time_us: None,
        tick_size_us: Some(10_000),
        price_ranges: Vec::new(),
        price_level_structure: None,
    }
}

fn book(bid: i64, ask: i64) -> BookSnapshot {
    BookSnapshot {
        bids: vec![Level {
            price: bid,
            quantity: 10_000_000,
        }],
        asks: vec![Level {
            price: ask,
            quantity: 5_000_000,
        }],
    }
}

fn snapshot_event_fenced(
    native: &str,
    bid: i64,
    ask: i64,
    clock: Option<i64>,
    received_at_us: i64,
    connection_epoch: u64,
    receive_seq: u64,
) -> ExchangeEvent {
    ExchangeEvent::Snapshot {
        native_symbol: native.to_owned(),
        book: book(bid, ask),
        meta: EventMeta {
            received_at_us,
            exchange_ts_us: Some(received_at_us - 1_000),
            seq: Some(1),
            clock,
            connection_epoch,
            receive_seq,
        },
    }
}

fn snapshot_event_at(
    native: &str,
    bid: i64,
    ask: i64,
    clock: i64,
    received_at_us: i64,
) -> ExchangeEvent {
    snapshot_event_fenced(
        native,
        bid,
        ask,
        (clock != 0).then_some(clock),
        received_at_us,
        0,
        0,
    )
}

fn snapshot_event(native: &str, bid: i64, ask: i64, clock: i64) -> ExchangeEvent {
    snapshot_event_at(native, bid, ask, clock, NOW)
}

#[allow(clippy::too_many_arguments)]
fn delta_event_fenced(
    native: &str,
    side: Side,
    price: i64,
    quantity: i64,
    clock: Option<i64>,
    received_at_us: i64,
    connection_epoch: u64,
    receive_seq: u64,
) -> ExchangeEvent {
    ExchangeEvent::LevelChange {
        native_symbol: native.to_owned(),
        side,
        price,
        change: QuantityChange::Absolute(quantity),
        meta: EventMeta {
            received_at_us,
            exchange_ts_us: Some(received_at_us - 1_000),
            seq: Some(2),
            clock,
            connection_epoch,
            receive_seq,
        },
    }
}

fn delta_event_at(
    native: &str,
    side: Side,
    price: i64,
    quantity: i64,
    clock: i64,
    received_at_us: i64,
) -> ExchangeEvent {
    delta_event_fenced(
        native,
        side,
        price,
        quantity,
        (clock != 0).then_some(clock),
        received_at_us,
        0,
        0,
    )
}

fn delta_event(
    native: &str,
    side: Side,
    price: i64,
    quantity: i64,
    received_at_us: i64,
) -> ExchangeEvent {
    delta_event_at(native, side, price, quantity, 0, received_at_us)
}

fn received_at(message: &OutputMessage) -> i64 {
    match message {
        OutputMessage::Snapshot(snapshot) => snapshot.received_at.timestamp_micros(),
        OutputMessage::PriceLevelUpdate(update) => update.backtest_timestamp.timestamp_micros(),
    }
}

fn connected_status(source: SourceId, connected: bool) -> SourceStatus {
    SourceStatus {
        source,
        connected,
        ..SourceStatus::new(source)
    }
}

fn connect_sources(core: &mut CollectorCore) {
    for source in [SOURCE_A, SOURCE_B] {
        core.handle_status(connected_status(source, true), NOW);
    }
}

fn discover_one(core: &mut CollectorCore) {
    core.apply_discovery(NOW, vec![market("A")]).unwrap();
    core.drain_controls();
}

fn recovery_control(
    core: &mut CollectorCore,
    expected_source: SourceId,
) -> (u64, Vec<String>, bool) {
    core.drain_controls()
        .into_iter()
        .find_map(|(source, control)| match control {
            Control::Recover {
                generation,
                symbols,
                reconnect,
            } if source == expected_source => Some((generation, symbols, reconnect)),
            _ => None,
        })
        .expect("expected a recovery control")
}

fn readiness(core: &CollectorCore) -> crate::metrics::ReadinessReport {
    let current = now_us().unwrap();
    let runtime = RuntimeStatus {
        coverage: core.coverage(NOW + 10, 120_000_000),
        catalog: CatalogHealth {
            last_discovery_finished_at_us: Some(current),
            ..CatalogHealth::default()
        },
        writer_healthy: true,
        ..RuntimeStatus::default()
    };
    let registry = MetricsRegistry::new("test", ReadinessPolicy::default());
    registry.update(
        core.stats(),
        WriterStats::default(),
        core.statuses(),
        runtime,
    );
    registry.readiness()
}

#[test]
fn discovery_adds_and_removes_with_controls() {
    let mut core = core();
    let outputs = core
        .apply_discovery(NOW, vec![market("A"), market("B")])
        .unwrap();
    assert!(outputs.is_empty());
    assert_eq!(core.symbol_count(), 2);
    assert_eq!(core.stats().added_symbols, 2);
    let controls = core.drain_controls();
    assert_eq!(controls.len(), 2);
    assert!(matches!(
        &controls[0].1,
        Control::SetSubscriptions(symbols) if symbols.len() == 2
    ));

    let outputs = core.apply_discovery(NOW + 1, vec![market("A")]).unwrap();
    assert_eq!(core.symbol_count(), 1);
    assert!(outputs.is_empty());
    let controls = core.drain_controls();
    assert!(controls.iter().any(|(_, control)| matches!(
        control,
        Control::SetSubscriptions(symbols) if symbols.len() == 1
    )));
}

#[test]
fn coverage_separates_sent_subscriptions_from_anchored_books() {
    let mut core = core();
    core.apply_discovery(NOW, vec![market("A"), market("B")])
        .unwrap();
    core.drain_controls();
    for source in [SOURCE_A, SOURCE_B] {
        let mut status = SourceStatus::new(source);
        status.connected = true;
        status.subscriptions = 2;
        core.handle_status(status, NOW);
    }

    let coverage = core.coverage(NOW, 1_000_000);
    assert_eq!(coverage.expected_symbols, 2);
    assert_eq!(coverage.anchored_symbols, 0);
    assert_eq!(coverage.expected_subscriptions, 4);
    assert_eq!(coverage.subscribed_symbols, 4);
    assert_eq!(coverage.shards_with_connected_source, 1);
    assert_eq!(coverage.sources_connected, 2);
    assert_eq!(coverage.sources_total, 2);
    assert_eq!(coverage.oldest_anchor_us, None);

    let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
    assert_eq!(outputs.len(), 1);
    let coverage = core.coverage(NOW, 1_000_000);
    assert_eq!(coverage.anchored_symbols, 1);
    assert_eq!(coverage.stale_symbols, 0);
    assert_eq!(coverage.oldest_anchor_us, Some(NOW));

    // An anchored book that has not received a valid update within the
    // freshness window is counted as stale, not silently healthy.
    let coverage = core.coverage(NOW + 2_000_000, 1_000_000);
    assert_eq!(coverage.anchored_symbols, 1);
    assert_eq!(coverage.stale_symbols, 1);
}

#[test]
fn snapshot_anchors_delta_application_in_per_symbol_order() {
    let mut core = core();
    discover_one(&mut core);
    let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
    assert_eq!(outputs.len(), 1);
    let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
        panic!("expected snapshot");
    };
    assert_eq!(snapshot.symbol, "A");
    assert_eq!(snapshot.received_at.timestamp_micros(), NOW);
    assert_eq!(snapshot.bids[0].price_level, 400_000);
    assert_eq!(snapshot.asks[0].price_level, 600_000);

    let outputs = core.handle_event(
        SOURCE_A,
        ExchangeEvent::LevelChange {
            native_symbol: "A".to_owned(),
            side: Side::Buy,
            price: 400_000,
            change: QuantityChange::Delta(2_000_000),
            meta: EventMeta {
                received_at_us: NOW + 1,
                exchange_ts_us: Some(NOW),
                seq: Some(2),
                clock: None,
                connection_epoch: 0,
                receive_seq: 0,
            },
        },
    );
    assert_eq!(outputs.len(), 1);
    let OutputMessage::PriceLevelUpdate(update) = &outputs[0] else {
        panic!("expected delta");
    };
    assert_eq!(update.quantity, 12_000_000);
    assert_eq!(update.price_level, 400_000);
    // The writer must not clamp or rewrite the receive timestamp.
    assert_eq!(update.backtest_timestamp.timestamp_micros(), NOW + 1);

    // A later delta applies to the state produced by the snapshot plus the
    // first delta, in processing order, without re-emitting a snapshot.
    let outputs = core.handle_event(
        SOURCE_A,
        ExchangeEvent::LevelChange {
            native_symbol: "A".to_owned(),
            side: Side::Buy,
            price: 400_000,
            change: QuantityChange::Absolute(9_000_000),
            meta: EventMeta {
                received_at_us: NOW + 2,
                exchange_ts_us: Some(NOW + 1),
                seq: Some(3),
                clock: None,
                connection_epoch: 0,
                receive_seq: 0,
            },
        },
    );
    assert_eq!(outputs.len(), 1);
    let OutputMessage::PriceLevelUpdate(update) = &outputs[0] else {
        panic!("expected delta");
    };
    assert_eq!(update.quantity, 9_000_000);
    assert_eq!(update.backtest_timestamp.timestamp_micros(), NOW + 2);
    assert_eq!(core.stats().deltas_emitted, 2);
    assert_eq!(core.stats().exchange_snapshots, 1);
}

#[test]
fn clocked_failover_anchors_resumed_deltas_and_preserves_receive_times() {
    let mut core = clocked_core();
    discover_one(&mut core);
    core.handle_status(connected_status(SOURCE_A, true), NOW);
    core.handle_status(connected_status(SOURCE_B, true), NOW);
    let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
    assert_eq!(received_at(&outputs[0]), NOW);

    // The shadow applies its own updates without emitting. Its clock is
    // current when the active source is lost.
    assert!(
        core.handle_event(
            SOURCE_B,
            snapshot_event_at("A", 405_000, 595_000, 11, NOW + 100)
        )
        .is_empty()
    );
    assert!(
        core.handle_event(
            SOURCE_B,
            delta_event_at("A", Side::Buy, 405_000, 11_000_000, 12, NOW + 200)
        )
        .is_empty()
    );

    core.handle_status(connected_status(SOURCE_A, true), NOW + 250);
    let outputs = core.handle_status(connected_status(SOURCE_A, false), NOW + 300);
    assert_eq!(outputs.len(), 1);
    let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
        panic!("expected failover snapshot");
    };
    assert_eq!(snapshot.received_at.timestamp_micros(), NOW + 300);
    assert_eq!(
        snapshot.exchange_timestamp.timestamp_micros(),
        NOW + 200 - 1_000
    );
    assert_eq!(snapshot.bids[0].price_level, 405_000);
    assert_eq!(snapshot.bids[0].quantity, 11_000_000);

    let outputs = core.handle_event(
        SOURCE_B,
        delta_event_at("A", Side::Sell, 595_000, 4_000_000, 13, NOW + 400),
    );
    assert_eq!(outputs.len(), 1);
    let OutputMessage::PriceLevelUpdate(update) = &outputs[0] else {
        panic!("expected resumed delta");
    };
    assert_eq!(update.backtest_timestamp.timestamp_micros(), NOW + 400);
    assert_eq!(update.quantity, 4_000_000);
}

#[test]
fn generated_snapshots_follow_the_supplied_wall_clock_across_a_step() {
    let mut core = core();
    discover_one(&mut core);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));

    let outputs = core.tick(NOW + 61_000_000);
    assert_eq!(outputs.len(), 1);
    let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
        panic!("expected periodic snapshot");
    };
    assert_eq!(snapshot.received_at.timestamp_micros(), NOW + 61_000_000);

    // A backward wall-clock step must not emit early: the stored deadline
    // is still ahead of the stepped time.
    assert!(core.tick(NOW + 30_000_000).is_empty());

    let outputs = core.tick(NOW + 122_000_000);
    assert_eq!(outputs.len(), 1);
    let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
        panic!("expected periodic snapshot");
    };
    assert_eq!(snapshot.received_at.timestamp_micros(), NOW + 122_000_000);
    assert_eq!(core.stats().periodic_snapshots, 2);
}

#[test]
fn cross_symbol_output_order_is_processing_order_and_not_globally_sorted() {
    let mut core = core();
    core.apply_discovery(NOW, vec![market("A"), market("B")])
        .unwrap();
    core.drain_controls();

    let first = core.handle_event(
        SOURCE_A,
        snapshot_event_at("A", 400_000, 600_000, 0, NOW + 20),
    );
    let second = core.handle_event(
        SOURCE_A,
        snapshot_event_at("B", 410_000, 590_000, 0, NOW + 10),
    );
    let first_at = received_at(&first[0]);
    let second_at = received_at(&second[0]);
    assert_eq!((first_at, second_at), (NOW + 20, NOW + 10));
    assert!(
        first_at > second_at,
        "the test must demonstrate a cross-symbol timestamp regression"
    );
}

#[test]
fn clocked_shadow_replica_takes_over_after_source_loss() {
    let mut core = clocked_core();
    discover_one(&mut core);
    core.handle_status(connected_status(SOURCE_A, true), NOW);
    core.handle_status(connected_status(SOURCE_B, true), NOW);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
    core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 12));

    let outputs = core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
    assert_eq!(outputs.len(), 1);
    let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
        panic!("expected failover snapshot");
    };
    assert_eq!(snapshot.bids[0].price_level, 410_000);
    assert_eq!(core.stats().source_switches, 1);
}

#[test]
fn shadow_snapshot_does_not_reset_active_probe_state() {
    let mut core = core();
    discover_one(&mut core);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));

    let mut shadow = snapshot_event("A", 405_000, 595_000, 0);
    if let ExchangeEvent::Snapshot { meta, .. } = &mut shadow {
        meta.received_at_us = NOW + 100;
    }
    core.handle_event(SOURCE_B, shadow);

    let state = core.markets.get("A").unwrap();
    assert_eq!(state.probes, 0);
    assert_eq!(state.last_probe_us, NOW);
}

#[test]
fn removal_emits_empty_snapshot() {
    let mut core = core();
    discover_one(&mut core);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
    let outputs = core.apply_discovery(NOW + 10, Vec::new()).unwrap();
    assert_eq!(outputs.len(), 1);
    let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
        panic!("expected removal snapshot");
    };
    assert!(snapshot.bids.is_empty());
    assert!(snapshot.asks.is_empty());
    assert_eq!(core.stats().empty_snapshots, 1);
}

#[test]
fn periodic_snapshot_is_spread_and_emitted() {
    let mut core = core();
    discover_one(&mut core);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
    let outputs = core.tick(NOW + 61_000_000);
    assert_eq!(outputs.len(), 1);
    let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
        panic!("expected periodic snapshot");
    };
    assert_eq!(snapshot.symbol, "A");
    assert_eq!(core.stats().periodic_snapshots, 1);
}

#[test]
fn unacknowledged_shadow_snapshot_does_not_emit_after_source_loss() {
    let mut core = core();
    discover_one(&mut core);
    core.handle_status(connected_status(SOURCE_A, true), NOW);
    core.handle_status(connected_status(SOURCE_B, true), NOW);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
    core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 0));

    let outputs = core.handle_event(
        SOURCE_A,
        ExchangeEvent::StreamReset {
            native_symbols: vec!["A".to_owned()],
            reason: ResetReason::SequenceGap,
            received_at_us: NOW + 2,
        },
    );
    assert!(outputs.is_empty());

    let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 0));
    assert!(outputs.is_empty());
    assert!(core.markets["A"].active.is_none());
}

#[test]
fn maintenance_clock_regression_starts_a_new_epoch() {
    let mut core = clocked_core();
    discover_one(&mut core);
    core.handle_status(connected_status(SOURCE_A, true), NOW);
    core.handle_status(connected_status(SOURCE_B, true), NOW);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 100));
    core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 110));

    let outputs = core.handle_event(
        SOURCE_A,
        ExchangeEvent::LevelChange {
            native_symbol: "A".to_owned(),
            side: Side::Buy,
            price: 400_000,
            change: QuantityChange::Delta(1_000_000),
            meta: EventMeta {
                received_at_us: NOW + 1,
                exchange_ts_us: Some(NOW),
                seq: Some(2),
                clock: Some(50),
                connection_epoch: 0,
                receive_seq: 0,
            },
        },
    );
    assert_eq!(outputs.len(), 1);
    assert_eq!(core.stats().clock_resets, 1);
    assert_eq!(core.markets["A"].emitted_clock, Some(50));
    assert!(!core.markets["A"].books[&SOURCE_B].valid);

    // A lower-clock shadow snapshot is stale in the new epoch and must not
    // replace the active book.
    let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 40));
    assert!(outputs.is_empty());
    assert!(!core.markets["A"].books[&SOURCE_B].valid);

    // Once the shadow catches up to the new epoch it can be a backup again.
    let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 60));
    assert!(outputs.is_empty());
    assert!(core.markets["A"].books[&SOURCE_B].valid);
}

#[test]
fn requested_recovery_snapshot_after_reconnect_accepts_new_lower_epoch() {
    let mut core = clocked_core();
    discover_one(&mut core);
    core.handle_status(connected_status(SOURCE_A, true), NOW);
    core.handle_status(connected_status(SOURCE_B, true), NOW);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 100));
    // B is stale and its book is rejected, leaving an observed clock behind.
    core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 90));
    // A fresh connection clears that cross-epoch history.
    core.handle_event(
        SOURCE_B,
        ExchangeEvent::StreamReset {
            native_symbols: vec!["A".to_owned()],
            reason: ResetReason::Connected,
            received_at_us: NOW + 1,
        },
    );
    let outputs = core.handle_event(
        SOURCE_A,
        ExchangeEvent::StreamReset {
            native_symbols: vec!["A".to_owned()],
            reason: ResetReason::SequenceGap,
            received_at_us: NOW + 2,
        },
    );
    assert!(outputs.is_empty());

    let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 50));
    assert_eq!(outputs.len(), 1);
    assert_eq!(core.stats().clock_resets, 1);
    assert_eq!(core.markets["A"].active, Some(SOURCE_B));
    assert_eq!(core.markets["A"].emitted_clock, Some(50));
}

#[test]
fn lifecycle_price_structure_updates_market_metadata() {
    let mut core = core();
    discover_one(&mut core);
    core.handle_event(
        SOURCE_A,
        ExchangeEvent::Lifecycle {
            native_symbol: "A".to_owned(),
            kind: LifecycleKind::PriceStructureUpdated,
            close_time_us: None,
            title: None,
            tick_size_us: Some(1_000),
            price_ranges: Some(vec![
                crate::model::PriceRange {
                    start_us: 0,
                    end_us: 100_000,
                    step_us: 1_000,
                },
                crate::model::PriceRange {
                    start_us: 100_000,
                    end_us: 900_000,
                    step_us: 10_000,
                },
            ]),
            price_level_structure: Some("tapered_deci_cent".to_owned()),
            meta: EventMeta {
                received_at_us: NOW,
                exchange_ts_us: None,
                seq: Some(2),
                clock: None,
                connection_epoch: 0,
                receive_seq: 0,
            },
        },
    );
    let info = &core.market_infos()[0];
    assert_eq!(info.tick_size_us, Some(1_000));
    assert_eq!(info.price_ranges.len(), 2);
    assert_eq!(
        info.price_level_structure.as_deref(),
        Some("tapered_deci_cent")
    );
}

#[test]
fn invalid_discovery_is_rejected_without_removing_current_symbols() {
    let mut core = core();
    discover_one(&mut core);
    let duplicate = core
        .apply_discovery(NOW + 1, vec![market("A"), market("A")])
        .unwrap_err();
    assert!(duplicate.to_string().contains("duplicate"));
    assert_eq!(core.symbol_count(), 1);

    let mut invalid = market("B");
    invalid.symbol.clear();
    assert!(
        core.apply_discovery(NOW + 2, vec![market("A"), invalid])
            .is_err()
    );
    assert_eq!(core.symbol_count(), 1);
    assert!(core.markets.contains_key("A"));
}

#[test]
fn duplicate_output_symbols_are_rejected() {
    let mut core = core();
    discover_one(&mut core);
    let mut impostor = market("B");
    impostor.symbol = "A".to_owned();
    let error = core
        .apply_discovery(NOW + 1, vec![market("A"), impostor])
        .unwrap_err();
    assert!(error.to_string().contains("duplicate output symbol"));
    assert_eq!(core.symbol_count(), 1);
}

#[test]
fn rediscovery_with_changed_metadata_keeps_symbol_stable() {
    let mut core = core();
    discover_one(&mut core);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));

    let mut renamed = market("A");
    renamed.title = Some("renamed".to_owned());
    let outputs = core.apply_discovery(NOW + 1, vec![renamed]).unwrap();
    assert!(outputs.is_empty());
    assert_eq!(core.markets["A"].info.title.as_deref(), Some("renamed"));
    assert_eq!(core.markets["A"].info.symbol, "A");
    assert!(core.markets["A"].emitted_any);
}

#[test]
fn identity_change_retires_old_symbol_and_reseeds_state() {
    let mut core = core();
    discover_one(&mut core);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));

    let mut migrated = market("A");
    migrated.symbol = "A-v2".to_owned();
    let outputs = core.apply_discovery(NOW + 1, vec![migrated]).unwrap();
    assert_eq!(outputs.len(), 1);
    let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
        panic!("expected retirement snapshot");
    };
    assert_eq!(snapshot.symbol, "A");
    assert!(snapshot.bids.is_empty());
    assert!(snapshot.asks.is_empty());
    assert_eq!(core.stats().empty_snapshots, 1);

    assert_eq!(core.markets["A"].info.symbol, "A-v2");
    assert!(!core.markets["A"].emitted_any);
    assert!(core.markets["A"].active.is_none());

    let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 410_000, 590_000, 0));
    assert_eq!(outputs.len(), 1);
    let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
        panic!("expected fresh snapshot under the new symbol");
    };
    assert_eq!(snapshot.symbol, "A-v2");
}

#[test]
fn clocked_sequence_reset_fails_over_to_valid_shadow() {
    let mut core = clocked_core();
    discover_one(&mut core);
    core.handle_status(connected_status(SOURCE_A, true), NOW);
    core.handle_status(connected_status(SOURCE_B, true), NOW);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
    core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 11));
    let outputs = core.handle_event(
        SOURCE_A,
        ExchangeEvent::StreamReset {
            native_symbols: vec!["A".to_owned()],
            reason: ResetReason::SequenceGap,
            received_at_us: NOW + 2,
        },
    );
    assert_eq!(outputs.len(), 1);
    let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
        panic!("expected failover snapshot");
    };
    assert_eq!(snapshot.bids[0].price_level, 405_000);
    assert_eq!(core.stats().sequence_resets, 1);
}

#[test]
fn source_disconnect_opens_one_unquantifiable_incident_and_blocks_deltas() {
    let mut core = core();
    assert!(!core.config.capabilities.has_sequence_clock);
    discover_one(&mut core);
    connect_sources(&mut core);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
    core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 0));

    let outputs = core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
    assert!(outputs.is_empty());
    assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
    let incident = core.markets["A"].incident.as_ref().unwrap();
    assert_eq!(incident.reason, DiscontinuityReason::ActiveSourceLost);
    assert!(!incident.quantifiable);
    assert_eq!(core.stats().discontinuity_incidents, 1);
    assert_eq!(core.stats().unquantifiable_gaps, 1);

    let delta = core.handle_event(
        SOURCE_B,
        delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 2),
    );
    assert!(delta.is_empty(), "deltas must not apply while Resyncing");
    assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
}

#[test]
fn clockless_full_recovery_requires_acknowledged_fence_and_fresh_snapshot() {
    let mut core = core();
    discover_one(&mut core);
    connect_sources(&mut core);

    let initial = core.handle_event(
        SOURCE_A,
        snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
    );
    assert_eq!(initial.len(), 1);
    assert!(
        core.handle_event(
            SOURCE_B,
            snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 50, 1, 2)
        )
        .is_empty()
    );
    assert_eq!(core.markets["A"].active, Some(SOURCE_A));

    // Active source loss: no shadow may be promoted before a new recovery
    // control and its acknowledgement.
    let lost = core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
    assert!(lost.is_empty());
    assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
    assert_eq!(core.stats().discontinuity_incidents, 1);
    assert_eq!(core.stats().unquantifiable_gaps, 1);

    let (generation, symbols, reconnect) = recovery_control(&mut core, SOURCE_B);
    assert_eq!(symbols, vec!["A".to_owned()]);
    assert!(reconnect, "a valid shadow must reconnect before promotion");

    // Before the worker acknowledges the control, even a newer connection
    // epoch cannot be promoted.
    let before_ack = core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 420_000, 580_000, None, NOW + 2, 2, 1),
    );
    assert!(before_ack.is_empty());
    assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
    assert!(core.markets["A"].active.is_none());
    assert_eq!(core.stats().rejected_recovery_snapshots, 1);

    core.handle_recovery_ready(SOURCE_B, generation, 1, 7);

    // Equal to the acknowledgement fence is not newer.
    let equal_fence = core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 420_000, 580_000, None, NOW + 3, 1, 7),
    );
    assert!(equal_fence.is_empty());
    assert_eq!(core.stats().rejected_recovery_snapshots, 2);

    // The production reconnect path resets the connection and resubscribes.
    core.handle_event(
        SOURCE_B,
        ExchangeEvent::StreamReset {
            native_symbols: vec!["A".to_owned()],
            reason: ResetReason::Connected,
            received_at_us: NOW + 4,
        },
    );
    let recovered = core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 425_000, 575_000, None, NOW + 10, 2, 1),
    );
    assert_eq!(recovered.len(), 1);
    let OutputMessage::Snapshot(snapshot) = &recovered[0] else {
        panic!("expected replacement snapshot");
    };
    assert_eq!(snapshot.bids[0].price_level, 425_000);
    assert_eq!(core.markets["A"].active, Some(SOURCE_B));
    assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
    assert!(core.markets["A"].incident.is_none());
    assert_eq!(core.markets["A"].resolved_incidents.len(), 1);
    assert_eq!(core.stats().discontinuity_incidents, 1);
    assert_eq!(core.stats().unquantifiable_gaps, 1);
    assert_eq!(core.stats().rejected_recovery_snapshots, 2);
    assert_eq!(core.stats().failover_snapshots, 1);
    assert_eq!(core.stats().source_switches, 1);
    assert!(core.drain_controls().is_empty());

    let delta = core.handle_event(
        SOURCE_B,
        delta_event_fenced("A", Side::Buy, 425_000, 12_000_000, None, NOW + 11, 2, 2),
    );
    assert_eq!(delta.len(), 1);
    assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
}

#[test]
fn disconnected_replica_reconnect_is_fenced_by_a_requested_recovery() {
    let mut core = core();
    discover_one(&mut core);
    connect_sources(&mut core);
    core.handle_event(
        SOURCE_A,
        snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
    );
    core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 1, 1, 2),
    );

    assert!(
        core.handle_status(connected_status(SOURCE_A, false), NOW + 2)
            .is_empty()
    );
    let controls = core.drain_controls();
    let (generation, reconnect) = controls
        .iter()
        .find_map(|(source, control)| match control {
            Control::Recover {
                generation,
                reconnect,
                ..
            } if *source == SOURCE_A => Some((*generation, *reconnect)),
            _ => None,
        })
        .expect("the lost source must receive a recovery request before it reconnects");
    assert!(
        !reconnect,
        "an invalidated book is repaired by a fenced snapshot request"
    );
    assert!(controls.iter().any(|(source, control)| {
        *source == SOURCE_B
            && matches!(
                control,
                Control::Recover {
                    reconnect: true,
                    ..
                }
            )
    }));

    // The worker acknowledges while backing off, then opens a new
    // connection. That connection's first snapshot must be eligible even
    // though the source was disconnected when the incident started.
    core.handle_recovery_ready(SOURCE_A, generation, 1, 5);
    core.handle_event(
        SOURCE_A,
        ExchangeEvent::StreamReset {
            native_symbols: vec!["A".to_owned()],
            reason: ResetReason::Connected,
            received_at_us: NOW + 3,
        },
    );
    let recovered = core.handle_event(
        SOURCE_A,
        snapshot_event_fenced("A", 425_000, 575_000, None, NOW + 10, 2, 1),
    );
    assert_eq!(recovered.len(), 1);
    assert_eq!(core.markets["A"].active, Some(SOURCE_A));
    assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
    assert!(core.markets["A"].incident.is_none());
    assert_eq!(core.stats().discontinuity_incidents, 1);
    assert_eq!(core.stats().unquantifiable_gaps, 1);
}

#[test]
fn recovery_snapshot_before_ack_is_rejected_without_mutation() {
    let mut core = core();
    discover_one(&mut core);
    connect_sources(&mut core);
    core.handle_event(
        SOURCE_A,
        snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
    );
    core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 50, 1, 2),
    );
    core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
    let (_generation, _symbols, _reconnect) = recovery_control(&mut core, SOURCE_B);

    let before_book = core.markets["A"].books[&SOURCE_B].book.clone();
    let before_valid = core.markets["A"].books[&SOURCE_B].valid;
    let before_received = core.markets["A"].books[&SOURCE_B].last_received_at_us;
    let before_exchange = core.markets["A"].books[&SOURCE_B].last_exchange_ts_us;
    let before_incident = core.markets["A"].incident.clone();
    let before_count = core.stats().discontinuity_incidents;
    let before_unquantifiable = core.stats().unquantifiable_gaps;

    let rejected = core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 420_000, 580_000, None, NOW + 2, 2, 1),
    );
    assert!(rejected.is_empty());
    assert_eq!(core.markets["A"].books[&SOURCE_B].book, before_book);
    assert_eq!(core.markets["A"].books[&SOURCE_B].valid, before_valid);
    assert_eq!(
        core.markets["A"].books[&SOURCE_B].last_received_at_us,
        before_received
    );
    assert_eq!(
        core.markets["A"].books[&SOURCE_B].last_exchange_ts_us,
        before_exchange
    );
    assert_eq!(core.markets["A"].incident, before_incident);
    assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
    assert!(core.markets["A"].active.is_none());
    assert_eq!(core.stats().discontinuity_incidents, before_count);
    assert_eq!(core.stats().unquantifiable_gaps, before_unquantifiable);
    assert_eq!(core.stats().rejected_recovery_snapshots, 1);
}

#[test]
fn delayed_pre_incident_frame_is_rejected_by_receive_fence() {
    let mut core = core();
    discover_one(&mut core);
    connect_sources(&mut core);
    core.handle_event(
        SOURCE_A,
        snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
    );
    core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 50, 1, 2),
    );
    core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
    let (generation, _symbols, _reconnect) = recovery_control(&mut core, SOURCE_B);
    core.handle_recovery_ready(SOURCE_B, generation, 1, 5);

    let before_book = core.markets["A"].books[&SOURCE_B].book.clone();
    let before_received = core.markets["A"].books[&SOURCE_B].last_received_at_us;

    // The frame was read before the recovery acknowledgement but is
    // processed afterwards. Its wall-clock receive time is before the
    // incident, and its receive sequence is behind the fence.
    let delayed = core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 430_000, 570_000, None, NOW, 1, 3),
    );
    assert!(delayed.is_empty());
    assert_eq!(core.markets["A"].books[&SOURCE_B].book, before_book);
    assert_eq!(
        core.markets["A"].books[&SOURCE_B].last_received_at_us,
        before_received
    );
    assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
    assert_eq!(core.stats().rejected_recovery_snapshots, 1);

    // A frame read after the acknowledgement repairs the gap.
    let accepted = core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 430_000, 570_000, None, NOW + 10, 1, 6),
    );
    assert_eq!(accepted.len(), 1);
    assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
}

#[test]
fn shadow_snapshot_does_not_consume_recovery_token() {
    let mut core = core();
    discover_one(&mut core);
    connect_sources(&mut core);
    core.handle_event(
        SOURCE_A,
        snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
    );
    core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 1, 1, 2),
    );

    core.handle_event(
        SOURCE_B,
        ExchangeEvent::StreamReset {
            native_symbols: vec!["A".to_owned()],
            reason: ResetReason::ProtocolError,
            received_at_us: NOW + 2,
        },
    );
    let (generation, _symbols, _reconnect) = recovery_control(&mut core, SOURCE_B);
    core.handle_recovery_ready(SOURCE_B, generation, 1, 3);

    // A fresh shadow snapshot is applied but must not resolve the active
    // symbol's incident or consume the token needed for a later failover.
    let outputs = core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 410_000, 590_000, None, NOW + 3, 1, 4),
    );
    assert!(outputs.is_empty());
    assert_eq!(core.markets["A"].active, Some(SOURCE_A));
    assert!(core.markets["A"].recovery_requests.contains_key(&SOURCE_B));
    assert!(core.markets["A"].incident.is_none());
}

#[test]
fn deltas_cannot_restore_live_in_any_uncertain_state() {
    let mut core = core();
    discover_one(&mut core);
    connect_sources(&mut core);

    // Unseeded: no snapshot has anchored a book.
    assert!(
        core.handle_event(
            SOURCE_A,
            delta_event("A", Side::Buy, 410_000, 1_000_000, NOW)
        )
        .is_empty()
    );

    core.handle_event(
        SOURCE_A,
        snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
    );
    core.handle_event(
        SOURCE_B,
        snapshot_event_fenced("A", 405_000, 595_000, None, NOW + 1, 1, 2),
    );

    // Stale: both active and shadow deltas are ignored.
    core.markets
        .get_mut("A")
        .unwrap()
        .set_continuity(ContinuityState::Stale);
    assert!(
        core.handle_event(
            SOURCE_A,
            delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 2)
        )
        .is_empty()
    );
    assert!(
        core.handle_event(
            SOURCE_B,
            delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 3)
        )
        .is_empty()
    );

    // Resyncing: the active source reconnects/resubscribes; deltas from
    // either source cannot re-anchor the book.
    core.handle_event(
        SOURCE_A,
        ExchangeEvent::StreamReset {
            native_symbols: vec!["A".to_owned()],
            reason: ResetReason::SequenceGap,
            received_at_us: NOW + 4,
        },
    );
    assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
    assert!(
        core.handle_event(
            SOURCE_A,
            delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 5)
        )
        .is_empty()
    );
    assert!(
        core.handle_event(
            SOURCE_B,
            delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 6)
        )
        .is_empty()
    );

    // Terminal: the retired identity is never reopened.
    core.markets
        .get_mut("A")
        .unwrap()
        .set_continuity(ContinuityState::Terminal);
    assert!(
        core.handle_event(
            SOURCE_A,
            delta_event("A", Side::Buy, 410_000, 1_000_000, NOW + 7)
        )
        .is_empty()
    );
    assert!(
        core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 420_000, 580_000, None, NOW + 8, 1, 7)
        )
        .is_empty()
    );
    assert_eq!(core.markets["A"].continuity, ContinuityState::Terminal);
}

#[test]
fn fresh_active_repair_requires_acknowledged_recovery_fence() {
    let mut core = core();
    discover_one(&mut core);
    core.handle_status(connected_status(SOURCE_A, true), NOW);
    core.handle_event(
        SOURCE_A,
        snapshot_event_fenced("A", 400_000, 600_000, None, NOW, 1, 1),
    );
    core.config.stale_probe_after_us = 1_000_000;
    core.config.probe_interval_us = 1_000_000;
    core.markets.get_mut("A").unwrap().next_snapshot_us = NOW + 1_000_000_000;

    assert!(core.tick(NOW + 2_000_000).is_empty());
    assert_eq!(core.markets["A"].continuity, ContinuityState::Stale);
    let (generation, symbols, reconnect) = recovery_control(&mut core, SOURCE_A);
    assert_eq!(symbols, vec!["A".to_owned()]);
    assert!(!reconnect);

    let before_ack = core.handle_event(
        SOURCE_A,
        snapshot_event_fenced("A", 410_000, 590_000, None, NOW + 2_000_001, 1, 2),
    );
    assert!(before_ack.is_empty());
    assert_eq!(core.markets["A"].continuity, ContinuityState::Stale);

    core.handle_recovery_ready(SOURCE_A, generation, 1, 1);
    let repaired = core.handle_event(
        SOURCE_A,
        snapshot_event_fenced("A", 410_000, 590_000, None, NOW + 2_000_002, 1, 2),
    );
    assert_eq!(repaired.len(), 1);
    assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
    assert_eq!(core.markets["A"].active, Some(SOURCE_A));
    assert!(core.markets["A"].incident.is_none());
    assert_eq!(core.stats().discontinuity_incidents, 0);
}

#[test]
fn repeated_failure_events_do_not_double_count_incident() {
    let mut core = core();
    discover_one(&mut core);
    connect_sources(&mut core);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));

    core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
    core.handle_status(connected_status(SOURCE_A, false), NOW + 2);
    core.handle_event(
        SOURCE_A,
        ExchangeEvent::StreamReset {
            native_symbols: vec!["A".to_owned()],
            reason: ResetReason::ProtocolError,
            received_at_us: NOW + 3,
        },
    );
    core.handle_event(
        SOURCE_A,
        ExchangeEvent::StreamReset {
            native_symbols: vec!["A".to_owned()],
            reason: ResetReason::SequenceGap,
            received_at_us: NOW + 4,
        },
    );

    assert_eq!(core.stats().discontinuity_incidents, 1);
    assert_eq!(core.stats().unquantifiable_gaps, 1);
    assert_eq!(core.markets["A"].continuity, ContinuityState::Resyncing);
}

#[test]
fn incident_quantifiability_follows_venue_semantics() {
    for (mut core, quantifiable) in [(core(), false), (clocked_core(), true)] {
        discover_one(&mut core);
        connect_sources(&mut core);
        let clock = quantifiable.then_some(10);
        core.handle_event(
            SOURCE_A,
            snapshot_event_fenced("A", 400_000, 600_000, clock, NOW, 1, 1),
        );
        core.handle_status(connected_status(SOURCE_A, false), NOW + 1);

        let incident = core.markets["A"].incident.as_ref().unwrap();
        assert_eq!(incident.quantifiable, quantifiable);
        assert_eq!(core.stats().discontinuity_incidents, 1);
        assert_eq!(core.stats().unquantifiable_gaps, u64::from(!quantifiable));
    }
}

#[test]
fn readiness_fails_for_each_blocking_continuity_state() {
    let mut unseeded = core();
    unseeded.apply_discovery(NOW, vec![market("A")]).unwrap();
    unseeded.drain_controls();
    connect_sources(&mut unseeded);
    let report = readiness(&unseeded);
    assert!(!report.ready);
    assert!(!report.checks["unseeded"].ok);
    assert!(report.checks["unseeded"].detail.contains('1'));

    let mut stale = core();
    discover_one(&mut stale);
    connect_sources(&mut stale);
    stale.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
    stale
        .markets
        .get_mut("A")
        .unwrap()
        .set_continuity(ContinuityState::Stale);
    let report = readiness(&stale);
    assert!(!report.ready);
    assert!(!report.checks["stale_detail"].ok);

    let mut resyncing = core();
    discover_one(&mut resyncing);
    connect_sources(&mut resyncing);
    resyncing.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
    resyncing
        .markets
        .get_mut("A")
        .unwrap()
        .set_continuity(ContinuityState::Resyncing);
    let report = readiness(&resyncing);
    assert!(!report.ready);
    assert!(!report.checks["resyncing"].ok);

    let mut terminal = core();
    discover_one(&mut terminal);
    connect_sources(&mut terminal);
    terminal.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
    terminal
        .markets
        .get_mut("A")
        .unwrap()
        .set_continuity(ContinuityState::Terminal);
    let report = readiness(&terminal);
    assert!(!report.ready);
    assert!(!report.checks["terminal"].ok);
}

#[test]
fn shutdown_disconnect_is_not_a_discontinuity_incident() {
    let mut core = core();
    discover_one(&mut core);
    connect_sources(&mut core);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
    assert_eq!(core.stats().discontinuity_incidents, 0);

    core.begin_shutdown();
    let outputs = core.handle_status(connected_status(SOURCE_A, false), NOW + 1);
    assert!(outputs.is_empty());
    assert_eq!(core.stats().discontinuity_incidents, 0);
    assert_eq!(core.stats().unquantifiable_gaps, 0);
    assert_eq!(core.markets["A"].continuity, ContinuityState::Live);
    assert_eq!(core.markets["A"].active, Some(SOURCE_A));
    assert!(!core.statuses()[&SOURCE_A].connected);
    assert!(core.drain_controls().is_empty());
}

#[test]
fn terminal_lifecycle_removes_market_without_discontinuity_incident() {
    let mut core = core();
    discover_one(&mut core);
    core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 0));
    let incident_count_before = core.stats().discontinuity_incidents;

    core.handle_event(
        SOURCE_A,
        ExchangeEvent::Lifecycle {
            native_symbol: "A".to_owned(),
            kind: LifecycleKind::Resolved,
            close_time_us: Some(NOW + 1),
            title: None,
            tick_size_us: None,
            price_ranges: None,
            price_level_structure: None,
            meta: EventMeta::new(NOW + 1),
        },
    );

    assert!(!core.markets.contains_key("A"));
    assert_eq!(core.stats().discontinuity_incidents, incident_count_before);
}
