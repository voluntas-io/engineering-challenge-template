#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::sync::{Mutex, broadcast, mpsc, watch};

use engineering_challenge::{
    journal::{
        DecodeOutcome, JournalConfig, JournalHandle, JournalMeta, RawFrame, RunState, SyncMode,
    },
    model::{BookSnapshot, Control, EventMeta, ExchangeEvent, Level, SourceId},
    session::{
        MarketDataSession, SessionFactory, SessionFailure, SessionFrame, WorkerConfig, WorkerEvent,
        run_worker,
    },
};

fn source() -> SourceId {
    SourceId::new(0, 0)
}

fn snapshot_event(received_at_us: i64) -> ExchangeEvent {
    ExchangeEvent::Snapshot {
        native_symbol: "A".to_owned(),
        book: BookSnapshot {
            bids: vec![Level {
                price: 400_000,
                quantity: 10_000_000,
            }],
            asks: vec![Level {
                price: 600_000,
                quantity: 5_000_000,
            }],
        },
        meta: EventMeta {
            received_at_us,
            exchange_ts_us: Some(received_at_us - 1_000),
            seq: Some(1),
            clock: None,
            connection_epoch: 0,
            receive_seq: 0,
        },
    }
}

fn data_frame(received_at_us: i64) -> SessionFrame {
    SessionFrame::data(
        vec![snapshot_event(received_at_us)],
        42,
        RawFrame::binary(b"payload".to_vec()),
        DecodeOutcome::decoded(1),
        received_at_us,
    )
}

struct MockSession {
    rx: Arc<Mutex<mpsc::Receiver<SessionFrame>>>,
    tx: mpsc::Sender<SessionFrame>,
    subscriptions: Arc<Mutex<BTreeSet<String>>>,
    closed: watch::Sender<bool>,
    snapshot_on_recover: Option<SessionFrame>,
}

#[async_trait]
impl MarketDataSession for MockSession {
    async fn connect(&mut self) -> Result<(), SessionFailure> {
        Ok(())
    }

    async fn apply_subscriptions(
        &mut self,
        desired: &BTreeSet<String>,
    ) -> Result<(), SessionFailure> {
        *self.subscriptions.lock().await = desired.clone();
        Ok(())
    }

    async fn next_frame(&mut self) -> Result<SessionFrame, SessionFailure> {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(frame) => Ok(frame),
            None => {
                std::future::pending::<()>().await;
                unreachable!()
            }
        }
    }

    async fn send_keepalive(&mut self) -> Result<(), SessionFailure> {
        Ok(())
    }

    async fn request_snapshot(&mut self, _symbols: &[String]) -> Result<(), SessionFailure> {
        if let Some(frame) = self.snapshot_on_recover.take() {
            // Journaling is bypassed here; the snapshot is a decoded frame,
            // and the worker will journal it as a normal payload.
            let _ = self.tx.send(frame).await;
        }
        Ok(())
    }

    async fn close(&mut self) {
        self.closed.send_replace(true);
    }
}

struct ScriptedFactory {
    next: Arc<Mutex<std::collections::VecDeque<MockSession>>>,
}

#[async_trait]
impl SessionFactory for ScriptedFactory {
    async fn create_session(
        &self,
        _source: SourceId,
    ) -> anyhow::Result<Box<dyn MarketDataSession>> {
        let mut next = self.next.lock().await;
        let session = next
            .pop_front()
            .expect("factory ran out of scripted sessions");
        Ok(Box::new(session))
    }
}

#[allow(clippy::type_complexity)]
fn make_session(
    snapshot_on_recover: Option<SessionFrame>,
) -> (
    MockSession,
    mpsc::Sender<SessionFrame>,
    watch::Receiver<bool>,
    Arc<Mutex<BTreeSet<String>>>,
) {
    let (tx, rx) = mpsc::channel::<SessionFrame>(64);
    let (closed, closed_rx) = watch::channel(false);
    let subscriptions = Arc::new(Mutex::new(BTreeSet::new()));
    let session = MockSession {
        rx: Arc::new(Mutex::new(rx)),
        tx: tx.clone(),
        subscriptions: Arc::clone(&subscriptions),
        closed,
        snapshot_on_recover,
    };
    (session, tx, closed_rx, subscriptions)
}

fn test_config(journal: Option<JournalHandle>) -> WorkerConfig {
    WorkerConfig {
        keepalive_interval: Duration::from_secs(60),
        status_interval: Duration::from_secs(60),
        idle_timeout: Duration::from_secs(60),
        reconnect_after: Duration::from_secs(60),
        reconnect_base_backoff: Duration::from_millis(10),
        reconnect_max_backoff: Duration::from_millis(50),
        connect_permits: Arc::new(tokio::sync::Semaphore::new(4)),
        journal,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_ack_precedes_snapshot_and_uses_last_payload_fence() {
    let recovery_snapshot = SessionFrame::data(
        vec![snapshot_event(110)],
        42,
        RawFrame::binary(b"recovery".to_vec()),
        DecodeOutcome::decoded(1),
        110,
    );
    let (session, tx, _closed, _subs) = make_session(Some(recovery_snapshot));
    let factory = Arc::new(ScriptedFactory {
        next: Arc::new(Mutex::new([session].into_iter().collect())),
    });

    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    let (evt_tx, mut evt_rx) = mpsc::channel(64);
    let (stop_tx, stop_rx) = broadcast::channel(1);
    let worker = tokio::spawn(run_worker(
        factory,
        source(),
        cmd_rx,
        evt_tx,
        test_config(None),
        stop_rx,
    ));

    // First payload: epoch 1, seq 1.
    tx.send(data_frame(100)).await.unwrap();
    let first = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let evt = tokio::time::timeout(remaining, evt_rx.recv())
                .await
                .expect("first event")
                .expect("channel open");
            match evt {
                WorkerEvent::Events { .. } => break evt,
                _ => continue,
            }
        }
    };
    match first {
        WorkerEvent::Events { events, .. } => {
            let meta = match &events[0] {
                ExchangeEvent::Snapshot { meta, .. } => meta,
                _ => panic!("expected snapshot"),
            };
            assert_eq!(meta.connection_epoch, 1);
            assert_eq!(meta.receive_seq, 1);
        }
        _ => panic!("expected Events"),
    }

    cmd_tx
        .send(Control::Recover {
            generation: 7,
            symbols: vec!["A".to_owned()],
            reconnect: false,
        })
        .await
        .unwrap();

    // RecoveryReady must arrive before the snapshot event.
    let mut order = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, evt_rx.recv()).await {
            Ok(Some(WorkerEvent::RecoveryReady {
                source: s,
                generation,
                connection_epoch,
                receive_seq,
            })) => {
                assert_eq!(s, source());
                assert_eq!(generation, 7);
                assert_eq!(connection_epoch, 1);
                assert_eq!(receive_seq, 1);
                order.push("ready");
            }
            Ok(Some(WorkerEvent::Events { events, .. })) => {
                for event in events {
                    if let ExchangeEvent::Snapshot { meta, .. } = event {
                        assert_eq!(meta.connection_epoch, 1);
                        assert_eq!(meta.receive_seq, 2);
                        order.push("snapshot");
                    }
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => break,
        }
        if order.len() >= 2 {
            break;
        }
    }
    assert_eq!(order, vec!["ready", "snapshot"]);

    let _ = stop_tx.send(());
    let _ = worker.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_recovery_advances_epoch_and_resets_sequence() {
    let (session1, tx1, mut closed1, subs1) = make_session(None);
    let (session2, tx2, _closed2, subs2) = make_session(None);
    let factory = Arc::new(ScriptedFactory {
        next: Arc::new(Mutex::new([session1, session2].into_iter().collect())),
    });

    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    let (evt_tx, mut evt_rx) = mpsc::channel(64);
    let (stop_tx, stop_rx) = broadcast::channel(1);
    let worker = tokio::spawn(run_worker(
        factory,
        source(),
        cmd_rx,
        evt_tx,
        test_config(None),
        stop_rx,
    ));

    cmd_tx
        .send(Control::SetSubscriptions(BTreeSet::from(["A".to_owned()])))
        .await
        .unwrap();
    tx1.send(data_frame(100)).await.unwrap();
    // Drain until the first payload event is observed.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match evt_rx.recv().await {
                Some(WorkerEvent::Events { .. }) => break,
                Some(_) => continue,
                None => break,
            }
        }
    })
    .await
    .unwrap();

    cmd_tx
        .send(Control::Recover {
            generation: 8,
            symbols: vec!["A".to_owned()],
            reconnect: true,
        })
        .await
        .unwrap();

    let ready = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match evt_rx.recv().await {
                Some(WorkerEvent::RecoveryReady {
                    connection_epoch,
                    receive_seq,
                    generation,
                    ..
                }) => return (generation, connection_epoch, receive_seq),
                Some(_) => continue,
                None => panic!("channel closed"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(ready, (8, 1, 1));

    tokio::time::timeout(Duration::from_secs(2), async {
        while !*closed1.borrow_and_update() {
            closed1
                .changed()
                .await
                .expect("mock session close signal sender dropped");
        }
    })
    .await
    .expect("first session did not close after recovery acknowledgement");

    // Session 2 snapshot must be epoch 2, seq 1.
    tx2.send(data_frame(200)).await.unwrap();
    let snapshot_meta = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match evt_rx.recv().await {
                Some(WorkerEvent::Events { events, .. }) => {
                    for event in events {
                        if let ExchangeEvent::Snapshot { meta, .. } = event {
                            return (meta.connection_epoch, meta.receive_seq);
                        }
                    }
                }
                Some(_) => continue,
                None => panic!("channel closed"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(snapshot_meta, (2, 1));
    assert_eq!(*subs1.lock().await, BTreeSet::from(["A".to_owned()]));
    assert_eq!(*subs2.lock().await, BTreeSet::from(["A".to_owned()]));

    let _ = stop_tx.send(());
    let _ = worker.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_journal_append_stops_worker_before_event_forwarding() {
    let temp = tempfile::tempdir().unwrap();
    let journal_dir = temp.path().join("journal");
    let config = JournalConfig {
        directory: journal_dir.clone(),
        queue_capacity: 16,
        enqueue_timeout: Duration::from_millis(250),
        flush_interval: Duration::from_millis(10),
        sync_interval: Duration::from_millis(20),
        segment_max_bytes: 4096,
        segment_max_seconds: 0,
        sync_mode: SyncMode::Interval,
    };
    let meta = JournalMeta {
        run_id: "run".to_owned(),
        venue: "test".to_owned(),
        code_commit: "test".to_owned(),
        config_sha256: "test".to_owned(),
    };
    let handle = JournalHandle::start(config, meta).expect("journal start");
    handle
        .record(
            source(),
            1,
            1,
            1,
            RawFrame::binary(b"first".to_vec()),
            DecodeOutcome::decoded(1),
        )
        .unwrap();
    let run_dir = {
        let status = handle.status();
        std::path::PathBuf::from(status.run_directory)
    };
    // Wait for the open segment.
    let partial = run_dir.join("00000000.pmj.partial");
    for _ in 0..200 {
        if partial.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(partial.exists(), "segment must appear before rename");

    let moved = temp.path().join("moved");
    std::fs::rename(&run_dir, &moved).unwrap();
    // Large payload forces rotation; the next segment cannot be created
    // under the old path.
    let large = vec![0x5a; 8192];
    let _ = handle.record(
        source(),
        1,
        2,
        2,
        RawFrame::binary(large),
        DecodeOutcome::decoded(1),
    );
    for _ in 0..200 {
        if handle.status().state == RunState::Failed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(handle.status().state, RunState::Failed);

    // Run the worker with the failed journal.
    let (session, tx, _closed, _subs) = make_session(None);
    let factory = Arc::new(ScriptedFactory {
        next: Arc::new(Mutex::new([session].into_iter().collect())),
    });
    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    let (evt_tx, mut evt_rx) = mpsc::channel(64);
    let (stop_tx, stop_rx) = broadcast::channel(1);
    let worker = tokio::spawn(run_worker(
        factory,
        source(),
        cmd_rx,
        evt_tx,
        test_config(Some(handle)),
        stop_rx,
    ));

    tx.send(data_frame(300)).await.unwrap();
    let mut saw_fatal = None;
    let mut saw_stopped = false;
    let mut forwarded = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, evt_rx.recv()).await {
            Ok(Some(WorkerEvent::Fatal { error, .. })) => {
                saw_fatal = Some(error);
            }
            Ok(Some(WorkerEvent::Stopped { .. })) => {
                saw_stopped = true;
            }
            Ok(Some(WorkerEvent::Events { .. })) => {
                forwarded = true;
            }
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => break,
        }
        if saw_fatal.is_some() && saw_stopped {
            break;
        }
    }
    let fatal = saw_fatal.expect("worker must emit Fatal");
    assert!(
        fatal.contains("journal"),
        "fatal must identify journal failure: {fatal}"
    );
    assert!(saw_stopped, "worker must emit Stopped after Fatal");
    assert!(!forwarded, "rejected frame must not reach Events");

    let _ = cmd_tx;
    let _ = stop_tx.send(());
    let _ = worker.await;
}
