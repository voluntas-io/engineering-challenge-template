use std::{
    env,
    process::Command as ProcessCommand,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use tempfile::TempDir;

use super::*;

fn test_config(directory: &Path) -> JournalConfig {
    JournalConfig {
        directory: directory.to_path_buf(),
        queue_capacity: 16,
        enqueue_timeout: Duration::from_millis(250),
        flush_interval: Duration::from_millis(10),
        sync_interval: Duration::from_millis(20),
        segment_max_bytes: 0,
        segment_max_seconds: 0,
        sync_mode: SyncMode::Interval,
    }
}

fn meta(run_id: &str) -> JournalMeta {
    JournalMeta {
        run_id: run_id.to_owned(),
        venue: "polymarket".to_owned(),
        code_commit: "test".to_owned(),
        config_sha256: "test".to_owned(),
    }
}

fn start(directory: &Path, run_id: &str) -> JournalHandle {
    JournalHandle::start(test_config(directory), meta(run_id)).expect("journal start")
}

fn append(
    handle: &JournalHandle,
    source: SourceId,
    epoch: u64,
    seq: u64,
    payload: &[u8],
    outcome: DecodeOutcome,
) {
    handle
        .record(
            source,
            epoch,
            seq,
            1_700_000_000_000_000 + seq as i64,
            RawFrame::binary(payload.to_vec()),
            outcome,
        )
        .expect("record");
}

fn run_directory(root: &Path, run_id: &str) -> PathBuf {
    root.join(run_id)
}

fn wait_failed(handle: &JournalHandle) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if handle.status().state == RunState::Failed {
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("journal did not report a failure");
}

fn wait_for_verification_state(directory: &Path, state: RunState) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(verification) = verify_run(directory)
            && verification.state == state
        {
            return;
        }
        if Instant::now() >= deadline {
            panic!("run directory never reached {state:?}");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

const CRASH_POINT_ENV: &str = "PMJ_TEST_CRASH_POINT";
const CRASH_ROOT_ENV: &str = "PMJ_TEST_CRASH_ROOT";
const CRASH_EXIT_CODE: i32 = 86;

fn spawn_crash_child(root: &Path, point: &str) {
    let executable = env::current_exe().expect("current test executable");
    let output = ProcessCommand::new(executable)
        .arg("--exact")
        .arg("journal::tests::crash_child_process")
        .arg("--nocapture")
        .env(CRASH_POINT_ENV, point)
        .env(CRASH_ROOT_ENV, root)
        .output()
        .expect("spawn crash child");
    assert_eq!(
        output.status.code(),
        Some(CRASH_EXIT_CODE),
        "child did not terminate at {point}; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Test-harness subprocess used by `crash_recovery_is_conservative_and_idempotent`.
/// `process::exit` bypasses Rust destructors and the journal's orderly
/// shutdown path, leaving the files at the selected persistence boundary.
#[test]
fn crash_child_process() {
    let Ok(point_name) = env::var(CRASH_POINT_ENV) else {
        return;
    };
    let root = PathBuf::from(env::var_os(CRASH_ROOT_ENV).expect("crash root"));
    let (target, rotate, finalize) = match point_name.as_str() {
        "after_record_write" => (FaultPoint::AfterRecordWrite, false, false),
        "after_segment_rename" => (FaultPoint::AfterSegmentRename, true, false),
        "after_manifest_update" => (FaultPoint::AfterManifestUpdate, true, false),
        "after_final_manifest_update" => (FaultPoint::AfterFinalManifestUpdate, false, true),
        other => panic!("unknown child crash point: {other}"),
    };
    let mut config = test_config(&root);
    if rotate {
        config.segment_max_bytes = 4096;
    }
    let hook: FaultHook = Arc::new(move |point| {
        if point == target {
            std::process::exit(CRASH_EXIT_CODE);
        }
        Ok(())
    });
    let handle = JournalHandle::start_inner(config, meta("crash-run"), Some(hook))
        .expect("start crash-test journal");

    if rotate {
        for seq in 1..=8 {
            append(
                &handle,
                SourceId::new(0, 0),
                1,
                seq,
                &[b'x'; 1200],
                DecodeOutcome::decoded(1),
            );
        }
    } else {
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            1,
            b"crash-boundary-record",
            DecodeOutcome::decoded(1),
        );
    }

    if finalize {
        let _ = handle.shutdown(Duration::from_secs(3));
    } else {
        // The producer only enqueues. Keep the process alive so the writer
        // thread can reach the requested persistence boundary.
        thread::sleep(Duration::from_secs(5));
    }
    // Returning normally is a failure in the parent: the selected hook
    // was not reached, so the test did not exercise the intended boundary.
}

// Called by an existing test to keep the generated test inventory stable.
fn assert_journal_directory_failures(root: &Path) {
    let blocker = root.join("not-a-directory");
    fs::write(&blocker, b"file").expect("create path blocker");
    let mut config = test_config(&blocker.join("runs"));
    let error = JournalHandle::start(config.clone(), meta("not-a-directory"))
        .expect_err("a file in the directory path must be rejected");
    assert!(
        format!("{error:#}").contains("creating journal directory"),
        "unexpected path-creation error: {error:#}"
    );
    assert!(!blocker.join("runs").exists());

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let uid = std::process::Command::new("id")
            .arg("-u")
            .output()
            .expect("read effective uid");
        let uid = String::from_utf8(uid.stdout).expect("decode uid");
        // Root bypasses directory mode bits. The required Linux CI runner
        // executes this test as an unprivileged account.
        if uid.trim() != "0" {
            let protected = root.join("read-only-parent");
            fs::create_dir(&protected).expect("create protected parent");
            let original = fs::metadata(&protected)
                .expect("stat protected parent")
                .permissions();
            let mut read_only = original.clone();
            read_only.set_mode(0o500);
            fs::set_permissions(&protected, read_only).expect("remove parent write bit");

            config.directory = protected.join("runs");
            let error = JournalHandle::start(config, meta("permission-denied"))
                .expect_err("a non-writable parent must be rejected");
            assert!(
                format!("{error:#}").contains("Permission denied"),
                "expected EACCES, got: {error:#}"
            );
            fs::set_permissions(&protected, original).expect("restore parent permissions");
            assert!(!protected.join("runs").exists());
        }
    }
}

#[test]
fn payload_bytes_survive_write_and_read() {
    let temp = TempDir::new().expect("tempdir");
    assert_journal_directory_failures(temp.path());
    let handle = start(temp.path(), "run");
    let text = br#"{"event_type":"book","asset_id":"0xabc","bids":[],"asks":[]}"#;
    let binary = [0u8, 159, 146, 150, 255];
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        text,
        DecodeOutcome::decoded(1),
    );
    append(
        &handle,
        SourceId::new(0, 1),
        1,
        2,
        &binary,
        DecodeOutcome::ignored("unknown_event_type"),
    );
    handle
        .shutdown(Duration::from_secs(2))
        .expect("journal shutdown");
    let records = read_records(&run_directory(temp.path(), "run")).expect("read records");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, text);
    assert_eq!(records[1].payload, binary);
    assert_eq!(records[1].frame_kind, FrameKind::Binary);
}

#[test]
fn rejected_and_unknown_frames_are_recorded() {
    let temp = TempDir::new().expect("tempdir");
    let handle = start(temp.path(), "run");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"{not json",
        DecodeOutcome::rejected("invalid_json"),
    );
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        2,
        br#"{"event_type":"future_event"}"#,
        DecodeOutcome::ignored("unknown_event_type"),
    );
    handle
        .shutdown(Duration::from_secs(2))
        .expect("journal shutdown");
    let records = read_records(&run_directory(temp.path(), "run")).expect("read records");
    assert!(matches!(records[0].outcome, DecodeOutcome::Rejected { .. }));
    assert!(matches!(records[1].outcome, DecodeOutcome::Ignored { .. }));
    assert_eq!(records[0].payload, b"{not json");
}

#[test]
fn checked_in_fixtures_round_trip_byte_for_byte() {
    let temp = TempDir::new().expect("tempdir");
    let handle = start(temp.path(), "run");
    let fixtures: [&[u8]; 3] = [
        include_bytes!("../../fixtures/polymarket/book_snapshot.json"),
        include_bytes!("../../fixtures/polymarket/price_change_multi_asset.json"),
        include_bytes!("../../fixtures/polymarket/market_resolved.json"),
    ];
    for (index, fixture) in fixtures.iter().enumerate() {
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            index as u64 + 1,
            fixture,
            DecodeOutcome::decoded(1),
        );
    }
    handle
        .shutdown(Duration::from_secs(2))
        .expect("journal shutdown");
    let records = read_records(&run_directory(temp.path(), "run")).expect("read records");
    for (record, fixture) in records.iter().zip(fixtures.iter()) {
        assert_eq!(record.payload.as_slice(), *fixture);
    }
}

#[test]
fn record_order_matches_source_epoch_and_sequence() {
    let temp = TempDir::new().expect("tempdir");
    let handle = start(temp.path(), "run");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"a",
        DecodeOutcome::decoded(1),
    );
    append(
        &handle,
        SourceId::new(0, 1),
        4,
        2,
        b"b",
        DecodeOutcome::decoded(1),
    );
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        3,
        b"c",
        DecodeOutcome::decoded(1),
    );
    append(
        &handle,
        SourceId::new(0, 0),
        3,
        4,
        b"d",
        DecodeOutcome::decoded(1),
    );
    handle
        .shutdown(Duration::from_secs(2))
        .expect("journal shutdown");
    let directory = run_directory(temp.path(), "run");
    let verification = verify_run(&directory).expect("verify");
    assert!(verification.complete, "{:?}", verification.errors);
    let records = read_records(&directory).expect("read records");
    let identity: Vec<_> = records
        .iter()
        .map(|record| {
            (
                record.source.as_str(),
                record.connection_epoch,
                record.receive_seq,
            )
        })
        .collect();
    assert_eq!(
        identity,
        vec![
            ("s0r0", 1, 1),
            ("s0r1", 4, 2),
            ("s0r0", 1, 3),
            ("s0r0", 3, 4)
        ]
    );
}

#[test]
fn record_order_is_checked_across_segments() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(temp.path());
    config.segment_max_bytes = 4096;
    let handle = JournalHandle::start(config, meta("run")).expect("start");
    for seq in 1..=12 {
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            seq,
            &[b'x'; 1200],
            DecodeOutcome::decoded(1),
        );
    }
    let summary = handle
        .shutdown(Duration::from_secs(2))
        .expect("journal shutdown");
    assert!(summary.segment_count >= 2);
    let directory = run_directory(temp.path(), "run");
    let verification = verify_run(&directory).expect("verify");
    assert!(verification.complete, "{:?}", verification.errors);
    let records = read_records(&directory).expect("read records");
    let sequences: Vec<_> = records.iter().map(|record| record.receive_seq).collect();
    assert_eq!(sequences, (1..=12).collect::<Vec<_>>());
}

#[test]
fn truncated_final_record_is_detected() {
    let temp = TempDir::new().expect("tempdir");
    let handle = start(temp.path(), "run");
    for seq in 1..=3 {
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            seq,
            &[b'x'; 256],
            DecodeOutcome::decoded(1),
        );
    }
    handle
        .shutdown(Duration::from_secs(2))
        .expect("journal shutdown");
    let directory = run_directory(temp.path(), "run");
    let path = directory.join("00000000.pmj");
    let length = fs::metadata(&path).expect("metadata").len();
    let file = OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open segment");
    file.set_len(length - 8).expect("truncate");
    drop(file);
    let verification = verify_run(&directory).expect("verify");
    assert!(!verification.complete);
    assert!(
        verification
            .errors
            .iter()
            .any(|error| error.contains("segment")),
        "{:?}",
        verification.errors
    );
}

#[test]
fn checksum_mismatch_and_corrupt_manifest_fail_verification() {
    let temp = TempDir::new().expect("tempdir");
    let handle = start(temp.path(), "run");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"payload-0001",
        DecodeOutcome::decoded(1),
    );
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        2,
        b"payload-0002",
        DecodeOutcome::decoded(1),
    );
    handle
        .shutdown(Duration::from_secs(2))
        .expect("journal shutdown");
    let directory = run_directory(temp.path(), "run");
    let segment = directory.join("00000000.pmj");
    let mut bytes = fs::read(&segment).expect("read segment");
    let offset = bytes
        .windows(b"payload-0002".len())
        .position(|window| window == b"payload-0002")
        .expect("payload offset");
    bytes[offset] = b'P';
    fs::write(&segment, &bytes).expect("write segment");
    let verification = verify_run(&directory).expect("verify");
    assert!(!verification.complete);
    assert!(
        verification
            .errors
            .iter()
            .any(|error| error.contains("checksum")),
        "{:?}",
        verification.errors
    );

    let manifest_path = directory.join(MANIFEST_FILE);
    fs::write(&manifest_path, b"{not json").expect("write manifest");
    assert!(verify_run(&directory).is_err());
}

#[test]
fn unfinalized_segment_is_reported_incomplete() {
    let temp = TempDir::new().expect("tempdir");
    let handle = start(temp.path(), "run");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"unfinished",
        DecodeOutcome::decoded(1),
    );
    handle.abort(Duration::from_secs(1));
    let directory = run_directory(temp.path(), "run");
    let verification = verify_run(&directory).expect("verify");
    assert!(!verification.complete);
    assert_eq!(verification.state, RunState::Incomplete);
    assert!(
        verification
            .errors
            .iter()
            .any(|error| error.contains("unfinalized segment")),
        "{:?}",
        verification.errors
    );
}

#[test]
fn storage_write_failure_marks_run_failed() {
    if std::env::var_os("JOURNAL_ENOSPC_TEST_ROOT").is_some() {
        assert_real_filesystem_enospc();
        return;
    }
    let temp = TempDir::new().expect("tempdir");
    let hook: FaultHook = Arc::new(|point| {
        if point == FaultPoint::Write {
            bail!("simulated write failure");
        }
        Ok(())
    });
    let handle = JournalHandle::start_inner(test_config(temp.path()), meta("run"), Some(hook))
        .expect("journal start");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"first",
        DecodeOutcome::decoded(1),
    );
    wait_failed(&handle);
    assert!(!handle.status().healthy());
    assert!(
        handle
            .record(
                SourceId::new(0, 0),
                1,
                2,
                now_us().unwrap_or(0),
                RawFrame::binary(b"second".to_vec()),
                DecodeOutcome::decoded(1),
            )
            .is_err()
    );
    let verification = verify_run(&run_directory(temp.path(), "run")).expect("verify");
    assert!(!verification.complete);
    assert_eq!(verification.state, RunState::Failed);
    validate_report_status(&handle.status(), &verification)
        .expect("failed report status must match the persisted manifest failure");
}

// Invoked by scripts/verify-journal-enospc.sh on an isolated ext4 loop
// filesystem. This exercises a real filesystem ENOSPC, not fault injection.
fn assert_real_filesystem_enospc() {
    let root = PathBuf::from(
        std::env::var_os("JOURNAL_ENOSPC_TEST_ROOT")
            .expect("JOURNAL_ENOSPC_TEST_ROOT must name the mounted filesystem"),
    );
    let filler = PathBuf::from(
        std::env::var_os("JOURNAL_ENOSPC_FILLER")
            .expect("JOURNAL_ENOSPC_FILLER must name the preallocated space file"),
    );
    let mut config = test_config(&root);
    config.queue_capacity = 1;
    config.enqueue_timeout = Duration::from_secs(60);
    config.flush_interval = Duration::from_millis(10);
    config.sync_interval = Duration::from_millis(20);

    let handle = JournalHandle::start(config, meta("enospc-run"))
        .expect("journal must start before filesystem exhaustion");
    let payload = vec![0x5a; 32 * 1024];
    for seq in 1..=16_384u64 {
        let result = handle.record(
            SourceId::new(0, 0),
            1,
            seq,
            1_700_000_000_000_000 + seq as i64,
            RawFrame::binary(payload.clone()),
            DecodeOutcome::decoded(1),
        );
        if result.is_err() || handle.status().state == RunState::Failed {
            break;
        }
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && handle.status().state != RunState::Failed {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        handle.status().state,
        RunState::Failed,
        "filesystem did not reach ENOSPC"
    );
    let directory = run_directory(&root, "enospc-run");
    let verification = verify_run(&directory).expect("verify ENOSPC run");
    assert!(!verification.complete);
    assert_eq!(verification.state, RunState::Failed);
    // The in-memory `last_error` is overwritten by the later
    // "journal writer stopped" symptom when a second record is attempted
    // after the writer exits, so the persisted manifest failure is the
    // stable record of the ENOSPC cause.
    let manifest_failure = verification
        .errors
        .iter()
        .find_map(|error| error.strip_prefix("run failure: "))
        .unwrap_or_default();
    assert!(
        manifest_failure.contains("No space left on device") || manifest_failure.contains("ENOSPC"),
        "expected a real ENOSPC manifest failure, got: {manifest_failure}"
    );

    // Free space only after the writer has latched the I/O error.
    fs::remove_file(&filler).expect("release preallocated filesystem space");
    assert!(
        handle.shutdown(Duration::from_secs(2)).is_err(),
        "a run that hit ENOSPC must not finalize"
    );
    drop(handle);

    let directory = run_directory(&root, "enospc-run");
    let partials_before = fs::read_dir(&directory)
        .expect("read run directory")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name())
        .filter(|name| name.to_string_lossy().ends_with(PARTIAL_SUFFIX))
        .collect::<Vec<_>>();
    assert!(
        !partials_before.is_empty(),
        "ENOSPC must leave an unfinalized segment"
    );
    let recovered = recover_incomplete_run(&directory).expect("recover ENOSPC run");
    assert!(!recovered.complete, "ENOSPC run cannot verify complete");
    assert!(
        matches!(recovered.state, RunState::Incomplete | RunState::Failed),
        "unexpected recovered state: {:?}",
        recovered.state
    );
    let partials_after = fs::read_dir(&directory)
        .expect("read recovered run directory")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name())
        .filter(|name| name.to_string_lossy().ends_with(PARTIAL_SUFFIX))
        .collect::<Vec<_>>();
    assert_eq!(
        partials_before, partials_after,
        "recovery must not promote partial segments"
    );
    let repeated = recover_incomplete_run(&directory).expect("repeat recovery");
    assert_eq!(repeated.state, recovered.state);
    assert_eq!(repeated.record_count, recovered.record_count);
    assert!(!repeated.complete);
}

#[test]
fn storage_sync_failure_marks_run_failed() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(temp.path());
    config.sync_mode = SyncMode::Always;
    let hook: FaultHook = Arc::new(|point| {
        if point == FaultPoint::Sync {
            bail!("simulated sync failure");
        }
        Ok(())
    });
    let handle = JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"record",
        DecodeOutcome::decoded(1),
    );
    wait_failed(&handle);
    let verification = verify_run(&run_directory(temp.path(), "run")).expect("verify");
    assert!(!verification.complete);
    assert_eq!(verification.state, RunState::Failed);
}

#[test]
fn storage_flush_failure_marks_run_failed() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(temp.path());
    config.sync_mode = SyncMode::Always;
    let hook: FaultHook = Arc::new(|point| {
        if point == FaultPoint::Flush {
            bail!("simulated flush failure");
        }
        Ok(())
    });
    let handle = JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"record",
        DecodeOutcome::decoded(1),
    );
    wait_failed(&handle);
    let status = handle.status();
    assert!(!status.healthy());
    assert!(
        status
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("simulated flush failure")),
        "{:?}",
        status.last_error
    );
    let verification = verify_run(&run_directory(temp.path(), "run")).expect("verify");
    assert!(!verification.complete);
    assert_eq!(verification.state, RunState::Failed);
}

#[test]
fn queue_saturation_then_shutdown_stays_failed() {
    let temp = TempDir::new().expect("tempdir");
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let (entered_tx, entered_rx) = mpsc::channel();
    let blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook: FaultHook = Arc::new(move |point| {
        if point == FaultPoint::Write && !blocked.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let _ = entered_tx.send(());
            if let Ok(receiver) = release_rx.lock() {
                let _ = receiver.recv_timeout(Duration::from_secs(10));
            }
        }
        Ok(())
    });
    let mut config = test_config(temp.path());
    config.queue_capacity = 1;
    config.enqueue_timeout = Duration::from_millis(30);
    let handle = JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"first",
        DecodeOutcome::decoded(1),
    );
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("writer entered hook");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        2,
        b"second",
        DecodeOutcome::decoded(1),
    );
    let error = handle
        .record(
            SourceId::new(0, 0),
            1,
            3,
            now_us().unwrap_or(0),
            RawFrame::binary(b"third".to_vec()),
            DecodeOutcome::decoded(1),
        )
        .expect_err("third record must fail");
    assert!(format!("{error:#}").contains("queue stayed full"));
    assert_eq!(handle.status().state, RunState::Failed);

    // The production coordinator finalizes through `shutdown`, not
    // `abort`. The latched failure must survive that path: the run may
    // never report a complete journal after a rejected frame.
    let _ = release_tx.send(());
    let shutdown = handle.shutdown(Duration::from_secs(2));
    assert!(shutdown.is_err(), "shutdown must fail after saturation");
    assert_eq!(handle.status().state, RunState::Failed);
    assert!(handle.status().last_error.is_some());
    let directory = run_directory(temp.path(), "run");
    let verification = verify_run(&directory).expect("verify");
    assert!(!verification.complete, "{:?}", verification.errors);
    assert_eq!(verification.state, RunState::Failed);
    assert!(
        verification
            .errors
            .iter()
            .any(|error| error.contains("Failed")),
        "verifier must surface the failed run state: {:?}",
        verification.errors
    );
    // The third payload was never accepted, so it must not appear anywhere.
    let records = read_records(&directory).unwrap_or_default();
    assert!(
        records.iter().all(|record| record.payload != b"third"),
        "a rejected payload must not be written"
    );
}

#[test]
fn shutdown_waits_for_a_producer_failure_latch() {
    let temp = TempDir::new().expect("tempdir");
    let (release_writer_tx, release_writer_rx) = mpsc::channel();
    let release_writer_rx = Arc::new(std::sync::Mutex::new(release_writer_rx));
    let (writer_entered_tx, writer_entered_rx) = mpsc::channel();
    let writer_blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook: FaultHook = Arc::new(move |point| {
        if point == FaultPoint::Write
            && !writer_blocked.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let _ = writer_entered_tx.send(());
            if let Ok(receiver) = release_writer_rx.lock() {
                let _ = receiver.recv_timeout(Duration::from_secs(10));
            }
        }
        Ok(())
    });
    let mut config = test_config(temp.path());
    config.queue_capacity = 1;
    config.enqueue_timeout = Duration::from_millis(150);
    let handle =
        Arc::new(JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start"));
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"first",
        DecodeOutcome::decoded(1),
    );
    writer_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("writer entered hook");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        2,
        b"second",
        DecodeOutcome::decoded(1),
    );

    // Hold the producer after the queue decision but before the failure
    // latch. Its admission guard must keep shutdown from observing an
    // idle producer set.
    let (producer_entered_tx, producer_entered_rx) = mpsc::channel();
    let (release_producer_tx, release_producer_rx) = mpsc::channel();
    let release_producer_rx = Arc::new(std::sync::Mutex::new(release_producer_rx));
    let producer_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    handle.set_record_hook(Arc::new(move || {
        if producer_gate.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let _ = producer_entered_tx.send(());
        if let Ok(receiver) = release_producer_rx.lock() {
            let _ = receiver.recv_timeout(Duration::from_secs(10));
        }
    }));

    let producer = {
        let handle = Arc::clone(&handle);
        thread::spawn(move || {
            handle.record(
                SourceId::new(0, 0),
                1,
                3,
                now_us().unwrap_or(0),
                RawFrame::binary(b"third".to_vec()),
                DecodeOutcome::decoded(1),
            )
        })
    };
    producer_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("producer reached the post-enqueue gate");

    let shutdown = {
        let handle = Arc::clone(&handle);
        thread::spawn(move || handle.shutdown(Duration::from_secs(2)))
    };
    thread::sleep(Duration::from_millis(100));
    assert!(
        !shutdown.is_finished(),
        "shutdown must wait for the active producer"
    );

    // Give shutdown queue space while the producer is still gated. If it
    // were allowed to proceed, it would submit Finalize before the queue
    // failure is latched.
    let _ = release_writer_tx.send(());
    thread::sleep(Duration::from_millis(150));
    assert!(
        !shutdown.is_finished(),
        "shutdown must not finalize before the producer latches its queue failure"
    );

    let _ = release_producer_tx.send(());
    assert!(
        producer.join().expect("producer joined").is_err(),
        "the timed-out enqueue must be reported"
    );
    assert!(
        shutdown.join().expect("shutdown joined").is_err(),
        "a latched queue failure must fail shutdown"
    );
    assert_eq!(handle.status().state, RunState::Failed);
    let directory = run_directory(temp.path(), "run");
    wait_for_verification_state(&directory, RunState::Failed);
    let verification = verify_run(&directory).expect("verify");
    assert!(!verification.complete, "{:?}", verification.errors);
    assert!(
        read_records(&directory)
            .unwrap_or_default()
            .iter()
            .all(|record| record.payload != b"third"),
        "the rejected payload must not be written"
    );
}

#[test]
fn abort_waits_for_an_active_producer_before_stopping_the_writer() {
    let temp = TempDir::new().expect("tempdir");
    let (release_writer_tx, release_writer_rx) = mpsc::channel();
    let release_writer_rx = Arc::new(std::sync::Mutex::new(release_writer_rx));
    let (writer_entered_tx, writer_entered_rx) = mpsc::channel();
    let writer_blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook: FaultHook = Arc::new(move |point| {
        if point == FaultPoint::Write
            && !writer_blocked.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let _ = writer_entered_tx.send(());
            if let Ok(receiver) = release_writer_rx.lock() {
                let _ = receiver.recv_timeout(Duration::from_secs(10));
            }
        }
        Ok(())
    });
    let mut config = test_config(temp.path());
    config.queue_capacity = 2;
    let handle =
        Arc::new(JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start"));
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"first",
        DecodeOutcome::decoded(1),
    );
    writer_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("writer entered hook");

    // This producer has already been admitted and enqueued successfully,
    // but is held inside `record` before the admission guard is released.
    let (producer_entered_tx, producer_entered_rx) = mpsc::channel();
    let (release_producer_tx, release_producer_rx) = mpsc::channel();
    let release_producer_rx = Arc::new(std::sync::Mutex::new(release_producer_rx));
    let producer_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    handle.set_record_hook(Arc::new(move || {
        if producer_gate.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let _ = producer_entered_tx.send(());
        if let Ok(receiver) = release_producer_rx.lock() {
            let _ = receiver.recv_timeout(Duration::from_secs(10));
        }
    }));
    let producer = {
        let handle = Arc::clone(&handle);
        thread::spawn(move || {
            handle.record(
                SourceId::new(0, 0),
                1,
                2,
                now_us().unwrap_or(0),
                RawFrame::binary(b"second".to_vec()),
                DecodeOutcome::decoded(1),
            )
        })
    };
    producer_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("producer reached the post-enqueue gate");

    let abort = {
        let handle = Arc::clone(&handle);
        thread::spawn(move || handle.abort(Duration::from_secs(2)))
    };
    thread::sleep(Duration::from_millis(100));
    assert!(
        !abort.is_finished(),
        "abort must wait for the active producer"
    );

    // Free the writer. An abort that only stopped the writer would now
    // complete while the producer is still inside `record`.
    let _ = release_writer_tx.send(());
    thread::sleep(Duration::from_millis(200));
    assert!(
        !abort.is_finished(),
        "abort must not stop the writer while a producer is still active"
    );

    let _ = release_producer_tx.send(());
    producer
        .join()
        .expect("producer joined")
        .expect("the record was accepted before abort closed admission");
    abort.join().expect("abort joined");
    assert_eq!(handle.status().state, RunState::Incomplete);
    let directory = run_directory(temp.path(), "run");
    wait_for_verification_state(&directory, RunState::Incomplete);
    assert!(!verify_run(&directory).expect("verify").complete);
}

#[test]
fn concurrent_producers_stop_after_failure() {
    let temp = TempDir::new().expect("tempdir");
    let hook: FaultHook = Arc::new(|point| {
        if point == FaultPoint::Write {
            bail!("simulated write failure");
        }
        Ok(())
    });
    let handle = JournalHandle::start_inner(test_config(temp.path()), meta("run"), Some(hook))
        .expect("start");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"trigger",
        DecodeOutcome::decoded(1),
    );
    wait_failed(&handle);

    let handle = Arc::new(handle);
    let workers: Vec<_> = (0..4)
        .map(|replica| {
            let handle = Arc::clone(&handle);
            thread::spawn(move || {
                let source = SourceId::new(0, replica);
                for seq in 1..=25u64 {
                    match handle.record(
                        source,
                        1,
                        seq,
                        now_us().unwrap_or(0),
                        RawFrame::binary(b"late".to_vec()),
                        DecodeOutcome::decoded(1),
                    ) {
                        Ok(()) => {}
                        Err(error) => return Err(format!("{error:#}")),
                    }
                }
                Ok(())
            })
        })
        .collect();
    for worker in workers {
        let outcome = worker.join().expect("worker joined");
        assert!(outcome.is_err(), "a latched journal must reject records");
    }
    assert_eq!(handle.status().state, RunState::Failed);
    let verification = verify_run(&run_directory(temp.path(), "run")).expect("verify");
    assert!(!verification.complete);
    assert_eq!(verification.state, RunState::Failed);
}

#[test]
fn blocked_writer_bounds_shutdown() {
    let temp = TempDir::new().expect("tempdir");
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let (entered_tx, entered_rx) = mpsc::channel();
    let blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook: FaultHook = Arc::new(move |point| {
        if point == FaultPoint::Write && !blocked.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let _ = entered_tx.send(());
            if let Ok(receiver) = release_rx.lock() {
                let _ = receiver.recv_timeout(Duration::from_secs(10));
            }
        }
        Ok(())
    });
    let mut config = test_config(temp.path());
    // One slot is held by the queued record, so the Finalize command can
    // only be delivered if the writer drains — which it cannot.
    config.queue_capacity = 1;
    let handle = JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"first",
        DecodeOutcome::decoded(1),
    );
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("writer entered hook");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        2,
        b"second",
        DecodeOutcome::decoded(1),
    );

    // The writer is stuck in a write and the queue is full: shutdown must
    // still return close to its deadline instead of blocking on the
    // bounded channel forever.
    let started = Instant::now();
    let shutdown = handle.shutdown(Duration::from_millis(300));
    let elapsed = started.elapsed();
    assert!(shutdown.is_err(), "a blocked writer cannot be finalized");
    assert!(
        elapsed >= Duration::from_millis(250) && elapsed <= Duration::from_secs(2),
        "shutdown elapsed {elapsed:?}, expected ~300ms"
    );
    assert_eq!(handle.status().state, RunState::Failed);

    let _ = release_tx.send(());
    let directory = run_directory(temp.path(), "run");
    wait_for_verification_state(&directory, RunState::Failed);
    assert!(!verify_run(&directory).expect("verify").complete);
}

#[test]
fn blocked_writer_bounds_abort() {
    let temp = TempDir::new().expect("tempdir");
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let (entered_tx, entered_rx) = mpsc::channel();
    let blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook: FaultHook = Arc::new(move |point| {
        if point == FaultPoint::Write && !blocked.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let _ = entered_tx.send(());
            if let Ok(receiver) = release_rx.lock() {
                let _ = receiver.recv_timeout(Duration::from_secs(10));
            }
        }
        Ok(())
    });
    let mut config = test_config(temp.path());
    // One slot is held by the queued record, so the Abort command can only
    // be delivered if the writer drains — which it cannot.
    config.queue_capacity = 1;
    let handle = JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"first",
        DecodeOutcome::decoded(1),
    );
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("writer entered hook");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        2,
        b"second",
        DecodeOutcome::decoded(1),
    );

    // `abort` must not delay shutdown either: the undeliverable command
    // marks the run failed and the call returns within the deadline.
    let started = Instant::now();
    handle.abort(Duration::from_millis(300));
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(250) && elapsed <= Duration::from_secs(2),
        "abort elapsed {elapsed:?}, expected ~300ms"
    );
    assert_eq!(handle.status().state, RunState::Failed);
    assert!(
        handle
            .status()
            .last_error
            .is_some_and(|error| error.contains("abort")),
        "abort must record why the command was not delivered"
    );

    let _ = release_tx.send(());
    let directory = run_directory(temp.path(), "run");
    wait_for_verification_state(&directory, RunState::Failed);
    assert!(!verify_run(&directory).expect("verify").complete);
}

#[test]
fn finalize_reply_timeout_stays_incomplete() {
    let temp = TempDir::new().expect("tempdir");
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let (entered_tx, _entered_rx) = mpsc::channel();
    let hook: FaultHook = Arc::new(move |point| {
        if point == FaultPoint::Finalize {
            let _ = entered_tx.send(());
            if let Ok(receiver) = release_rx.lock() {
                // A dropped release channel means the stall never ends and
                // finalization must not complete.
                if receiver.recv_timeout(Duration::from_secs(10)).is_err() {
                    bail!("simulated finalize stall aborted");
                }
            }
        }
        Ok(())
    });
    let handle = JournalHandle::start_inner(test_config(temp.path()), meta("run"), Some(hook))
        .expect("start");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"record",
        DecodeOutcome::decoded(1),
    );

    // The Finalize command was accepted but the writer never replies
    // within the deadline: shutdown returns an error and the run is not
    // reported complete.
    let started = Instant::now();
    let shutdown = handle.shutdown(Duration::from_millis(300));
    let elapsed = started.elapsed();
    assert!(shutdown.is_err(), "a stalled finalization cannot complete");
    assert!(
        elapsed >= Duration::from_millis(250) && elapsed <= Duration::from_secs(2),
        "shutdown elapsed {elapsed:?}, expected ~300ms"
    );
    let directory = run_directory(temp.path(), "run");
    assert!(!verify_run(&directory).expect("verify").complete);

    // Ending the stall lets the writer resolve the run to a terminal
    // failed state instead of leaving a finalization claim behind.
    drop(release_tx);
    wait_for_verification_state(&directory, RunState::Failed);
}

#[test]
fn repeated_concurrent_and_dead_writer_shutdowns_are_bounded() {
    let temp = TempDir::new().expect("tempdir");
    let handle = start(temp.path(), "run");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"record",
        DecodeOutcome::decoded(1),
    );
    let first = handle.shutdown(Duration::from_secs(2)).expect("shutdown");
    assert_eq!(first.record_count, 1);

    // A second shutdown is refused immediately instead of re-entering the
    // writer lifecycle.
    let started = Instant::now();
    let repeated = handle.shutdown(Duration::from_secs(2));
    assert!(repeated.is_err());
    assert!(started.elapsed() <= Duration::from_secs(1));

    // Concurrent shutdowns: exactly one proceeds, the rest are refused
    // without blocking.
    let handle = Arc::new(handle);
    let racers: Vec<_> = (0..2)
        .map(|_| {
            let handle = Arc::clone(&handle);
            thread::spawn(move || handle.shutdown(Duration::from_secs(2)))
        })
        .collect();
    let mut accepted = 0;
    for racer in racers {
        match racer.join().expect("racer joined") {
            Ok(_) => accepted += 1,
            Err(error) => assert!(format!("{error:#}").contains("already requested")),
        }
    }
    assert_eq!(accepted, 0, "a joined writer cannot finalize again");

    // A writer that died in a storage failure fails shutdown promptly.
    let failed = TempDir::new().expect("tempdir");
    let hook: FaultHook = Arc::new(|point| {
        if point == FaultPoint::Write {
            bail!("simulated write failure");
        }
        Ok(())
    });
    let handle = JournalHandle::start_inner(test_config(failed.path()), meta("run"), Some(hook))
        .expect("start");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"record",
        DecodeOutcome::decoded(1),
    );
    wait_failed(&handle);
    let started = Instant::now();
    assert!(handle.shutdown(Duration::from_secs(2)).is_err());
    assert!(
        started.elapsed() <= Duration::from_secs(1),
        "shutdown against a dead writer must not wait for the full timeout"
    );
}

#[test]
fn successful_shutdown_reconciles_counters() {
    let temp = TempDir::new().expect("tempdir");
    let handle = start(temp.path(), "run");
    for seq in 1..=5 {
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            seq,
            &[b'r'; 64],
            DecodeOutcome::decoded(1),
        );
    }
    let summary = handle
        .shutdown(Duration::from_secs(2))
        .expect("journal shutdown");
    let status = handle.status();
    assert_eq!(status.state, RunState::Finalized);
    assert!(status.last_error.is_none());
    assert_eq!(status.counters.queued, 5);
    assert_eq!(status.counters.written, 5);
    assert_eq!(status.counters.flushed, 5);
    assert_eq!(status.counters.synced, 5);
    assert_eq!(status.counters.finalized, 5);
    assert_eq!(status.counters.queue_depth, 0);
    assert_eq!(summary.record_count, 5);
    assert_eq!(summary.record_count, status.counters.finalized);
    let directory = run_directory(temp.path(), "run");
    let verification = verify_run(&directory).expect("verify");
    assert!(verification.complete, "{:?}", verification.errors);
    assert_eq!(verification.record_count, status.counters.finalized);
    assert_eq!(verification.segments.len() as u64, status.counters.segments);
    validate_report_status(&status, &verification)
        .expect("run report status must agree with the verified manifest");

    let mut mismatched = status.clone();
    mismatched.counters.finalized = mismatched.counters.finalized.saturating_add(1);
    assert!(
        validate_report_status(&mismatched, &verification).is_err(),
        "a report with a mismatched finalized count must be rejected"
    );
    mismatched = status.clone();
    mismatched.state = RunState::Incomplete;
    assert!(
        validate_report_status(&mismatched, &verification).is_err(),
        "a report with a mismatched terminal state must be rejected"
    );
}

#[test]
fn graceful_shutdown_finalizes_rotated_segments() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(temp.path());
    config.segment_max_bytes = 4096;
    let handle = JournalHandle::start(config, meta("run")).expect("start");
    for seq in 1..=12 {
        append(
            &handle,
            SourceId::new(seq as u16 % 2, 0),
            1,
            seq,
            &[b'z'; 1200],
            DecodeOutcome::decoded(1),
        );
    }
    let summary = handle
        .shutdown(Duration::from_secs(2))
        .expect("journal shutdown");
    assert_eq!(summary.record_count, 12);
    assert!(summary.segment_count >= 3);
    let directory = run_directory(temp.path(), "run");
    let verification = verify_run(&directory).expect("verify");
    assert!(verification.complete, "{:?}", verification.errors);
    assert_eq!(verification.state, RunState::Finalized);
    assert_eq!(verification.record_count, 12);
    assert_eq!(verification.segments.len() as u64, summary.segment_count);
}

#[test]
fn identical_input_produces_identical_records() {
    let first = TempDir::new().expect("tempdir");
    let second = TempDir::new().expect("tempdir");
    for root in [&first, &second] {
        let handle = start(root.path(), "run");
        for (index, payload) in [b"alpha".as_slice(), b"beta".as_slice(), b"gamma"]
            .iter()
            .enumerate()
        {
            append(
                &handle,
                SourceId::new(0, 0),
                2,
                index as u64 + 1,
                payload,
                DecodeOutcome::decoded(1),
            );
        }
        handle
            .shutdown(Duration::from_secs(2))
            .expect("journal shutdown");
    }
    let left = read_records(&run_directory(first.path(), "run")).expect("left");
    let right = read_records(&run_directory(second.path(), "run")).expect("right");
    let identity = |records: Vec<JournalRecord>| {
        records
            .into_iter()
            .map(|record| {
                (
                    record.source,
                    record.connection_epoch,
                    record.receive_seq,
                    record.received_at_us,
                    record.payload,
                    record.outcome,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(identity(left), identity(right));
}

#[test]
fn initial_manifest_failure_leaves_no_recoverable_run() {
    let temp = TempDir::new().expect("tempdir");
    let hook: FaultHook = Arc::new(|point| {
        if point == FaultPoint::ManifestWrite {
            bail!("injected initial manifest failure");
        }
        Ok(())
    });
    let result = JournalHandle::start_inner(test_config(temp.path()), meta("run"), Some(hook));
    assert!(result.is_err());
    assert!(
        !run_directory(temp.path(), "run")
            .join(MANIFEST_FILE)
            .exists()
    );
    assert!(
        fs::read_dir(run_directory(temp.path(), "run"))
            .expect("run directory")
            .next()
            .is_none()
    );
}

#[test]
fn rotation_rename_failure_is_failed_and_never_reported_complete() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(temp.path());
    config.segment_max_bytes = 4096;
    let hook: FaultHook = Arc::new(|point| {
        if point == FaultPoint::SegmentRename {
            bail!("injected segment rename failure");
        }
        Ok(())
    });
    let handle =
        JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start journal");
    for seq in 1..=8 {
        if handle.status().state == RunState::Failed {
            break;
        }
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            seq,
            &[b'r'; 1200],
            DecodeOutcome::decoded(1),
        );
    }
    wait_failed(&handle);
    assert!(handle.shutdown(Duration::from_secs(2)).is_err());
    let directory = run_directory(temp.path(), "run");
    let verification = verify_run(&directory).expect("verify failed run");
    assert!(!verification.complete);
    assert_eq!(verification.state, RunState::Failed);
    assert!(
        verification
            .errors
            .iter()
            .any(|error| error.contains("unfinalized segment"))
    );
}

#[test]
fn directory_sync_failure_leaves_an_uncommitted_segment() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(temp.path());
    config.segment_max_bytes = 4096;
    let hook: FaultHook = Arc::new(|point| {
        if point == FaultPoint::DirectorySync {
            bail!("injected directory sync failure");
        }
        Ok(())
    });
    let handle =
        JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start journal");
    for seq in 1..=8 {
        if handle.status().state == RunState::Failed {
            break;
        }
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            seq,
            &[b'd'; 1200],
            DecodeOutcome::decoded(1),
        );
    }
    wait_failed(&handle);
    let directory = run_directory(temp.path(), "run");
    let verification = verify_run(&directory).expect("verify failed run");
    assert!(!verification.complete);
    assert_eq!(verification.state, RunState::Failed);
    assert!(
        verification
            .errors
            .iter()
            .any(|error| error.contains("absent from the manifest"))
    );
}

#[test]
fn manifest_update_failure_does_not_commit_a_renamed_segment() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(temp.path());
    config.segment_max_bytes = 4096;
    let manifest_writes = Arc::new(AtomicUsize::new(0));
    let writes = Arc::clone(&manifest_writes);
    let hook: FaultHook = Arc::new(move |point| {
        if point == FaultPoint::ManifestWrite && writes.fetch_add(1, Ordering::SeqCst) == 1 {
            bail!("injected rotated manifest failure");
        }
        Ok(())
    });
    let handle =
        JournalHandle::start_inner(config, meta("run"), Some(hook)).expect("start journal");
    for seq in 1..=8 {
        if handle.status().state == RunState::Failed {
            break;
        }
        append(
            &handle,
            SourceId::new(0, 0),
            1,
            seq,
            &[b'm'; 1200],
            DecodeOutcome::decoded(1),
        );
    }
    wait_failed(&handle);
    let directory = run_directory(temp.path(), "run");
    let verification = verify_run(&directory).expect("verify failed run");
    assert!(!verification.complete);
    assert_eq!(verification.state, RunState::Failed);
    assert!(
        verification
            .errors
            .iter()
            .any(|error| error.contains("absent from the manifest"))
    );
}

#[test]
fn final_manifest_write_failure_is_failed_and_not_complete() {
    let temp = TempDir::new().expect("tempdir");
    let manifest_writes = Arc::new(AtomicUsize::new(0));
    let writes = Arc::clone(&manifest_writes);
    let hook: FaultHook = Arc::new(move |point| {
        if point == FaultPoint::ManifestWrite && writes.fetch_add(1, Ordering::SeqCst) == 1 {
            bail!("injected final manifest failure");
        }
        Ok(())
    });
    let handle = JournalHandle::start_inner(test_config(temp.path()), meta("run"), Some(hook))
        .expect("start journal");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"committed",
        DecodeOutcome::decoded(1),
    );
    let error = handle
        .shutdown(Duration::from_secs(2))
        .expect_err("final manifest failure must fail shutdown");
    assert!(
        format!("{error:#}").contains("injected final manifest failure"),
        "unexpected error: {error:#}"
    );
    let verification = verify_run(&run_directory(temp.path(), "run")).expect("verify");
    assert!(!verification.complete);
    assert_eq!(verification.state, RunState::Failed);
}

#[test]
fn crash_recovery_is_conservative_and_idempotent() {
    for (point, expected_manifest_segments) in [
        ("after_record_write", 0),
        ("after_segment_rename", 0),
        ("after_manifest_update", 1),
    ] {
        let temp = TempDir::new().expect("tempdir");
        spawn_crash_child(temp.path(), point);
        let directory = run_directory(temp.path(), "crash-run");
        let before_recovery = verify_run(&directory).expect("verify crashed run");
        assert_eq!(before_recovery.state, RunState::Open);
        assert!(!before_recovery.complete);
        assert_eq!(
            before_recovery.segment_count, expected_manifest_segments,
            "unexpected manifest segment count at {point}: {:?}",
            before_recovery.errors
        );

        let recovered = recover_incomplete_run(&directory).expect("recover crashed run");
        assert_eq!(recovered.state, RunState::Incomplete);
        assert!(!recovered.complete);
        assert!(
            recovered
                .errors
                .iter()
                .any(|error| error.contains("Incomplete"))
        );

        let manifest_after_first_recovery =
            fs::read(directory.join(MANIFEST_FILE)).expect("read recovered manifest");
        let recovered_again = recover_incomplete_run(&directory).expect("repeat recovery");
        let manifest_after_second_recovery =
            fs::read(directory.join(MANIFEST_FILE)).expect("read manifest again");
        assert_eq!(
            manifest_after_first_recovery, manifest_after_second_recovery,
            "recovery rewrote a terminal manifest at {point}"
        );
        assert_eq!(recovered.errors, recovered_again.errors);
        assert!(!recovered_again.complete);
    }
}

#[test]
fn crash_after_final_manifest_update_keeps_committed_run_finalized() {
    let temp = TempDir::new().expect("tempdir");
    spawn_crash_child(temp.path(), "after_final_manifest_update");
    let directory = run_directory(temp.path(), "crash-run");
    let before_recovery = verify_run(&directory).expect("verify finalized run");
    assert!(before_recovery.complete, "{:?}", before_recovery.errors);
    assert_eq!(before_recovery.state, RunState::Finalized);
    assert_eq!(before_recovery.record_count, 1);

    let manifest_before = fs::read(directory.join(MANIFEST_FILE)).expect("read manifest");
    let recovered = recover_incomplete_run(&directory).expect("recover finalized run");
    let manifest_after = fs::read(directory.join(MANIFEST_FILE)).expect("read manifest");
    assert!(recovered.complete, "{:?}", recovered.errors);
    assert_eq!(recovered.state, RunState::Finalized);
    assert_eq!(manifest_before, manifest_after);
}

#[test]
fn post_commit_failure_does_not_downgrade_finalized_run() {
    let temp = TempDir::new().expect("tempdir");
    let hook: FaultHook = Arc::new(|point| {
        if point == FaultPoint::AfterFinalManifestUpdate {
            bail!("injected post-commit fault");
        }
        Ok(())
    });
    let handle = JournalHandle::start_inner(test_config(temp.path()), meta("run"), Some(hook))
        .expect("start journal");
    append(
        &handle,
        SourceId::new(0, 0),
        1,
        1,
        b"committed",
        DecodeOutcome::decoded(1),
    );
    let summary = handle
        .shutdown(Duration::from_secs(2))
        .expect("post-commit hook cannot undo finalization");
    assert_eq!(summary.state, RunState::Finalized);
    let verification = verify_run(&run_directory(temp.path(), "run")).expect("verify");
    assert!(verification.complete, "{:?}", verification.errors);
    assert_eq!(verification.state, RunState::Finalized);
}
