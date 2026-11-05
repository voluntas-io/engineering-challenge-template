//! The binary must fail visibly rather than run without producing real data.

use std::process::{Command, Stdio};
#[cfg(target_os = "linux")]
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::Instant;

fn collector() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_engineering-challenge"));
    command
        .current_dir(std::env::temp_dir())
        .env_remove("KALSHI_API_KEY_ID")
        .env_remove("KALSHI_PRIVATE_KEY")
        .env_remove("KALSHI_PRIVATE_KEY_PATH")
        .env_remove("MOCK")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command
}

#[test]
fn missing_credentials_exit_instead_of_writing_synthetic_data() {
    let output_file = tempfile::NamedTempFile::new().unwrap();
    let output = collector()
        .env("OUTPUT_PATH", output_file.path())
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("credentials are required"),
        "stderr: {stderr}"
    );
    assert_eq!(std::fs::metadata(output_file.path()).unwrap().len(), 0);
}

#[cfg(target_os = "linux")]
#[test]
fn fatal_output_error_exits_with_failure() {
    let mut child = collector()
        .arg("--mock")
        .env("OUTPUT_PATH", "/dev/full")
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            child.kill().unwrap();
            panic!("collector kept running after its output failed");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(!status.success());
}
