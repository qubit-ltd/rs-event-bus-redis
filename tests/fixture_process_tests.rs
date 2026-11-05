// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for the fixture process watchdog.

mod support;

use std::error::Error;
#[cfg(target_os = "linux")]
use std::fs::File;
use std::process::Command;
use std::process::Stdio;
use std::thread;
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::Instant;

#[cfg(target_os = "linux")]
use nix::sys::signal::Signal;
#[cfg(target_os = "linux")]
use nix::sys::signal::kill;
#[cfg(target_os = "linux")]
use nix::sys::signal::killpg;
#[cfg(target_os = "linux")]
use nix::unistd::Pid;
use support::fixture_process::run_logged;

const CHILD_MODE: &str = "QUBIT_FIXTURE_PROCESS_CHILD";

#[test]
// The grandchild modes intentionally let descendants outlive their leader so
// the watchdog's process-group cleanup can be observed by the parent test.
#[allow(clippy::zombie_processes)]
fn test_child_process_entrypoint() {
    match std::env::var(CHILD_MODE).as_deref() {
        Ok("success") => {
            println!("child stdout marker");
            eprintln!("child stderr marker");
        }
        Ok("failure") => {
            eprintln!("child failure marker");
            std::process::exit(17);
        }
        Ok("hang") => {
            println!("child pid={}", std::process::id());
            println!("child ready");
            loop {
                thread::sleep(Duration::from_secs(60));
            }
        }
        Ok("mark") => {
            std::fs::write(std::env::var_os("QUBIT_FIXTURE_MARKER").unwrap(), b"spawned").unwrap();
        }
        Ok("grandchild") => {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "test_child_process_entrypoint", "--nocapture"])
                .env(CHILD_MODE, "hang")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let child = command.spawn().unwrap();
            println!("grandchild pid={}", child.id());
            loop {
                thread::sleep(Duration::from_secs(60));
            }
        }
        Ok("grandchild-success" | "grandchild-failure") => {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "test_child_process_entrypoint", "--nocapture"])
                .env(CHILD_MODE, "hang")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let child = command.spawn().unwrap();
            println!("grandchild pid={}", child.id());
            drop(child);
            if std::env::var(CHILD_MODE).as_deref() == Ok("grandchild-failure") {
                std::process::exit(17);
            }
        }
        _ => (),
    }
}

fn child_command(mode: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "test_child_process_entrypoint", "--nocapture"])
        .env(CHILD_MODE, mode);
    command
}

#[test]
#[cfg(target_os = "linux")]
fn test_returns_success_stdout_and_logs_both_streams() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let output = run_logged(
        &mut child_command("success"),
        &temp.path().join("success.log"),
        Duration::from_secs(5),
    )?;
    assert!(output.contains("child stdout marker"));
    let log = std::fs::read_to_string(temp.path().join("success.log"))?;
    assert!(log.contains("child stderr marker"));
    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_nonzero_exit_returns_error_with_stderr_in_log() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let log = temp.path().join("failure.log");
    let error = run_logged(&mut child_command("failure"), &log, Duration::from_secs(5)).unwrap_err();
    assert!(error.to_string().contains("17"));
    assert!(std::fs::read_to_string(log)?.contains("child failure marker"));
    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_hanging_process_times_out_and_is_reaped() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let log = temp.path().join("hang.log");
    let started = Instant::now();
    let error = run_logged(&mut child_command("hang"), &log, Duration::from_secs(5)).unwrap_err();
    assert!(error.to_string().contains("timed out"));
    assert!(started.elapsed() < Duration::from_secs(15));
    assert!(std::fs::read_to_string(temp.path().join("hang.log.stdout.log"))?.contains("child ready"));
    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_maximum_timeout_returns_error_without_spawning() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let marker = temp.path().join("spawned");
    let mut command = child_command("mark");
    command.env("QUBIT_FIXTURE_MARKER", &marker);
    let error = run_logged(&mut command, &temp.path().join("overflow.log"), Duration::MAX).unwrap_err();
    assert!(error.to_string().contains("deadline"));
    assert!(!marker.exists(), "overflowing timeout spawned the command");
    Ok(())
}

#[cfg(not(target_os = "linux"))]
#[test]
fn test_unsupported_platform_returns_error_without_spawning() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let marker = temp.path().join("spawned");
    let mut command = child_command("mark");
    command.env("QUBIT_FIXTURE_MARKER", &marker);
    let error = run_logged(
        &mut command,
        &temp.path().join("unsupported.log"),
        Duration::from_secs(1),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires Linux process-group and waitid support")
    );
    assert!(!marker.exists(), "unsupported platform spawned the command");
    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_poll_error_reaps_running_child_and_logs_its_path() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let log = temp.path().join("poll-error.log");
    let stdout_log = temp.path().join("poll-error.log.stdout.log");
    let started = Instant::now();
    let error = support::fixture_process::run_logged_with_poll(
        &mut child_command("hang"),
        &log,
        Duration::from_secs(5),
        |child| {
            let until = Instant::now() + Duration::from_secs(10);
            while Instant::now() < until {
                if std::fs::read_to_string(&stdout_log).is_ok_and(|text| text.contains("child ready")) {
                    assert!(child.try_wait()?.is_none());
                    return Err(std::io::Error::other("injected poll failure"));
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(std::io::Error::other("child did not become ready"))
        },
    )
    .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(20));
    assert!(error.to_string().contains("injected poll failure"));
    assert!(error.to_string().contains(log.to_str().unwrap()));
    let output = std::fs::read_to_string(&stdout_log)?;
    assert!(output.contains("child ready"));
    let child_pid: i32 = output
        .lines()
        .find_map(|line| line.strip_prefix("child pid="))
        .ok_or("child pid was not recorded")?
        .parse()?;
    assert!(!process_is_live(child_pid), "poll-error child {child_pid} survived");
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn test_timeout_reclaims_child_process_group() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let log = temp.path().join("tree.log");
    let started = Instant::now();
    let error = run_logged(&mut child_command("grandchild"), &log, Duration::from_secs(5)).unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(15));
    assert!(error.to_string().contains("timed out"));
    // The watchdog owns the command's process group. The live grandchild must
    // disappear when the deadline kills that group.
    let text = std::fs::read_to_string(temp.path().join("tree.log.stdout.log"))?;
    let pid = text.lines().find_map(|line| line.strip_prefix("grandchild pid="));
    let pid: i32 = pid.ok_or("grandchild pid was not recorded")?.parse()?;
    for _ in 0..250 {
        if !process_is_live(pid) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(!process_is_live(pid), "grandchild {pid} survived watchdog cleanup");
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn test_successful_leader_reclaims_its_process_group() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let log = temp.path().join("leader-success.log");
    let output = run_logged(&mut child_command("grandchild-success"), &log, Duration::from_secs(5))?;
    let pid: i32 = output
        .lines()
        .find_map(|line| line.strip_prefix("grandchild pid="))
        .ok_or("grandchild pid was not recorded")?
        .parse()?;
    assert_process_stops(pid);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn test_failing_leader_reclaims_its_process_group() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let log = temp.path().join("leader-failure.log");
    let error = run_logged(&mut child_command("grandchild-failure"), &log, Duration::from_secs(5)).unwrap_err();
    assert!(error.to_string().contains("17"));
    let output = std::fs::read_to_string(temp.path().join("leader-failure.log.stdout.log"))?;
    let pid: i32 = output
        .lines()
        .find_map(|line| line.strip_prefix("grandchild pid="))
        .ok_or("grandchild pid was not recorded")?
        .parse()?;
    assert_process_stops(pid);
    Ok(())
}

#[cfg(target_os = "linux")]
fn assert_process_stops(pid: i32) {
    for _ in 0..250 {
        if !process_is_live(pid) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(!process_is_live(pid), "grandchild {pid} survived group cleanup");
}

#[cfg(target_os = "linux")]
#[test]
fn test_timeout_kills_only_its_own_process_group() -> Result<(), Box<dyn Error>> {
    use std::os::unix::process::CommandExt;

    let mut unrelated_command = child_command("hang");
    unrelated_command.process_group(0);
    let temp = tempfile::tempdir()?;
    let unrelated_stdout = temp.path().join("unrelated.stdout.log");
    unrelated_command.stdout(Stdio::from(File::create(&unrelated_stdout)?));
    unrelated_command.stderr(Stdio::null());
    let unrelated = unrelated_command.spawn()?;
    let unrelated_pid = unrelated.id() as i32;
    let mut guard = UnrelatedChildGuard::new(unrelated, unrelated_pid);
    wait_for_marker(&unrelated_stdout, "child ready", Duration::from_secs(15))?;
    let log = temp.path().join("isolated-tree.log");
    let started = Instant::now();
    let error = run_logged(&mut child_command("grandchild"), &log, Duration::from_secs(5)).unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(15));
    let unrelated_text = std::fs::read_to_string(&unrelated_stdout)?;
    assert!(
        unrelated_text.contains(&format!("child pid={unrelated_pid}")),
        "unrelated child did not report its pid"
    );
    assert!(error.to_string().contains("timed out"));
    assert!(process_is_live(unrelated_pid), "unrelated process was killed");

    let text = std::fs::read_to_string(temp.path().join("isolated-tree.log.stdout.log"))?;
    let own_pid: i32 = text
        .lines()
        .find_map(|line| line.strip_prefix("grandchild pid="))
        .ok_or("grandchild pid was not recorded")?
        .parse()?;
    for _ in 0..250 {
        if !process_is_live(own_pid) {
            guard.terminate_and_wait()?;
            assert!(
                !process_is_live(unrelated_pid),
                "unrelated process {unrelated_pid} survived cleanup"
            );
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(!process_is_live(own_pid), "owned grandchild {own_pid} survived");
    guard.terminate_and_wait()?;
    assert!(!process_is_live(unrelated_pid));
    Ok(())
}

#[cfg(target_os = "linux")]
fn wait_for_marker(path: &std::path::Path, marker: &str, timeout: Duration) -> std::io::Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if std::fs::read_to_string(path).is_ok_and(|text| text.contains(marker)) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("process did not write readiness marker {marker:?}"),
    ))
}

#[cfg(target_os = "linux")]
struct UnrelatedChildGuard {
    child: Option<std::process::Child>,
    process_group: i32,
}

#[cfg(target_os = "linux")]
impl UnrelatedChildGuard {
    fn new(child: std::process::Child, process_group: i32) -> Self {
        Self {
            child: Some(child),
            process_group,
        }
    }

    fn terminate_and_wait(&mut self) -> std::io::Result<()> {
        let _ = killpg(Pid::from_raw(self.process_group), Some(Signal::SIGKILL));
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            child.wait()?;
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl Drop for UnrelatedChildGuard {
    fn drop(&mut self) {
        let _ = self.terminate_and_wait();
    }
}

#[cfg(target_os = "linux")]
fn process_is_live(pid: i32) -> bool {
    if kill(Pid::from_raw(pid), None::<Signal>).is_err() {
        return false;
    }
    let path = format!("/proc/{pid}/stat");
    if let Ok(stat) = std::fs::read_to_string(path) {
        if let Some((_, fields)) = stat.rsplit_once(')') {
            return !matches!(fields.split_whitespace().next(), Some("Z" | "X" | "x"));
        }
        return true;
    }
    false
}
