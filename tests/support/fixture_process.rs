// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded execution support for standalone fixture processes.

use std::error::Error;
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::fs::File;
#[cfg(target_os = "linux")]
use std::io;
#[cfg(target_os = "linux")]
use std::io::Read;
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::process::Child;
use std::process::Command;
#[cfg(target_os = "linux")]
use std::process::Stdio;
#[cfg(target_os = "linux")]
use std::thread;
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::Instant;

#[cfg(target_os = "linux")]
use nix::errno::Errno;
#[cfg(target_os = "linux")]
use nix::sys::signal::Signal;
#[cfg(target_os = "linux")]
use nix::sys::signal::killpg;
#[cfg(target_os = "linux")]
use nix::sys::wait::Id;
#[cfg(target_os = "linux")]
use nix::sys::wait::WaitPidFlag;
#[cfg(target_os = "linux")]
use nix::sys::wait::WaitStatus;
#[cfg(target_os = "linux")]
use nix::sys::wait::waitid;
#[cfg(target_os = "linux")]
use nix::unistd::Pid;

#[cfg(target_os = "linux")]
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Runs a command with file-backed output and an overall deadline.
///
/// The requested log receives stderr. Stdout is written to a sibling file and
/// returned on success. The sibling path has the same stem with `.stdout.log`.
/// This watchdog requires Linux process-group and non-reaping wait support.
#[cfg(target_os = "linux")]
pub fn run_logged(command: &mut Command, log: &Path, timeout: Duration) -> Result<String, Box<dyn Error>> {
    run_logged_with_poll(command, log, timeout, poll_child)
}

#[cfg(not(target_os = "linux"))]
pub fn run_logged(_command: &mut Command, _log: &Path, _timeout: Duration) -> Result<String, Box<dyn Error>> {
    Err("fixture process watchdog requires Linux process-group and waitid support".into())
}

#[cfg(target_os = "linux")]
pub(crate) fn run_logged_with_poll<P>(
    command: &mut Command,
    log: &Path,
    timeout: Duration,
    mut poll: P,
) -> Result<String, Box<dyn Error>>
where
    P: FnMut(&mut Child) -> io::Result<Option<ChildExit>>,
{
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or("fixture process timeout exceeds the representable deadline")?;
    if let Some(parent) = log.parent() {
        fs::create_dir_all(parent)?;
    }
    let stdout_path = stdout_log_path(log);
    let stdout = File::create(&stdout_path)?;
    let stderr = File::create(log)?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    command.process_group(0);

    let child = command.spawn()?;
    let mut guard = ChildGuard::new(child.id(), child);
    loop {
        match poll(guard.child_mut()) {
            Ok(Some(status)) => {
                // waitid(WNOWAIT) leaves the leader as a zombie until this
                // group kill completes, so the process-group id cannot be reused.
                guard.terminate_and_wait()?;
                let mut output = String::new();
                File::open(&stdout_path)?.read_to_string(&mut output)?;
                if status.success {
                    return Ok(output);
                }
                return Err(format!("process exited with {}; stderr log: {}", status.display, log.display()).into());
            }
            Ok(None) => {}
            Err(error) => {
                return Err(format!("failed waiting for process; stderr log: {}: {error}", log.display()).into());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("process timed out after {timeout:?}; stderr log: {}", log.display()).into());
        }
        thread::sleep(POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct ChildExit {
    success: bool,
    display: String,
}

#[cfg(target_os = "linux")]
fn poll_child(child: &mut Child) -> io::Result<Option<ChildExit>> {
    let pid = Pid::from_raw(child.id() as i32);
    let status = waitid(
        Id::Pid(pid),
        WaitPidFlag::WEXITED | WaitPidFlag::WNOHANG | WaitPidFlag::WNOWAIT,
    )
    .map_err(io::Error::from)?;
    match status {
        WaitStatus::StillAlive => Ok(None),
        WaitStatus::Exited(_, code) => Ok(Some(ChildExit {
            success: code == 0,
            display: format!("exit status: {code}"),
        })),
        WaitStatus::Signaled(_, signal, core_dumped) => Ok(Some(ChildExit {
            success: false,
            display: format!("signal: {signal}, core dumped: {core_dumped}"),
        })),
        status => Err(io::Error::other(format!("unexpected child status: {status:?}"))),
    }
}

#[cfg(target_os = "linux")]
fn stdout_log_path(log: &Path) -> PathBuf {
    let name = log.file_name().unwrap_or_default().to_string_lossy();
    log.with_file_name(format!("{name}.stdout.log"))
}

#[cfg(target_os = "linux")]
struct ChildGuard {
    pid: u32,
    child: Option<Child>,
}

#[cfg(target_os = "linux")]
impl ChildGuard {
    fn new(pid: u32, child: Child) -> Self {
        Self {
            pid,
            child: Some(child),
        }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("child is present until wait")
    }

    fn terminate_and_wait(&mut self) -> io::Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        terminate_process_group(self.pid)?;
        child.wait()?;
        self.child.take();
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = terminate_process_group(self.pid);
            let _ = child.wait();
        }
    }
}

#[cfg(target_os = "linux")]
fn terminate_process_group(pid: u32) -> io::Result<()> {
    match killpg(Pid::from_raw(pid as i32), Some(Signal::SIGKILL)) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(error) => Err(io::Error::from(error)),
    }
}
