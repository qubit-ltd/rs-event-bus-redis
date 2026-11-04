// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Builds and runs standalone consumers with independent Cargo feature graphs.

use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::fixture_process::run_logged;

const TREE_TIMEOUT: Duration = Duration::from_secs(120);
const BUILD_TIMEOUT: Duration = Duration::from_secs(600);
const RUN_TIMEOUT: Duration = Duration::from_secs(120);
static INVOCATION_ID: AtomicU64 = AtomicU64::new(0);

/// Executes standalone `fixture` against the real Redis endpoint `url`.
///
/// When `production_async` is true, asserts smol-comp without tokio-comp.
/// Returns success after locked Cargo feature inspection and executable
/// completion. Tree, build, and execution have separate deadlines, with logs
/// under the isolated target. Returns filesystem, process, or feature errors.
pub fn run(fixture: &str, url: &str, production_async: bool) -> Result<(), Box<dyn Error>> {
    run_with_tls_ca(fixture, url, None, production_async)
}

/// Executes a standalone fixture with an optional Redis TLS CA certificate.
///
/// # Parameters
///
/// - `fixture`: Fixture directory name under `tests/fixtures`.
/// - `url`: Real Redis endpoint passed to the fixture process.
/// - `tls_ca_path`: Optional PEM CA path passed as the second fixture argument.
/// - `production_async`: Whether to verify the production async feature graph.
///
/// # Returns
///
/// Success after locked feature inspection and bounded fixture execution.
///
/// # Errors
///
/// Returns filesystem and process errors, including failed exit status,
/// deadline expiry, and invalid requested feature graphs.
pub fn run_with_tls_ca(
    fixture: &str,
    url: &str,
    tls_ca_path: Option<&str>,
    production_async: bool,
) -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest = root.join("tests/fixtures").join(fixture).join("Cargo.toml");
    let target = root.join("target/isolated-fixtures");
    let invocation = INVOCATION_ID.fetch_add(1, Ordering::Relaxed);
    let evidence = target.join(format!("evidence-{}-{invocation}", std::process::id()));
    fs::create_dir_all(&evidence)?;
    let graph = run_logged(
        Command::new("cargo")
            .args(["tree", "--locked", "--edges", "features", "--manifest-path"])
            .arg(&manifest)
            .env("CARGO_TARGET_DIR", &target),
        &evidence.join(format!("{fixture}-tree.stderr.log")),
        TREE_TIMEOUT,
    )?;
    fs::write(evidence.join(format!("{fixture}-features.txt")), &graph)?;
    if production_async {
        if !graph.contains("redis feature \"smol-comp\"") {
            return Err("production smol-comp feature must be present".into());
        }
        if graph.contains("redis feature \"tokio-comp\"") {
            return Err("fixture must not unify the provider's Redis dev features".into());
        }
    }
    let mut command = Command::new("cargo");
    command
        .args([
            "build",
            "--locked",
            "--message-format=json-render-diagnostics",
            "--manifest-path",
        ])
        .arg(manifest)
        .env("CARGO_TARGET_DIR", &target);
    let build_json = run_logged(
        &mut command,
        &evidence.join(format!("{fixture}-build.stderr.log")),
        BUILD_TIMEOUT,
    )?;
    fs::write(evidence.join(format!("{fixture}-build.jsonl")), &build_json)?;
    let executables = build_json
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|message| {
            (message.get("reason")?.as_str()? == "compiler-artifact"
                && message
                    .get("target")?
                    .get("kind")?
                    .as_array()?
                    .iter()
                    .any(|kind| kind.as_str() == Some("bin")))
            .then(|| message.get("executable")?.as_str().map(PathBuf::from))
            .flatten()
        })
        .collect::<Vec<_>>();
    let executable = match executables.as_slice() {
        [executable] => executable,
        [] => return Err("Cargo build output did not contain a fixture binary".into()),
        _ => return Err("Cargo build output contained multiple fixture binaries".into()),
    };
    let mut run = Command::new(executable);
    run.arg(url);
    if let Some(tls_ca_path) = tls_ca_path {
        run.arg(tls_ca_path);
    }
    run_logged(
        &mut run,
        &evidence.join(format!("{fixture}-run.stderr.log")),
        RUN_TIMEOUT,
    )?;
    Ok(())
}
