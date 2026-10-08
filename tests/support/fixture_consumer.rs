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

/// Executes standalone `fixture` against the real Redis endpoint `url`.
///
/// When `production_async` is true, asserts smol-comp without tokio-comp.
/// Returns success after locked Cargo feature inspection and executable
/// completion. Spawns blocking Cargo processes and writes logs under the
/// isolated target. Returns filesystem, process-spawn, or feature-output UTF-8
/// errors; panics if a process fails, the feature graph violates isolation, or
/// the executable fails.
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
/// Success after locked feature inspection and fixture execution.
///
/// # Errors
///
/// Returns filesystem, process-spawn, and UTF-8 errors; panics when Cargo or
/// the fixture exits unsuccessfully or the requested feature graph is invalid.
pub fn run_with_tls_ca(
    fixture: &str,
    url: &str,
    tls_ca_path: Option<&str>,
    production_async: bool,
) -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest = root.join("tests/fixtures").join(fixture).join("Cargo.toml");
    let target = root.join("target/isolated-fixtures");
    let evidence = target.join("evidence");
    fs::create_dir_all(&evidence)?;
    let graph = Command::new("cargo")
        .args(["tree", "--locked", "--edges", "features", "--manifest-path"])
        .arg(&manifest)
        .env("CARGO_TARGET_DIR", &target)
        .output()?;
    fs::write(evidence.join(format!("{fixture}-features.txt")), &graph.stdout)?;
    assert!(
        graph.status.success(),
        "fixture feature graph failed: {}",
        String::from_utf8_lossy(&graph.stderr)
    );
    let graph = String::from_utf8(graph.stdout)?;
    if production_async {
        assert!(
            graph.contains("redis feature \"smol-comp\""),
            "production smol-comp feature must be present"
        );
        assert!(
            !graph.contains("redis feature \"tokio-comp\""),
            "fixture must not unify the provider's Redis dev features"
        );
    }
    let mut command = Command::new("cargo");
    command
        .args(["run", "--locked", "--manifest-path"])
        .arg(manifest)
        .arg("--")
        .arg(url)
        .env("CARGO_TARGET_DIR", &target);
    if let Some(tls_ca_path) = tls_ca_path {
        command.arg(tls_ca_path);
    }
    let output = command.output()?;
    fs::write(evidence.join(format!("{fixture}-stdout.log")), &output.stdout)?;
    fs::write(evidence.join(format!("{fixture}-stderr.log")), &output.stderr)?;
    assert!(
        output.status.success(),
        "fixture {fixture} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    print!("{}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}
