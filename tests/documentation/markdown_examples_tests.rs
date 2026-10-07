// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validates marked fragments and complete programs from both published guides.

use std::env::consts::EXE_SUFFIX;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
use redis::Client;
use redis::Value;
use redis::cmd;
use serde_json::to_string;
use tempfile::TempDir;

use crate::support::redis_server::RedisServer;

/// One source block as it appears in the current Markdown file.
struct MarkdownBlock {
    tag: String,
    code: String,
}

/// Reads every Rust block and requires an explicit documented example identity.
///
/// Reads `path` synchronously and returns source plus marker for each Rust
/// fence. I/O and an unmarked fence return an error; duplicate, unused or
/// unclosed markers panic with the document context so untested snippets cannot
/// slip in.
fn rust_blocks(path: &Path) -> Result<Vec<MarkdownBlock>, Box<dyn Error>> {
    let markdown = fs::read_to_string(path)?;
    let mut blocks = Vec::new();
    let mut marker = None;
    let mut lines = markdown.lines();
    while let Some(line) = lines.next() {
        if let Some(tag) = line
            .strip_prefix("<!-- doc-example: ")
            .and_then(|value| value.strip_suffix(" -->"))
        {
            assert!(marker.is_none(), "{} has an unused marker", path.display());
            marker = Some(tag.to_owned());
        } else if line.starts_with("```rust") {
            let tag = marker
                .take()
                .ok_or_else(|| format!("{} has an unmarked Rust block", path.display()))?;
            let mut code = String::new();
            let mut closed = false;
            for line in lines.by_ref() {
                if line == "```" {
                    closed = true;
                    break;
                }
                code.push_str(line);
                code.push('\n');
            }
            assert!(closed, "{} has an unterminated Rust block", path.display());
            assert!(
                !blocks.iter().any(|block: &MarkdownBlock| block.tag == tag),
                "duplicate {tag}"
            );
            blocks.push(MarkdownBlock { tag, code });
        }
    }
    assert!(
        marker.is_none(),
        "{} has a trailing unused marker",
        path.display()
    );
    Ok(blocks)
}

/// Finds the latest fragment by marker rather than copying its source into
/// tests.
///
/// Returns source borrowed from `blocks` for `tag`, panicking when it is
/// missing.
fn code<'a>(blocks: &'a [MarkdownBlock], tag: &str) -> &'a str {
    &blocks
        .iter()
        .find(|block| block.tag == tag)
        .unwrap_or_else(|| panic!("missing Markdown block {tag}"))
        .code
}

/// Implements only the guide's explicit paste/registry replacement
/// instructions.
///
/// Returns a complete main assembled from `blocks` for `tag`. Unknown tags or
/// ambiguous replacement instructions panic before building a consumer. No I/O
/// occurs, and production code is never duplicated or loaded as test source.
fn assemble(blocks: &[MarkdownBlock], tag: &str) -> String {
    let codec = code(blocks, "codec");
    match tag {
        "codec" | "sync-discovery" => format!("{codec}\n{}", code(blocks, "sync-discovery")),
        "async-discovery" => format!("{codec}\n{}", code(blocks, "async-discovery")),
        "existing-group-resume" => format!("fn main() {{\n{}\n}}", code(blocks, tag)),
        "sync-manual" | "async-manual" => {
            let asynchronous = tag == "async-manual";
            let registry = if asynchronous {
                "AsyncEventBusRegistry"
            } else {
                "EventBusRegistry"
            };
            let main_tag = if asynchronous {
                "async-discovery"
            } else {
                "sync-discovery"
            };
            let main = code(blocks, main_tag);
            let import = format!("use qubit_event_bus::{registry};\n");
            let creation = format!("{registry}::discover()?");
            assert_eq!(
                main.matches(&import).count(),
                1,
                "manual instructions must name one import"
            );
            assert_eq!(
                main.matches(&creation).count(),
                1,
                "manual instructions must name one discovery call"
            );
            let main = main
                .replace(&import, "")
                .replace(&creation, "order_registry()?");
            format!("{codec}\n{}\n{main}", code(blocks, tag))
        }
        _ => panic!("undocumented assembly for block {tag}"),
    }
}

/// Runs a subprocess with file-backed logs and a watchdog, preserving failures.
///
/// Spawns `command` with stdout/stderr beside `log`, polls it until `timeout`,
/// and returns captured stdout after successful completion. This blocks and
/// briefly sleeps while waiting. Spawn/log/kill errors or watchdog expiry
/// return an error; a nonzero subprocess status panics with both logs. On
/// expiry it kills and waits for its owned child before returning.
fn run_command(
    command: &mut Command,
    log: &Path,
    timeout: Duration,
) -> Result<String, Box<dyn Error>> {
    let stdout_path = log.with_extension("stdout.log");
    let stderr_path = log.with_extension("stderr.log");
    command.stdout(Stdio::from(File::create(&stdout_path)?));
    command.stderr(Stdio::from(File::create(&stderr_path)?));
    command.stdin(Stdio::null());
    let mut child = command.spawn()?;
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill()?;
            let _ = child.wait();
            return Err(format!(
                "documentation subprocess exceeded {timeout:?}; logs at {}",
                log.display()
            )
            .into());
        }
        sleep(Duration::from_millis(25));
    };
    let stdout = fs::read_to_string(stdout_path)?;
    let stderr = fs::read_to_string(stderr_path)?;
    assert!(
        status.success(),
        "documentation command failed: {command:?}\n{stdout}\n{stderr}"
    );
    Ok(stdout)
}

/// Emits a consumer manifest with no provider/dev dependency feature
/// unification.
///
/// Uses `root` and its sibling facade path plus `tag` to select sync/async and
/// discovery/manual features. Returns TOML without I/O. A root lacking a parent
/// panics; path strings are serialized rather than passed through a shell.
fn manifest(root: &Path, tag: &str) -> String {
    let asynchronous = tag.starts_with("async");
    let discovery = !tag.ends_with("manual");
    let adapter = if asynchronous { "async" } else { "sync" };
    let provider_features = if discovery {
        format!("[\"{adapter}\", \"discovery\"]")
    } else {
        format!("[\"{adapter}\"]")
    };
    let facade_features = if discovery { "[\"discovery\"]" } else { "[]" };
    // JSON quoted strings also encode TOML basic strings without shell
    // interpolation.
    let provider_path = to_string(&root.to_string_lossy()).expect("path string serializes");
    let facade_root = std::env::var_os("QUBIT_EVENT_BUS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            root.parent()
                .expect("sibling facade parent")
                .join("rs-event-bus")
        });
    let facade_path = to_string(&facade_root.to_string_lossy()).expect("path string serializes");
    format!(
        "[package]\nname = \"markdown-consumer\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n[workspace]\n\n[dependencies]\nqubit-event-bus-redis = {{ path = {provider_path}, default-features = false, features = {provider_features} }}\nqubit-event-bus = {{ path = {facade_path}, default-features = false, features = {facade_features} }}\nqubit-spi = \"0.13\"\nfutures-lite = \"2\"\nfutures-channel = \"0.3\"\n"
    )
}

/// Runs each current fragment/program, then checks Redis rather than trusting
/// output alone.
///
/// Starts an owned disposable Redis container and creates temporary independent
/// Cargo projects. Persistent evidence goes under the local target directory.
/// Fixture/I/O/subprocess errors return an error; assertions identify untested
/// source, wrong features, missing delivery, unfinished ACK or shutdown
/// failure. Docker and cached dependencies are required; no shared Redis data
/// is changed.
#[test]
fn test_markdown_fragments_and_programs_publish_settle_and_close() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let evidence = root.join("target/markdown-documentation/evidence");
    fs::create_dir_all(&evidence)?;
    for document in [
        "README.md",
        "README.zh_CN.md",
        "doc/design.md",
        "doc/design.zh_CN.md",
        "doc/coverage-review.md",
        "doc/coverage-review.zh_CN.md",
    ] {
        assert!(
            rust_blocks(&root.join(document))?.is_empty(),
            "new Rust blocks must be added to executable acceptance"
        );
    }
    let redis = RedisServer::start()?;
    let mut observer = Client::open(redis.url())?.get_connection()?;
    observer.set_read_timeout(Some(Duration::from_secs(2)))?;
    observer.set_write_timeout(Some(Duration::from_secs(2)))?;
    for (language, guide) in [
        ("en", "doc/user_guide.md"),
        ("zh", "doc/user_guide.zh_CN.md"),
    ] {
        let blocks = rust_blocks(&root.join(guide))?;
        let tags: Vec<_> = blocks.iter().map(|block| block.tag.as_str()).collect();
        assert_eq!(
            tags,
            [
                "codec",
                "sync-discovery",
                "async-discovery",
                "sync-manual",
                "async-manual",
                "existing-group-resume"
            ],
            "every actual Markdown Rust block must be executed"
        );
        for tag in tags {
            let id = format!("{language}-{tag}");
            let project = TempDir::new()?;
            let source = assemble(&blocks, tag);
            let cargo = manifest(&root, tag);
            fs::create_dir(project.path().join("src"))?;
            fs::write(project.path().join("src/main.rs"), &source)?;
            fs::write(project.path().join("Cargo.toml"), &cargo)?;
            fs::write(evidence.join(format!("{id}.rs")), source)?;
            fs::write(evidence.join(format!("{id}.toml")), cargo)?;
            let cargo_manifest = project.path().join("Cargo.toml");
            let target = root.join("target/markdown-documentation/consumer-target");
            run_command(
                Command::new("cargo")
                    .args(["generate-lockfile", "--manifest-path"])
                    .arg(&cargo_manifest)
                    .env("CARGO_TARGET_DIR", &target)
                    .env("CARGO_NET_RETRY", "10"),
                &evidence.join(format!("{id}-lock")),
                Duration::from_secs(180),
            )?;
            fs::copy(
                project.path().join("Cargo.lock"),
                evidence.join(format!("{id}.lock")),
            )?;
            run_command(
                Command::new("cargo")
                    .args(["fetch", "--locked", "--manifest-path"])
                    .arg(&cargo_manifest)
                    .env("CARGO_TARGET_DIR", &target)
                    .env("CARGO_NET_RETRY", "10"),
                &evidence.join(format!("{id}-fetch")),
                Duration::from_secs(180),
            )?;
            let graph = run_command(
                Command::new("cargo")
                    .args([
                        "tree",
                        "--locked",
                        "--offline",
                        "--edges",
                        "features",
                        "--manifest-path",
                    ])
                    .arg(&cargo_manifest)
                    .env("CARGO_TARGET_DIR", &target),
                &evidence.join(format!("{id}-features")),
                Duration::from_secs(60),
            )?;
            if tag.starts_with("async") {
                assert!(
                    graph.contains("redis feature \"smol-comp\""),
                    "async consumer must use production feature graph"
                );
                assert!(
                    !graph.contains("redis feature \"tokio-comp\""),
                    "provider dev features must not leak into consumer"
                );
            }
            run_command(
                Command::new("cargo")
                    .args(["build", "--locked", "--offline", "--manifest-path"])
                    .arg(&cargo_manifest)
                    .env("CARGO_TARGET_DIR", &target),
                &evidence.join(format!("{id}-build")),
                Duration::from_secs(300),
            )?;
            let namespace = format!("markdown-{id}");
            let binary = target
                .join("debug")
                .join(format!("markdown-consumer{}", EXE_SUFFIX));
            let output = run_command(
                Command::new(binary)
                    .env("REDIS_URL", redis.url())
                    .env("REDIS_NAMESPACE", &namespace),
                &evidence.join(format!("{id}-run")),
                Duration::from_secs(15),
            )?;
            if tag == "existing-group-resume" {
                println!("validated Markdown {guide} block {tag}: options compile and execute");
                continue;
            }
            let expected = if tag.starts_with("async") {
                "order-43"
            } else {
                "order-42"
            };
            assert!(
                output.contains(&format!("consumed order event: {expected}")),
                "{id} did not consume its own event"
            );
            let stream = stream_key(&namespace, "orders.created");
            let group = group_name(
                &namespace,
                "orders.created",
                "billing-worker",
                Some("billing"),
            );
            let length: usize = cmd("XLEN").arg(&stream).query(&mut observer)?;
            assert_eq!(length, 1, "{id} must publish one real stream record");
            let pending: Value = cmd("XPENDING")
                .arg(&stream)
                .arg(group)
                .query(&mut observer)?;
            assert!(
                matches!(pending, Value::Array(ref values) if matches!(values.first(), Some(Value::Int(0)))),
                "{id} shutdown must finish settlement; actual PEL: {pending:?}"
            );
            println!(
                "validated Markdown {guide} block {tag}: consumed {expected}, PEL empty, shutdown complete"
            );
        }
    }
    Ok(())
}
