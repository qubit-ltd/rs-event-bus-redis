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
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::naming::group_name;
use qubit_event_bus_redis::naming::stream_key;
use qubit_event_bus_redis::wire::WireFields;
use redis::Client;
use redis::Value;
use redis::cmd;
use redis::streams::StreamRangeReply;
use redis::streams::StreamReadReply;
use serde_json::from_str;
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
    assert!(marker.is_none(), "{} has a trailing unused marker", path.display());
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
    if tag == "readme-sync-discovery" {
        return code(blocks, tag).to_owned();
    }
    let codec = code(blocks, "codec");
    match tag {
        "codec" | "sync-discovery" => format!("{codec}\n{}", code(blocks, "sync-discovery")),
        "async-discovery" => format!("{codec}\n{}", code(blocks, "async-discovery")),
        "existing-group-resume" => {
            let program = format!(
                r#"
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus_redis::RedisSubscriptionProfile;
use qubit_spi::ProviderSelection;

fn main() -> Result<(), Box<dyn std::error::Error>> {{
    let url = std::env::var("REDIS_URL")?;
    let namespace = std::env::var("REDIS_NAMESPACE")?;
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("redis-streams")?)
        .with_provider_options([("redis.url".into(), url), ("redis.namespace".into(), namespace)].into())
        .with_facade_config(facade_config()?);
    let bus = EventBusRegistry::discover()?.create(&config)?;
    let topic = Topic::<String>::new("orders.created")?;
    let first_group = RedisSubscriptionProfile::new(StartPosition::Earliest)
        .options()
        .build();
    let (first_tx, first_rx) = mpsc::channel();
    let first_subscription = bus.subscribe(
        SubscribeRequest::new("billing-worker", topic.clone())?.with_options(first_group),
        move |delivery| first_tx.send(delivery.payload().clone())
            .map_err(|source| DeliveryError::Handler {{ source: Box::new(source) }}),
    )?;
    bus.publish(PublishRequest::new(topic.clone(), "order-42".to_owned())?)?;
    let first = first_rx.recv_timeout(Duration::from_secs(5))?;
    if first != "order-42" {{ return Err(format!("unexpected first event: {{first}}").into()); }}
    first_subscription.cancel()?;
    let report = bus.shutdown(ShutdownMode::Graceful {{ timeout: Duration::from_secs(5) }})?;
    if report.outcome != ShutdownOutcome::Complete {{ return Err("first bus shutdown incomplete".into()); }}
    let bus = EventBusRegistry::discover()?.create(&config)?;

    bus.publish(PublishRequest::new(topic.clone(), "order-43".to_owned())?)?;
    let (resume_tx, resume_rx) = mpsc::channel();
    let options = {{
{}
    options
    }};
    let resumed = bus.subscribe(
        SubscribeRequest::new("billing-worker", topic)?.with_options(options),
        move |delivery| resume_tx.send(delivery.payload().clone())
            .map_err(|source| DeliveryError::Handler {{ source: Box::new(source) }}),
    )?;
    let second = resume_rx.recv_timeout(Duration::from_secs(5))?;
    println!("resumed event: {{second}}");
    if second != "order-43" {{ return Err(format!("resume replayed or missed event: {{second}}").into()); }}
    resumed.cancel()?;
    let report = bus.shutdown(ShutdownMode::Graceful {{ timeout: Duration::from_secs(5) }})?;
    if report.outcome != ShutdownOutcome::Complete {{ return Err("graceful shutdown incomplete".into()); }}
    Ok(())
}}
"#,
                code(blocks, tag)
            );
            format!("{codec}\n{program}")
        }
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
            let main = main.replace(&import, "").replace(&creation, "order_registry()?");
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
fn run_command(command: &mut Command, log: &Path, timeout: Duration) -> Result<String, Box<dyn Error>> {
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
        .unwrap_or_else(|| root.parent().expect("sibling facade parent").join("rs-event-bus"));
    let facade_path = to_string(&facade_root.to_string_lossy()).expect("path string serializes");
    format!(
        "[package]\nname = \"markdown-consumer\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n[workspace]\n\n[dependencies]\nqubit-event-bus-redis = {{ path = {provider_path}, default-features = false, features = {provider_features} }}\nqubit-event-bus = {{ path = {facade_path}, default-features = false, features = {facade_features} }}\nqubit-spi = \"0.14\"\nfutures-lite = \"2\"\nfutures-channel = \"0.3\"\n\n[patch.crates-io]\nqubit-event-bus = {{ path = {facade_path} }}\n"
    )
}

/// Builds an isolated consumer from the README's real marked TOML block.
///
/// Only the documented sibling paths are relocated to absolute checkouts; no
/// dependency, feature, or patch entry is added by the test.
fn source_install_manifest(root: &Path, core_root: &Path) -> Result<String, Box<dyn Error>> {
    let readme = fs::read_to_string(root.join("README.md"))?;
    let marked = readme
        .split_once("<!-- current-source-dependencies -->")
        .ok_or("README is missing current-source-dependencies marker")?
        .1;
    let toml = marked
        .split_once("```toml\n")
        .ok_or("README marker is not followed by a TOML block")?
        .1
        .split_once("\n```")
        .ok_or("README install TOML block is not closed")?
        .0;
    let provider_path = to_string(&root.to_string_lossy())?;
    let core_path = to_string(&core_root.to_string_lossy())?;
    let relocated = toml
        .replace("path = \"../rs-event-bus-redis\"", &format!("path = {provider_path}"))
        .replace("path = \"../rs-event-bus\"", &format!("path = {core_path}"));
    Ok(format!(
        "[package]\nname = \"source-install-consumer\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n[workspace]\n\n{relocated}\n"
    ))
}

/// Checks the source install contract without resolving dependencies.
fn source_install_has_matching_core_patch(manifest: &str) -> bool {
    let Some((dependencies, patch)) = manifest.split_once("[patch.crates-io]") else {
        return false;
    };
    let direct_path = core_path_from(dependencies, "qubit-event-bus = { version =");
    let patch_path = core_path_from(patch, "qubit-event-bus = { path =");
    direct_path.is_some() && direct_path == patch_path
}

fn core_path_from<'section>(section: &'section str, prefix: &str) -> Option<&'section str> {
    section
        .lines()
        .find(|line| line.trim_start().starts_with(prefix))
        .and_then(|line| line.split_once("path = \"")?.1.split_once('"').map(|(path, _)| path))
}

/// Seeds one same-payload legacy event into the README consumer group's PEL.
///
/// The old consumer first receives the event normally; XCLAIM's IDLE option
/// makes it immediately eligible for the README provider's recovery scan.
fn seed_old_pending(connection: &mut redis::Connection, namespace: &str) -> Result<(), Box<dyn Error>> {
    let stream = stream_key(namespace, "orders.created");
    let group = group_name(namespace, "orders.created", "billing-worker", Some("billing"));
    let old = WireFields::from_outbound(&OutboundMessage::new(
        TopicAddress::new("orders.created")?,
        EventId::new("legacy-order-42")?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"order-42"[..]),
            ContentType::new("text/plain")?,
            None,
        )),
    ))?;
    let wire = to_string(&old)?;
    let old_stream_id: String = cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("wire")
        .arg(wire)
        .query(connection)?;
    let _: () = cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream)
        .arg(&group)
        .arg("0-0")
        .query(connection)?;
    let received: Option<StreamReadReply> = cmd("XREADGROUP")
        .arg("GROUP")
        .arg(&group)
        .arg("readme-old-consumer")
        .arg("STREAMS")
        .arg(&stream)
        .arg(">")
        .query(connection)?;
    assert_eq!(
        received.map(|reply| reply.keys.into_iter().map(|key| key.ids.len()).sum::<usize>()),
        Some(1),
        "old README event must enter the PEL before the program starts"
    );
    let claimed: Value = cmd("XCLAIM")
        .arg(&stream)
        .arg(&group)
        .arg("readme-old-consumer")
        .arg(0)
        .arg(&old_stream_id)
        .arg("IDLE")
        .arg(60_000)
        .query(connection)?;
    assert!(matches!(claimed, Value::Array(ref rows) if rows.len() == 1));
    Ok(())
}

/// Confirms the documented source installation resolves one shared core crate
/// and compiles its real README automatic-discovery consumer.
#[test]
fn test_source_install_manifest_has_one_core_identity() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let core_root = std::env::var_os("QUBIT_EVENT_BUS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.parent().expect("sibling facade parent").join("rs-event-bus"));
    let temp = TempDir::new()?;
    let project = temp.path();
    let install_manifest = source_install_manifest(&root, &core_root)?;
    assert!(source_install_has_matching_core_patch(&install_manifest));
    let missing_patch = install_manifest.replacen("[patch.crates-io]", "[removed.patch]", 1);
    assert!(
        !source_install_has_matching_core_patch(&missing_patch),
        "a source install without the root core patch must fail before Cargo runs"
    );
    fs::write(project.join("Cargo.toml"), install_manifest)?;
    fs::create_dir(project.join("src"))?;
    let readme = rust_blocks(&root.join("README.md"))?;
    fs::write(project.join("src/main.rs"), code(&readme, "readme-sync-discovery"))?;

    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target"));
    let log = target.join("markdown-documentation/source-install-metadata");
    fs::create_dir_all(log.parent().expect("metadata log parent"))?;
    let metadata = run_command(
        Command::new("cargo")
            .args(["+1.94.0", "metadata", "--format-version", "1", "--manifest-path"])
            .arg(project.join("Cargo.toml")),
        &log,
        Duration::from_secs(120),
    )?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata)?;
    let packages = metadata["packages"].as_array().ok_or("metadata packages missing")?;
    let core_ids: Vec<_> = packages
        .iter()
        .filter(|package| package["name"] == "qubit-event-bus")
        .filter_map(|package| package["id"].as_str())
        .collect();
    assert_eq!(core_ids.len(), 1, "metadata must contain one core package identity");
    let nodes = metadata["resolve"]["nodes"]
        .as_array()
        .ok_or("metadata resolve graph missing")?;
    let package_id = |name: &str| {
        packages
            .iter()
            .find(|package| package["name"] == name)
            .and_then(|package| package["id"].as_str())
    };
    let consumer_id = package_id("source-install-consumer").ok_or("consumer package missing")?;
    let provider_id = package_id("qubit-event-bus-redis").ok_or("provider package missing")?;
    let depends_on_core = |id: &str| {
        nodes
            .iter()
            .find(|node| node["id"] == id)
            .and_then(|node| node["deps"].as_array())
            .is_some_and(|deps| deps.iter().any(|dep| dep["pkg"] == core_ids[0]))
    };
    assert!(depends_on_core(consumer_id), "consumer must resolve the shared core");
    assert!(depends_on_core(provider_id), "provider must resolve the shared core");

    let build_log = target.join("markdown-documentation/source-install-build");
    let mut build = Command::new("cargo");
    build
        .args(["+1.94.0", "check", "--locked", "--manifest-path"])
        .arg(project.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", &target);
    run_command(&mut build, &build_log, Duration::from_secs(600))?;
    Ok(())
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
    let target_root = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target"));
    let evidence = target_root.join("markdown-documentation/evidence");
    fs::create_dir_all(&evidence)?;
    let english_readme = rust_blocks(&root.join("README.md"))?;
    let chinese_readme = rust_blocks(&root.join("README.zh_CN.md"))?;
    assert_eq!(
        english_readme.len(),
        1,
        "English README needs one marked Rust quick start"
    );
    assert_eq!(
        chinese_readme.len(),
        1,
        "Chinese README needs one marked Rust quick start"
    );
    assert_eq!(
        english_readme[0].tag, "readme-sync-discovery",
        "README Rust quick start must have its dedicated marker"
    );
    assert_eq!(
        chinese_readme[0].tag, "readme-sync-discovery",
        "README Rust quick start must have its dedicated marker"
    );
    assert_eq!(
        english_readme[0].code, chinese_readme[0].code,
        "bilingual README quick starts must use the same compilable program"
    );
    for document in [
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
        ("en", "README.md"),
        ("zh", "README.zh_CN.md"),
        ("en", "doc/user_guide.md"),
        ("zh", "doc/user_guide.zh_CN.md"),
    ] {
        let blocks = rust_blocks(&root.join(guide))?;
        let tags: Vec<_> = blocks.iter().map(|block| block.tag.as_str()).collect();
        if guide.starts_with("README") {
            assert_eq!(
                tags,
                ["readme-sync-discovery"],
                "README must expose one marked Rust quick start"
            );
        } else {
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
        }
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
            let target = target_root.join("markdown-documentation/consumer-target");
            run_command(
                Command::new("cargo")
                    .args(["generate-lockfile", "--manifest-path"])
                    .arg(&cargo_manifest)
                    .env("CARGO_TARGET_DIR", &target)
                    .env("CARGO_NET_RETRY", "10"),
                &evidence.join(format!("{id}-lock")),
                Duration::from_secs(180),
            )?;
            fs::copy(project.path().join("Cargo.lock"), evidence.join(format!("{id}.lock")))?;
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
            let binary = target.join("debug").join(format!("markdown-consumer{}", EXE_SUFFIX));
            let output = run_command(
                Command::new(&binary)
                    .env("REDIS_URL", redis.url())
                    .env("REDIS_NAMESPACE", &namespace),
                &evidence.join(format!("{id}-run")),
                Duration::from_secs(15),
            )?;
            if tag == "existing-group-resume" {
                assert!(
                    output.contains("resumed event: order-43"),
                    "{id} did not resume at the stored group cursor: {output}"
                );
                let stream = stream_key(&namespace, "orders.created");
                let group = group_name(&namespace, "orders.created", "billing-worker", None);
                let length: usize = cmd("XLEN").arg(&stream).query(&mut observer)?;
                assert_eq!(length, 2, "{id} must preserve both records");
                let pending: Value = cmd("XPENDING").arg(&stream).arg(group).query(&mut observer)?;
                assert!(
                    matches!(pending, Value::Array(ref values) if matches!(values.first(), Some(Value::Int(0)))),
                    "{id} must ACK the resumed record; actual PEL: {pending:?}"
                );
                println!("validated Markdown {guide} block {tag}: resumed second event, preserved first, PEL empty");
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
            let group = group_name(&namespace, "orders.created", "billing-worker", Some("billing"));
            let length: usize = cmd("XLEN").arg(&stream).query(&mut observer)?;
            assert_eq!(length, 1, "{id} must publish one real stream record");
            let pending: Value = cmd("XPENDING").arg(&stream).arg(group).query(&mut observer)?;
            assert!(
                matches!(pending, Value::Array(ref values) if matches!(values.first(), Some(Value::Int(0)))),
                "{id} shutdown must finish settlement; actual PEL: {pending:?}"
            );
            println!("validated Markdown {guide} block {tag}: consumed {expected}, PEL empty, shutdown complete");
            if guide.starts_with("README") {
                let recovery_namespace = format!("{namespace}-old-pending");
                seed_old_pending(&mut observer, &recovery_namespace)?;
                let recovery_output = run_command(
                    Command::new(&binary)
                        .env("REDIS_URL", redis.url())
                        .env("REDIS_NAMESPACE", &recovery_namespace),
                    &evidence.join(format!("{id}-old-pending-run")),
                    Duration::from_secs(15),
                )?;
                assert!(
                    recovery_output.contains("recovered earlier event: legacy-order-42"),
                    "{id} must observe the seeded old pending event before succeeding: {recovery_output}"
                );
                let recovery_stream = stream_key(&recovery_namespace, "orders.created");
                let recovery_group =
                    group_name(&recovery_namespace, "orders.created", "billing-worker", Some("billing"));
                let records: StreamRangeReply = cmd("XRANGE")
                    .arg(&recovery_stream)
                    .arg("-")
                    .arg("+")
                    .query(&mut observer)?;
                assert_eq!(records.ids.len(), 2, "{id} must retain the legacy and new records");
                let decoded: Vec<WireFields> = records
                    .ids
                    .iter()
                    .map(|record| {
                        let wire = redis::from_redis_value::<String>(
                            record.map.get("wire").expect("stream record has wire field").clone(),
                        )?;
                        Ok(from_str(&wire)?)
                    })
                    .collect::<Result<_, Box<dyn Error>>>()?;
                let old_event = decoded
                    .iter()
                    .find(|fields| fields.event_id == "legacy-order-42")
                    .expect("old wire event remains in source stream");
                let new_event = decoded
                    .iter()
                    .find(|fields| fields.event_id != "legacy-order-42")
                    .expect("new wire event remains in source stream");
                assert_eq!(new_event.payload, b"order-42");
                assert_ne!(old_event.event_id, new_event.event_id);
                assert!(
                    recovery_output.contains(&format!(
                        "consumed order event: order-42; event_id={}",
                        new_event.event_id
                    )),
                    "{id} success must identify the new wire EventId: {recovery_output}"
                );
                let pending: Value = cmd("XPENDING")
                    .arg(&recovery_stream)
                    .arg(&recovery_group)
                    .query(&mut observer)?;
                assert!(
                    matches!(pending, Value::Array(ref values) if matches!(values.first(), Some(Value::Int(0)))),
                    "{id} must settle both recovered and new records; actual PEL: {pending:?}"
                );
                println!(
                    "validated Markdown {guide} block {tag}: observed old EventId, succeeded on new EventId, source retained, PEL empty"
                );
            }
        }
    }
    Ok(())
}
