// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures whether an unanswered ordinary publish blocks a settlement EVAL.
#![cfg(feature = "async")]

mod support;

use std::any::TypeId;
use std::error::Error;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::Shutdown;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use futures_channel::oneshot;
use futures_lite::future::block_on;
use futures_lite::future::race;
use futures_lite::future::zip;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus_redis::r#async::AsyncRedisEventBusProvider;
use qubit_event_bus_redis::naming::stream_key;
use qubit_id::Id;
use qubit_spi::AsyncServiceProvider;
use support::redis_server::RedisServer;

const WATCHDOG: Duration = Duration::from_secs(20);

#[test]
fn test_settlement_completes_while_publish_reply_is_held() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let proxy = IndependentGateProxy::start(server.url())?;
    let namespace = format!("settlement-probe-{}", std::process::id());
    let bus = create_bus(&proxy.url(), &namespace)?;
    block_on(async {
        let _ = bus.publish(message("probe", "seed")?).await?;
        let mut receiver = bus.subscribe(request("probe", "workers")?).await?;
        let ReceiveOutcome::Message(delivery) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("seed event missing".into());
        };
        let token = delivery.settlement().ok_or("settlement token missing")?;

        proxy.arm_xadd_reply_gate();
        let (release_tx, release_rx) = mpsc::channel();
        let controller_proxy = ProxyControl {
            gate: Arc::clone(&proxy.gate),
        };
        let controller = thread::spawn(move || {
            let held = controller_proxy.wait_held_xadd(WATCHDOG);
            let eval = controller_proxy.wait_eval(WATCHDOG);
            thread::sleep(Duration::from_millis(150));
            controller_proxy.release_gate();
            let _ = release_tx.send((held, eval, Instant::now()));
        });
        let settlement_started = Instant::now();
        let (publish_result, (settle_completed, settle_result)) =
            zip(bus.publish(message("probe", "held-publish")?), async {
                let result = receiver.settle(token, DeliveryDisposition::Accept).await;
                (Instant::now(), result)
            })
            .await;
        let (held, eval_arrival, released_at) = release_rx.recv_timeout(WATCHDOG)?;
        controller.join().map_err(|_| "gate controller panicked")?;
        held?;
        let eval_arrival = eval_arrival?;
        let _ = publish_result?;
        settle_result?;
        let completed_before_release = settle_completed < released_at;
        assert!(
            completed_before_release,
            "settlement on the same provider must finish before the held publish reply is released"
        );
        eprintln!(
            "settlement probe: EVAL forwarded at {:?}; completed_before_release={completed_before_release}; settle_elapsed={:?}; release_delay={:?}",
            eval_arrival,
            settle_completed.duration_since(settlement_started),
            released_at.duration_since(eval_arrival)
        );
        receiver.close().await?;

        // A separately constructed provider owns an independent async
        // connection cache, while both operations still traverse this proxy
        // and Redis instance.
        let independent_bus = create_bus(&proxy.url(), &namespace)?;
        let _ = independent_bus
            .publish(message("comparison", "seed-independent")?)
            .await?;
        let mut independent_receiver = independent_bus
            .subscribe(request("comparison", "workers-independent")?)
            .await?;
        let ReceiveOutcome::Message(independent_delivery) =
            independent_receiver.receive(Duration::from_secs(2)).await?
        else {
            return Err("independent comparison event missing".into());
        };
        let independent_token = independent_delivery
            .settlement()
            .ok_or("independent settlement token missing")?;
        proxy.arm_xadd_reply_gate();
        let (release_tx, release_rx) = mpsc::channel();
        let controller_proxy = ProxyControl {
            gate: Arc::clone(&proxy.gate),
        };
        let controller = thread::spawn(move || {
            let held = controller_proxy.wait_held_xadd(WATCHDOG);
            let eval = controller_proxy.wait_eval(WATCHDOG);
            thread::sleep(Duration::from_millis(150));
            controller_proxy.release_gate();
            let _ = release_tx.send((held, eval, Instant::now()));
        });
        let settlement_started = Instant::now();
        let (publish_result, (settle_completed, settle_result)) =
            zip(bus.publish(message("probe", "held-with-independent-settle")?), async {
                let result = independent_receiver
                    .settle(independent_token, DeliveryDisposition::Accept)
                    .await;
                (Instant::now(), result)
            })
            .await;
        let (held, eval_arrival, released_at) = release_rx.recv_timeout(WATCHDOG)?;
        controller.join().map_err(|_| "comparison gate controller panicked")?;
        held?;
        let eval_arrival = eval_arrival?;
        let _ = publish_result?;
        settle_result?;
        let completed_before_release = settle_completed < released_at;
        eprintln!(
            "separate-provider comparison: EVAL forwarded at {:?}; completed_before_release={completed_before_release}; settle_elapsed={:?}; release_delay={:?}",
            eval_arrival,
            settle_completed.duration_since(settlement_started),
            released_at.duration_since(eval_arrival)
        );
        assert!(
            completed_before_release,
            "settlement on a distinct provider connection should pass the held XADD response"
        );
        independent_receiver.close().await?;

        // Cancellation happens only after the proxy has observed the XADD and
        // held its response. This checks post-send cancellation separately
        // from the settlement timing above.
        proxy.arm_xadd_reply_gate();
        let (observed_tx, observed_rx) = oneshot::channel();
        let cancel_control = ProxyControl {
            gate: Arc::clone(&proxy.gate),
        };
        let cancel_controller = thread::spawn(move || {
            let observed = cancel_control.wait_held_xadd(WATCHDOG);
            if observed.is_ok() {
                let _ = observed_tx.send(());
            }
            thread::sleep(Duration::from_millis(100));
            cancel_control.release_gate();
            observed
        });
        let cancelled_after_send = race(
            async {
                let _ = bus
                    .publish(message("probe", "cancel-after-send").expect("valid message"))
                    .await;
                false
            },
            async {
                let _ = observed_rx.await;
                true
            },
        )
        .await;
        let observed = cancel_controller.join().map_err(|_| "cancel controller panicked")?;
        observed?;
        assert!(
            cancelled_after_send,
            "XADD response gate should cancel the pending publish future"
        );
        let mut observer = redis::Client::open(server.url())?.get_connection()?;
        let count: usize = redis::cmd("XLEN")
            .arg(stream_key(&namespace, "probe"))
            .query(&mut observer)?;
        assert_eq!(
            count, 4,
            "Redis should have applied the XADD before its response was held"
        );
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_settlement_completes_while_large_publish_body_is_blocked() -> Result<(), Box<dyn Error>> {
    const PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
    let server = RedisServer::start()?;
    let proxy = IndependentGateProxy::start(server.url())?;
    let namespace = format!("settlement-write-stall-{}", std::process::id());
    let options: ProviderOptions = [
        ("redis.url".into(), proxy.url()),
        ("redis.namespace".into(), namespace),
        ("redis.max_payload_bytes".into(), PAYLOAD_BYTES.to_string()),
        ("redis.max_wire_bytes".into(), "268435456".into()),
    ]
    .into();
    let bus = block_on(
        AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(options)),
    )
    .map_err(|failure| failure.into_error())?;
    block_on(async {
        let _ = bus.publish(message("probe", "seed-write-stall")?).await?;
        let mut receiver = bus.subscribe(request("probe", "write-stall-workers")?).await?;
        let ReceiveOutcome::Message(mut delivery) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("seed event missing".into());
        };
        let token = delivery.take_settlement().ok_or("settlement token missing")?;

        proxy.arm_xadd_request_gate();
        let (release_tx, release_rx) = mpsc::channel();
        let controller = ProxyControl {
            gate: Arc::clone(&proxy.gate),
        };
        let worker = thread::spawn(move || {
            let request = controller.wait_held_xadd_request(WATCHDOG);
            let eval = controller.wait_eval(WATCHDOG);
            thread::sleep(Duration::from_millis(150));
            controller.release_gate();
            let _ = release_tx.send((request, eval, Instant::now()));
        });
        let started = Instant::now();
        let large = large_message("probe", "large-write", PAYLOAD_BYTES)?;
        let publish_bus = Arc::clone(&bus);
        let (publish_tx, publish_rx) = mpsc::channel();
        let publisher = thread::spawn(move || {
            let result = block_on(publish_bus.publish(large)).map(|_| ());
            let _ = publish_tx.send(result.map_err(|error| error.to_string()));
        });
        let (settle_tx, settle_rx) = mpsc::channel();
        let settlement = thread::spawn(move || {
            let result = block_on(receiver.settle(&token, DeliveryDisposition::Accept));
            let _ = settle_tx.send((result.map_err(|error| error.to_string()), Instant::now()));
            let _ = block_on(receiver.close());
        });
        let publish_result = publish_rx.recv_timeout(WATCHDOG)?;
        let (settle_result, settled_at) = settle_rx.recv_timeout(WATCHDOG)?;
        publisher.join().map_err(|_| "publisher panicked")?;
        settlement.join().map_err(|_| "settlement panicked")?;
        let (request_blocked, eval_at, released_at) = release_rx.recv_timeout(WATCHDOG)?;
        worker.join().map_err(|_| "write-stall controller panicked")?;
        eprintln!(
            "large-write gate observations: request={request_blocked:?}, eval={eval_at:?}, publish_error={:?}",
            publish_result.as_ref().err().map(ToString::to_string)
        );
        request_blocked?;
        let eval_at = eval_at?;
        publish_result.map_err(std::io::Error::other)?;
        settle_result.map_err(std::io::Error::other)?;
        assert!(
            settled_at < released_at,
            "settlement should finish while the large XADD body remains unread"
        );
        eprintln!(
            "large-write settlement probe: EVAL forwarded at {:?}; settle_elapsed={:?}; release_window={:?}; payload_bytes={PAYLOAD_BYTES}",
            eval_at,
            settled_at.duration_since(started),
            released_at.duration_since(eval_at)
        );
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn test_settlement_waits_during_global_redis_write_pause() -> Result<(), Box<dyn Error>> {
    let server = RedisServer::start()?;
    let namespace = format!("settlement-global-pause-{}", std::process::id());
    let bus = create_bus(server.url(), &namespace)?;
    block_on(async {
        let _ = bus.publish(message("probe", "seed-global-pause")?).await?;
        let mut receiver = bus.subscribe(request("probe", "global-pause-workers")?).await?;
        let ReceiveOutcome::Message(delivery) = receiver.receive(Duration::from_secs(2)).await? else {
            return Err("seed event missing".into());
        };
        let token = delivery.settlement().ok_or("settlement token missing")?;
        let mut admin = redis::Client::open(server.url())?.get_connection()?;
        redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(300)
            .arg("WRITE")
            .query::<()>(&mut admin)?;
        let started = Instant::now();
        receiver.settle(token, DeliveryDisposition::Accept).await?;
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(250),
            "a separate client connection cannot bypass Redis' global write pause: {elapsed:?}"
        );
        eprintln!("global Redis write-pause control: settlement_elapsed={elapsed:?}");
        receiver.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

fn create_bus(url: &str, namespace: &str) -> Result<Arc<dyn qubit_event_bus::spi::AsyncEventBusSpi>, Box<dyn Error>> {
    let options: ProviderOptions = [
        ("redis.url".into(), url.into()),
        ("redis.namespace".into(), namespace.into()),
        ("redis.max_unsettled_per_subscription".into(), "1".into()),
    ]
    .into();
    block_on(AsyncRedisEventBusProvider.create_configured(&EventBusConfig::default().with_provider_options(options)))
        .map_err(|failure| failure.into_error())
        .map_err(Into::into)
}
fn message(topic: &str, id: &str) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new(topic)?,
        EventId::new(id)?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"payload"[..]),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}

fn large_message(topic: &str, id: &str, payload_bytes: usize) -> Result<OutboundMessage, Box<dyn Error>> {
    Ok(OutboundMessage::new(
        TopicAddress::new(topic)?,
        EventId::new(id)?,
        SystemTime::now(),
        Headers::new(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(vec![b'x'; payload_bytes]),
            ContentType::new("application/octet-stream")?,
            None,
        )),
    ))
}
fn request(topic: &str, group: &str) -> Result<SpiSubscriptionRequest, Box<dyn Error>> {
    Ok(SpiSubscriptionRequest::new(
        Id::new(991),
        TopicAddress::new(topic)?,
        SubscriberId::new("worker")?,
        Some(ConsumerGroup::new(group)?),
        SubscriptionDurability::Durable,
        StartPosition::Earliest,
        ProviderOptions::new(),
        TypeId::of::<Vec<u8>>(),
    ))
}

/// Each connection has independent request and reply pumps. Holding one reply
/// therefore never prevents subsequent commands from reaching Redis.
struct IndependentGateProxy {
    address: String,
    stopping: Arc<AtomicBool>,
    gate: Arc<(Mutex<GateState>, Condvar)>,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    clients: Arc<Mutex<Vec<TcpStream>>>,
    accept: Option<JoinHandle<()>>,
}
#[derive(Clone, Default)]
struct GateState {
    armed: bool,
    held: bool,
    eval_at: Option<Instant>,
    released: bool,
    request_armed: bool,
    request_held: bool,
    request_released: bool,
}
impl IndependentGateProxy {
    fn start(upstream: &str) -> std::io::Result<Self> {
        let upstream = redis::Client::open(upstream)
            .map_err(std::io::Error::other)?
            .get_connection_info()
            .clone();
        let redis::ConnectionAddr::Tcp(host, port) = upstream.addr() else {
            return Err(std::io::Error::other("TCP Redis URL required"));
        };
        let upstream_addr = format!("{host}:{port}");
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?.to_string();
        let stopping = Arc::new(AtomicBool::new(false));
        let gate = Arc::new((Mutex::new(GateState::default()), Condvar::new()));
        let workers = Arc::new(Mutex::new(Vec::new()));
        let clients = Arc::new(Mutex::new(Vec::new()));
        let (stop, gate_copy, workers_copy, clients_copy, upstream_addr) = (
            Arc::clone(&stopping),
            Arc::clone(&gate),
            Arc::clone(&workers),
            Arc::clone(&clients),
            upstream_addr.clone(),
        );
        let accept = thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((client, _)) => {
                        if let Ok(clone) = client.try_clone()
                            && let Ok(mut active) = clients_copy.lock()
                        {
                            active.push(clone);
                        }
                        let worker_gate = Arc::clone(&gate_copy);
                        let remote = upstream_addr.clone();
                        let worker = thread::spawn(move || {
                            let _ = forward_connection(client, &remote, worker_gate);
                        });
                        if let Ok(mut owned) = workers_copy.lock() {
                            owned.push(worker);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            address,
            stopping,
            gate,
            workers,
            clients,
            accept: Some(accept),
        })
    }
    fn url(&self) -> String {
        format!("redis://{}/", self.address)
    }
    fn arm_xadd_reply_gate(&self) {
        let (lock, _) = &*self.gate;
        let mut state = lock.lock().unwrap();
        state.armed = true;
        state.held = false;
        state.released = false;
        state.eval_at = None;
        state.request_armed = false;
        state.request_held = false;
        state.request_released = false;
    }
    fn arm_xadd_request_gate(&self) {
        let (lock, _) = &*self.gate;
        let mut state = lock.lock().unwrap();
        state.armed = false;
        state.held = false;
        state.released = false;
        state.request_armed = true;
        state.request_held = false;
        state.request_released = false;
        state.eval_at = None;
    }
    fn release_gate(&self) {
        let (lock, cv) = &*self.gate;
        let mut state = lock.lock().unwrap();
        state.released = true;
        state.request_released = true;
        cv.notify_all();
    }
}
struct ProxyControl {
    gate: Arc<(Mutex<GateState>, Condvar)>,
}
impl ProxyControl {
    fn wait_held_xadd(&self, timeout: Duration) -> std::io::Result<()> {
        wait_gate(&self.gate, timeout, |s| s.held).map(|_| ())
    }
    fn wait_held_xadd_request(&self, timeout: Duration) -> std::io::Result<()> {
        wait_gate(&self.gate, timeout, |s| s.request_held).map(|_| ())
    }
    fn wait_eval(&self, timeout: Duration) -> std::io::Result<Instant> {
        wait_gate(&self.gate, timeout, |s| s.eval_at.is_some())
            .and_then(|s| s.eval_at.ok_or_else(|| std::io::Error::other("EVAL not observed")))
    }
    fn release_gate(&self) {
        let (lock, cv) = &*self.gate;
        let mut state = lock.lock().unwrap();
        state.released = true;
        state.request_released = true;
        cv.notify_all();
    }
}
fn wait_gate(
    gate: &Arc<(Mutex<GateState>, Condvar)>,
    timeout: Duration,
    predicate: impl Fn(&GateState) -> bool,
) -> std::io::Result<GateState> {
    let (lock, cv) = &**gate;
    let deadline = Instant::now() + timeout;
    let mut state = lock.lock().unwrap();
    while !predicate(&state) {
        let now = Instant::now();
        if now >= deadline {
            state.released = true;
            state.request_released = true;
            cv.notify_all();
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "proxy watchdog expired",
            ));
        }
        let (next, result) = cv.wait_timeout(state, deadline - now).unwrap();
        state = next;
        if result.timed_out() && !predicate(&state) {
            state.released = true;
            state.request_released = true;
            cv.notify_all();
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "proxy watchdog expired",
            ));
        }
    }
    Ok(state.clone())
}
impl Drop for IndependentGateProxy {
    fn drop(&mut self) {
        self.release_gate();
        self.stopping.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(&self.address);
        if let Some(handle) = self.accept.take() {
            let _ = handle.join();
        }
        if let Ok(clients) = self.clients.lock() {
            for client in clients.iter() {
                let _ = client.shutdown(Shutdown::Both);
            }
        }
        if let Ok(mut workers) = self.workers.lock() {
            for handle in workers.drain(..) {
                let _ = handle.join();
            }
        }
    }
}

fn forward_connection(
    client: TcpStream,
    upstream: &str,
    gate: Arc<(Mutex<GateState>, Condvar)>,
) -> std::io::Result<()> {
    let server = TcpStream::connect(upstream)?;
    let mut request_reader = BufReader::new(client.try_clone()?);
    let mut request_writer = server.try_clone()?;
    let mut reply_reader = BufReader::new(server);
    let mut reply_writer = client;
    let commands = Arc::new(Mutex::new(Vec::<String>::new()));
    let commands_for_request = Arc::clone(&commands);
    let gate_for_request = Arc::clone(&gate);
    let requests = thread::spawn(move || -> std::io::Result<()> {
        let result = loop {
            let request = match read_request(&mut request_reader, &gate_for_request) {
                Ok(Some(request)) => request,
                Ok(None) => break Ok(()),
                Err(error) => break Err(error),
            };
            let (name, bytes) = request;
            let forwarded = request_writer.write_all(&bytes).and_then(|()| request_writer.flush());
            if let Err(error) = forwarded {
                break Err(error);
            }
            commands_for_request
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&name).to_ascii_uppercase());
            if name.eq_ignore_ascii_case(b"EVAL") {
                let (lock, cv) = &*gate_for_request;
                lock.lock().unwrap().eval_at = Some(Instant::now());
                cv.notify_all();
            }
        };
        let _ = request_writer.shutdown(Shutdown::Both);
        result
    });
    loop {
        let command = {
            let mut queue = commands.lock().unwrap();
            if queue.is_empty() {
                drop(queue);
                thread::sleep(Duration::from_millis(1));
                if requests.is_finished() {
                    break;
                }
                continue;
            }
            queue.remove(0)
        };
        let reply = match read_response(&mut reply_reader) {
            Ok(reply) => reply,
            Err(_error) if requests.is_finished() => break,
            Err(error) => return Err(error),
        };
        if command == "XADD" {
            let (lock, cv) = &*gate;
            let mut state = lock.lock().unwrap();
            if state.armed {
                state.armed = false;
                state.held = true;
                cv.notify_all();
                while !state.released {
                    state = cv.wait(state).unwrap();
                }
            }
        }
        reply_writer.write_all(&reply)?;
        reply_writer.flush()?;
    }
    let _ = requests.join();
    let _ = reply_writer.shutdown(Shutdown::Both);
    Ok(())
}

fn read_request(
    reader: &mut BufReader<TcpStream>,
    gate: &Arc<(Mutex<GateState>, Condvar)>,
) -> std::io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let Some((line, mut bytes)) = read_line(reader)? else {
        return Ok(None);
    };
    if line.first() != Some(&b'*') {
        return Err(std::io::Error::other("expected RESP array"));
    }
    let count: usize = std::str::from_utf8(&line[1..])
        .map_err(std::io::Error::other)?
        .parse()
        .map_err(std::io::Error::other)?;
    let mut name = Vec::new();
    for i in 0..count {
        let (header, mut part) = read_line(reader)?.ok_or_else(|| std::io::Error::other("truncated request"))?;
        let n: usize = std::str::from_utf8(&header[1..])
            .map_err(std::io::Error::other)?
            .parse()
            .map_err(std::io::Error::other)?;
        let mut body = vec![0; n + 2];
        reader.read_exact(&mut body)?;
        part.extend_from_slice(&body);
        bytes.extend_from_slice(&part);
        if i == 0 {
            name = body[..n].to_vec();
            if name.eq_ignore_ascii_case(b"XADD") {
                let (lock, cv) = &**gate;
                let mut state = lock.lock().unwrap();
                if state.request_armed {
                    state.request_armed = false;
                    state.request_held = true;
                    cv.notify_all();
                    while !state.request_released {
                        state = cv.wait(state).unwrap();
                    }
                }
            }
        }
    }
    Ok(Some((name, bytes)))
}
fn read_line(reader: &mut BufReader<TcpStream>) -> std::io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut bytes = Vec::new();
    let mut byte = [0];
    loop {
        match reader.read_exact(&mut byte) {
            Ok(()) => bytes.push(byte[0]),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && bytes.is_empty() => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        }
        if bytes.ends_with(b"\r\n") {
            return Ok(Some((bytes[..bytes.len() - 2].to_vec(), bytes)));
        }
    }
}
fn read_response(reader: &mut BufReader<TcpStream>) -> std::io::Result<Vec<u8>> {
    let (line, mut bytes) = read_line(reader)?.ok_or_else(|| std::io::Error::other("missing Redis reply"))?;
    match line.first() {
        Some(b'$') => {
            let n: isize = std::str::from_utf8(&line[1..])
                .map_err(std::io::Error::other)?
                .parse()
                .map_err(std::io::Error::other)?;
            if n >= 0 {
                let mut body = vec![0; n as usize + 2];
                reader.read_exact(&mut body)?;
                bytes.extend(body);
            }
        }
        Some(b'*') => {
            let n: isize = std::str::from_utf8(&line[1..])
                .map_err(std::io::Error::other)?
                .parse()
                .map_err(std::io::Error::other)?;
            for _ in 0..n.max(0) {
                bytes.extend(read_response(reader)?);
            }
        }
        _ => {}
    }
    Ok(bytes)
}
