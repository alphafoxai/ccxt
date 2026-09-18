//! WebSocket transport for the pro (`watch*`) API.
//!
//! This is the Rust port of the runtime side of `ts/src/base/ws/{Client,WsClient}.ts`.
//! The transpiled `pro/<id>.rs` exchanges call — via the base `watch()` /
//! `watch_multiple()` — into a *client* object keyed by URL that:
//!   * owns the live `tokio-tungstenite` connection (one per URL),
//!   * exposes `resolve` / `reject` / `future` / `send` so the venue's
//!     `handle_message` can push parsed data back to the awaiting `watch`,
//!   * tracks `subscriptions` (so a subscribe frame is sent once per hash),
//!   * runs ping/pong keep-alive.
//!
//! Ownership model (why this is a global registry, not a field on `Exchange`):
//! the base `watch()` borrows `&mut self` to drive `handle_message`, which
//! itself needs the client. Keeping the connection + futures in a process-wide
//! registry keyed by URL (behind its own locks) keeps it disjoint from the
//! `&mut Exchange` borrow, so the drive loop can read the next frame and call
//! `handle_message(self, client, msg)` without aliasing.

#![allow(dead_code)]

mod bounds;
mod lifecycle;
#[cfg(test)]
mod lifecycle_tests;
use bounds::{ParsedQueue, MAX_PAYLOAD_BYTES, OUTGOING_BYTES, OUTGOING_CAPACITY};
pub use lifecycle::{ClientScope, ScopeJoinReport};

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Instant;

use futures::{SinkExt, StreamExt};
use once_cell::sync::Lazy;
use tokio::sync::{broadcast, mpsc, Notify};
use tokio_tungstenite::tungstenite::Message;

use crate::Value;

/// Per-connection state. Lives in [`REGISTRY`] behind an `Arc`, so both the
/// background reader/writer tasks and the `watch` drive loop share it.
pub struct ClientState {
    pub url: String,
    /// Frames queued for the writer task → socket.
    outgoing: mpsc::Sender<Outgoing>,
    outgoing_budget: Arc<tokio::sync::Semaphore>,
    scope_id: u64,
    tasks: Mutex<lifecycle::Tasks>,
    join_gate: tokio::sync::Mutex<()>,
    close_notify: Notify,
    error: Mutex<Option<String>>,
    /// Receiver half held until the socket connects (a slot is pre-registered
    /// before connecting so `client.subscriptions` writes persist — upbit builds
    /// its subscribe frame from subscriptions set before the socket is up). The
    /// writer task takes this on connect.
    pending_rx: Mutex<Option<mpsc::Receiver<Outgoing>>>,
    /// Serializes the (async) connect so concurrent `ensure_client`s for a
    /// pre-registered slot don't open two sockets.
    connect_gate: tokio::sync::Mutex<()>,
    /// Parsed inbound messages awaiting dispatch to `handle_message`.
    incoming: Mutex<ParsedQueue>,
    /// Taken from `NEXT_GENERATION` at construction.
    generation: u64,
    /// Woken on: new inbound message, a resolve/reject, or close.
    notify: Notify,
    /// messageHash → resolved value (set by `handle_message` via `resolve`).
    resolved: Mutex<HashMap<String, Value>>,
    /// messageHash → error (set by `reject`; delivered before/instead of value).
    rejections: Mutex<HashMap<String, Value>>,
    /// subscribeHash → subscription object (TS `client.subscriptions[hash]`).
    /// A subscribe frame is sent only the first time a hash is inserted.
    subscriptions: Mutex<HashMap<String, Value>>,
    /// messageHashes some `watch` call is currently waiting on. Mirrors TS
    /// `client.futures` (read by a few venues' `handle_message`).
    futures: Mutex<HashSet<String>>,
    /// In-flight single-flight slots keyed by messageHash — the registry behind
    /// TS's `client.future ()` / `client.reusableFuture ()` leader election
    /// (binance/bybit/kucoin/... `authenticate`). Separate from `resolved`
    /// because a flight has *many* waiters (all of whom must observe the
    /// settled value, not consume it) and because settling one must also clear
    /// its `futures` entry so the NEXT cycle can elect a fresh leader.
    flights: Mutex<HashMap<String, FlightSlot>>,
    connected: Mutex<bool>,
    closed: Mutex<bool>,
    last_pong_ms: Mutex<i64>,
    /// Static-WS-test mock transport. When `mock` is set, `send_text` records the
    /// (JSON-parsed) outgoing frame into `mock_sent` instead of relying on a
    /// socket, and no real connection is ever opened. `ws_test_completed` is the
    /// watch side's done-flag the frame injector's rejection loop polls.
    mock: Mutex<bool>,
    mock_sent: Mutex<Vec<Value>>,
    ws_test_completed: Mutex<bool>,
}

/// One single-flight slot. `settled` stays populated after the flight closes so
/// waiters that poll late still observe the result, and so a caller that
/// re-awaits a hash nobody is working on any more gets the last value instead
/// of blocking (TS's already-resolved Future). `open` is what a concurrent
/// caller joins.
#[derive(Clone)]
struct FlightSlot {
    open: bool,
    settled: Option<Result<Value, Value>>,
}

/// Capacity of the bounded reader-boundary raw handoff, per URL.
/// With MAX_PAYLOAD_BYTES this bounds retained raw payload to 16 MiB per URL.
const RAW_BUS_CAPACITY: usize = 64;
const MAX_RAW_URLS: usize = 256;

struct Outgoing {
    message: Message,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct TaskExit(Arc<ClientState>);
impl Drop for TaskExit {
    fn drop(&mut self) {
        self.0.request_close();
    }
}

/// Monotonic generation counter. Each `ClientState` takes one, so a reconnect that
/// replaces a URL socket also replaces its generation, which lets an observer tell
/// frames from a dead socket apart from frames of the live one.
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

static REGISTRY: Lazy<Mutex<HashMap<String, Arc<ClientState>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// One inbound WebSocket message as it arrived at the CCXT reader boundary.
///
/// The parsed `Value` the drive loop consumes deliberately drops venue fields — a
/// Binance kline loses `x`/`q`/`E`/`T` — so this handoff carries the original bytes
/// plus the reader-side ingress instants. It is an observation channel beside the
/// parsed queue, never a replacement for it: the reader still feeds `incoming` and
/// the drive loop stays the only consumer of the parsed messages.
#[derive(Clone, Debug)]
pub struct RawFrame {
    /// URL slot the frame arrived on.
    pub url: String,
    /// Generation of the socket that delivered the frame. Compare it with the live
    /// generation to discard frames from a socket that has already been replaced.
    pub generation: u64,
    /// Reader-side monotonic instant, taken before parsing.
    pub ingress_at: Instant,
    /// Reader-side wall-clock milliseconds for the same instant.
    pub ingress_unix_ms: i64,
    /// Whether the frame arrived as a binary WebSocket message.
    pub is_binary: bool,
    /// Original payload bytes, unconverted.
    pub payload: Vec<u8>,
}

/// Per-URL bounded raw handoff. It lives beside the client registry rather than
/// inside a `ClientState` so that a reconnect replacing the socket for a URL cannot
/// silently detach an observer.
struct RawBus {
    sender: broadcast::Sender<RawFrame>,
    overflow: AtomicU64,
}

static RAW_BUS: Lazy<Mutex<HashMap<String, Arc<RawBus>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Capacity of the bounded global raw handoff. See `subscribe_raw_all`.
/// At most 64 MiB of retained raw payload (not whole-process RSS).
const RAW_ALL_CAPACITY: usize = 256;

/// The global handoff is created on first use, so a process that never asks for it
/// pays one relaxed atomic load per frame and nothing else.
struct RawAllBus {
    sender: broadcast::Sender<RawFrame>,
    overflow: AtomicU64,
}

static RAW_ALL_OPEN: AtomicBool = AtomicBool::new(false);

static RAW_ALL_BUS: Lazy<Mutex<Option<Arc<RawAllBus>>>> = Lazy::new(|| Mutex::new(None));

fn raw_all_bus() -> Arc<RawAllBus> {
    let mut slot = RAW_ALL_BUS.lock().unwrap();
    let bus = slot.get_or_insert_with(|| {
        Arc::new(RawAllBus {
            sender: broadcast::channel(RAW_ALL_CAPACITY).0,
            overflow: AtomicU64::new(0),
        })
    });
    // Published after the sender exists, so a reader that observes `true` always
    // finds a bus.
    RAW_ALL_OPEN.store(true, Ordering::Release);
    Arc::clone(bus)
}

fn raw_all_bus_if_open() -> Option<Arc<RawAllBus>> {
    if !RAW_ALL_OPEN.load(Ordering::Acquire) {
        return None;
    }
    RAW_ALL_BUS.lock().unwrap().clone()
}

/// Reclaim unsubscribed URL keys. Active observers survive socket replacement.
/// Called on subscribe/publish/close; no background registry sweeper is spawned.
pub fn reclaim_raw_buses() {
    RAW_BUS
        .lock()
        .unwrap()
        .retain(|_, bus| bus.sender.receiver_count() != 0);
}

fn raw_bus(url: &str) -> broadcast::Receiver<RawFrame> {
    assert!(
        url.len() <= 4096,
        "[NetworkError] raw observer URL exceeds limit"
    );
    let mut bus = RAW_BUS.lock().unwrap();
    bus.retain(|_, bus| bus.sender.receiver_count() != 0);
    if !bus.contains_key(url) && bus.len() >= MAX_RAW_URLS {
        drop(bus);
        panic!("[NetworkError] raw observer URL registry is full");
    }
    bus.entry(url.to_string())
        .or_insert_with(|| {
            Arc::new(RawBus {
                sender: broadcast::channel(RAW_BUS_CAPACITY).0,
                overflow: AtomicU64::new(0),
            })
        })
        .sender
        .subscribe()
}

/// Monotonic-ish wall clock in ms. `SystemTime` is fine here (keep-alive only).
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Decode a text frame into a `Value` — JSON when it parses, else a raw string
/// (some venues send bare `"pong"` etc. that `handle_message` matches on).
fn parse_text(t: &str) -> Value {
    match serde_json::from_str::<serde_json::Value>(t) {
        Ok(j) => Value::from_json(&j),
        Err(_) => Value::Str(t.to_string()),
    }
}

/// Decode a binary frame: try raw-inflate then gzip (the two schemes venues
/// use), fall back to the bytes as UTF-8. Then parse as text.
#[cfg(test)]
fn parse_binary(b: &[u8]) -> Value {
    bounds::decode(b, true).expect("valid binary fixture")
}

impl ClientState {
    /// Publish raw bytes to the URL bounded handoff before the parsed value is
    /// queued. Never blocks the reader: a full channel drops the frame and increments
    /// the URL overflow counter for an observer to notice.
    fn observe_raw(
        &self,
        payload: Vec<u8>,
        is_binary: bool,
        ingress_at: Instant,
        ingress_unix_ms: i64,
    ) {
        // Never create URL keys merely because a socket receives traffic.
        let mut buses = RAW_BUS.lock().unwrap();
        buses.retain(|_, bus| bus.sender.receiver_count() != 0);
        let frame = RawFrame {
            url: self.url.clone(),
            generation: self.generation,
            ingress_at,
            ingress_unix_ms,
            is_binary,
            payload,
        };
        if let Some(bus) = buses.get(&self.url) {
            if bus.sender.len() >= RAW_BUS_CAPACITY {
                bus.overflow.fetch_add(1, Ordering::Relaxed);
            }
            let _ = bus.sender.send(frame.clone());
        }
        // Serialize publishers through the same lock so overflow accounting has
        // no concurrent-publisher undercount. Lagged remains receiver-authoritative.
        // The global handoff is a second, deliberately broader tap: a caller that does
        // not know which URL the venue will derive for a subscription can still
        // observe the socket. It is bounded and counted the same way.
        if let Some(all) = raw_all_bus_if_open() {
            if all.sender.len() >= RAW_ALL_CAPACITY {
                all.overflow.fetch_add(1, Ordering::Relaxed);
            }
            let _ = all.sender.send(frame);
        }
    }

    /// Generation of this socket.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    fn push_incoming(&self, value: Value) {
        if self.is_closed() {
            return;
        }
        let result = self.incoming.lock().unwrap().push(value);
        if let Err(error) = result {
            self.fail(error);
        }
        self.notify.notify_waiters();
    }

    fn receive_payload(&self, payload: Vec<u8>, is_binary: bool) {
        if self.is_closed() {
            return;
        }
        let ingress_at = Instant::now();
        let ingress_unix_ms = now_ms();
        match bounds::decode(&payload, is_binary) {
            Ok(value) => {
                // Do not retain spare caller/decoder Vec capacity behind a tiny
                // payload length. Box conversion gives the bus an exact-size Vec.
                let payload = payload.into_boxed_slice().into_vec();
                self.observe_raw(payload, is_binary, ingress_at, ingress_unix_ms);
                self.push_incoming(value);
            }
            Err(error) => self.fail(error),
        }
    }

    fn send_message(&self, message: Message) -> bool {
        if self.is_closed() {
            return false;
        }
        if message.len() > MAX_PAYLOAD_BYTES {
            self.fail("[NetworkError] outgoing payload exceeds limit".into());
            return false;
        }
        // Charge retained allocation capacity, not just logical payload length:
        // a caller can pass a short String with a very large reservation.
        let bytes = match &message {
            Message::Text(value) => value.capacity(),
            Message::Binary(value) | Message::Ping(value) | Message::Pong(value) => {
                value.capacity()
            }
            _ => message.len(),
        };
        if bytes > OUTGOING_BYTES {
            self.fail("[NetworkError] outgoing byte budget exceeded".into());
            return false;
        }
        let permit = match self
            .outgoing_budget
            .clone()
            .try_acquire_many_owned(bytes.max(1) as u32)
        {
            Ok(permit) => permit,
            Err(_) => {
                self.fail("[NetworkError] outgoing byte budget exceeded".into());
                return false;
            }
        };
        if self
            .outgoing
            .try_send(Outgoing {
                message,
                _permit: permit,
            })
            .is_err()
        {
            self.fail("[NetworkError] outgoing queue full or closed".into());
            return false;
        }
        true
    }

    /// Await the next inbound message. `None` once the socket is closed and
    /// the backlog is drained. Uses the create-future-before-check pattern so
    /// a resolve/push racing the await is never lost.
    pub async fn next_message(&self) -> Option<Value> {
        let mock = *self.mock.lock().unwrap();
        loop {
            let notified = self.notify.notified();
            if let Some(error) = self.terminal_error() {
                panic!("{error}");
            }
            if let Some(v) = self.incoming.lock().unwrap().pop() {
                return Some(v);
            }
            if *self.closed.lock().unwrap() {
                return None;
            }
            if mock {
                // Static WS test: the mock queue is fed by the frame injector.
                // If nothing arrives within a short window the fixture is
                // finished (or under-feeds this watch) — stop instead of
                // blocking the drive loop forever, so the test completes.
                if tokio::time::timeout(std::time::Duration::from_millis(1500), notified)
                    .await
                    .is_err()
                {
                    return None;
                }
            } else {
                notified.await;
            }
        }
    }

    /// Store a resolved value for `hash` (TS `client.resolve`). Also settles an
    /// open single-flight on the same hash: TS's `client.resolve ()` settles
    /// the future AND removes it from `client.futures`, which is what lets the
    /// next cycle elect a fresh leader. Both paths run — a hash can in
    /// principle be someone's flight and someone else's `watch`, and only the
    /// waiter that exists will read its side.
    pub fn resolve(&self, hash: &str, value: Value) {
        self.flight_settle(hash, Ok(value.clone()));
        self.resolved
            .lock()
            .unwrap()
            .insert(hash.to_string(), value);
        self.notify.notify_waiters();
    }

    /// Store an error for `hash` (TS `client.reject`), settling an open
    /// single-flight on the same hash — see `resolve`.
    pub fn reject(&self, hash: &str, err: Value) {
        self.flight_settle(hash, Err(err.clone()));
        self.rejections
            .lock()
            .unwrap()
            .insert(hash.to_string(), err);
        self.notify.notify_waiters();
    }

    /// If any of `hashes` has a resolved value or rejection, remove and return
    /// it (`Ok` for value, `Err` for rejection). Rejections take priority.
    pub fn take_settled(&self, hashes: &[String]) -> Option<Result<Value, Value>> {
        {
            let mut rj = self.rejections.lock().unwrap();
            for h in hashes {
                if let Some(e) = rj.remove(h) {
                    return Some(Err(e));
                }
            }
        }
        let mut r = self.resolved.lock().unwrap();
        for h in hashes {
            if let Some(v) = r.remove(h) {
                return Some(Ok(v));
            }
        }
        None
    }

    /// Register interest in `hashes` (TS `client.future`). Returns true if the
    /// subscribe frame still needs sending for `subscribe_hash`.
    pub fn note_futures(&self, hashes: &[String]) {
        let mut f = self.futures.lock().unwrap();
        for h in hashes {
            f.insert(h.clone());
        }
    }

    /// Open a single-flight for `hash`, or join one already in progress. `true`
    /// means this caller is the leader and must do the work; `false` means a
    /// flight is already open and the caller should wait on it.
    ///
    /// The insert into `futures` is what makes the transpiled
    /// `if (messageHash in client.futures)` follower test observable, and it
    /// happens under the same lock as the check — this is TS's documented
    /// "atomic check-and-insert".
    pub fn flight_begin(&self, hash: &str) -> bool {
        let mut fl = self.flights.lock().unwrap();
        match fl.get_mut(hash) {
            // Open flight — join it as a follower.
            Some(slot) if slot.open => false,
            // Closed: re-open, keeping the last value for anyone who re-awaits
            // without new work being done.
            Some(slot) => {
                slot.open = true;
                self.futures.lock().unwrap().insert(hash.to_string());
                true
            }
            None => {
                fl.insert(
                    hash.to_string(),
                    FlightSlot {
                        open: true,
                        settled: None,
                    },
                );
                self.futures.lock().unwrap().insert(hash.to_string());
                true
            }
        }
    }

    /// The flight's last outcome, if it has one. Does NOT consume it: every
    /// waiter must be able to observe the same result.
    pub fn flight_peek(&self, hash: &str) -> Option<Result<Value, Value>> {
        self.flights
            .lock()
            .unwrap()
            .get(hash)
            .and_then(|s| s.settled.clone())
    }

    /// Whether a flight for `hash` exists and is still open.
    pub fn flight_is_open(&self, hash: &str) -> bool {
        self.flights
            .lock()
            .unwrap()
            .get(hash)
            .map(|s| s.open)
            .unwrap_or(false)
    }

    /// Settle an open flight and wake its waiters. Returns false (and does
    /// nothing) when `hash` names no open flight, so `resolve`/`reject` can
    /// fall through to their normal watch-hash behaviour.
    pub fn flight_settle(&self, hash: &str, res: Result<Value, Value>) -> bool {
        {
            let mut fl = self.flights.lock().unwrap();
            match fl.get_mut(hash) {
                Some(slot) if slot.open => {
                    slot.open = false;
                    slot.settled = Some(res);
                }
                _ => return false,
            }
        }
        // Clear the `futures` entry as part of settling, mirroring TS: the
        // flight is over, so the next cycle must elect a fresh leader.
        self.futures.lock().unwrap().remove(hash);
        self.notify.notify_waiters();
        true
    }

    /// Record `subscribe_hash` → `subscription`; returns true the first time
    /// (so the caller sends the subscribe frame exactly once). Mirrors TS
    /// `client.subscriptions[subscribeHash] = subscription || true`.
    pub fn subscribe_once(&self, subscribe_hash: &str, subscription: Value) -> bool {
        let mut subs = self.subscriptions.lock().unwrap();
        if subs.contains_key(subscribe_hash) {
            return false;
        }
        let stored = if matches!(subscription, Value::Null) {
            Value::Bool(true)
        } else {
            subscription
        };
        subs.insert(subscribe_hash.to_string(), stored);
        true
    }

    /// Directly set a subscription entry (TS `client.subscriptions[h] = x`
    /// written from `handle_message`).
    pub fn set_subscription(&self, subscribe_hash: &str, subscription: Value) {
        self.subscriptions
            .lock()
            .unwrap()
            .insert(subscribe_hash.to_string(), subscription);
    }

    pub fn is_subscribed(&self, subscribe_hash: &str) -> bool {
        self.subscriptions
            .lock()
            .unwrap()
            .contains_key(subscribe_hash)
    }

    pub fn send_text(&self, s: String) -> bool {
        if std::env::var("CCXT_WS_DEBUG").is_ok() {
            eprintln!("[wssend] {}", s.chars().take(200).collect::<String>());
        }
        if self.is_closed() {
            return false;
        }
        if s.len() > MAX_PAYLOAD_BYTES {
            self.fail("[NetworkError] outgoing payload exceeds limit".into());
            return false;
        }
        // Fixture capture is bounded too; it does not consume a socket queue.
        if *self.mock.lock().unwrap() {
            let mut sent = self.mock_sent.lock().unwrap();
            if sent.len() >= OUTGOING_CAPACITY {
                drop(sent);
                self.fail("[NetworkError] mock capture full".into());
                return false;
            }
            sent.push(parse_text(&s));
            return true;
        }
        self.send_message(Message::Text(s))
    }

    /// Enable the mock transport: mark connected (so `ensure_client` never opens
    /// a socket) and route `send_text` to the capture buffer.
    pub fn mock_enable(&self) {
        *self.mock.lock().unwrap() = true;
        *self.connected.lock().unwrap() = true;
    }
    /// Injected inbound frame → the same queue the socket reader feeds.
    pub fn mock_inject(&self, msg: Value) {
        self.push_incoming(msg);
    }

    /// Inject a raw fixture through both the raw handoff and the drive queue, which is
    /// what the socket reader does for a real frame. Socket-less tests use this;
    /// production paths never call it.
    pub fn mock_inject_raw(&self, payload: Vec<u8>, is_binary: bool) {
        self.receive_payload(payload, is_binary);
    }
    pub fn is_mock(&self) -> bool {
        *self.mock.lock().unwrap()
    }
    /// The captured outgoing frames as a `Value::List`.
    pub fn mock_sent_value(&self) -> Value {
        Value::Array(self.mock_sent.lock().unwrap().clone())
    }
    pub fn has_pending_futures(&self) -> bool {
        !self.futures.lock().unwrap().is_empty()
    }
    /// Whether the mock inbound queue still holds un-consumed frames.
    pub fn has_queued_messages(&self) -> bool {
        !self.incoming.lock().unwrap().is_empty()
    }
    pub fn mark_ws_test_completed(&self) {
        *self.ws_test_completed.lock().unwrap() = true;
    }
    pub fn is_ws_test_completed(&self) -> bool {
        *self.ws_test_completed.lock().unwrap()
    }
    /// Reject every pending future so a fixture whose frames don't resolve the
    /// watch fails (with a message) rather than hanging the drive loop.
    pub fn reject_pending_futures(&self, err: Value) {
        let hashes: Vec<String> = self.futures.lock().unwrap().iter().cloned().collect();
        for h in hashes {
            self.reject(&h, err.clone());
        }
    }

    pub fn is_closed(&self) -> bool {
        *self.closed.lock().unwrap()
    }

    pub fn on_pong(&self) {
        *self.last_pong_ms.lock().unwrap() = now_ms();
    }

    /// Drop resolved/rejected/subscription/future state (TS `client.reset`),
    /// e.g. after a reconnect so stale hashes don't resolve new waiters.
    pub fn reset(&self) {
        self.resolved.lock().unwrap().clear();
        self.rejections.lock().unwrap().clear();
        self.subscriptions.lock().unwrap().clear();
        self.futures.lock().unwrap().clear();
        self.flights.lock().unwrap().clear();
    }

    /// Snapshot of `subscriptions` as a `Value::Map { hash: subscription }` —
    /// the shape the transpiled `get_value(&client, "subscriptions")` reads.
    /// Tagged with `__ws_subs_url` so that writes performed on the snapshot
    /// (`client.subscriptions[chanId] = …`, common in bitfinex/chan-id venues)
    /// route back to this live `ClientState` instead of a discarded clone.
    pub fn subscriptions_value(&self) -> Value {
        let subs = self.subscriptions.lock().unwrap();
        let mut m = indexmap::IndexMap::new();
        m.insert("__ws_subs_url".to_string(), Value::Str(self.url.clone()));
        for (h, sub) in subs.iter() {
            // Tag each subscription DICT with a back-reference so a field write
            // on it (`subscription['receivedSnapshot'] = true`) persists to this
            // live client — venues mutate a subscription retrieved from
            // client.subscriptions and rely on JS object identity.
            let tagged = match sub {
                Value::Dict(d) => {
                    let mut inner = (**d).clone();
                    inner.insert(
                        "__ws_sub_ref".to_string(),
                        Value::Str(format!("{}\u{1}{}", self.url, h)),
                    );
                    Value::Dict(std::sync::Arc::new(inner))
                }
                other => other.clone(),
            };
            m.insert(h.clone(), tagged);
        }
        Value::Map(m)
    }

    /// Set a field on a stored subscription dict (`client.subscriptions[hash]
    /// [key] = val`). Creates the entry if missing.
    pub fn set_subscription_field(&self, hash: &str, key: &str, val: Value) {
        let mut subs = self.subscriptions.lock().unwrap();
        match subs.get_mut(hash) {
            Some(Value::Dict(d)) => {
                std::sync::Arc::make_mut(d).insert(key.to_string(), val);
            }
            _ => {
                let mut inner = indexmap::IndexMap::new();
                inner.insert(key.to_string(), val);
                subs.insert(hash.to_string(), Value::Dict(std::sync::Arc::new(inner)));
            }
        }
    }

    /// Snapshot of `futures` as a `Value::Map { hash: true }`.
    pub fn futures_value(&self) -> Value {
        let f = self.futures.lock().unwrap();
        let mut m = indexmap::IndexMap::new();
        for h in f.iter() {
            // Each entry is a future *handle* carrying the url + its own hash, so
            // transpiled `client.futures[hash].resolve(x)` (bitget/cryptocom auth)
            // routes back to this ClientState — see value_resolve/value_reject.
            let mut fh = indexmap::IndexMap::new();
            fh.insert("url".to_string(), Value::Str(self.url.clone()));
            fh.insert("__ws_future_hash".to_string(), Value::Str(h.clone()));
            m.insert(h.clone(), Value::Map(fh));
        }
        Value::Map(m)
    }
}

/// Get the existing client for `url`, or `None` if not yet connected.
pub fn get_client(url: &str) -> Option<Arc<ClientState>> {
    let reg = REGISTRY.lock().unwrap();
    reg.get(url).filter(|c| !c.is_closed()).cloned()
}

/// Attach a raw observer to `url`.
///
/// The channel lives per URL and outlives individual sockets, so calling this before
/// the first `watch*` call attaches the observer before the reader can deliver the
/// first frame, and a later reconnect does not detach it.
pub fn subscribe_raw(url: &str) -> broadcast::Receiver<RawFrame> {
    raw_bus(url)
}

/// Frames dropped on `url` because the bounded handoff was full. A lagging receiver
/// also observes `broadcast::error::RecvError::Lagged`, so a dropped frame is either
/// counted here or reported to the receiver, never silently lost.
pub fn raw_overflow_count(url: &str) -> u64 {
    let bus = RAW_BUS.lock().unwrap();
    bus.get(url)
        .map(|bus| bus.overflow.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// Attach a raw observer to **every** socket this process opens.
///
/// The per-URL handoff requires the caller to know the URL, but the venue derives it
/// inside the watch call -- a configured base plus a per-subscription stream index the
/// venue allocates and records for itself. This handoff removes that requirement: a
/// caller subscribes before calling `watch*` and reads the URL from the frames, which
/// is what a collector needs when the venue owns the socket layout.
///
/// It is broader by construction, so a subscriber must discriminate: frames carry the
/// URL and generation, and a session that already knows its URL should reject the rest
/// rather than trust the channel.
pub fn subscribe_raw_all() -> broadcast::Receiver<RawFrame> {
    raw_all_bus().sender.subscribe()
}

/// Frames dropped on the global handoff because it was full.
pub fn raw_all_overflow_count() -> u64 {
    if !RAW_ALL_OPEN.load(Ordering::Acquire) {
        return 0;
    }
    RAW_ALL_BUS
        .lock()
        .unwrap()
        .as_ref()
        .map(|bus| bus.overflow.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// Generation of the live `url` socket, if one exists.
pub fn client_generation(url: &str) -> Option<u64> {
    get_client(url).map(|client| client.generation())
}

/// `client.subscriptions[key] = val` written on a tagged snapshot — persist it
/// to the live client so subsequent `handle_message` snapshots see it.
pub fn value_subs_insert(url: &str, key: &str, val: Value) {
    if let Some(c) = get_client(url) {
        c.set_subscription(key, val);
    }
}

/// `delete client.subscriptions[key]` on a tagged snapshot.
pub fn value_subs_remove(url: &str, key: &str) {
    if let Some(c) = get_client(url) {
        c.subscriptions.lock().unwrap().remove(key);
    }
}

/// `client.subscriptions[hash][key] = val` written on a tagged subscription
/// dict (carrying `__ws_sub_ref` = "url\u{1}hash").
pub fn value_sub_field_write(subref: &str, key: &str, val: Value) {
    if let Some((url, hash)) = subref.split_once('\u{1}') {
        if let Some(c) = get_client(url) {
            c.set_subscription_field(hash, key, val);
        }
    }
}

/// Ensure a live connection to `url`, connecting (and spawning the reader /
/// writer / keep-alive tasks) if needed. Idempotent per URL.
/// Open an HTTP CONNECT tunnel to `proxy` and hand the raw stream to
/// tungstenite. `connect_async` has no proxy support of its own, so a venue
/// reached through `wsProxy` / `wssProxy` has to be dialed this way.
async fn connect_via_proxy(
    url: &str,
    proxy: &str,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let target = url::Url::parse(url).map_err(|e| format!("[NetworkError] ws url {url}: {e}"))?;
    let host = target
        .host_str()
        .ok_or_else(|| format!("[NetworkError] ws url {url} has no host"))?;
    let port = target.port_or_known_default().unwrap_or(443);
    let p = url::Url::parse(proxy).map_err(|e| format!("[NetworkError] ws proxy {proxy}: {e}"))?;
    let p_host = p
        .host_str()
        .ok_or_else(|| format!("[NetworkError] ws proxy {proxy} has no host"))?;
    let p_port = p.port_or_known_default().unwrap_or(8080);
    let mut stream = tokio::net::TcpStream::connect((p_host, p_port))
        .await
        .map_err(|e| format!("[NetworkError] ws proxy connect {proxy}: {e}"))?;
    let connect = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    stream
        .write_all(connect.as_bytes())
        .await
        .map_err(|e| format!("[NetworkError] ws proxy CONNECT {proxy}: {e}"))?;
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        let n = stream
            .read(&mut byte)
            .await
            .map_err(|e| format!("[NetworkError] ws proxy CONNECT {proxy}: {e}"))?;
        if n == 0 {
            return Err(format!(
                "[NetworkError] ws proxy {proxy} closed during CONNECT"
            ));
        }
        head.push(byte[0]);
        if head.len() > 8192 {
            return Err(format!(
                "[NetworkError] ws proxy {proxy} sent an oversized CONNECT reply"
            ));
        }
    }
    let status = String::from_utf8_lossy(&head);
    let first = status.lines().next().unwrap_or_default();
    if !first.contains(" 200") {
        return Err(format!(
            "[NetworkError] ws proxy {proxy} refused CONNECT: {first}"
        ));
    }
    let (ws, _resp) =
        tokio_tungstenite::client_async_tls_with_config(url, stream, Some(socket_config()), None)
            .await
            .map_err(|e| format!("[NetworkError] ws connect {url} via {proxy}: {e}"))?;
    Ok(ws)
}

fn socket_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        max_message_size: Some(MAX_PAYLOAD_BYTES),
        max_frame_size: Some(MAX_PAYLOAD_BYTES),
        write_buffer_size: 0,
        max_write_buffer_size: OUTGOING_BYTES,
        ..Default::default()
    }
}

pub async fn ensure_client(url: &str, proxy: Option<String>) -> Result<Arc<ClientState>, String> {
    let state = ensure_slot(url);
    let gate_holder = state.clone();
    let _gate = gate_holder.connect_gate.lock().await;
    let closing = gate_holder.close_notify.notified();
    if state.is_closed() {
        return Err("[NetworkError] client closed before connect".into());
    }
    if *state.connected.lock().unwrap() {
        return Ok(state);
    }
    let connect = async {
        match proxy.as_deref().filter(|p| !p.is_empty()) {
            Some(p) => connect_via_proxy(url, p).await,
            None => tokio_tungstenite::connect_async_with_config(url, Some(socket_config()), false)
                .await
                .map(|(ws, _)| ws)
                .map_err(|e| format!("[NetworkError] ws connect: {e}")),
        }
    };
    let ws = tokio::select! {
        biased;
        _ = closing => return Err("[NetworkError] client closed during connect".into()),
        result = tokio::time::timeout(std::time::Duration::from_secs(30), connect) => {
            result.map_err(|_| "[NetworkError] connect deadline exceeded".to_owned())??
        }
    };
    // Register all handles atomically with respect to request_close. A close
    // racing this region either rejects startup or sees every spawned handle.
    let mut tasks = state.tasks.lock().unwrap();
    if state.is_closed() {
        return Err("[NetworkError] client closed during connect".into());
    }
    let (mut write, mut read) = ws.split();
    let mut rx = state
        .pending_rx
        .lock()
        .unwrap()
        .take()
        .ok_or_else(|| "[NetworkError] slot has no writer channel".to_owned())?;
    *state.last_pong_ms.lock().unwrap() = now_ms();
    *state.connected.lock().unwrap() = true;
    let writer_state = state.clone();
    tasks.handles.push(tokio::spawn(async move {
        let _exit = TaskExit(writer_state.clone());
        while let Some(outgoing) = rx.recv().await {
            if let Err(error) = write.send(outgoing.message).await {
                writer_state.fail(format!("[NetworkError] writer: {error}"));
                return;
            }
            // The permit includes the in-flight write, not just queue residency.
            drop(outgoing._permit);
        }
        writer_state.request_close();
    }));
    let reader_state = state.clone();
    tasks.handles.push(tokio::spawn(async move {
        let _exit = TaskExit(reader_state.clone());
        while let Some(frame) = read.next().await {
            match frame {
                Ok(Message::Text(text)) => reader_state.receive_payload(text.into_bytes(), false),
                Ok(Message::Binary(bytes)) => reader_state.receive_payload(bytes, true),
                Ok(Message::Pong(_)) => reader_state.on_pong(),
                Ok(Message::Ping(bytes)) => {
                    reader_state.send_message(Message::Pong(bytes));
                }
                Ok(Message::Close(_)) => {
                    reader_state.request_close();
                    return;
                }
                Err(error) => {
                    reader_state.fail(format!("[NetworkError] reader: {error}"));
                    return;
                }
                _ => {}
            }
            if reader_state.is_closed() {
                return;
            }
        }
        reader_state.fail("[NetworkError] reader reached EOF".into());
    }));
    let keepalive_state = state.clone();
    tasks.handles.push(tokio::spawn(async move {
        let _exit = TaskExit(keepalive_state.clone());
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        interval.tick().await;
        loop {
            interval.tick().await;
            if !keepalive_state.send_message(Message::Ping(Vec::new())) {
                return;
            }
        }
    }));
    drop(tasks);
    Ok(state)
}

/// Live read of a client handle's `subscriptions` / `futures` field, straight
/// from the registry so a read after a write (upbit builds its subscribe frame
/// from subscriptions it just set) is coherent, not a stale embedded snapshot.
pub fn client_field_live(url: &str, field: &str) -> Value {
    match get_client(url) {
        Some(c) if field == "futures" => c.futures_value(),
        Some(c) => c.subscriptions_value(),
        None => Value::Map(indexmap::IndexMap::new()),
    }
}

/// Get-or-create the registry slot for `url` WITHOUT connecting the socket, so
/// `client.subscriptions` written before `watch()` connects still persist.
fn ensure_slot(url: &str) -> Arc<ClientState> {
    lifecycle::acquire_slot(url)
}

fn new_slot(url: &str, scope_id: u64) -> Arc<ClientState> {
    let (tx, rx) = mpsc::channel(OUTGOING_CAPACITY);
    Arc::new(ClientState {
        url: url.to_string(),
        outgoing: tx,
        outgoing_budget: Arc::new(tokio::sync::Semaphore::new(OUTGOING_BYTES)),
        scope_id,
        tasks: Mutex::new(lifecycle::Tasks::default()),
        join_gate: tokio::sync::Mutex::new(()),
        close_notify: Notify::new(),
        error: Mutex::new(None),
        pending_rx: Mutex::new(Some(rx)),
        connect_gate: tokio::sync::Mutex::new(()),
        incoming: Mutex::new(ParsedQueue::default()),
        generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
        notify: Notify::new(),
        resolved: Mutex::new(HashMap::new()),
        rejections: Mutex::new(HashMap::new()),
        subscriptions: Mutex::new(HashMap::new()),
        futures: Mutex::new(HashSet::new()),
        flights: Mutex::new(HashMap::new()),
        connected: Mutex::new(false),
        closed: Mutex::new(false),
        last_pong_ms: Mutex::new(now_ms()),
        mock: Mutex::new(false),
        mock_sent: Mutex::new(Vec::new()),
        ws_test_completed: Mutex::new(false),
    })
}

/// Request cancellation and remove a matching caller-owned slot. This synchronous
/// operation is not join evidence; hold the Arc and call close_and_join for that.
pub fn drop_client(url: &str) {
    let client = {
        let mut registry = REGISTRY.lock().unwrap();
        if registry
            .get(url)
            .is_some_and(|c| c.scope_id == lifecycle::scope_id())
        {
            registry.remove(url)
        } else {
            None
        }
    };
    if let Some(client) = client {
        client.request_close();
    }
    reclaim_raw_buses();
}

// ── `Value`-handle bridge ────────────────────────────────────────────────────
//
// The transpiled `handle_message(&mut self, client: Value, message: Value)`
// receives a *client* as a `Value`. We model it as `Value::Map { "url": <url>,
// "subscriptions": <snapshot>, "futures": <snapshot> }`. The `Value` methods
// `resolve`/`reject`/`send`/… (in value.rs) extract `url` and route here.

// ── Static-WS-test mock transport (url-keyed façade over ClientState) ────────

/// `setupWsMockTransport(url)` — register a connected, socket-less client whose
/// sends are captured. Clears any prior state so each fixture starts fresh.
pub fn mock_setup(url: &str) {
    let c = ensure_slot(url);
    c.reset();
    c.mock_enable();
    c.mock_sent.lock().unwrap().clear();
    // Discard any messages left unconsumed by a prior test on the same URL (e.g.
    // a heartbeat a watchTrades drive loop returned past before reading) so they
    // don't leak an extra handler dispatch / sent frame into the next test.
    c.incoming.lock().unwrap().clear();
    *c.ws_test_completed.lock().unwrap() = false;
}
pub fn mock_inject(url: &str, msg: Value) {
    if let Some(c) = get_client(url) {
        c.mock_inject(msg);
    }
}
pub fn mock_sent_messages(url: &str) -> Value {
    match get_client(url) {
        Some(c) => c.mock_sent_value(),
        None => Value::Array(vec![]),
    }
}
pub fn mock_has_pending_futures(url: &str) -> bool {
    get_client(url)
        .map(|c| c.has_pending_futures())
        .unwrap_or(false)
}
pub fn mock_has_queued_messages(url: &str) -> bool {
    get_client(url)
        .map(|c| c.has_queued_messages())
        .unwrap_or(false)
}
pub fn mock_mark_completed(url: &str) {
    if let Some(c) = get_client(url) {
        c.mark_ws_test_completed();
    }
}
pub fn mock_is_completed(url: &str) -> bool {
    get_client(url)
        .map(|c| c.is_ws_test_completed())
        .unwrap_or(true)
}
pub fn mock_reject_futures(url: &str) {
    if let Some(c) = get_client(url) {
        c.reject_pending_futures(Value::Str(
            "[ExchangeError] static ws test: the injected messages did not resolve the watch future".to_string()));
    }
}

/// Build the client-handle `Value` passed to `handle_message`: the URL plus
/// live snapshots of `subscriptions` / `futures` (the fields venues read via
/// `get_value(&client, "subscriptions")`).
pub fn client_value(url: &str) -> Value {
    // Pre-register the slot so subscriptions written on this handle (before the
    // socket connects) persist and read back — upbit-style subscribe building.
    let c = ensure_slot(url);
    let mut m = indexmap::IndexMap::new();
    m.insert("url".to_string(), Value::Str(url.to_string()));
    m.insert("subscriptions".to_string(), c.subscriptions_value());
    m.insert("futures".to_string(), c.futures_value());
    Value::Map(m)
}

/// Open-or-join the single-flight for `hash` on `url`, creating the registry
/// slot if the client has not been dialed yet (venue `authenticate` parks its
/// flights on a never-dialed pseudo-url, because the real user-data url embeds
/// the credential the flight is fetching).
pub fn begin_flight(url: &str, hash: &str) -> bool {
    ensure_slot(url).flight_begin(hash)
}

/// The `Value` a `client.future ()` / `client.reusableFuture ()` call hands
/// back: enough to find the flight again (its client's url + the hash). TS
/// returns a real Future object; a `Value` can't hold one, so the port returns
/// a handle and `ws_await_flight` does the awaiting.
pub fn flight_handle(url: &str, hash: &str, led: bool) -> Value {
    let mut m = indexmap::IndexMap::new();
    m.insert("url".to_string(), Value::Str(url.to_string()));
    m.insert("__ws_flight_hash".to_string(), Value::Str(hash.to_string()));
    // Whether the call that produced this handle opened the flight. A leader
    // settles its own flight (and TS's `await future` resolves the instant it
    // does), so it must never block on itself — including when the venue
    // decides, after taking the lead, that there is no work to do because it
    // is already subscribed (bitget's `authenticate`).
    m.insert("__ws_flight_led".to_string(), Value::Bool(led));
    Value::Map(m)
}

/// The hash a flight handle refers to. Accepts the `futures_value ()` handle
/// shape too, so `client.futures[hash]` entries can be awaited as well.
fn flight_hash_of(handle: &Value) -> Option<String> {
    for key in ["__ws_flight_hash", "__ws_future_hash"] {
        if let Value::Str(h) = crate::get_value(handle, &Value::Str(key.to_string())) {
            return Some(h);
        }
    }
    None
}

/// Await a flight handle — the port of TS's `await client.future (hash)` and
/// of the trailing `await future` a single-flight leader runs after settling.
///
/// Returns the resolved value; a rejected flight panics with the error, which
/// is how the port rethrows (the drive loop does the same with `take_settled`).
/// A handle that names no flight returns `Value::Null` rather than hanging.
pub async fn ws_await_flight(handle: &Value) -> Value {
    let (url, hash) = match (url_of(handle), flight_hash_of(handle)) {
        (Some(u), Some(h)) => (u, h),
        _ => return Value::Null,
    };
    let client = match get_client(&url) {
        Some(c) => c,
        None => return Value::Null,
    };
    let settled = client.flight_peek(&hash);
    // The leader: its own `client.resolve ()` has already run by the time it
    // reaches its trailing await, so take the value and go. Waiting here would
    // deadlock the (common) case where the venue took the lead and then found
    // it had nothing to do.
    let led = matches!(
        crate::get_value(handle, &Value::Str("__ws_flight_led".to_string())),
        Value::Bool(true)
    );
    if led || !client.flight_is_open(&hash) {
        return match settled {
            Some(Ok(v)) => v,
            Some(Err(e)) => panic!(
                "{}",
                match &e {
                    Value::Str(s) => s.clone(),
                    v => crate::runtime::stringify_param(v),
                }
            ),
            // A flight that never opened is not an error — the value it would
            // have carried is already in the venue's own cache, which is what
            // the caller reads next.
            None => Value::Null,
        };
    }
    let budget = if client.is_mock() {
        std::time::Duration::from_millis(1500)
    } else {
        std::time::Duration::from_secs(60)
    };
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        // Create the wakeup before peeking, so a settle racing this loop is
        // never missed (same pattern as `next_message`).
        let notified = client.notify.notified();
        if let Some(res) = client.flight_peek(&hash) {
            return match res {
                Ok(v) => v,
                Err(e) => panic!(
                    "{}",
                    match &e {
                        Value::Str(s) => s.clone(),
                        v => crate::runtime::stringify_param(v),
                    }
                ),
            };
        }
        if !client.flight_is_open(&hash) {
            // Closed without a value (reset between fixture cases).
            return Value::Null;
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            panic!(
                "[NetworkError] timed out waiting for the in-flight '{}' on {}",
                hash, url
            );
        }
    }
}

/// Extract the `url` from a client-handle `Value` (`Map{"url": ...}`).
pub fn url_of(client: &Value) -> Option<String> {
    match crate::get_value(client, &Value::Str("url".to_string())) {
        Value::Str(s) => Some(s),
        _ => None,
    }
}

fn hash_str(v: &Value) -> Option<String> {
    match v {
        Value::Str(s) => Some(s.clone()),
        _ => None,
    }
}

/// `client.resolve(value, messageHash)` routed by URL. Returns `value`.
pub fn value_resolve(client: &Value, args: &[Value]) -> Value {
    let value = args.get(0).cloned().unwrap_or(Value::Null);
    // `client.futures[hash].resolve(value)` — the future handle carries its own
    // hash, so there is no second arg. Resolve that hash.
    if let Some(hash) = future_hash_of(client) {
        if let Some(url) = url_of(client) {
            if let Some(c) = get_client(&url) {
                c.resolve(&hash, value.clone());
            }
        }
        return value;
    }
    if let (Some(url), Some(hash)) = (url_of(client), args.get(1).and_then(hash_str)) {
        if let Some(c) = get_client(&url) {
            c.resolve(&hash, value.clone());
        }
    }
    value
}

/// Extract the hash a future handle (`client.futures[hash]`) resolves/rejects.
fn future_hash_of(client: &Value) -> Option<String> {
    match crate::get_value(client, &Value::Str("__ws_future_hash".to_string())) {
        Value::Str(s) => Some(s),
        _ => None,
    }
}

/// `client.reject(error, messageHash)` routed by URL.
pub fn value_reject(client: &Value, args: &[Value]) -> Value {
    let err = args.get(0).cloned().unwrap_or(Value::Null);
    // `client.futures[hash].reject(error)` — future handle carries its own hash.
    if let Some(hash) = future_hash_of(client) {
        if let Some(url) = url_of(client) {
            if let Some(c) = get_client(&url) {
                c.reject(&hash, err.clone());
            }
        }
        return err;
    }
    if let Some(url) = url_of(client) {
        if let Some(c) = get_client(&url) {
            match args.get(1).and_then(hash_str) {
                Some(hash) => c.reject(&hash, err.clone()),
                // reject with no hash → reject every pending future.
                None => {
                    let hashes: Vec<String> = c.futures.lock().unwrap().iter().cloned().collect();
                    for h in hashes {
                        c.reject(&h, err.clone());
                    }
                }
            }
        }
    }
    err
}

/// `client.send(message)` routed by URL. Serialises non-string payloads to JSON.
pub fn value_send(client: &Value, args: &[Value]) -> Value {
    if let Some(url) = url_of(client) {
        if let Some(c) = get_client(&url) {
            let payload = match args.get(0) {
                Some(Value::Str(s)) => s.clone(),
                Some(v) => v.to_json().to_string(),
                None => String::new(),
            };
            c.send_text(payload);
        }
    }
    Value::Null
}

/// `client.reset(...)` routed by URL.
pub fn value_reset(client: &Value) -> Value {
    if let Some(url) = url_of(client) {
        if let Some(c) = get_client(&url) {
            c.reset();
        }
    }
    Value::Null
}

/// `client.on_pong(...)` routed by URL.
pub fn value_on_pong(client: &Value) -> Value {
    if let Some(url) = url_of(client) {
        if let Some(c) = get_client(&url) {
            c.on_pong();
        }
    }
    Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::Message as WsMessage;

    // A minimal mock exchange WS server: accepts one connection, waits for a
    // subscribe frame, then streams a few JSON "ticker" messages. Proves the
    // full connect → send(subscribe) → receive → parse → resolve round-trip
    // without touching a live venue.
    async fn spawn_mock_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                // Wait for the client's subscribe frame.
                if let Some(Ok(WsMessage::Text(sub))) = ws.next().await {
                    assert!(sub.contains("subscribe"), "expected subscribe, got {sub}");
                }
                // Stream three ticker updates.
                for px in ["100.5", "101.0", "101.5"] {
                    let msg = format!(
                        "{{\"channel\":\"ticker\",\"symbol\":\"BTC/USDT\",\"last\":\"{px}\"}}"
                    );
                    ws.send(WsMessage::Text(msg)).await.unwrap();
                }
                // Give the client time to drain before closing.
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let _ = ws.close(None).await;
            }
        });
        format!("ws://{addr}")
    }

    #[tokio::test]
    async fn transport_roundtrip() {
        let url = spawn_mock_server().await;
        let client = ensure_client(&url, None).await.expect("connect");

        // Send a subscribe frame once (idempotent per hash).
        assert!(client.subscribe_once("ticker:BTC/USDT", Value::Null));
        assert!(!client.subscribe_once("ticker:BTC/USDT", Value::Null));
        assert!(client.send_text("{\"op\":\"subscribe\",\"channel\":\"ticker\"}".to_string()));

        // Drive: pull each inbound message, mimic handle_message resolving the
        // "ticker" hash, and collect the resolved values.
        let mut lasts = Vec::new();
        while lasts.len() < 3 {
            let msg = client.next_message().await.expect("message before close");
            let last = crate::get_value(&msg, &Value::Str("last".to_string()));
            if let Value::Str(s) = &last {
                client.resolve("ticker", Value::Str(s.clone()));
            }
            if let Some(Ok(Value::Str(v))) = client.take_settled(&["ticker".to_string()]) {
                lasts.push(v);
            }
        }
        assert_eq!(lasts, vec!["100.5", "101.0", "101.5"]);

        // Field snapshots the transpiled code reads off the client handle.
        let subs = client.subscriptions_value();
        assert!(crate::runtime::is_true(&crate::get_value(
            &subs,
            &Value::Str("ticker:BTC/USDT".to_string())
        )));

        drop_client(&url);
    }

    // A minimal Core whose `handle_message` resolves the "ticker" hash with the
    // inbound message's `last` field — exactly what a real venue's
    // handle_message does. Lets us drive the *actual* `watch()` runtime end to
    // end (connect → subscribe → frame → dispatch_to_derived("handle_message")
    // → resolve → return) against the mock server, independent of venue quirks.
    struct TestWsCore {
        exchange: crate::exchange::Exchange,
    }
    impl std::ops::Deref for TestWsCore {
        type Target = crate::exchange::Exchange;
        fn deref(&self) -> &Self::Target {
            &self.exchange
        }
    }
    impl std::ops::DerefMut for TestWsCore {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.exchange
        }
    }
    impl crate::exchange::DerivedExchange for TestWsCore {}
    impl crate::exchange_generated::ExchangeBase for TestWsCore {
        fn call_dynamic<'a>(
            &'a mut self,
            method: &'a str,
            args: Vec<Value>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Value> + Send + 'a>> {
            Box::pin(async move {
                match method {
                    "handle_message" => {
                        let client = args.get(0).cloned().unwrap_or(Value::Null);
                        let message = args.get(1).cloned().unwrap_or(Value::Null);
                        let last = crate::get_value(&message, &Value::Str("last".to_string()));
                        // `client.resolve(value, messageHash)` — routes to the registry.
                        client.resolve(&[last, Value::Str("ticker".to_string())]);
                        Value::Null
                    }
                    _ => self.call_dynamic_base(method, args).await,
                }
            })
        }
    }

    #[tokio::test]
    async fn watch_drive_loop_end_to_end() {
        use crate::exchange::ExchangeRuntime;
        let url = spawn_mock_server().await;
        let mut core = TestWsCore {
            exchange: crate::exchange::Exchange::new(None),
        };
        // Drive the real base `watch()`: connect, send subscribe, read frames,
        // dispatch each to handle_message, return once "ticker" resolves.
        let result = ExchangeRuntime::watch(
            &mut core,
            Value::Str(url.clone()),
            Value::Str("ticker".to_string()),
            &[
                Value::Str("{\"op\":\"subscribe\",\"channel\":\"ticker\"}".to_string()),
                Value::Str("ticker".to_string()),
                Value::Null,
            ],
        )
        .await;
        // First streamed ticker.
        assert_eq!(result, Value::Str("100.5".to_string()));
        drop_client(&url);
    }

    // ── single-flight (client.future / client.reusableFuture) ───────────────
    //
    // The venue `authenticate` idiom: the first caller leads and fetches the
    // credential, concurrent callers join the flight and wake when it settles.
    // Regression cover for the port's original no-op `future()` stub, where
    // every caller led and nobody waited.

    #[tokio::test]
    async fn flight_elects_one_leader_and_registers_it() {
        let url = "flight-test-election";
        let c = ensure_slot(url);
        assert!(c.flight_begin("authenticate:future"), "first caller leads");
        assert!(
            !c.flight_begin("authenticate:future"),
            "second caller follows"
        );
        // The follower's `messageHash in client.futures` test must see it.
        assert!(in_op_futures(&c, "authenticate:future"));
        drop_client(url);
    }

    #[tokio::test]
    async fn flight_settle_wakes_waiters_and_reopens() {
        let url = "flight-test-settle";
        let c = ensure_slot(url);
        assert!(c.flight_begin("auth"));
        // Every waiter observes the value — settling must not consume it.
        c.resolve("auth", Value::Str("listen-key".to_string()));
        assert_eq!(
            ws_await_flight(&flight_handle(url, "auth", false)).await,
            Value::Str("listen-key".to_string())
        );
        assert_eq!(
            ws_await_flight(&flight_handle(url, "auth", false)).await,
            Value::Str("listen-key".to_string())
        );
        // Settled ⇒ cleared from `futures`, so the next cycle re-leads rather
        // than parking every caller on the follower branch forever.
        assert!(!in_op_futures(&c, "auth"));
        assert!(c.flight_begin("auth"), "a settled flight re-elects");
        drop_client(url);
    }

    #[tokio::test]
    async fn flight_follower_waits_for_the_leader() {
        let url = "flight-test-wait";
        let c = ensure_slot(url);
        assert!(c.flight_begin("auth"));
        let settler = {
            let c = c.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(60)).await;
                c.resolve("auth", Value::Str("late".to_string()));
            })
        };
        let started = std::time::Instant::now();
        let got = ws_await_flight(&flight_handle(url, "auth", false)).await;
        assert_eq!(got, Value::Str("late".to_string()));
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(50),
            "waited for the leader"
        );
        settler.await.unwrap();
        drop_client(url);
    }

    #[tokio::test]
    async fn a_leader_never_blocks_on_its_own_flight() {
        // A venue can take the lead and then find it has nothing to do — bitget
        // re-`authenticate`s on an already-subscribed client. Its trailing
        // `await future` must return, not wait for a settle that never comes.
        let url = "flight-test-leader";
        let c = ensure_slot(url);
        let led = c.flight_begin("authenticated");
        assert!(led);
        let started = std::time::Instant::now();
        assert_eq!(
            ws_await_flight(&flight_handle(url, "authenticated", led)).await,
            Value::Null
        );
        assert!(
            started.elapsed() < std::time::Duration::from_millis(200),
            "returned at once"
        );
        // Having led once and settled, the value is still there for a re-await.
        c.resolve("authenticated", Value::Bool(true));
        let led2 = c.flight_begin("authenticated");
        assert_eq!(
            ws_await_flight(&flight_handle(url, "authenticated", led2)).await,
            Value::Bool(true),
            "a re-lead with no new work reports the last value"
        );
        drop_client(url);
    }

    #[tokio::test]
    async fn flight_handle_without_a_flight_returns_null() {
        // Never-opened flight: return rather than hang, the value the caller
        // wanted is already in the venue's own cache.
        let url = "flight-test-missing";
        ensure_slot(url);
        assert_eq!(
            ws_await_flight(&flight_handle(url, "nope", false)).await,
            Value::Null
        );
        drop_client(url);
    }

    /// The transpiled follower test, `messageHash in client.futures`.
    fn in_op_futures(c: &Arc<ClientState>, hash: &str) -> bool {
        !matches!(
            crate::get_value(&c.futures_value(), &Value::Str(hash.to_string())),
            Value::Null
        )
    }

    #[tokio::test]
    async fn parse_helpers() {
        // JSON text → dict; non-JSON → Value::Str.
        assert!(matches!(parse_text("{\"a\":1}"), Value::Dict(_)));
        assert_eq!(parse_text("pong"), Value::Str("pong".to_string()));
        // gzip binary → parsed JSON.
        use std::io::Write;
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(b"{\"x\":true}").unwrap();
        let gz = e.finish().unwrap();
        assert!(matches!(parse_binary(&gz), Value::Dict(_)));
    }
}
