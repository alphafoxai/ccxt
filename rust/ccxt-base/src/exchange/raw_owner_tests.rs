use super::*;
use crate::exchange_generated::ExchangeBase;
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

struct CountingCore {
    exchange: Exchange,
    dispatched: Arc<AtomicUsize>,
}
impl std::ops::Deref for CountingCore {
    type Target = Exchange;
    fn deref(&self) -> &Exchange {
        &self.exchange
    }
}
impl std::ops::DerefMut for CountingCore {
    fn deref_mut(&mut self) -> &mut Exchange {
        &mut self.exchange
    }
}
impl DerivedExchange for CountingCore {}
impl ExchangeBase for CountingCore {
    fn call_dynamic<'a>(
        &'a mut self,
        method: &'a str,
        args: Vec<Value>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Value> + Send + 'a>> {
        Box::pin(async move {
            if method == "handle_message" {
                self.dispatched.fetch_add(1, Ordering::SeqCst);
                args[0].resolve(&[Value::Int(7), Value::Str("trade".into())]);
                Value::Null
            } else {
                self.call_dynamic_base(method, args).await
            }
        })
    }
}

#[derive(Clone, Copy)]
enum WatchMode {
    ScopedRaw,
    LegacyRaw,
    Parsed,
}

// Some venue methods (OKX watch_trades_for_symbols at b06134c) construct a new
// request without extending params. The runtime must honor a caller's explicit
// task-local drain scope even when no internal marker reaches this request.
async fn markerless_watch(mode: WatchMode) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (wire_tx, wire_rx) = tokio::sync::oneshot::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
        wire_tx.send(request).unwrap();
        ws.send(Message::Text("{\"price\":\"0.000000123456789\"}".into()))
            .await
            .unwrap();
        let _ = stop_rx.await;
    });
    let scope = crate::pro::ws_client::ClientScope::new();
    let dispatched = Arc::new(AtomicUsize::new(0));
    let mut core = CountingCore {
        exchange: Exchange::new(None),
        dispatched: dispatched.clone(),
    };
    let message = Value::Map({
        let mut map = crate::value::HashMap::new();
        map.insert("op".into(), Value::Str("subscribe".into()));
        if matches!(mode, WatchMode::LegacyRaw) {
            map.insert("rawOwnerDrain".into(), Value::Bool(true));
        }
        map
    });
    let args = [message];
    let watch = core.watch_multiple(
        Value::Str(url),
        Value::List(vec![Value::Str("trade".into())]),
        &args,
    );
    let result = if matches!(mode, WatchMode::ScopedRaw) {
        tokio::time::timeout(
            Duration::from_millis(200),
            scope.run(with_raw_owner_drain(watch)),
        )
        .await
    } else {
        tokio::time::timeout(Duration::from_millis(200), scope.run(watch)).await
    };
    let wire = tokio::time::timeout(Duration::from_secs(1), wire_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&wire).unwrap(),
        serde_json::json!({"op":"subscribe"})
    );
    let count = dispatched.load(Ordering::SeqCst);
    let _ = stop_tx.send(());
    let joined = scope.close_and_join(Duration::from_secs(1)).await;
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    assert!(joined.cleanup_complete && !joined.timed_out);
    assert!(
        RAW_OWNER_DRAIN.try_with(|_| ()).is_err(),
        "cancelled scope leaked into caller"
    );
    if !matches!(mode, WatchMode::Parsed) {
        assert!(
            result.is_err(),
            "explicit raw-owner scope incorrectly dispatched/settled markerless watch: {result:?}"
        );
        assert_eq!(count, 0, "raw-owner must bypass venue handlers");
    } else {
        assert_eq!(result.unwrap(), Value::Int(7));
        assert_eq!(count, 1, "ordinary watches retain parsed dispatch");
    }
}

#[tokio::test]
async fn explicit_scope_drains_when_venue_drops_params() {
    markerless_watch(WatchMode::ScopedRaw).await;
}

#[tokio::test]
async fn unscoped_watch_still_dispatches() {
    markerless_watch(WatchMode::Parsed).await;
}

#[tokio::test]
async fn legacy_marker_still_drains_and_is_not_on_wire() {
    markerless_watch(WatchMode::LegacyRaw).await;
}

#[tokio::test]
async fn scope_does_not_leak_to_siblings_spawned_tasks_or_after_panic() {
    use futures::FutureExt;
    with_raw_owner_drain(async {
        assert!(RAW_OWNER_DRAIN.try_with(|_| ()).is_ok());
        assert!(
            tokio::spawn(async { RAW_OWNER_DRAIN.try_with(|_| ()).is_err() })
                .await
                .unwrap()
        );
        with_raw_owner_drain(async {
            assert!(RAW_OWNER_DRAIN.try_with(|_| ()).is_ok());
        })
        .await;
        assert!(RAW_OWNER_DRAIN.try_with(|_| ()).is_ok());
    })
    .await;
    assert!(RAW_OWNER_DRAIN.try_with(|_| ()).is_err());
    let failure = std::panic::AssertUnwindSafe(with_raw_owner_drain(async {
        panic!("fixture");
    }))
    .catch_unwind()
    .await;
    assert!(failure.is_err());
    assert!(RAW_OWNER_DRAIN.try_with(|_| ()).is_err());
    // Force a same-task sibling poll while the scoped sibling is suspended;
    // timeout or future completion cannot make this assertion vacuously pass.
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    tokio::join!(
        with_raw_owner_drain(async {
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
            assert!(RAW_OWNER_DRAIN.try_with(|_| ()).is_ok());
        }),
        async {
            entered_rx.await.unwrap();
            assert!(RAW_OWNER_DRAIN.try_with(|_| ()).is_err());
            release_tx.send(()).unwrap();
        }
    );
}

#[tokio::test]
async fn raw_branch_drains_batches_and_propagates_terminal_overflow() {
    use crate::pro::ws_client;
    use futures::FutureExt;
    // Socket-free reader injection avoids timer-based throughput assertions.
    let url = format!("ws://raw-drain-batches-{}.invalid", std::process::id());
    ws_client::mock_setup(&url);
    let client = ws_client::get_client(&url).unwrap();
    let dispatched = Arc::new(AtomicUsize::new(0));
    let mut core = CountingCore {
        exchange: Exchange::new(None),
        dispatched: dispatched.clone(),
    };
    let args = [Value::Null];
    let mut watch = Box::pin(with_raw_owner_drain(core.watch_multiple(
        Value::Str(url.clone()),
        Value::List(vec![Value::Str("trade".into())]),
        &args,
    )));
    assert!(watch.as_mut().now_or_never().is_none());
    for batch in 0..32 {
        for row in 0..64 {
            client.mock_inject_raw(
                format!("{{\"seq\":{}}}", batch * 64 + row).into_bytes(),
                false,
            );
        }
        assert!(watch.as_mut().now_or_never().is_none());
        assert!(client.terminal_error().is_none());
        // After polling the real raw watch, even one unread parsed row is a
        // failure: a dummy pending loop cannot pass this regression.
        assert!(
            client.next_message().now_or_never().is_none(),
            "raw branch left parsed backlog"
        );
    }
    assert_eq!(dispatched.load(Ordering::SeqCst), 0);
    // Genuinely stop draining: existing 1024 bound must still be terminal,
    // and polling the raw watch itself must propagate the original failure.
    for _ in 0..1025 {
        client.mock_inject_raw(b"{}".to_vec(), false);
    }
    assert!(client
        .terminal_error()
        .unwrap()
        .contains("incoming WebSocket queue capacity exceeded (1024)"));
    let terminal = std::panic::AssertUnwindSafe(watch.as_mut())
        .catch_unwind()
        .now_or_never();
    assert!(
        matches!(terminal, Some(Err(_))),
        "raw watch hid terminal overflow"
    );
    drop(watch);
    ws_client::drop_client(&url);
    assert!(ws_client::get_client(&url).is_none());
    assert!(RAW_OWNER_DRAIN.try_with(|_| ()).is_err());
}

// ── raw-owner heartbeat ──────────────────────────────────────────────────────
//
// A raw-owner socket sends nothing on the wire but the subscribe frame: no venue
// dispatch runs, so nothing answers bitget's documented "send `ping` every 30s or
// be disconnected" rule and the socket is reset minutes into a session. The
// heartbeat therefore has to come out of the raw drive loop, from the venue's own
// `ping` (TS `Client.onPingInterval` → `this.ping (client)`), with exactly one
// frame per window no matter how many watchers share the URL.

/// A core whose `ping` is the transpiled venue `ping`, verbatim: bitget/okx return
/// the literal text, hyperliquid/bybit a method frame, and `reply: None` falls
/// through to `call_dynamic_base` — which has no `ping` arm, exactly like binance
/// and gate, so the base answers Null and the runtime owes a control ping.
struct HeartbeatCore {
    exchange: Exchange,
    calls: Arc<std::sync::Mutex<Vec<String>>>,
    reply: Option<Value>,
}
impl std::ops::Deref for HeartbeatCore {
    type Target = Exchange;
    fn deref(&self) -> &Exchange {
        &self.exchange
    }
}
impl std::ops::DerefMut for HeartbeatCore {
    fn deref_mut(&mut self) -> &mut Exchange {
        &mut self.exchange
    }
}
impl DerivedExchange for HeartbeatCore {}
impl ExchangeBase for HeartbeatCore {
    fn call_dynamic<'a>(
        &'a mut self,
        method: &'a str,
        args: Vec<Value>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Value> + Send + 'a>> {
        Box::pin(async move {
            if method == "ping" {
                // The venue `ping (client)` receives this exact socket generation's
                // handle, the same shape `Exchange.client (url)` binds onto a
                // `WsClient`. A venue that reads its client (or a future one that
                // does) must not be handed a stub.
                let handle = args.get(0).cloned().unwrap_or(Value::Null);
                for field in ["url", "__ws_reference", "subscriptions", "futures"] {
                    assert!(
                        !matches!(
                            crate::get_value(&handle, &Value::Str(field.to_string())),
                            Value::Null
                        ),
                        "venue ping got a client handle without {field}"
                    );
                }
                let called_url = crate::runtime::stringify_param(&crate::get_value(
                    &handle,
                    &Value::Str("url".to_string()),
                ));
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("{method} {called_url}"));
                if let Some(reply) = self.reply.clone() {
                    return reply;
                }
            }
            self.call_dynamic_base(method, args).await
        })
    }
}

fn heartbeat_url(tag: &str) -> String {
    format!("ws://{tag}-{}", std::process::id())
}

/// Every client→server frame a peer sees for `window`, in order. Control pings and
/// text frames stay distinguishable, which is the whole point: the defect is a
/// socket that only ever answers with the wrong one.
async fn record_wire_frames(window: Duration) -> (String, tokio::task::JoinHandle<Vec<Message>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let mut frames = Vec::new();
        let deadline = tokio::time::Instant::now() + window;
        while let Ok(Some(Ok(frame))) = tokio::time::timeout_at(deadline, ws.next()).await {
            frames.push(frame);
        }
        frames
    });
    (url, peer)
}

fn text_frames(frames: &[Message]) -> Vec<String> {
    frames
        .iter()
        .filter_map(|frame| match frame {
            Message::Text(text) => Some(text.to_string()),
            _ => None,
        })
        .collect()
}

/// Run one raw-owner watch against a real socket for `window`, with the heartbeat
/// cadence retuned *before* the drive loop builds its timer. Returns what the peer
/// saw, plus how many times the venue `ping` was actually dispatched.
async fn raw_watch_on_the_wire(
    tag: &str,
    reply: Option<Value>,
    window: Duration,
) -> (Vec<Message>, usize) {
    use crate::pro::ws_client;
    let (url, peer) = record_wire_frames(window).await;
    let scope = ws_client::ClientScope::new();
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let watch_url = url.clone();
    let core_calls = calls.clone();
    let watch = scope.run(async move {
        // The cadence is read once per tick, so it must be set before the loop's
        // first tick — and the slot has to be created inside the same scope the
        // watch runs in, or the watch would reject the URL as foreign-owned.
        ws_client::client_value(&watch_url);
        ws_client::set_heartbeat_interval_for_test(&watch_url, Duration::from_millis(60));
        let mut core = HeartbeatCore {
            exchange: Exchange::new(None),
            calls: core_calls,
            reply,
        };
        with_raw_owner_drain(core.watch_multiple(
            Value::Str(watch_url),
            Value::List(vec![Value::Str("trade".into())]),
            &[Value::Null],
        ))
        .await
    });
    // A raw watch never returns on its own; the timeout is how the test cancels it.
    let cancelled = tokio::time::timeout(window, watch).await;
    assert!(cancelled.is_err(), "the raw drain ended on its own");
    let frames = tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .expect("the peer never released the connection")
        .unwrap();
    // Cancellation must hand the socket back: no heartbeat driver may outlive the
    // watch, and cleanup must still prove every internal task joined.
    let client = ws_client::get_client(&url).expect("the socket outlived its scope");
    assert_eq!(
        client.raw_heartbeat_drivers(),
        0,
        "a cancelled raw watch left its heartbeat claimed"
    );
    assert!(
        client.control_ping_owned(),
        "the generic keep-alive must resume once the raw driver is gone"
    );
    let report = scope.close_and_join(Duration::from_secs(5)).await;
    assert!(report.cleanup_complete && !report.timed_out, "{report:?}");
    assert!(report.all_joined(), "{report:?}");
    ws_client::drop_client(&url);
    let recorded = calls.lock().unwrap().clone();
    // The dispatched ping must have been handed THIS socket's client handle, not a
    // stub or a handle borrowed from another generation.
    assert!(
        recorded.iter().all(|call| *call == format!("ping {url}")),
        "the venue ping was dispatched against another socket: {recorded:?}"
    );
    (frames, recorded.len())
}

#[tokio::test]
async fn raw_heartbeat_is_the_venue_text_ping_not_a_control_frame() {
    // bitget/okx: `ping (client) { return 'ping' }`. A socket that answers with an
    // RFC-6455 control frame instead is exactly what got reset mid-session.
    let (frames, dispatched) = raw_watch_on_the_wire(
        "raw-hb-text",
        Some(Value::Str("ping".to_string())),
        Duration::from_millis(600),
    )
    .await;
    assert!(dispatched > 0, "the venue ping was never dispatched");
    let texts = text_frames(&frames);
    assert!(
        texts.len() >= 2,
        "a 600ms window at a 60ms cadence must not be silent: {frames:?}"
    );
    assert!(
        texts.iter().all(|text| text == "ping"),
        "every heartbeat frame must be the venue's own payload: {texts:?}"
    );
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame, Message::Ping(_)))
            .count(),
        0,
        "a venue that requires a text ping must not also be sent a control ping"
    );
}

#[tokio::test]
async fn raw_heartbeat_falls_back_to_a_control_ping() {
    // binance/gate define no `ping`, so the base answers Null and the runtime owes
    // the same RFC-6455 control frame the generic keep-alive has always sent.
    let (frames, dispatched) =
        raw_watch_on_the_wire("raw-hb-control", None, Duration::from_millis(600)).await;
    assert!(dispatched > 0, "the venue ping was never dispatched");
    assert!(
        frames.iter().any(|frame| matches!(frame, Message::Ping(_))),
        "a Null venue ping must still produce a heartbeat, got {frames:?}"
    );
    assert!(
        text_frames(&frames).is_empty(),
        "no payload was invented for a venue that defines no ping"
    );
}

/// `watchers` raw-owner watch futures on one URL, advanced one window at a time.
/// Each window must add exactly one heartbeat frame to the socket, whichever
/// watcher won the claim — the shape that matters for a multi-market owner where
/// many raw watchers share a single venue socket (48 on a hyperliquid owner).
async fn one_heartbeat_per_window(watchers: usize) {
    use crate::pro::ws_client;
    let url = heartbeat_url(&format!("shared-raw-hb-{watchers}"));
    ws_client::mock_setup(&url);
    ws_client::set_heartbeat_interval_for_test(&url, Duration::from_millis(100));
    let mut handles = Vec::new();
    for _ in 0..watchers {
        let watch_url = url.clone();
        handles.push(tokio::spawn(async move {
            let mut core = HeartbeatCore {
                exchange: Exchange::new(None),
                calls: Arc::new(std::sync::Mutex::new(Vec::new())),
                reply: Some(Value::Str("ping".to_string())),
            };
            with_raw_owner_drain(core.watch_multiple(
                Value::Str(watch_url),
                Value::List(vec![Value::Str("trade".into())]),
                &[Value::Null],
            ))
            .await
        }));
    }
    tokio::task::yield_now().await;
    for window in 1..=4usize {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        let (sent, drivers, _) = ws_client::mock_heartbeat_state(&url);
        assert_eq!(
            drivers, watchers,
            "every watcher must be driving the same socket's heartbeat"
        );
        assert_eq!(
            sent.len(),
            window,
            "window {window} must add exactly one heartbeat frame, not one per watcher: {sent:?}"
        );
    }
    for handle in &handles {
        handle.abort();
    }
    for handle in handles {
        assert!(handle.await.unwrap_err().is_cancelled());
    }
    assert_eq!(
        ws_client::get_client(&url).unwrap().raw_heartbeat_drivers(),
        0,
        "cancelling the watchers must release the heartbeat claim"
    );
    ws_client::drop_client(&url);
}

#[tokio::test(start_paused = true)]
async fn two_raw_watchers_share_one_heartbeat_per_window() {
    one_heartbeat_per_window(2).await;
}

#[tokio::test(start_paused = true)]
async fn many_raw_watchers_share_one_heartbeat_per_window() {
    one_heartbeat_per_window(8).await;
}

#[tokio::test]
async fn the_generic_ping_stands_down_at_its_own_tick_boundary() {
    use crate::pro::ws_client;
    // The boundary that matters: the generic keep-alive task is on its own 30s
    // clock and cannot know about a raw owner. Its only access is this predicate,
    // so drive its step directly rather than waiting half a minute for a tick.
    //
    // The mock has no writer task, so a control ping piles up in the bounded
    // outgoing queue. That queue is the observable: 64 frames fit, the 65th fails.
    let free_url = heartbeat_url("generic-ping-free");
    ws_client::mock_setup(&free_url);
    let free = ws_client::get_client(&free_url).unwrap();
    for tick in 0..64 {
        assert!(
            free.generic_keepalive_step(),
            "control ping {tick} was refused early"
        );
    }
    assert!(
        !free.generic_keepalive_step(),
        "with no raw owner the generic control ping must keep flowing"
    );
    ws_client::drop_client(&free_url);

    let owned_url = heartbeat_url("generic-ping-owned");
    ws_client::mock_setup(&owned_url);
    let owned = ws_client::get_client(&owned_url).unwrap();
    let driver = owned.drive_heartbeat();
    // Far past the queue depth: every single tick must stand down silently.
    for tick in 0..200 {
        assert!(
            owned.generic_keepalive_step(),
            "a raw owner must silence the generic ping at every tick (tick {tick})"
        );
    }
    assert_eq!(
        owned.terminal_error(),
        None,
        "a stood-down generic tick must not queue, spend budget or fail anything"
    );
    drop(driver);
    assert!(
        owned.generic_keepalive_step(),
        "the last raw owner leaving must hand the socket back to the generic ping"
    );
    ws_client::drop_client(&owned_url);
}

#[tokio::test(start_paused = true)]
async fn raw_heartbeat_uses_merged_venue_keepalive_and_is_not_starved_by_backlog() {
    use crate::pro::ws_client;
    use futures::FutureExt;
    let url = heartbeat_url("venue-cadence-backlog");
    ws_client::mock_setup(&url);
    let client = ws_client::get_client(&url).unwrap();
    let mut exchange = Exchange::new(Some(
        serde_json::json!({
            "streaming": { "keepAlive": 18_000 }
        })
        .into(),
    ));
    exchange.initialize_properties(
        serde_json::json!({
            "streaming": { "keepAlive": 20_000 }
        })
        .into(),
    );
    let mut core = HeartbeatCore {
        exchange,
        calls: Arc::new(std::sync::Mutex::new(Vec::new())),
        reply: Some(Value::Str("ping".into())),
    };
    let args = [Value::Null];
    let mut watch = Box::pin(with_raw_owner_drain(core.watch_multiple(
        Value::Str(url.clone()),
        Value::List(vec![Value::Str("trade".into())]),
        &args,
    )));
    assert!(watch.as_mut().now_or_never().is_none());
    for _ in 0..17 {
        tokio::time::advance(Duration::from_secs(1)).await;
        client.mock_inject_raw(b"{}".to_vec(), false);
        assert!(watch.as_mut().now_or_never().is_none());
    }
    assert_eq!(ws_client::mock_sent_messages(&url).len(), 0);
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..512 {
        client.mock_inject_raw(b"{}".to_vec(), false);
    }
    assert!(watch.as_mut().now_or_never().is_none());
    let sent = ws_client::mock_sent_messages(&url);
    drop(watch);
    ws_client::drop_client(&url);
    assert_eq!(sent.to_json(), serde_json::json!(["ping"]));
}

#[tokio::test]
async fn invalid_raw_keepalive_is_explicit_and_leaves_no_heartbeat_driver() {
    use crate::pro::ws_client;
    use futures::FutureExt;
    for (index, invalid) in [
        Value::Int(0),
        Value::Int(-1),
        Value::Float(0.5),
        Value::Float(f64::NAN),
        Value::Float(f64::INFINITY),
        Value::Bool(false),
        Value::Str("30000".into()),
    ]
    .into_iter()
    .enumerate()
    {
        let url = heartbeat_url(&format!("invalid-keepalive-{index}"));
        ws_client::mock_setup(&url);
        let client = ws_client::get_client(&url).unwrap();
        let mut core = HeartbeatCore {
            exchange: Exchange::new(None),
            calls: Arc::new(std::sync::Mutex::new(Vec::new())),
            reply: Some(Value::Str("ping".into())),
        };
        core.streaming = Value::Map(indexmap::IndexMap::from_iter([(
            "keepAlive".into(),
            invalid,
        )]));
        let args = [Value::Null];
        let outcome = std::panic::AssertUnwindSafe(with_raw_owner_drain(core.watch_multiple(
            Value::Str(url.clone()),
            Value::List(vec![Value::Str("trade".into())]),
            &args,
        )))
        .catch_unwind()
        .now_or_never();
        // Cleanup also happens for the negative discriminator if admission regresses.
        ws_client::drop_client(&url);
        let error = match outcome {
            Some(Err(error)) => error,
            _ => panic!("invalid cadence {index} was not synchronously rejected"),
        };
        let message = error
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| error.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            message.contains("raw-owner streaming.keepAlive must be positive milliseconds"),
            "{message}"
        );
        assert_eq!(client.raw_heartbeat_drivers(), 0);
        assert!(client.control_ping_owned());
    }
}

#[tokio::test]
async fn concurrent_conflicting_cadences_admit_exactly_one_driver_without_poisoning() {
    use crate::pro::ws_client;
    let url = heartbeat_url("conflicting-keepalive");
    ws_client::mock_setup(&url);
    let client = ws_client::get_client(&url).unwrap();
    let start = Arc::new(std::sync::Barrier::new(3));
    let admitted = Arc::new(std::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for millis in [18_000, 20_000] {
        let client = client.clone();
        let start = start.clone();
        let admitted = admitted.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            let claim = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                client.drive_heartbeat_with_interval(Some(Duration::from_millis(millis)))
            }));
            // Keep the successful driver alive until both admission attempts finish.
            admitted.wait();
            claim
        }));
    }
    start.wait();
    admitted.wait();
    let claims: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(claims.iter().filter(|claim| claim.is_ok()).count(), 1);
    assert_eq!(client.raw_heartbeat_drivers(), 1);
    assert!(matches!(
        client.heartbeat_interval().as_millis(),
        18_000 | 20_000
    ));
    let rejected = claims
        .iter()
        .find_map(|claim| claim.as_ref().err())
        .unwrap();
    let message = rejected
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| rejected.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("raw watches sharing a URL disagree on keepAlive"),
        "{message}"
    );
    drop(claims);
    assert_eq!(client.raw_heartbeat_drivers(), 0);
    assert!(client.control_ping_owned());
    // A refused peer must not poison the mutex or pin a dead driver's cadence.
    let replacement = client.drive_heartbeat_with_interval(Some(Duration::from_millis(25_000)));
    assert_eq!(client.heartbeat_interval(), Duration::from_millis(25_000));
    drop(replacement);
    ws_client::drop_client(&url);
}

#[tokio::test(start_paused = true)]
async fn one_socket_claims_one_window_at_a_time() {
    use crate::pro::ws_client;
    let url = heartbeat_url("claim-raw-hb");
    ws_client::mock_setup(&url);
    ws_client::set_heartbeat_interval_for_test(&url, Duration::from_millis(100));
    let client = ws_client::get_client(&url).unwrap();
    let start = tokio::time::Instant::now();
    assert!(
        client.claim_heartbeat(start),
        "the first claim of a socket wins"
    );
    assert!(
        !client.claim_heartbeat(start),
        "a second watcher in the same window must not emit"
    );
    assert!(
        !client.claim_heartbeat(start + Duration::from_millis(99)),
        "the window must be the full cadence, not shorter"
    );
    assert!(
        client.claim_heartbeat(start + Duration::from_millis(100)),
        "the next window must be claimable"
    );
    ws_client::drop_client(&url);
}

#[tokio::test]
async fn a_raw_driver_stands_the_generic_ping_down() {
    use crate::pro::ws_client;
    let url = heartbeat_url("owner-raw-hb");
    ws_client::mock_setup(&url);
    let client = ws_client::get_client(&url).unwrap();
    assert!(
        client.control_ping_owned(),
        "an unwatched socket keeps the generic control-ping keep-alive"
    );
    let first = client.drive_heartbeat();
    let second = client.drive_heartbeat();
    assert!(
        !client.control_ping_owned(),
        "a raw owner must own the socket's single heartbeat"
    );
    assert_eq!(first.peers(), 2);
    drop(second);
    assert!(
        !client.control_ping_owned(),
        "one watcher leaving must not hand the socket back to the generic ping"
    );
    drop(first);
    assert!(
        client.control_ping_owned(),
        "the last watcher leaving must hand the socket back"
    );
    ws_client::drop_client(&url);
}

#[tokio::test(start_paused = true)]
async fn a_heartbeat_cannot_bypass_the_bounded_outgoing_queue() {
    use crate::pro::ws_client;
    use futures::FutureExt;
    // The heartbeat is a frame like any other: if it cannot go out, the socket has
    // failed and the raw drain must report that, not keep waiting in silence.
    let url = heartbeat_url("bounded-raw-hb");
    ws_client::mock_setup(&url);
    ws_client::set_heartbeat_interval_for_test(&url, Duration::from_millis(10));
    // Hold the generation directly: a terminal failure closes the socket, and a
    // closed socket is deliberately no longer reachable through the registry.
    let client = ws_client::get_client(&url).unwrap();
    let mut core = HeartbeatCore {
        exchange: Exchange::new(None),
        calls: Arc::new(std::sync::Mutex::new(Vec::new())),
        reply: Some(Value::Str("ping".to_string())),
    };
    let mut watch = Box::pin(with_raw_owner_drain(core.watch_multiple(
        Value::Str(url.clone()),
        Value::List(vec![Value::Str("trade".into())]),
        &[Value::Null],
    )));
    assert!(watch.as_mut().now_or_never().is_none());
    let mut caught = None;
    for _ in 0..100 {
        tokio::time::advance(Duration::from_millis(10)).await;
        let polled = std::panic::AssertUnwindSafe(watch.as_mut())
            .catch_unwind()
            .now_or_never();
        match polled {
            None => {}
            Some(result) => {
                caught = Some(result);
                break;
            }
        }
    }
    assert!(
        matches!(caught, Some(Err(_))),
        "a refused heartbeat must not leave the raw drain looping silently"
    );
    let error = client
        .terminal_error()
        .expect("the outgoing bound failure was swallowed");
    assert!(
        error.contains("mock capture full") || error.contains("outgoing"),
        "{error}"
    );
    drop(watch);
    ws_client::drop_client(&url);
}
