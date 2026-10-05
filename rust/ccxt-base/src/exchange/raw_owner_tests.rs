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
