use super::*;
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::time::Duration;
const BOUND: Duration = Duration::from_secs(2);

async fn server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(_)) = ws.next().await {}
    });
    (url, task)
}

#[tokio::test]
async fn scoped_join_observes_three_tasks_and_peer_eof() {
    let (url, peer) = server().await;
    let scope = ClientScope::new();
    let client = scope.run(ensure_client(&url, None)).await.unwrap();
    assert_eq!(client.tasks.lock().unwrap().handles.len(), 3);
    let report = scope.close_and_join(BOUND).await;
    assert!(report.all_joined(), "{report:?}");
    assert_eq!(report.clients, 1);
    assert_eq!(report.tasks_joined, 3);
    assert!(client.tasks.lock().unwrap().handles.is_empty());
    assert!(!REGISTRY.lock().unwrap().contains_key(&url));
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_join_retains_handles_and_retry_observes_them() {
    let (url, peer) = server().await;
    let scope = ClientScope::new();
    let client = scope.run(ensure_client(&url, None)).await.unwrap();
    // Deterministically park the join, poll it once, then cancel it. This is
    // independent of whether the aborted children have already been destroyed.
    let gate = client.join_gate.lock().await;
    let mut joining = Box::pin(scope.close_and_join(BOUND));
    assert!(matches!(
        futures::poll!(&mut joining),
        std::task::Poll::Pending
    ));
    drop(joining);
    assert_eq!(client.tasks.lock().unwrap().handles.len(), 3);
    drop(gate);
    let report = scope.close_and_join(BOUND).await;
    assert!(report.all_joined(), "{report:?}");
    assert_eq!(report.tasks_joined, 3);
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn concurrent_join_is_idempotent() {
    let (url, peer) = server().await;
    let scope = ClientScope::new();
    scope.run(ensure_client(&url, None)).await.unwrap();
    let (a, b) = tokio::join!(scope.close_and_join(BOUND), scope.close_and_join(BOUND));
    assert!(a.all_joined() && b.all_joined(), "{a:?} {b:?}");
    assert_eq!(a.tasks_joined, 3);
    assert_eq!(b.tasks_joined, 3);
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn timeout_is_not_success_and_retry_can_join() {
    let (url, peer) = server().await;
    let scope = ClientScope::new();
    let client = scope.run(ensure_client(&url, None)).await.unwrap();
    let gate = client.join_gate.lock().await;
    let report = scope.close_and_join(Duration::from_millis(1)).await;
    assert!(report.timed_out && !report.all_joined());
    drop(gate);
    let report = scope.close_and_join(BOUND).await;
    assert!(report.all_joined(), "{report:?}");
    assert_eq!(report.tasks_joined, 3);
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn foreign_scope_rejected_without_poison_or_cancellation() {
    let (url, peer) = server().await;
    let first = ClientScope::new();
    let client = first.run(ensure_client(&url, None)).await.unwrap();
    let second = ClientScope::new();
    let conflict = AssertUnwindSafe(second.run(ensure_client(&url, None)))
        .catch_unwind()
        .await;
    assert!(conflict.is_err());
    assert!(!client.is_closed());
    assert_eq!(get_client(&url).unwrap().generation(), client.generation());
    assert_eq!(second.close_and_join(BOUND).await.clients, 0);
    assert!(client.send_text("still-owned".to_owned()));
    assert!(first.close_and_join(BOUND).await.all_joined());
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn scope_cancels_pending_handshake_before_first_frame() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let scope = ClientScope::new();
    let connect = scope.run(ensure_client(&url, None));
    tokio::pin!(connect);
    let (mut stream, _) = tokio::select! {
        result = &mut connect => panic!("handshake unexpectedly settled: {}", result.is_ok()),
        accepted = listener.accept() => accepted.unwrap(),
    };
    scope.request_close();
    assert!(tokio::time::timeout(BOUND, connect).await.unwrap().is_err());
    let report = scope.close_and_join(BOUND).await;
    assert!(report.all_joined(), "{report:?}");
    assert_eq!(report.clients, 1);
    assert_eq!(report.tasks_joined, 0);
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    tokio::time::timeout(BOUND, stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn scope_drop_releases_socket_without_claiming_join() {
    let (url, peer) = server().await;
    let scope = ClientScope::new();
    let client = scope.run(ensure_client(&url, None)).await.unwrap();
    drop(scope);
    assert!(client.is_closed());
    assert!(!REGISTRY.lock().unwrap().contains_key(&url));
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
    assert_eq!(client.close_and_join(BOUND).await.tasks_joined, 3);
}

#[tokio::test]
async fn old_generation_cannot_remove_replacement_and_raw_observer_survives() {
    let url = "fixture-generation-replacement";
    let mut raw = subscribe_raw(url);
    let scope = ClientScope::new();
    let old = scope.run(async { ensure_slot(url) }).await;
    old.mock_inject_raw(b"old".to_vec(), false);
    assert_eq!(raw.recv().await.unwrap().generation, old.generation());
    assert!(old.close_and_join(BOUND).await.all_joined());
    let new = scope.run(async { ensure_slot(url) }).await;
    assert_ne!(old.generation(), new.generation());
    assert!(old.close_and_join(BOUND).await.all_joined());
    assert_eq!(get_client(url).unwrap().generation(), new.generation());
    new.mock_inject_raw(b"new".to_vec(), false);
    assert_eq!(raw.recv().await.unwrap().generation, new.generation());
    assert!(scope.close_and_join(BOUND).await.all_joined());
    drop(raw);
    reclaim_raw_buses();
    assert!(!RAW_BUS.lock().unwrap().contains_key(url));
}

#[tokio::test]
async fn outgoing_frame_count_and_bytes_are_independent_terminal_limits() {
    for (url, payload, accepted, error) in [
        (
            "fixture-out-count",
            "x".to_owned(),
            OUTGOING_CAPACITY,
            "queue",
        ),
        (
            "fixture-out-bytes",
            "x".repeat(MAX_PAYLOAD_BYTES),
            OUTGOING_BYTES / MAX_PAYLOAD_BYTES,
            "byte",
        ),
        (
            "fixture-out-payload",
            "x".repeat(MAX_PAYLOAD_BYTES + 1),
            0,
            "payload",
        ),
    ] {
        let scope = ClientScope::new();
        let client = scope.run(async { ensure_slot(url) }).await;
        for _ in 0..accepted {
            assert!(client.send_text(payload.clone()));
        }
        assert!(!client.send_text(payload));
        assert!(client.terminal_error().unwrap().contains(error));
        assert!(client.is_closed());
        assert_eq!(client.outgoing_budget.available_permits(), OUTGOING_BYTES);
        assert!(!scope.close_and_join(BOUND).await.all_joined());
    }
}

#[tokio::test]
async fn incoming_overflow_is_not_successful_empty() {
    let scope = ClientScope::new();
    let client = scope
        .run(async { ensure_slot("fixture-incoming-overflow") })
        .await;
    for _ in 0..=bounds::INCOMING_CAPACITY {
        client.mock_inject(Value::Null);
    }
    assert!(client.is_closed());
    assert!(client.terminal_error().unwrap().contains("capacity"));
    assert!(AssertUnwindSafe(client.next_message())
        .catch_unwind()
        .await
        .is_err());
    assert!(!scope.close_and_join(BOUND).await.all_joined());
}

#[tokio::test]
async fn raw_churn_reclaims_keys_without_breaking_live_observer() {
    let live = subscribe_raw("fixture-raw-live");
    for index in 0..1024 {
        let url = format!("fixture-raw-churn-{index}");
        drop(subscribe_raw(&url));
    }
    reclaim_raw_buses();
    let registry = RAW_BUS.lock().unwrap();
    assert!(registry.contains_key("fixture-raw-live"));
    assert!(!registry
        .keys()
        .any(|key| key.starts_with("fixture-raw-churn-")));
    drop(registry);
    drop(live);
    reclaim_raw_buses();
}

#[tokio::test]
async fn raw_overflow_reports_lag_and_payload_cap_is_not_bypassed() {
    let scope = ClientScope::new();
    let url = "fixture-raw-overflow";
    let client = scope.run(async { ensure_slot(url) }).await;
    let mut raw = subscribe_raw(url);
    for _ in 0..=RAW_BUS_CAPACITY {
        client.mock_inject_raw(b"pong".to_vec(), false);
        assert!(client.next_message().await.is_some());
    }
    assert_eq!(raw_overflow_count(url), 1);
    assert!(matches!(
        raw.recv().await,
        Err(broadcast::error::RecvError::Lagged(1))
    ));
    client.mock_inject_raw(vec![0; MAX_PAYLOAD_BYTES + 1], true);
    assert!(client.terminal_error().unwrap().contains("payload"));
    assert!(!scope.close_and_join(BOUND).await.all_joined());
}

#[tokio::test]
async fn socket_rejects_fragmented_aggregate_over_wire_limit() {
    use tokio_tungstenite::tungstenite::protocol::frame::{
        coding::{Data, OpCode},
        Frame,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let first = Frame::message(
            vec![b'x'; MAX_PAYLOAD_BYTES],
            OpCode::Data(Data::Text),
            false,
        );
        ws.send(Message::Frame(first)).await.unwrap();
        let second = Frame::message(vec![b'x'], OpCode::Data(Data::Continue), true);
        let _ = ws.send(Message::Frame(second)).await;
        while let Some(Ok(_)) = ws.next().await {}
    });
    let scope = ClientScope::new();
    let client = scope.run(ensure_client(&url, None)).await.unwrap();
    let result = tokio::time::timeout(
        BOUND,
        AssertUnwindSafe(client.next_message()).catch_unwind(),
    )
    .await
    .unwrap();
    assert!(
        result.is_err(),
        "oversized aggregate must fail, not be queued"
    );
    assert!(client.terminal_error().unwrap().contains("reader"));
    let report = scope.close_and_join(BOUND).await;
    assert_eq!(report.tasks_joined, 3);
    assert!(!report.all_joined() && !report.timed_out);
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn internal_panic_is_joined_but_not_reported_as_success() {
    let scope = ClientScope::new();
    let client = scope
        .run(async { ensure_slot("fixture-internal-panic") })
        .await;
    let handle = tokio::spawn(async { panic!("fixture internal panic") });
    while !handle.is_finished() {
        tokio::task::yield_now().await;
    }
    client.tasks.lock().unwrap().handles.push(handle);
    let report = scope.close_and_join(BOUND).await;
    assert_eq!(report.tasks_joined, 1);
    assert!(!report.all_joined() && !report.timed_out);
    assert!(report.failures[0].contains("panic"));
}

#[tokio::test]
async fn spare_payload_capacity_cannot_bypass_retained_byte_limits() {
    let scope = ClientScope::new();
    let outgoing = scope
        .run(async { ensure_slot("fixture-spare-outgoing") })
        .await;
    let mut text = String::with_capacity(OUTGOING_BYTES + 1);
    text.push('x');
    assert!(!outgoing.send_text(text));
    assert!(outgoing.terminal_error().unwrap().contains("byte budget"));

    let incoming = scope.run(async { ensure_slot("fixture-spare-raw") }).await;
    let mut raw = subscribe_raw(&incoming.url);
    let mut bytes = Vec::with_capacity(OUTGOING_BYTES + 1);
    bytes.extend_from_slice(b"pong");
    incoming.mock_inject_raw(bytes, false);
    let frame = raw.recv().await.unwrap();
    assert_eq!(frame.payload.capacity(), frame.payload.len());
    assert_eq!(frame.payload, b"pong");
    assert!(!scope.close_and_join(BOUND).await.all_joined());
}

#[tokio::test]
async fn stale_value_handles_and_subscription_references_cannot_mutate_replacement() {
    let scope = ClientScope::new();
    scope
        .run(async {
            let url = "fixture-stale-value-handle";
            let old = ensure_slot(url);
            old.set_subscription("channel", Value::Map(indexmap::IndexMap::new()));
            let handle = client_value(url);
            let old_subscriptions = old.subscriptions_value();
            let reference = old.reference();
            let field_reference = serde_json::to_string(&(old.reference(), "channel")).unwrap();
            let future = value_open_flight(&handle, Value::Str("flight".into()));
            assert!(old.close_and_join(BOUND).await.all_joined());
            let new = ensure_slot(url);
            new.set_subscription("keep", Value::Bool(true));
            new.flight_begin("flight");
            for stale in [&handle, &future] {
                value_resolve(stale, &[Value::Int(1), Value::Str("flight".into())]);
                value_reject(
                    stale,
                    &[Value::Str("bad".into()), Value::Str("flight".into())],
                );
                value_send(stale, &[Value::Str("unexpected".into())]);
                value_reset(stale);
                value_open_flight(stale, Value::Str("unwanted".into()));
            }
            value_subs_insert(&reference, "new", Value::Bool(true));
            value_subs_remove(&reference, "keep");
            value_sub_field_write(&field_reference, "changed", Value::Bool(true));
            assert!(new.take_settled(&["flight".into()]).is_none());
            assert!(new.flight_peek("flight").is_none());
            assert!(new.is_subscribed("keep"));
            assert!(!new.is_subscribed("new") && !new.is_subscribed("channel"));
            assert!(!new.flight_is_open("unwanted"));
            assert_eq!(new.outgoing_budget.available_permits(), OUTGOING_BYTES);
            assert_eq!(ws_await_flight(&future).await, Value::Null);
            assert_eq!(
                crate::get_value(&handle, &Value::Str("subscriptions".into())),
                Value::Map(indexmap::IndexMap::new())
            );
            assert_ne!(old_subscriptions, new.subscriptions_value());
            // Positive control: current-generation bridge still resolves correctly.
            let fresh = client_value(url);
            value_resolve(&fresh, &[Value::Int(2), Value::Str("flight".into())]);
            assert_eq!(
                new.take_settled(&["flight".into()]),
                Some(Ok(Value::Int(2)))
            );
        })
        .await;
    assert!(scope.close_and_join(BOUND).await.all_joined());
}

#[tokio::test]
async fn value_handle_from_foreign_live_scope_is_not_routable() {
    let owner = ClientScope::new();
    let url = "fixture-live-foreign-handle";
    let handle = owner.run(async { client_value(url) }).await;
    let foreign = ClientScope::new();
    foreign
        .run(async {
            value_send(&handle, &[Value::Str("not-owned".into())]);
            value_resolve(&handle, &[Value::Int(1), Value::Str("hash".into())]);
        })
        .await;
    let client = get_client(url).unwrap();
    assert_eq!(client.outgoing_budget.available_permits(), OUTGOING_BYTES);
    assert!(client.take_settled(&["hash".into()]).is_none());
    assert!(owner.close_and_join(BOUND).await.all_joined());
}

#[tokio::test]
async fn mock_capture_has_an_aggregate_byte_budget() {
    let scope = ClientScope::new();
    let client = scope
        .run(async { ensure_slot("fixture-mock-byte-bound") })
        .await;
    client.mock_enable();
    for _ in 0..5 {
        if !client.send_text("x".repeat(MAX_PAYLOAD_BYTES)) {
            break;
        }
    }
    assert!(client
        .terminal_error()
        .unwrap()
        .contains("mock capture byte budget"));
    assert!(client.mock_sent.lock().unwrap().len() < OUTGOING_CAPACITY);
    assert!(!scope.close_and_join(BOUND).await.all_joined());
}
