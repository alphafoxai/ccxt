use super::*;
use bounds::{MAX_DECODED_BYTES, SCOPED_MAX_PAYLOAD_BYTES};
use futures::FutureExt;
use std::{io::Write, panic::AssertUnwindSafe, time::Duration};
use tokio_tungstenite::tungstenite::protocol::{
    frame::{
        coding::{Data, OpCode},
        Frame,
    },
    Message,
};

const BOUND: Duration = Duration::from_secs(3);

enum Reply {
    Text(usize),
    Fragmented(usize),
    Expansion,
}

fn payload(size: usize) -> String {
    let prefix = r#"{"seq":1,"pad":""#;
    let suffix = "\"}";
    format!(
        "{prefix}{}{suffix}",
        "x".repeat(size - prefix.len() - suffix.len())
    )
}

async fn peer(reply: Reply, expected_rejection: bool) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        tokio::time::timeout(BOUND * 2, async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let first = socket.next().await.unwrap().unwrap();
            assert_eq!(first.into_text().unwrap(), "subscribe");
            let sent: Result<(), tokio_tungstenite::tungstenite::Error> = async {
            match reply {
                Reply::Text(size) => socket.send(Message::Text(payload(size))).await?,
                Reply::Fragmented(size) => {
                    let bytes = payload(size).into_bytes();
                    let middle = size / 2;
                    socket
                        .send(Message::Frame(Frame::message(
                            bytes[..middle].to_vec(),
                            OpCode::Data(Data::Text),
                            false,
                        )))
                        .await?;
                    socket
                        .send(Message::Frame(Frame::message(
                            bytes[middle..].to_vec(),
                            OpCode::Data(Data::Continue),
                            true,
                        )))
                        .await?;
                }
                Reply::Expansion => {
                    let mut gzip =
                        flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                    gzip.write_all(&vec![b'x'; MAX_DECODED_BYTES + 1]).unwrap();
                    let bytes = gzip.finish().unwrap();
                    assert!(bytes.len() < DEFAULT_MAX_PAYLOAD_BYTES);
                    socket.send(Message::Binary(bytes)).await?;
                }
            }
                Ok(())
            }.await;
            if let Err(error) = sent {
                // An over-limit header may be rejected before the remaining
                // bytes are written. Positive cases must still finish sending.
                assert!(expected_rejection && matches!(&error, tokio_tungstenite::tungstenite::Error::Io(e)
                    if matches!(e.kind(), std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset)), "unexpected fixture write: {error}");
            }
            while let Some(Ok(_)) = socket.next().await {}
        })
        .await
        .expect("bounded fixture teardown");
    });
    (url, task)
}

async fn physical(envelope: PayloadEnvelope, reply: Reply, expected_bytes: Option<usize>) {
    let (url, peer) = peer(reply, expected_bytes.is_none()).await;
    let scope = ClientScope::new();
    let admitted = scope.acquire_with_envelope(&url, envelope).unwrap();
    let mut raw = subscribe_raw(&url);
    // Exact ordinary ensure_client path, not a parallel connect implementation.
    let client = scope.run(ensure_client(&url, None)).await.unwrap();
    assert_eq!(admitted.generation(), client.generation());
    assert_eq!(client.payload_envelope(), envelope);
    assert!(client.send_text("subscribe".into()));
    let result = tokio::time::timeout(
        BOUND,
        AssertUnwindSafe(client.next_message()).catch_unwind(),
    )
    .await
    .unwrap();
    if let Some(size) = expected_bytes {
        let value = result
            .expect("accepted envelope must not throw")
            .expect("parsed frame");
        assert_eq!(
            crate::get_value(&value, &Value::Str("seq".into())),
            Value::Int(1)
        );
        let frame = tokio::time::timeout(BOUND, raw.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.payload.len(), size);
        assert_eq!(frame.payload.as_slice(), payload(size).as_bytes());
        assert!(client.terminal_error().is_none());
    } else {
        assert!(
            result.is_err(),
            "an oversize frame may not be returned or truncated"
        );
        let error = client.terminal_error().unwrap();
        assert!(
            error.starts_with("[ExchangeError]"),
            "capacity must not re-enter call_typed as NetworkError: {error}"
        );
        assert!(matches!(
            raw.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }
    let report = scope.close_and_join(BOUND).await;
    assert!(report.cleanup_complete, "{report:?}");
    if expected_bytes.is_some() {
        assert!(report.all_joined(), "{report:?}");
    } else {
        assert!(!report.failures.is_empty(), "{report:?}");
        assert!(
            report
                .failures
                .iter()
                .all(|f| f.source == ScopeFailureSource::Capacity),
            "{report:?}"
        );
    }
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn recorded_size_is_rejected_by_default_and_admitted_only_after_scope_opt_in() {
    physical(PayloadEnvelope::Default, Reply::Text(267291), None).await;
    physical(
        PayloadEnvelope::Scoped512KiB,
        Reply::Text(267291),
        Some(267291),
    )
    .await;
}

#[tokio::test]
async fn scope_envelopes_accept_exact_bound_and_reject_one_more() {
    for envelope in [PayloadEnvelope::Default, PayloadEnvelope::Scoped512KiB] {
        physical(
            envelope,
            Reply::Text(envelope.bytes()),
            Some(envelope.bytes()),
        )
        .await;
        physical(envelope, Reply::Text(envelope.bytes() + 1), None).await;
    }
}

#[tokio::test]
async fn fragmented_messages_and_compressed_expansion_remain_bounded() {
    physical(
        PayloadEnvelope::Scoped512KiB,
        Reply::Fragmented(SCOPED_MAX_PAYLOAD_BYTES + 1),
        None,
    )
    .await;
    physical(PayloadEnvelope::Default, Reply::Expansion, None).await;
    physical(PayloadEnvelope::Scoped512KiB, Reply::Expansion, None).await;
}

#[tokio::test]
async fn envelopes_cannot_change_after_implicit_admission_or_cross_owners() {
    let scope = ClientScope::new();
    let implicit = scope
        .run(async { ensure_slot("fixture-envelope-implicit-default") })
        .await;
    assert_eq!(implicit.payload_envelope(), PayloadEnvelope::Default);
    assert_eq!(scope.payload_envelope(), Some(PayloadEnvelope::Default));
    for url in [
        "fixture-envelope-implicit-default",
        "fixture-envelope-late-widen",
    ] {
        assert!(scope
            .acquire_with_envelope(url, PayloadEnvelope::Scoped512KiB)
            .is_err());
    }
    let larger = ClientScope::new();
    let admitted = larger
        .acquire_with_envelope("fixture-envelope-larger", PayloadEnvelope::Scoped512KiB)
        .unwrap();
    assert!(scope
        .acquire_with_envelope("fixture-envelope-larger", PayloadEnvelope::Scoped512KiB)
        .is_err());
    let adopted = larger
        .run(async { ensure_slot("fixture-envelope-larger") })
        .await;
    assert_eq!(adopted.generation(), admitted.generation());
    assert_eq!(adopted.payload_envelope(), PayloadEnvelope::Scoped512KiB);
    assert!(larger
        .acquire_with_envelope("fixture-envelope-larger-other", PayloadEnvelope::Default)
        .is_err());
    assert!(scope.close_and_join(BOUND).await.all_joined());
    assert!(larger.close_and_join(BOUND).await.all_joined());
    assert!(larger
        .acquire_with_envelope(
            "fixture-envelope-after-close",
            PayloadEnvelope::Scoped512KiB
        )
        .is_err());
}

#[tokio::test]
async fn an_earlier_transport_error_never_hides_capacity_or_internal_evidence() {
    let scope = ClientScope::new();
    let client = scope
        .run(async { ensure_slot("fixture-transport-then-capacity") })
        .await;
    client.fail_with_source(
        ScopeFailureSource::TransportTerminal,
        "[NetworkError] gone".into(),
    );
    for _ in 0..3 {
        client.fail_capacity("[NetworkError] payload exceeds bound".into());
        client.fail("[ExchangeError] decoder invariant".into());
    }
    let report = scope.close_and_join(BOUND).await;
    assert!(report.cleanup_complete);
    for source in [
        ScopeFailureSource::TransportTerminal,
        ScopeFailureSource::Capacity,
        ScopeFailureSource::Internal,
    ] {
        assert_eq!(
            report
                .failures
                .iter()
                .filter(|f| f.source == source)
                .count(),
            1,
            "{report:?}"
        );
    }
}

#[tokio::test]
#[ignore = "explicit 256MiB Linux cgroup capacity proof; never part of fast suite"]
async fn scoped_ring_retention_is_bounded_and_overwrite_is_explicit() {
    let scope = ClientScope::new();
    let client = scope
        .acquire_with_envelope("fixture-large-ring", PayloadEnvelope::Scoped512KiB)
        .unwrap();
    let mut per_url = subscribe_raw(&client.url);
    let mut all = subscribe_raw_all();
    let bytes = vec![b'x'; SCOPED_MAX_PAYLOAD_BYTES];
    let frames = RAW_ALL_CAPACITY + RAW_BUS_CAPACITY + 1;
    for _ in 0..frames {
        client.receive_payload(bytes.clone(), false);
        let decoded = client.next_message().await.unwrap();
        assert_eq!(
            decoded.as_str().map(str::len),
            Some(SCOPED_MAX_PAYLOAD_BYTES)
        );
        assert_eq!(client.incoming.lock().unwrap().bytes(), 0);
    }
    assert_eq!(
        RAW_BUS
            .lock()
            .unwrap()
            .get(&client.url)
            .unwrap()
            .sender
            .len(),
        RAW_BUS_CAPACITY
    );
    assert_eq!(
        raw_all_bus_if_open().unwrap().sender.len(),
        RAW_ALL_CAPACITY
    );
    assert_eq!(
        per_url.try_recv().unwrap_err(),
        broadcast::error::TryRecvError::Lagged((frames - RAW_BUS_CAPACITY) as u64)
    );
    assert_eq!(
        all.try_recv().unwrap_err(),
        broadcast::error::TryRecvError::Lagged((frames - RAW_ALL_CAPACITY) as u64)
    );
    assert_eq!(
        per_url.try_recv().unwrap().payload.len(),
        SCOPED_MAX_PAYLOAD_BYTES
    );
    assert_eq!(
        all.try_recv().unwrap().payload.len(),
        SCOPED_MAX_PAYLOAD_BYTES
    );
    assert!(scope.close_and_join(BOUND).await.all_joined());
}

#[tokio::test]
async fn direct_and_proxy_handshake_capacity_remains_typed_and_nontransport() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for case in 0..3 {
        let through_proxy = case != 0;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let url = if through_proxy {
            "ws://fixture.invalid/handshake-limit".to_string()
        } else {
            format!("ws://{address}/handshake-limit")
        };
        let proxy = through_proxy.then(|| format!("http://{address}"));
        let peer = tokio::spawn(async move {
            tokio::time::timeout(BOUND, async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                async fn headers(stream: &mut tokio::net::TcpStream) {
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        assert!(request.len() < 8192);
                        let mut byte = [0];
                        stream.read_exact(&mut byte).await.unwrap();
                        request.push(byte[0]);
                    }
                }
                if through_proxy {
                    headers(&mut stream).await;
                    if case == 2 {
                        let reply = format!(
                            "HTTP/1.1 200 Connection established\r\nX-Limit: {}",
                            "x".repeat(8193)
                        );
                        stream.write_all(reply.as_bytes()).await.unwrap();
                        return;
                    }
                    stream
                        .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                        .await
                        .unwrap();
                }
                headers(&mut stream).await;
                // More than tungstenite's handshake parser permits, without a
                // huge payload or public connection. CapacityError::TooManyHeaders.
                let mut response = "HTTP/1.1 101 Switching Protocols\r\n".to_string();
                for i in 0..256 {
                    response.push_str(&format!("X-Bound-{i}: x\r\n"));
                }
                response.push_str("\r\n");
                stream.write_all(response.as_bytes()).await.unwrap();
            })
            .await
            .unwrap();
        });
        let scope = ClientScope::new();
        let result = tokio::time::timeout(BOUND, scope.run(ensure_client(&url, proxy)))
            .await
            .unwrap();
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("oversized handshake accepted"),
        };
        let report = scope.close_and_join(BOUND).await;
        peer.await.unwrap();
        assert!(report.cleanup_complete, "{report:?}");
        assert!(
            error.starts_with("[ExchangeError]"),
            "capacity became retryable: {error}; {report:?}"
        );
        assert!(
            !report.failures.is_empty(),
            "missing pre-socket failure evidence: {report:?}"
        );
        assert!(
            report
                .failures
                .iter()
                .all(|failure| failure.source == ScopeFailureSource::Capacity),
            "{report:?}"
        );
    }
}

#[tokio::test]
async fn peer_closed_during_handshake_keeps_existing_network_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/early-close", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        drop(stream);
    });
    let scope = ClientScope::new();
    let result = tokio::time::timeout(BOUND, scope.run(ensure_client(&url, None)))
        .await
        .unwrap();
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("closed handshake accepted"),
    };
    let report = scope.close_and_join(BOUND).await;
    peer.await.unwrap();
    assert!(error.starts_with("[NetworkError]"), "{error}");
    assert!(
        report.cleanup_complete && report.failures.is_empty(),
        "{report:?}"
    );
}
