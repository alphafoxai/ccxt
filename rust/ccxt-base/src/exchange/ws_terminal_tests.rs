//! A terminal WebSocket outcome (peer reset, EOF, close) is an expected
//! transport result, not a Rust invariant violation. The watch drive loops
//! must hand it to the caller as the same `[NetworkError] …` error the
//! transpiled exception convention always produced — via `call_typed` — but
//! without running the panic hook, which used to log one worker panic per
//! socket reset.

use super::*;
use crate::exchange_generated::ExchangeBase;
use crate::pro::ws_client::{self, ClientScope};
use futures::{SinkExt, StreamExt};
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const BOUND: Duration = Duration::from_secs(5);

thread_local! {
    static HOOK_CALLS: Cell<usize> = const { Cell::new(0) };
}

/// Count panic-hook invocations on the calling thread, then defer to the
/// previous hook. Per-thread so sibling tests that panic on purpose cannot
/// race the count; `#[tokio::test]` is current-thread, so the watch, the
/// reader and the writer all run here.
fn hook_calls() -> usize {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            HOOK_CALLS.with(|calls| calls.set(calls.get() + 1));
            previous(info);
        }));
    });
    HOOK_CALLS.with(Cell::get)
}

struct PlainCore(Exchange);
impl std::ops::Deref for PlainCore {
    type Target = Exchange;
    fn deref(&self) -> &Exchange {
        &self.0
    }
}
impl std::ops::DerefMut for PlainCore {
    fn deref_mut(&mut self) -> &mut Exchange {
        &mut self.0
    }
}
impl DerivedExchange for PlainCore {}
impl ExchangeBase for PlainCore {
    fn call_dynamic<'a>(
        &'a mut self,
        method: &'a str,
        args: Vec<Value>,
    ) -> Pin<Box<dyn Future<Output = Value> + Send + 'a>> {
        Box::pin(async move { self.call_dynamic_base(method, args).await })
    }
}

#[derive(Clone, Copy, Debug)]
enum Path {
    /// `ws_drive_loop`: ordinary parsed dispatch.
    Parsed,
    /// `ws_run`'s raw-owner drain branch.
    RawOwner,
}

#[derive(Clone, Copy, Debug)]
enum PeerEnd {
    /// Drop the TCP stream without a closing handshake (production reset).
    Reset,
    /// Send a WebSocket Close frame.
    Close,
}

/// Run one real watch against a localhost peer that reads the subscribe frame
/// and then ends the socket, returning the URL and the typed watch result.
async fn watch_until_peer_ends(path: Path, end: PeerEnd) -> (String, crate::Result<Value>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        ws.next().await.unwrap().unwrap();
        match end {
            PeerEnd::Reset => drop(ws),
            PeerEnd::Close => {
                ws.send(Message::Close(None)).await.unwrap();
                while let Some(Ok(_)) = ws.next().await {}
            }
        }
    });
    let scope = ClientScope::new();
    let mut core = PlainCore(Exchange::new(None));
    let args = [Value::Str("{\"op\":\"subscribe\"}".into())];
    let watch = crate::runtime::call_typed(core.watch_multiple(
        Value::Str(url.clone()),
        Value::List(vec![Value::Str("trade".into())]),
        &args,
    ));
    let result = match path {
        Path::Parsed => tokio::time::timeout(BOUND, scope.run(watch)).await,
        Path::RawOwner => tokio::time::timeout(BOUND, scope.run(with_raw_owner_drain(watch))).await,
    }
    .unwrap_or_else(|_| panic!("{path:?} watch did not observe the peer {end:?}"));
    let report = scope.close_and_join(BOUND).await;
    assert!(report.cleanup_complete && !report.timed_out, "{report:?}");
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
    (url, result)
}

#[tokio::test]
async fn socket_reset_reaches_the_watch_caller_as_the_recorded_network_error() {
    for path in [Path::Parsed, Path::RawOwner] {
        let before = hook_calls();
        let (_, result) = watch_until_peer_ends(path, PeerEnd::Reset).await;
        let error = result.expect_err("a reset socket must not be a successful watch");
        // Byte-for-byte the production text (`fail_socket("reader", …)`) that
        // Market Node's transport classifier keys on.
        assert_eq!(error.kind, "NetworkError", "{path:?}: {error}");
        assert_eq!(
            error.to_string(),
            "[NetworkError] reader: WebSocket protocol error: \
             Connection reset without closing handshake",
            "{path:?}"
        );
        assert_eq!(hook_calls(), before, "{path:?} reset ran the panic hook");
    }
}

#[tokio::test]
async fn peer_close_still_raises_connection_closed_without_the_panic_hook() {
    for path in [Path::Parsed, Path::RawOwner] {
        let before = hook_calls();
        let (url, result) = watch_until_peer_ends(path, PeerEnd::Close).await;
        let error = result.expect_err("a closed socket must not be a successful watch");
        assert_eq!(
            error.to_string(),
            format!("[NetworkError] {url} websocket connection closed"),
            "{path:?}"
        );
        assert_eq!(hook_calls(), before, "{path:?} close ran the panic hook");
    }
}

#[tokio::test]
async fn next_message_returns_backlog_then_none_on_close_and_err_on_terminal() {
    // The incoming bound (`bounds::INCOMING_CAPACITY`), as in the raw-owner tests.
    const INCOMING_CAPACITY: usize = 1024;
    let before = hook_calls();
    let closed_url = format!("ws://next-message-close-{}.invalid", std::process::id());
    ws_client::mock_setup(&closed_url);
    let closed = ws_client::get_client(&closed_url).unwrap();
    closed.mock_inject(Value::Int(1));
    closed.request_close();
    // A normal close drains the backlog, then reports end of stream.
    assert_eq!(closed.next_message().await, Ok(Some(Value::Int(1))));
    assert_eq!(closed.next_message().await, Ok(None));
    assert_eq!(closed.terminal_error(), None);
    ws_client::drop_client(&closed_url);

    let failed_url = format!("ws://next-message-terminal-{}.invalid", std::process::id());
    ws_client::mock_setup(&failed_url);
    let failed = ws_client::get_client(&failed_url).unwrap();
    for _ in 0..=INCOMING_CAPACITY {
        failed.mock_inject(Value::Null);
    }
    let terminal = failed.terminal_error().expect("overflow is terminal");
    // Capacity keeps its non-retryable `ExchangeError` kind (`fail_capacity`).
    assert_eq!(
        terminal,
        format!("[ExchangeError] incoming WebSocket queue capacity exceeded ({INCOMING_CAPACITY})")
    );
    // The terminal cause wins over the backlog, verbatim, every time.
    assert_eq!(failed.next_message().await, Err(terminal.clone()));
    assert_eq!(failed.next_message().await, Err(terminal));
    assert_eq!(hook_calls(), before, "next_message ran the panic hook");
    ws_client::drop_client(&failed_url);
}
