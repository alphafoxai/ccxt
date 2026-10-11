use super::*;
use std::time::Duration;
const BOUND: Duration = Duration::from_secs(3);

async fn peer(frames: usize) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/scoped", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
        for seq in 0..frames {
            if socket
                .send(Message::Text(format!(r#"{{"seq":{seq}}}"#)))
                .await
                .is_err()
            {
                return;
            }
        }
        while let Some(Ok(_)) = socket.next().await {}
    });
    (url, task)
}

#[tokio::test]
async fn scoped_raw_socket_burst_waits_for_consumer_without_lag() {
    let (url, peer) = peer(600).await;
    let (scope, mut rx) = ClientScope::new_with_raw_handoff();
    let client = scope.run(ensure_client(&url, None)).await.unwrap();
    tokio::time::timeout(BOUND, async {
        while rx.len() != 256 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let before_resume = Instant::now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let resumed = Instant::now();
    let mut blocked_frame = None;
    for expected in 0..600 {
        let frame = tokio::time::timeout(BOUND, rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap()["seq"],
            expected
        );
        assert_eq!(frame.url, url);
        assert_eq!(frame.generation, client.generation());
        if expected == 256 {
            blocked_frame = Some(frame.ingress_at);
        }
    }
    assert!(blocked_frame.unwrap() < resumed);
    assert!(before_resume <= resumed);
    assert!(client.terminal_error().is_none());
    let report = scope.close_and_join(BOUND).await;
    assert!(report.all_joined() && report.cleanup_complete, "{report:?}");
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn scoped_raw_stalled_consumer_is_terminal_not_silent_loss() {
    let (url, peer) = peer(300).await;
    let (scope, rx) = ClientScope::new_with_raw_handoff();
    let client = scope.run(ensure_client(&url, None)).await.unwrap();
    tokio::time::timeout(BOUND, async {
        while client.terminal_error().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(client
        .terminal_error()
        .unwrap()
        .contains("raw handoff deadline"));
    assert_eq!(rx.len(), 256);
    let report = scope.close_and_join(BOUND).await;
    assert!(report.cleanup_complete && !report.all_joined());
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn scoped_raw_receiver_drop_is_terminal() {
    let (url, peer) = peer(1).await;
    let (scope, rx) = ClientScope::new_with_raw_handoff();
    drop(rx);
    let client = scope.run(ensure_client(&url, None)).await.unwrap();
    tokio::time::timeout(BOUND, async {
        while client.terminal_error().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(client
        .terminal_error()
        .unwrap()
        .contains("raw handoff receiver closed"));
    assert!(scope.close_and_join(BOUND).await.cleanup_complete);
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn scoped_raw_full_queue_cancellation_joins_without_false_failure() {
    let (url, peer) = peer(600).await;
    let (scope, mut rx) = ClientScope::new_with_raw_handoff();
    let client = scope.run(ensure_client(&url, None)).await.unwrap();
    tokio::time::timeout(BOUND, async {
        while rx.len() != 256 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let start = Instant::now();
    let report = scope.close_and_join(BOUND).await;
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(report.all_joined() && report.cleanup_complete, "{report:?}");
    assert!(client.terminal_error().is_none());
    let accepted = rx.len();
    rx.close();
    let mut drained = 0;
    while rx.recv().await.is_some() {
        drained += 1;
    }
    assert_eq!(accepted, drained);
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_raw_cancelled_generation_cannot_record_a_late_admission_failure() {
    let (url, peer) = peer(300).await;
    let (scope, rx) = ClientScope::new_with_raw_handoff();
    let client = scope.run(ensure_client(&url, None)).await.unwrap();
    tokio::time::timeout(BOUND, async {
        while rx.len() != 256 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    scope.request_close();
    // Models a timeout/closed-receiver branch already selected on another
    // runtime thread when intentional cancellation wins the client lock.
    client.fail_raw_handoff("late admission failure".into());
    drop(rx);
    let report = scope.close_and_join(BOUND).await;
    assert!(client.terminal_error().is_none());
    assert!(report.all_joined() && report.cleanup_complete, "{report:?}");
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn scoped_raw_replacement_generation_remains_in_the_same_owned_receiver() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/reconnect", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        for i in 0..2 {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            socket
                .send(Message::Text(format!(r#"{{"seq":{i}}}"#)))
                .await
                .unwrap();
            while let Some(Ok(_)) = socket.next().await {}
        }
    });
    let (scope, mut rx) = ClientScope::new_with_raw_handoff();
    let first = scope.run(ensure_client(&url, None)).await.unwrap();
    let a = tokio::time::timeout(BOUND, rx.recv())
        .await
        .unwrap()
        .unwrap();
    first.request_close();
    let second = scope.run(ensure_client(&url, None)).await.unwrap();
    let b = tokio::time::timeout(BOUND, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.url, b.url);
    assert_eq!(a.generation, first.generation());
    assert_eq!(b.generation, second.generation());
    assert!(b.generation > a.generation);
    assert!(b.ingress_at >= a.ingress_at);
    let report = scope.close_and_join(BOUND).await;
    assert!(report.all_joined() && report.cleanup_complete, "{report:?}");
    tokio::time::timeout(BOUND, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn scoped_raw_isolation_and_legacy_loss_are_independent() {
    let (url_a, peer_a) = peer(400).await;
    let (url_b, peer_b) = peer(400).await;
    let (scope_a, mut rx_a) = ClientScope::new_with_raw_handoff();
    let (scope_b, mut rx_b) = ClientScope::new_with_raw_handoff();
    let mut legacy = subscribe_raw_all();
    let a = scope_a.run(ensure_client(&url_a, None)).await.unwrap();
    let b = scope_b.run(ensure_client(&url_b, None)).await.unwrap();
    for _ in 0..400 {
        let fa = tokio::time::timeout(BOUND, rx_a.recv())
            .await
            .unwrap()
            .unwrap();
        let fb = tokio::time::timeout(BOUND, rx_b.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fa.url, url_a);
        assert_eq!(fb.url, url_b);
        assert_eq!(fa.generation, a.generation());
        assert_eq!(fb.generation, b.generation());
    }
    assert!(matches!(
        legacy.try_recv(),
        Err(broadcast::error::TryRecvError::Lagged(_))
    ));
    assert!(scope_a.close_and_join(BOUND).await.cleanup_complete);
    assert!(scope_b.close_and_join(BOUND).await.cleanup_complete);
    tokio::time::timeout(BOUND, peer_a).await.unwrap().unwrap();
    tokio::time::timeout(BOUND, peer_b).await.unwrap().unwrap();
}
