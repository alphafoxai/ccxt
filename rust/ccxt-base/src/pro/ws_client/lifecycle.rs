//! Explicit caller ownership without changing the generated watch signatures.
use super::*;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

tokio::task_local! {
    static ACTIVE_SCOPE: Arc<ScopeInner>;
}
static NEXT_SCOPE: AtomicU64 = AtomicU64::new(1);
const MAX_SCOPE_CLIENTS: usize = 256;

struct ScopeInner {
    id: u64,
    state: Mutex<ScopeState>,
}
struct ScopeState {
    closed: bool,
    clients: Vec<Arc<ClientState>>,
}

/// Owns the exact socket generations accessed by futures passed to [`Self::run`].
/// A URL already owned by another scope (including an unscoped caller) is rejected,
/// never shared. Dropping the scope requests cancellation; only the async report
/// proves that internal reader/writer/keepalive tasks have actually terminated.
pub struct ClientScope {
    inner: Arc<ScopeInner>,
}
impl Default for ClientScope {
    fn default() -> Self {
        Self::new()
    }
}
impl ClientScope {
    /// Create an isolated, initially empty owner (at most 256 generations).
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ScopeInner {
                id: NEXT_SCOPE.fetch_add(1, Ordering::Relaxed),
                state: Mutex::new(ScopeState {
                    closed: false,
                    clients: Vec::new(),
                }),
            }),
        }
    }
    /// Drive a complete watch future in this scope. Spawned caller tasks must each
    /// call `run`: Tokio task locals are deliberately not inherited by `spawn`.
    pub async fn run<F: Future>(&self, future: F) -> F::Output {
        ACTIVE_SCOPE.scope(self.inner.clone(), future).await
    }
    /// Permanently prevent acquisition and request cancellation of owned sockets.
    /// Does not claim a WebSocket close handshake or task join.
    pub fn request_close(&self) {
        let mut scope = self.inner.state.lock().unwrap();
        scope.closed = true;
        for client in &scope.clients {
            client.request_close();
        }
    }
    /// Cancel and await all owned internal tasks within one total bound. Handles
    /// stay in ClientState when this future is cancelled/times out; a retry can
    /// finish observing them. Stop/join outer watch futures first to also prove
    /// that no connect future remains in flight.
    pub async fn close_and_join(&self, bound: Duration) -> ScopeJoinReport {
        self.request_close();
        let clients = self.inner.state.lock().unwrap().clients.clone();
        let deadline = tokio::time::Instant::now() + bound;
        let mut report = ScopeJoinReport {
            clients: clients.len(),
            cleanup_complete: true,
            ..Default::default()
        };
        for client in clients {
            if tokio::time::timeout_at(deadline, client.join_tasks())
                .await
                .is_err()
            {
                report.timed_out = true;
                // This caller did not observe this client's tasks (it may be
                // parked behind another joiner's gate lock). Even if another
                // joiner emptied the handles meanwhile, this report cannot
                // claim cleanup proof.
                report.cleanup_complete = false;
            }
            let tasks = client.tasks.lock().unwrap();
            report.tasks_joined += tasks.joined;
            report.failures.extend(tasks.failures.iter().cloned());
            if !tasks.handles.is_empty() {
                // Handles still in flight (cancelled/timed-out join, or a
                // caller-owned connect future that has not been awaited).
                report.cleanup_complete = false;
            }
            drop(tasks);
            if let Some(error) = client.error.lock().unwrap().clone() {
                report.failures.push(error);
            }
            if !report.timed_out {
                remove_exact(&client);
            }
        }
        reclaim_raw_buses();
        // Authoritative closure check: the scope must have refused further
        // acquisition (request_close ran at entry) and this call must have
        // observed every owned handle with no timeout.
        let closed = self.inner.state.lock().unwrap().closed;
        report.cleanup_complete = report.cleanup_complete && closed && !report.timed_out;
        report
    }
}
impl Drop for ClientScope {
    fn drop(&mut self) {
        self.request_close();
        for client in &self.inner.state.lock().unwrap().clients {
            remove_exact(client);
        }
        reclaim_raw_buses();
    }
}

/// Evidence from a scoped internal shutdown. Aborted-and-awaited tasks count as
/// joined; this is task destruction evidence, not a graceful wire close handshake.
///
/// `cleanup_complete` proves the scope is closed and this call awaited every
/// owned internal handle (success, panic and cancelled outcomes all count as
/// destruction evidence once awaited). It is independent of `all_joined()`:
/// terminal transport errors and panics keep `cleanup_complete == true` while
/// `all_joined()` stays false. Timeout, a cancelled join future, retained
/// handles or a lock-wait timeout all force `cleanup_complete == false`.
/// An empty scope is vacuously complete, but that only covers fork-held
/// internal tasks; caller-owned connect/watch futures need separate proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScopeFailureSource {
    TransportTerminal,
    TaskPanic,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeFailure {
    pub source: ScopeFailureSource,
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScopeJoinReport {
    pub clients: usize,
    pub tasks_joined: usize,
    pub timed_out: bool,
    pub failures: Vec<ScopeFailure>,
    pub cleanup_complete: bool,
}
impl ScopeJoinReport {
    /// True only after all handles were observed and no internal failure occurred.
    pub fn all_joined(&self) -> bool {
        !self.timed_out && self.failures.is_empty()
    }
}

#[derive(Default)]
pub(super) struct Tasks {
    pub handles: Vec<tokio::task::JoinHandle<()>>,
    pub joined: usize,
    pub failures: Vec<ScopeFailure>,
}
impl Drop for Tasks {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

pub(super) fn scope_id() -> u64 {
    ACTIVE_SCOPE.try_with(|scope| scope.id).unwrap_or(0)
}
pub(super) fn acquire_slot(url: &str) -> Arc<ClientState> {
    // Generated Value APIs carry failures by panic. Never panic while holding a
    // registry lock: an ownership rejection must not poison unrelated clients.
    fn acquire(url: &str) -> Result<Arc<ClientState>, &'static str> {
        if url.len() > 4096 {
            return Err("WebSocket URL exceeds limit");
        }
        match ACTIVE_SCOPE.try_with(Arc::clone) {
            Ok(scope) => {
                let mut owner = scope.state.lock().unwrap();
                if owner.closed {
                    return Err("WebSocket owner is closed");
                }
                let mut registry = REGISTRY.lock().unwrap();
                if let Some(client) = registry.get(url) {
                    if client.scope_id != scope.id {
                        return Err("WebSocket URL belongs to another owner");
                    }
                    if !client.is_closed() {
                        return Ok(client.clone());
                    }
                }
                if owner.clients.len() >= MAX_SCOPE_CLIENTS {
                    return Err("WebSocket owner generation limit");
                }
                if registry.len() >= 1024 && !registry.contains_key(url) {
                    return Err("WebSocket registry limit");
                }
                let client = new_slot(url, scope.id);
                owner.clients.push(client.clone());
                registry.insert(url.to_owned(), client.clone());
                Ok(client)
            }
            Err(_) => {
                let mut registry = REGISTRY.lock().unwrap();
                if let Some(client) = registry.get(url) {
                    if client.scope_id != 0 {
                        return Err("WebSocket URL belongs to a scoped owner");
                    }
                    if !client.is_closed() {
                        return Ok(client.clone());
                    }
                    client.request_close();
                }
                if registry.len() >= 1024 && !registry.contains_key(url) {
                    return Err("WebSocket registry limit");
                }
                let client = new_slot(url, 0);
                registry.insert(url.to_owned(), client.clone());
                Ok(client)
            }
        }
    }
    acquire(url).unwrap_or_else(|error| panic!("[NetworkError] {error}"))
}
fn remove_exact(client: &ClientState) {
    let mut registry = REGISTRY.lock().unwrap();
    if registry
        .get(&client.url)
        .is_some_and(|current| current.generation == client.generation)
    {
        registry.remove(&client.url);
    }
}
impl ClientState {
    /// Request cancellation without losing join handles. No replacement generation
    /// is affected. Idempotent; synchronous Drop paths may safely call this.
    pub fn request_close(&self) {
        let tasks = self.tasks.lock().unwrap();
        *self.closed.lock().unwrap() = true;
        *self.connected.lock().unwrap() = false;
        for handle in &tasks.handles {
            handle.abort();
        }
        drop(tasks);
        self.pending_rx.lock().unwrap().take();
        self.close_notify.notify_waiters();
        self.notify.notify_waiters();
    }
    /// The first terminal transport/budget failure, if any.
    pub fn terminal_error(&self) -> Option<String> {
        self.error
            .lock()
            .unwrap()
            .as_ref()
            .map(|error| error.message.clone())
    }
    pub(super) fn fail(&self, message: String) {
        self.fail_with_source(ScopeFailureSource::Internal, message);
    }
    pub(super) fn fail_with_source(&self, source: ScopeFailureSource, message: String) {
        {
            // Atomic first-terminal + fatal-retention decision. No path holds
            // error while acquiring tasks; release both before request_close.
            let mut tasks = self.tasks.lock().unwrap();
            let mut terminal = self.error.lock().unwrap();
            let failure = ScopeFailure { source, message };
            if let Some(first) = terminal.as_ref() {
                // Preserve the first message API, but never let an earlier
                // transport error hide a racing decoder/budget failure. At most
                // one suppressed Internal per client; actual JoinErrors below
                // are always appended, never deduplicated.
                if first.source != source
                    && source == ScopeFailureSource::Internal
                    && !tasks
                        .failures
                        .iter()
                        .any(|f| f.source == ScopeFailureSource::Internal)
                {
                    tasks.failures.push(failure);
                }
            } else {
                *terminal = Some(failure);
            }
        }
        self.incoming.lock().unwrap().clear();
        self.request_close();
    }
    async fn join_tasks(&self) {
        let _join = self.join_gate.lock().await;
        // No async ownership transfer: each poll borrows handles under the lock.
        // Serial and concurrent retrying join callers cannot double-poll a ready handle.
        poll_fn(|cx| {
            let mut tasks = self.tasks.lock().unwrap();
            let mut index = 0;
            while index < tasks.handles.len() {
                match Pin::new(&mut tasks.handles[index]).poll(cx) {
                    Poll::Ready(result) => {
                        drop(tasks.handles.swap_remove(index));
                        tasks.joined += 1;
                        if let Err(error) = result {
                            if !error.is_cancelled() {
                                tasks.failures.push(ScopeFailure {
                                    source: if error.is_panic() {
                                        ScopeFailureSource::TaskPanic
                                    } else {
                                        ScopeFailureSource::Internal
                                    },
                                    message: error.to_string(),
                                });
                            }
                        }
                    }
                    Poll::Pending => index += 1,
                }
            }
            if tasks.handles.is_empty() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
    /// Cancel and join this exact generation; cancellation retains its handles.
    pub async fn close_and_join(&self, bound: Duration) -> ScopeJoinReport {
        self.request_close();
        let timed_out = tokio::time::timeout(bound, self.join_tasks())
            .await
            .is_err();
        let tasks = self.tasks.lock().unwrap();
        let mut report = ScopeJoinReport {
            clients: 1,
            tasks_joined: tasks.joined,
            timed_out,
            failures: tasks.failures.clone(),
            cleanup_complete: !timed_out && tasks.handles.is_empty(),
        };
        drop(tasks);
        if let Some(error) = self.error.lock().unwrap().clone() {
            report.failures.push(error);
        }
        if !timed_out {
            remove_exact(self);
        }
        reclaim_raw_buses();
        report
    }
}
