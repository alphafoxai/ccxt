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
    /// Explicit payload envelope admitted by this scope, if any caller declared
    /// one. `None` means every socket this scope creates keeps the default
    /// envelope. Recorded on the first explicit admission and never silently
    /// changed afterwards.
    payload_envelope: Option<PayloadEnvelope>,
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
                    payload_envelope: None,
                }),
            }),
        }
    }
    /// Drive a complete watch future in this scope. Spawned caller tasks must each
    /// call `run`: Tokio task locals are deliberately not inherited by `spawn`.
    pub async fn run<F: Future>(&self, future: F) -> F::Output {
        ACTIVE_SCOPE.scope(self.inner.clone(), future).await
    }
    /// Admit the socket for `url` under an explicit payload envelope, recording the
    /// choice on this scope for future conflict detection.
    ///
    /// This is the validated admission seam for a caller that needs more than the
    /// default 256 KiB wire frame/message envelope: nothing expands implicitly, and a
    /// request that disagrees with the envelope this scope already recorded, or with
    /// the envelope already recorded on `url`'s socket, is refused. The ambient
    /// generated-watch path (`ensure_client`) keeps the default 256 KiB and adopts an
    /// already admitted slot as-is. Returns the exact generation that owns `url`, so
    /// `Self::run` drives that same socket.
    pub fn acquire_with_envelope(
        &self,
        url: &str,
        envelope: PayloadEnvelope,
    ) -> Result<Arc<ClientState>, String> {
        acquire_in_scope(&self.inner, url, Some(envelope))
    }
    /// Payload envelope this scope explicitly admitted, if any.
    pub fn payload_envelope(&self) -> Option<PayloadEnvelope> {
        self.inner.state.lock().unwrap().payload_envelope
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
    /// The connection is gone: EOF, tungstenite I/O, closed/already-closed, or a
    /// reset without a closing handshake. The owner may replace the socket after
    /// cleanup proof.
    TransportTerminal,
    /// An admitted capacity/admission bound was exhausted, including a wire frame or
    /// message above the scope's declared payload envelope, a compressed payload
    /// whose expansion exceeds the decode bound, parsed inbound count/byte
    /// exhaustion, the outgoing queue/payload/byte budgets and mock capture bounds.
    ///
    /// This is independent of the transport: a capacity failure is explicit evidence
    /// that the declared envelope (or an unchanged queue bound) was too small for
    /// observed load, and it is never remapped to `TransportTerminal` for a restart.
    Capacity,
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
    acquire_ambient(url).unwrap_or_else(|error| panic!("[NetworkError] {error}"))
}

/// Explicit-envelope admission for the ambient/task-local caller path. Unlike
/// [`acquire_slot`] this never panics on an envelope conflict.
pub(super) fn acquire_slot_with_envelope(
    url: &str,
    envelope: PayloadEnvelope,
) -> Result<Arc<ClientState>, String> {
    match ACTIVE_SCOPE.try_with(Arc::clone) {
        Ok(scope) => acquire_in_scope(&scope, url, Some(envelope)),
        // An explicit non-default envelope has no owner to be scoped to; refusing
        // here is what keeps "opt-in" explicit instead of ambient.
        Err(_) if envelope == PayloadEnvelope::Default => acquire_unscoped(url),
        Err(_) => Err("WebSocket payload envelope request requires a ClientScope owner".to_owned()),
    }
}

/// Admit the socket for `url`, and for an explicit request also its payload envelope.
///
/// `requested == None` is the ambient path used by the generated watch code: an
/// already admitted slot is adopted as-is and a new slot gets the scope's recorded
/// envelope (default when the scope declared none). `requested == Some(envelope)` is
/// the explicit owner opt-in: a request that disagrees with the scope's recorded
/// envelope, or with the envelope already recorded on `url`'s socket, is refused
/// instead of silently shared.
fn acquire_in_scope(
    scope: &Arc<ScopeInner>,
    url: &str,
    requested: Option<PayloadEnvelope>,
) -> Result<Arc<ClientState>, String> {
    // Generated Value APIs carry failures by panic. Never panic while holding a
    // registry lock: an ownership rejection must not poison unrelated clients.
    if url.len() > 4096 {
        return Err("WebSocket URL exceeds limit".to_owned());
    }
    let mut owner = scope.state.lock().unwrap();
    if owner.closed {
        return Err("WebSocket owner is closed".to_owned());
    }
    let mut registry = REGISTRY.lock().unwrap();
    if let Some(client) = registry.get(url) {
        if client.scope_id != scope.id {
            return Err("WebSocket URL belongs to another owner".to_owned());
        }
        if let Some(requested) = requested {
            let admitted = client.payload_envelope;
            if admitted != requested {
                return Err(format!(
                    "WebSocket URL is admitted under payload envelope {} not {}",
                    admitted.name(),
                    requested.name()
                ));
            }
        }
        if !client.is_closed() {
            return Ok(client.clone());
        }
    }
    if let Some(requested) = requested {
        if let Some(chosen) = owner.payload_envelope {
            if chosen != requested {
                return Err(format!(
                    "WebSocket scope already admitted payload envelope {} not {}",
                    chosen.name(),
                    requested.name()
                ));
            }
        }
    }
    if owner.clients.len() >= MAX_SCOPE_CLIENTS {
        return Err("WebSocket owner generation limit".to_owned());
    }
    if registry.len() >= 1024 && !registry.contains_key(url) {
        return Err("WebSocket registry limit".to_owned());
    }
    let envelope = match requested {
        Some(requested) => {
            owner.payload_envelope = Some(requested);
            requested
        }
        None => {
            // Implicit admission also freezes the scope's envelope. A later
            // explicit call for another URL must not widen an already-used owner.
            let envelope = owner.payload_envelope.unwrap_or(PayloadEnvelope::Default);
            owner.payload_envelope = Some(envelope);
            envelope
        }
    };
    let client = new_slot(url, scope.id, envelope);
    owner.clients.push(client.clone());
    registry.insert(url.to_owned(), client.clone());
    Ok(client)
}

fn acquire_ambient(url: &str) -> Result<Arc<ClientState>, String> {
    match ACTIVE_SCOPE.try_with(Arc::clone) {
        Ok(scope) => acquire_in_scope(&scope, url, None),
        Err(_) => acquire_unscoped(url),
    }
}

fn acquire_unscoped(url: &str) -> Result<Arc<ClientState>, String> {
    if url.len() > 4096 {
        return Err("WebSocket URL exceeds limit".to_owned());
    }
    let mut registry = REGISTRY.lock().unwrap();
    if let Some(client) = registry.get(url) {
        if client.scope_id != 0 {
            return Err("WebSocket URL belongs to a scoped owner".to_owned());
        }
        if !client.is_closed() {
            return Ok(client.clone());
        }
        client.request_close();
    }
    if registry.len() >= 1024 && !registry.contains_key(url) {
        return Err("WebSocket registry limit".to_owned());
    }
    let client = new_slot(url, 0, PayloadEnvelope::Default);
    registry.insert(url.to_owned(), client.clone());
    Ok(client)
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
                // one suppressed failure of each fatal class per client; actual
                // JoinErrors below are always appended, never deduplicated.
                if first.source != source
                    && matches!(
                        source,
                        ScopeFailureSource::Internal | ScopeFailureSource::Capacity
                    )
                    && !tasks.failures.iter().any(|f| f.source == source)
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
