# Rust WebSocket transport ownership and resource limits

This is a fork-local transport change, not a venue capability or capacity acceptance.
`src/pro/ws_client.rs` is hand-written infrastructure (see `HAND_WRITTEN_SIBLINGS`
in `build/rustTranspiler.ts`), not a generated exchange implementation.

## Ownership

Use one `ClientScope` per caller-owned collector. Drive each complete watch future
with `scope.run(future)`; task locals are not inherited by `tokio::spawn`.
Generated watch signatures do not change. Each scope tracks exact client generations,
including slots acquired before connecting or before the first inbound frame. A URL
owned by another scope/unscoped caller is rejected, not silently shared. Value client,
future and subscription references also carry scope/generation identity. Bridge operations
retain the validated Arc rather than re-looking up the URL; a stale handle cannot send,
resolve, reset, open a flight or mutate subscriptions on a replacement generation.

Stop and join caller-owned watch/driver tasks, then await
`scope.close_and_join(Duration)`. `ScopeJoinReport` reports client and awaited-task
counts, timeout and failures, plus a `cleanup_complete` bool proving the scope
is closed and this call awaited every owned handle: success, panic and
cancelled outcomes all count as destruction evidence once awaited, while
timeout, a cancelled join future, retained handles or a lock-wait timeout
force false. An empty scope is vacuously complete but covers only fork-held
internal tasks. `all_joined()` requires no timeout and no internal
failure and is unchanged: terminal transport errors keep `cleanup_complete`
true while `all_joined()` stays false. Aborted **and awaited** reader/writer/keepalive tasks count as joined: this
proves task destruction, not a graceful WebSocket close handshake.

Failures are typed `ScopeFailure { source, message }`, preserving the original text.
`TransportTerminal` is assigned at EOF, for tungstenite I/O, closed/already-closed,
and reset-without-closing-handshake variants, and for parsed incoming count/byte
exhaustion (the merged price-queue classification change). Decoder, UTF-8, payload,
outgoing budget, other protocol and unknown errors remain `Internal`; awaited task
panics are `TaskPanic`.
No classification parses error text. The first terminal message remains available via
`terminal_error()`. A racing later Internal cannot be hidden by that first message:
one suppressed Internal per client is retained in `Tasks.failures`; actual non-cancelled
JoinErrors are always appended. This is bounded fatal-cause evidence, not a complete
per-error audit. Record locking is tasks → error; both are released before cancellation.
Consumers must inspect every failure independently of cleanup proof.

`request_close`, `drop_client`, and scope Drop only request cancellation. They must
not be presented as join proof. Join handles remain inside `ClientState` across
cancelled/timed-out join futures, so a retained scope/client can retry. Dropping the
scope removes its exact registry generations; it never closes a replacement or a
foreign owner. No detached cleanup worker is spawned.

Connect and proxy handshake have a 30-second ceiling, are cancellable by scope close,
and use the same tungstenite limits. The scope join report concerns internal spawned
tasks, **not** caller-owned connect/watch futures: join those outer futures first.

## Explicit public raw-owner drain

`exchange::with_raw_owner_drain(watch_future)` opts a complete public watch future
into continuously consuming the parsed queue without venue dispatch/settling. It is
task-local, survives migration, does not cross `tokio::spawn`, and is removed on
cancellation/panic/return. The legacy internal `rawOwnerDrain` marker is still
supported and stripped before wire serialization; the scoped form works even when
a venue does not forward params. Ordinary watch behavior is unchanged.

This narrow seam is for callers already validating reader-boundary raw frames and
handling raw lag/transport terminal failures. Use `ClientScope` separately for
transport ownership/cleanup. A URL must be exclusively raw-owned: several
subscription futures may discard parsed rows on the same URL (e.g. Hyperliquid
per-coin subscriptions), because the authoritative raw broadcast is independent.
Never mix an ordinary parsed consumer on that URL. Authentication, venue delayed
coroutines, nested watches and parsed callback-dependent feeds are
not supported in raw mode; those paths must retain normal dispatch. It does not
provide another socket owner or change semantic validation.

The reader yields every 32 frames to give ready parsed/raw consumers an opportunity
to run during a buffered burst. This is scheduling fairness, not an increased queue
bound or a promise that a loaded host can keep up. Genuine lag remains observable.

## Opt-in scoped raw admission

`ClientScope::new_with_raw_handoff()` returns `(ClientScope, mpsc::Receiver<RawFrame>)`.
The scope owns one FIFO of **256** raw frames across all its socket generations,
created before any watch starts. Only readers acquired in `scope.run(...)` send to
it. Default `ClientScope::new()`, ordinary watch APIs and the legacy lossy URL/global
broadcast taps keep their behavior; they are not the authoritative path for this mode.

The real reader decodes and stamps each frame at initial ingress, then reserves a
slot asynchronously. Its original URL, generation, monotonic instant and wall-clock
stamp survive the wait. Full admission backpressures the socket reader for at most
**one second**; this fixed ceiling is a failure bound, not a freshness promise.
Deadline or receiver loss is `Internal`, not recoverable source discontinuity or a
fallback to the broadcast bus. This mode requires a live consumer and the parsed
raw-owner drain above. Synchronous `mock_inject_raw` on an opted-in client fails
explicitly instead of bypassing async admission; real socket tests cover this mode.

No standard/registry/scope mutex is held across admission. Cancellation closes the
exact generation and serializes against final send and failure recording; a queued
cancellation cannot leave a post-close send or a false admission failure. The FIFO
receiver may call `close()` and drain already accepted frames after source close.
A retained scope keeps its sender, so callers should not await EOF without closing
admission themselves. The usual caller-task and internal-scope join proof remains
required; no new cleanup, forwarding or collector task is introduced.

Worst-case retained raw payload is 256 × 256 KiB = 64 MiB **per opted-in scope**, plus
one bounded raw payload and decoded value per active reader waiting for admission.
Legacy tap storage, parsed queues, socket buffers and application relays are separate
budgets; this is not a total RSS bound. Legacy observers can still report `Lagged`
independently without corrupting a successfully admitted scoped FIFO.

Tests in `src/pro/ws_client/scoped_raw_tests.rs` cover 600 real localhost frames with
consumer suspension, deadline, receiver loss, cancel/join, cancel/failure ordering,
replacement generation, scope isolation and legacy lag. They establish transport
contracts only, not production capacity or full application recovery.

## Raw-owner heartbeat

Previously a raw-owned socket sent only subscription frames and generic control
pings, with no venue-level heartbeat dispatch. Bitget requires literal text `ping`
every 30s; a bounded one-market target probe reset after approximately two minutes
of valid trades. This is protocol-mismatch evidence, not a parsed-path/live comparison
or proof that a new binary has recovered. The raw loop now owns the socket heartbeat
while alive, and the frame is the venue's own: the runtime
dispatches `ping` on the concrete core with the live client handle, exactly as
`Exchange.client (url)` binds `this.ping` onto a `WsClient` and `Client.onPingInterval`
calls it. A venue with no `ping` (binance, gate) answers Null and the runtime sends
the same RFC-6455 control frame the generic keep-alive sends. No venue URL or payload
is hardcoded, and no heartbeat path bypasses the outgoing queue/byte bounds — a
refused frame fails the socket and the drain reports the terminal cause.

Cadence comes from merged `describe().streaming.keepAlive` plus constructor
`streaming` configuration, matching TS's reflective constructor assignment
(`this[property]`) for those fields. The pin declares OKX/Bybit18s, Hyperliquid20s,
Binance180s; Bitget and absent values use TS `Client.keepAlive`'s30s default.
This narrow raw-owner seam accepts only positive integral milliseconds or an absent
value. Unlike TS, `0`/`false` do not disable its heartbeat: unsupported raw-mode
values and disagreement on a shared URL fail explicitly. TS's additional
`options.ws.keepAlive` override is not implemented here; no admitted price owner
uses it. This affects raw-owned sockets only; ordinary parsed keepalive behavior
is unchanged. A due timer is polled
before discard rows so a buffered price stream cannot suppress the heartbeat. A
late poll emits one heartbeat, never a catch-up burst.

**The missing-pong deadline remains unimplemented.** TS closes a client whose
`lastPong` is older than `keepAlive * maxPingPongMisses` (60s by default). This port
records control-pong time but has no corresponding application-pong parser or
comparison on either path. This change does not invent that terminal-error surface;
consumers must not infer blackhole detection or venue liveness from ping emission.

The heartbeat has exactly one owner per socket. Several raw watchers may share a URL
(per-coin subscriptions on one hyperliquid socket), so the window is claimed on the
socket by monotonic instant and only the claimer emits: one frame per window, not one
per watcher. While a raw driver is alive the generic control-ping task stands down, so
no socket is pinged twice per window by two independent owners; the driver is released
on return, panic or cancellation, so a cancelled raw watch can never leave the socket
unpinged.

## Fixed limits

| Boundary | Limit |
| --- | --- |
| Wire message, including fragmented aggregate | 256 KiB |
| Individual wire frame | 256 KiB |
| Expanded gzip/raw-deflate payload | 1 MiB |
| Parsed inbound queue | 1024 entries and 4 MiB accounting budget |
| Outgoing queue | 64 entries and 1 MiB retained payload-capacity permits, including the in-flight write |
| Tungstenite write buffer | 1 MiB |
| Raw URL bus | 64 entries × 256 KiB = 16 MiB retained payload |
| Global raw bus | 256 entries × 256 KiB = 64 MiB retained payload |
| URL length | 4096 bytes |
| Active raw-observer URL keys | 256 |
| Registry client slots | 1024 |
| Generations acquired by one scope | 256 |

The parsed accounting includes a conservative structural estimate, not serialized
JSON bytes alone. It is not an allocator/RSS measurement. Raw bounds are payload
bounds: URL/record metadata and consumer-owned clones are additional. Raw payload
vectors shed spare capacity before retention; outgoing permits charge retained
String/Vec capacity, not just length. Queue limits
do not bound venue caches, resolved/subscription/flight maps, or the entire process.
A reconnecting long-lived caller must finish its scope and create another before the
generation ceiling; exceeding a limit is an explicit failure, not implicit recovery.

Payload/decoded/parsed/outgoing overflow terminates that client and records a terminal
error. The watch drive observes a failure instead of a successful empty result.
Malformed/oversized compressed input cannot evade the expansion limit by falling
back to another format. Raw broadcast saturation instead preserves existing broadcast
semantics: overwrite oldest, increment overflow counter, and report `Lagged` to
lagging receivers. Readers must resynchronize on any loss; no continuity is inferred.

Raw URL keys are created only by explicit subscriptions, never by ordinary inbound
traffic. Subscribe/publish/close and explicit `reclaim_raw_buses()` reclaim keys with
no receivers. Active subscribers keep their channel across socket generation changes.
The global observer has a single fixed bus, not one entry per URL.

## Validation scope

Tests use in-process socket-free fixtures or physical `127.0.0.1` WebSocket peers.
No credentials or public exchange requests are needed. Explicit test commands and
actual results belong in the PR evidence; a bounded localhost test does not prove
recovery scheduling, whole-catalog admission, Engine integration, public exchange
behavior, production RSS or a long-window soak.

Original scoped-transport validation (before the raw-owner heartbeat change) ran
from the fork root with
`RUSTC_WRAPPER= CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0`:

```sh
rustfmt --edition 2021 --check rust/ccxt-base/src/pro/ws_client.rs
cargo test --locked --offline --manifest-path rust/ccxt-base/Cargo.toml
cargo test --locked --offline --manifest-path rust/ccxt-base/Cargo.toml --features transpiled-base
cargo clippy --locked --offline --manifest-path rust/ccxt-base/Cargo.toml --all-targets -- -D warnings
cargo clippy --locked --offline --manifest-path rust/ccxt-base/Cargo.toml --features transpiled-base --all-targets -- -D warnings
```

Those original results: **80 default tests** (23 lifecycle), **85 transpiled-base tests** passed; three pre-existing doctests remain ignored in each mode. Both
Clippy modes and the targeted rustfmt check passed. Typed-cause regressions cover
transport → Internal and Internal → transport first-message ordering on real connected
clients, racing transport/decoder failure, and transport plus two independently awaited
panics (no deduplication of actual JoinErrors). Tests prove peer EOF after scoped
join, zero-started-task pending-handshake cancellation, join cancellation/retry,
concurrent joins, explicit timeout and panic outcomes, owner conflict isolation,
old-generation removal fencing, raw key churn and observer continuity, separate
queue byte/count limits, compressed expansion boundaries and fragmented aggregate
wire rejection. A spare-capacity regression proves a short payload cannot hide an
oversized allocation behind its length. These tests do not prove every venue's
real-world payload fits these defaults. Review regressions also cover stale/foreign
Value-handle mutation fencing and the mock capture's aggregate byte budget.

The pre-existing single-flight API deliberately retains the prior settlement on reopen
(see the existing leader/no-op regression). A separate flight-cycle token redesign is
not included here. Scope/socket generation fencing is not a claim that all concurrent
authentication-flight semantics or venue retained-state budgets have been corrected.
