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
`TransportTerminal` is assigned only at EOF or for tungstenite I/O, closed/already-closed,
and reset-without-closing-handshake variants. Budget, decoder, UTF-8, capacity, other
protocol and unknown errors are `Internal`; awaited task panics are `TaskPanic`.
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

## Fixed limits

| Boundary | Limit |
| --- | --- |
| Wire message, including fragmented aggregate | 256 KiB |
| Individual wire frame | 256 KiB |
| Expanded gzip/raw-deflate payload | 1 MiB |
| Parsed inbound queue | 256 entries and 4 MiB accounting budget |
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

Validated from the fork root with
`RUSTC_WRAPPER= CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0`:

```sh
rustfmt --edition 2021 --check rust/ccxt-base/src/pro/ws_client.rs
cargo test --locked --offline --manifest-path rust/ccxt-base/Cargo.toml
cargo test --locked --offline --manifest-path rust/ccxt-base/Cargo.toml --features transpiled-base
cargo clippy --locked --offline --manifest-path rust/ccxt-base/Cargo.toml --all-targets -- -D warnings
cargo clippy --locked --offline --manifest-path rust/ccxt-base/Cargo.toml --features transpiled-base --all-targets -- -D warnings
```

Actual results: **80 default tests** (23 lifecycle), **85 transpiled-base tests** passed; three pre-existing doctests remain ignored in each mode. Both
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
