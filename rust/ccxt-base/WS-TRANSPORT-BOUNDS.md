# Rust WebSocket transport ownership and resource limits

This is a fork-local transport change, not a venue capability or capacity acceptance.
`src/pro/ws_client.rs` is hand-written infrastructure (see `HAND_WRITTEN_SIBLINGS`
in `build/rustTranspiler.ts`), not a generated exchange implementation.

## Ownership

Use one `ClientScope` per caller-owned collector. Drive each complete watch future
with `scope.run(future)`; task locals are not inherited by `tokio::spawn`.
Generated watch signatures do not change. Each scope tracks exact client generations,
including slots acquired before connecting or before the first inbound frame. A URL
owned by another scope/unscoped caller is rejected, not silently shared.

Stop and join caller-owned watch/driver tasks, then await
`scope.close_and_join(Duration)`. `ScopeJoinReport` reports client and awaited-task
counts, timeout and failures. `all_joined()` requires no timeout and no internal
failure. Aborted **and awaited** reader/writer/keepalive tasks count as joined: this
proves task destruction, not a graceful WebSocket close handshake.

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
| Outgoing queue | 64 entries and 1 MiB payload permits, including the in-flight write |
| Tungstenite write buffer | 1 MiB |
| Raw URL bus | 64 entries × 256 KiB = 16 MiB retained payload |
| Global raw bus | 256 entries × 256 KiB = 64 MiB retained payload |
| URL length | 4096 bytes |
| Active raw-observer URL keys | 256 |
| Registry client slots | 1024 |
| Generations acquired by one scope | 256 |

The parsed accounting includes a conservative structural estimate, not serialized
JSON bytes alone. It is not an allocator/RSS measurement. Raw bounds are payload
bounds: URL/record metadata and consumer-owned clones are additional. Queue limits
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

Actual results: **71 default tests**, **76 transpiled-base tests** passed (33 are
transport tests); three pre-existing doctests remain ignored in each mode. Both
Clippy modes and the targeted rustfmt check passed. Tests prove peer EOF after scoped
join, zero-started-task pending-handshake cancellation, join cancellation/retry,
concurrent joins, explicit timeout and panic outcomes, owner conflict isolation,
old-generation removal fencing, raw key churn and observer continuity, separate
queue byte/count limits, compressed expansion boundaries and fragmented aggregate
wire rejection. They do not prove every venue's real-world payload fits these defaults.
