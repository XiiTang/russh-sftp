# Boundless client transport patches

The fork retains upstream history. The wire-value and bounded-client work at
`13c0cc468d391cb982e450c21ccee7c55b5eaa25` remains required: opaque handles,
extended attributes and status values, controlled transport bounds, cancellable
requests and joined shutdown.

## Preserve the first transport termination cause

Upstream HEAD `e8de8f903cb019f32beb26cc1195c7c71bef121f` was checked: its reader
and writer still log errors and drop pending senders. The caller can therefore
receive `sender dropped` instead of the actual transport or codec failure.

- Retain typed, shareable I/O errors, including their OS code and source chain.
  Preserve codec and handler errors through both error enums.
- Record the first terminal cause before cancellation wakes the other transport
  half. Notify the handler before pending requests are released. Explicit close
  never replaces an already recorded cause.
- Serialize output admission with terminal publication. Distinguish a full queue
  from an ended transport; `NotSent` wraps the retained cause for refused output.
  Already admitted requests retain an unknown wire outcome. The unused
  task-local `DispatchTracker` and its enqueue probe were removed: neither
  Boundless nor other fork code used them. Queue admission, request ownership
  and per-handle CLOSE accounting remain unchanged.
- Pending requests all receive the first cause; subsequent requests are refused
  with that cause. Rejecting a duplicate request ID does not remove its original
  waiter. Request cancellation still removes only its own pending entry.
- Preserve clean EOF at a frame boundary separately from partial-header/body EOF.
  Shutdown cancels and joins a blocked writer rather than waiting for the peer.

`tests/terminal_errors.rs` exercises read reset with concurrent pending requests,
write failure, codec/truncation errors, subsequent requests, queue full/closed,
blocked shutdown and duplicate initialization. `tests/runtime_contract.rs` retains
wire and lifecycle regressions. No automatic retry, reconnect or replay is added.

## CLOSE ownership (2026-10-02)

Adapt upstream 9ec98bd0920b9b4eceeee3f3ed6e18b479b2d417: retire an open handle
after synchronous CLOSE queue admission, rather than waiting for STATUS OK.
Refused admission preserves ownership. A lost/error reply does not authorize
reuse or retry of that handle. Track the actual opaque byte handles so repeated
CLOSE cannot decrement a sibling's count; unknown/closed handles are refused
locally. File shutdown marks an admitted close once, even if its waiter is
cancelled. Preserve bounded framing, request IDs and joined transport shutdown.

`cargo test` passes on macOS arm64, including queue refusal, cancelled waiter,
error reply and duplicate-close accounting plus terminal/codec regressions.

After removing the unused tracker, `cargo +1.97.0 test --lib close`
(3 tests) and `cargo +1.97.0 test --test runtime_contract --test terminal_errors`
(9 tests) pass on macOS arm64.
