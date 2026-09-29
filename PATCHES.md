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
  Already admitted requests retain an unknown wire outcome.
- Pending requests all receive the first cause; subsequent requests are refused
  with that cause. Rejecting a duplicate request ID does not remove its original
  waiter. Request cancellation still removes only its own pending entry.
- Preserve clean EOF at a frame boundary separately from partial-header/body EOF.
  Shutdown cancels and joins a blocked writer rather than waiting for the peer.

`tests/terminal_errors.rs` exercises read reset with concurrent pending requests,
write failure, codec/truncation errors, subsequent requests, queue full/closed,
blocked shutdown and duplicate initialization. `tests/runtime_contract.rs` retains
wire and lifecycle regressions. No automatic retry, reconnect or replay is added.
