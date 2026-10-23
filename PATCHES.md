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

## Expose synchronous CLOSE submission (#63, batch 2)

`RawSftpSession::close_submit(&self, handle: Vec<u8>) -> SftpResult<Request>`
exposes the existing single CLOSE admission checkpoint. The public `Request`
future owns the pending ACK and returns `SftpResult<Packet>`; STATUS packets
must be inspected for success or failure. `close` and file shutdown use the
same checkpoint. Refused admission preserves the handle; admitted CLOSE retires
it synchronously, regardless of cancelled observation or a failed STATUS. No
retry is introduced. The existing upstream CLOSE path and Tokio's synchronous
`mpsc::Sender::try_send` admission were inspected.

Regression: `cargo test --locked --test close_submit --lib close` passes
(4 tests, exit 0) on macOS arm64. On the unchanged fork the public regression
fails to compile because `Request` is private and `close_submit` absent (exit
101). The peer holds back STATUS until the retained request is observed pending,
then sends a failure; a sibling handle still completes FSTAT. Unit regressions
cover full-queue refusal, cancelled observation and failed STATUS accounting.
`cargo test --locked --test runtime_contract --test terminal_errors` also passes
(9 tests, exit 0).

## Expose synchronous READ submission (Boundless #68)

`RawSftpSession::read_submit(&self, handle: &[u8], offset: u64, len: u32)
-> SftpResult<ReadRequest>` admits one READ under the same handle lock
`close_submit` takes: a handle that is not open, or whose CLOSE was admitted,
sends nothing (`UnexpectedBehavior`), so CLOSE and READ admission of one handle
are ordered. The public `ReadRequest` future owns the pending reply and checks
it as `read` does, through one shared function: DATA of at most `len` bytes,
`UnexpectedPacket` for more, and a STATUS as `Error::Status`. A refused
admission (full queue, read limit) leaves no pending request. Dropping the
future cancels only reply observation; no retry is introduced.

Boundless reads an SFTP file as a body fed one READ at a time: it reserves
room, submits the READ in a short critical section beside its own handle
table, and awaits the reply outside it, so other requests run while a read
waits and a CLOSE it admits stops further READs. Upstream's crate-private
`read_nowait`, used by `File`, is the admission this wraps.

Regression: `cargo +1.97.0 test` passes on macOS arm64 (lib 8, including
`read_is_admitted_only_for_a_handle_held_open`,
`refused_read_admission_leaves_no_pending_request` and
`a_submitted_read_checks_its_reply_as_read_does`; the integration tests 1, 4
and 5), and `cargo +1.97.0 clippy --all-targets` reports nothing. On the
unchanged fork the regressions do not compile: `read_submit` and
`ReadRequest` are absent.

## Expose the stream's end between packets (Boundless #68)

`client::error::Error::is_stream_end(&self) -> bool` says whether the reader
met the server's stream ending where its next packet would begin: `read_packet`
reads no byte of a length header, which only that produces
(`error::Error::UnexpectedEof`, carried as `Error::Protocol`). The crate-level
error is private, so a caller could not tell that end from a malformed packet,
which also arrives as `Protocol`. A stream ending inside a packet stays an I/O
error, and a later refused request stays `NotSent`; neither is a stream end.

Boundless reports such an end as the subsystem's: its connection's published
end, else `protocol_closed` by the peer, not a malformed reply. OpenSSH's
`sftp-client.c` and node ssh2's `SFTP.js` also keep the input's end apart from
a packet they cannot parse.

Regression: `tests/terminal_errors.rs`
`a_stream_ended_between_packets_is_its_end` (the end, and a later request's
`NotSent` that is not one) and `malformed_and_truncated_responses_retain_the_driver_reason`
(neither a malformed nor a truncated reply is a stream end). On the unchanged
fork they do not compile: `is_stream_end` is absent. `cargo +1.97.0 test`
passes on macOS arm64 (lib 8; the integration tests 1, 4 and 6), and
`cargo +1.97.0 clippy --all-targets` reports nothing.

## Preserve NAME octets (W10-D3)

NAME's `File.filename` and `File.longname` are byte vectors decoded and encoded
through `serde_bytes`, as opaque handles already are. The NAME codec never
passes these fields to the lossy string reader. The raw client exposes those
same fields to READDIR, REALPATH, READLINK and expand-path callers. The high-level
text convenience API keeps its existing UTF-8 replacement behavior at its own
conversion; it does not discard octets before a raw client can read them.

Boundless converts these fields directly to its shared `binary::Bytes` boundary.
The failure was distinct non-UTF-8 filenames collapsing to the same replacement
text. OpenSSH V_10_0_P1 `sftp-client.c` was inspected: READDIR and
`sftp_realpath_expand` read filename/longname with `sshbuf_get_cstring`, retaining
their octets rather than decoding UTF-8.

Regression: `tests/runtime_contract.rs::name_reply_preserves_filename_and_longname_octets`
decodes an independently assembled NAME packet containing ASCII, UTF-8, invalid
UTF-8, embedded NUL and empty strings, checks both byte fields, and re-encodes
exactly the original packet.
