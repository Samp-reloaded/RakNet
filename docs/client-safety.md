# Client role safety

Source checkpoint, 2026-10-14. **Unbuilt and untested.** No configure, build,
automated tests, runtime launch, or connection was performed for this change.

`SAMPRakNet::IsClientRole()` separates the server application logon watchdog and
the server ACK-range, ACK-rate, message-rate, cumulative out-of-order and ban
policies. The client does not set `isLogon` to impersonate the server receipt.
Server policy defaults and wire encodings remain unchanged.

Independent structural and resource checks still apply. ACK expansion is
validated across every range before applying any ACK, with at most 65536 entries
per datagram and a full-width iterator (including the inclusive 65535 endpoint).
Client ordered backlog is limited to 4096 packets and 16 MiB across all streams;
the current packet is released on failure, and the reliability layer closes with
`Packet::GetDisconnectOrigin() == DisconnectOrigin::LocalResourceLimit`.
Existing split count/channel/byte checks remain active for both roles.

`DisconnectOrigin` is SDK metadata and is never serialized. Native notification
32 can mean `RemoteNotification`, `LocalRequest`, or `LocalAdmissionPolicy`.
Native notification 33 can report `ReliableAckTimeout` or `LocalResourceLimit`.
The disconnect fallback explicitly reports `LocalRequest`. `Unspecified` means
the SDK has not proved an origin and must not be displayed as a server kick.

## Bounded owner receive

Include `<raknet/ReceiveBudget.h>` and retain one ledger across the complete
application update, including every call made while draining that update:

```cpp
RakNet::ReceiveBudget budget{256, 2u * 1024u * 1024u, 1024};
auto result = client.ReceiveBounded(budget);
```

`ReceiveResult::GetStatus()` is `ReceiveStatus::Packet`, `Empty`, `Yield`, or
`Failure`. Only `Packet` transfers a nonnull `GetPacket()` to the caller, which
must release it with the same client's `DeallocatePacket()`. `Empty` means the
queue was empty or the peer inactive at the observation; `Yield` means this
ledger cannot admit more work. Neither establishes remote disconnect.

`GetFailure()` distinguishes `InvalidBudget`, `OversizedPacket`,
`MalformedPacket`, and `ReentrantReceive`; other statuses report `None`.
Zero capacities or a plugin chain larger than the full work budget are invalid.
A packet larger than the full byte capacity is removed and released with
`OversizedPacket`, so a fresh valid ledger cannot yield forever on that head.
The caller should treat a failure as a terminal admission error for its slice.

`GetProcessedPackets()` and `GetProcessedBytes()` count every admitted native
packet, including RPCs, replies, intercepted packets and returned packets.
`GetWorkUnits()` also charges queue transfers and reserves one unit per plugin
invocation; each packet reserves its full current plugin chain before dequeue.
`GetQueueTransfers()` counts movement from SPSC or the deferred tail queue.
Oversized disposal charges one work unit and reports separate
`GetDiscardedPackets()` / `GetDiscardedBytes()` counts without parsing its bytes.
These bounds are conservative: a reply or early interception may reserve plugin
units it does not use. No nested callback resets the supplied ledger.

Plugin `Update` runs once per shared bounded ledger and is charged before its
callbacks. Callback receive reentry reports `ReentrantReceive`; compatibility
receive returns null in that case. RPC/plugin callbacks run synchronously on the
owner lane. Their arbitrary execution time is not bounded by an operation count.

FIFO is retained across bounded transfers and client control interception.
Tail pushback records an exact monotonic producer watermark and waits behind
those preceding published packets; head pushback retains its explicit priority.
No approximate SPSC size is used, no read lock survives a yield, and no admitted
packet is popped and tail-requeued merely to defer it.

Timestamp envelopes stay in returned packet data, with unchanged logical
`Packet::bitSize`. A timestamp is adjusted once, including after pushback.
Timestamped RPC/reply envelopes are internally intercepted; all others are
returned with the original envelope. RPC and reply parsing uses the logical bit
extent, excluding padding in the final storage byte. The client accepted,
remote/static, broadcast ping and seed postprocessing is shared with legacy
receive; the bounded path continues through absorbed client control messages.

The existing `Receive()` API retains an unlimited compatibility ledger and the
legacy plugin-update cadence. Existing server callers need no migration.
The new client and peer interfaces require source recompilation; packet metadata
is local SDK state with explicit object construction, never wire bytes.

Owner receive bounds and producer-queue allocation are separate boundaries. The
worker still allocates and publishes packets while the owner is not draining;
the SDK SPSC queue itself has no aggregate allocation cap. This change does not
claim complete ingress memory bounding. Split metadata/reassembly and the client
ordered backlog have their own limits described above.

## Source scenario fixtures for the later execution gate

The following inputs and expected observations are recorded for review; none was
executed here. They require the actual SDK target and an owner-lane harness.

| Input | Expected observation |
| --- | --- |
| Connected client older than 30 s with `isLogon=false` | No application logon disconnect; server role still applies its watchdog |
| Valid server burst beyond the server messages/ACK sampling limits | Client processes it without admission ban; server role retains its limits |
| ACK range `[0,65535]` | Exactly 65536 finite iterator steps; no wrap |
| ACK ranges `[0,65535]` followed by `[7,7]` | Reject before applying any ACK; total work exceeds 65536 |
| 4096 delayed ordered client packets followed by one more | Bounded backlog; typed local resource failure, no server ban |
| RPC-only stream, packet capacity 3 | Exactly three admitted callbacks, then yield with next FIFO head retained |
| Reply-only or plugin-absorbed stream, capacity 3 | At most three admitted packets in the same ledger |
| Two raw packets totaling the byte cap, then a one-byte head | Yield; a fresh ledger returns that head without timestamp adjustment replay |
| A head one byte larger than the full cap | One charged disposal, `OversizedPacket`, no repeated head |
| Tail pushback while older packets remain in SPSC | Older producer packets precede pushed tail, newer packets follow it |
| Timestamped raw packet returned, pushed back, returned again | Same shifted timestamp, unchanged logical bit count |
| 7-bit RPC payload in a final storage byte | Callback reports 7 bits; an advertised eighth bit outside the packet extent is rejected |
| Plugin `Update` or RPC callback calls receive recursively | Reentry failure/null; no younger dispatch and no new ledger |
