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

Owner receive bounds and producer-queue allocation are separate boundaries. A
bounded receive API is the next checkpoint; this change alone does not bound the
producer queue or callback work in the existing compatibility receive API.
