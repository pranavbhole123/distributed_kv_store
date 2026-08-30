# Raft design decisions

This file records decisions that change correctness, recovery, or performance
characteristics. Each decision should state both the choice and why it is safe.

## Durable log writes and conflicting suffixes

Normal Raft replication appends only the entries a follower is missing. A
successful append is followed by `fsync` before the follower replies
`Success=true`.

When a valid leader discovers a conflicting **uncommitted** follower suffix,
the follower atomically rewrites the retained prefix and then appends the
leader's replacement suffix. Rewriting is deliberately limited to this rare
truncation path; it must not happen for ordinary appends or empty heartbeats.

The WAL uses length-prefixed records. During recovery, an incomplete final
frame is discarded and the file is truncated back to the last complete frame.
There is currently no per-record checksum, so silent corruption of a complete
frame is not yet detected.

## Per-follower replication progress

The leader keeps `nextIndex` and `matchIndex` in maps keyed by node ID, rather
than slices: configuration IDs are identifiers, not necessarily contiguous
array offsets.

- `nextIndex[peer]` is the leader's optimistic sending cursor. A rejected
  AppendEntries moves it backward so the next RPC tries an earlier prefix.
- `matchIndex[peer]` is the highest log index that peer has confirmed
  replicated. It only moves forward; it is not the commit index.

The leader itself counts as one replica when deciding whether a majority has
replicated an entry. It only advances `commitIndex` for entries from its
current term, as required by Raft's commitment rule.

## Commit, apply, and client acknowledgement

`commitIndex` means an entry is immutable Raft history; it does not by itself
mean the KV state machine has executed that entry. A single Raft apply loop
therefore advances `lastApplied` one entry at a time, invokes the injected
`Applier` outside the Raft mutex, persists `lastApplied`, and only then wakes
the waiting proposal result.

The current memory store is rebuilt from committed history after a process
restart, so recovery starts application from index zero even if a previous
`lastApplied` value was persisted. The durable `lastApplied` checkpoint becomes
authoritative once the project adds snapshots or a durable state machine.
