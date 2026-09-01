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

## HTTP writes enter through the leader

HTTP handlers do not mutate `MemoryStore` directly. The current leader creates
a Raft proposal and waits until its applied result; a follower returns a 307
redirect to its known leader instead of becoming an internal forwarding proxy.
This keeps client retries, request context, and final response ownership with
the client and leader rather than adding another request/response protocol.

The leader's wait is bounded. An isolated node that still believes it is leader
cannot obtain a majority, so its HTTP request returns a retryable 503 rather
than a false success.

## Leader no-op establishes inherited commitment

A newly elected leader appends a durable internal no-op in its own term. When a
quorum replicates that no-op, Raft can commit it by the current-term rule and
therefore commits every preceding inherited entry too. This prevents a
successfully committed older-term write from remaining invisible after its
original leader crashes before followers receive the updated commit index.
  
## Client request IDs are a later durable feature

An HTTP timeout cannot tell a client whether its write committed before the
response was lost. A request ID must therefore live in the durable Raft command
and in the reconstructed state-machine deduplication state, not merely in an
HTTP-local map. The current API deliberately does not claim duplicate-request
suppression; request IDs are deferred until that complete end-to-end contract
is implemented.
