# Distributed KV Store in Go

A three-node distributed key-value store built from the Raft consensus
algorithm. It demonstrates leader election, durable log replication,
majority-acknowledged writes, crash recovery, and snapshots.

## Demo video

▶️ [Watch the 2-minute live demo](<Screencast%20From%202026-09-06%2018-35-25.mp4>)

The demo starts the actual three-node cluster, performs a quorum write, kills
the leader, shows election of a replacement, and brings the failed node back
to demonstrate catch-up.

## What it demonstrates

- Raft leader election with persistent `currentTerm` and `votedFor`
- gRPC `RequestVote`, `AppendEntries`, and `InstallSnapshot` RPCs
- Durable Raft log with prefix matching and conflicting-suffix replacement
- Majority-acknowledged `SET` and `DELETE` writes through the leader
- Local `GET` requests on any node; these are intentionally eventually
  consistent and may be stale
- Crash recovery: committed entries replay into the in-memory state machine;
  uncommitted entries remain durable but invisible
- Snapshot creation, log compaction, startup recovery, and lagging-follower
  snapshot installation
- Docker Compose dashboard for a live election/failover demonstration

## Architecture

```text
Browser
  │
  ▼
Caddy dashboard / reverse proxy
  ├── node-1 HTTP API ──┐
  ├── node-2 HTTP API ──┼── private gRPC Raft network
  └── node-3 HTTP API ──┘

Each node: Raft state + durable log + snapshot + in-memory KV state machine
```

Writes go only to the current leader:

```text
client SET/DELETE → leader durable append → replicate to a majority
                  → commit → apply in log order → HTTP success
```

Reads are local by design:

```text
client GET → any node's in-memory store → possibly stale value
```

## Run the live demo

Prerequisites: Docker Engine and Docker Compose v2.

```bash
cp .env.demo.example .env.demo
sudo docker compose --env-file .env.demo -f compose.demo.yaml up --build -d
```

Open [http://localhost:8088/?demo=3](http://localhost:8088/?demo=3). Within a
few seconds the dashboard identifies one leader and two followers.

Verify the election directly:

```bash
for n in 1 2 3; do
  curl -s "http://localhost:8088/api/node-$n/leader"
  echo
done
```

For the complete failover script, deployment details, reset instructions, and
security boundaries, see [DEPLOY_DEMO.md](DEPLOY_DEMO.md).

## Test

```bash
go test ./...
go test -race ./...
go vet ./...
```

## Repository layout

```text
cmd/distributed_kv_store/  process entry point
internal/raft/             election, replication, apply pipeline, snapshots
internal/transport/        gRPC transport and protobuf contract
internal/node/             composition root for one member
internal/store/            in-memory KV state machine
internal/wal/              durable record storage
internal/snapshot/         checksummed, atomically-written snapshots
internal/server/           HTTP API
demo/                      browser dashboard
deploy/                    Caddy and Docker deployment configuration
```

## Scope and deliberate limitations

This is an educational, portfolio-quality implementation—not a production
database. Membership is a fixed three-node configuration; there is no dynamic
membership/joint consensus, authentication or mTLS, request-ID deduplication,
linearizable reads, or chunked snapshot transfer. The Docker demo runs all
three members on one host, so it demonstrates container/process failure, not
loss of the entire machine.
