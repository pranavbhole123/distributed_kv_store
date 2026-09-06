# Deploy the Raft KV live demo

This deployment runs the real three-node Raft cluster, its durable per-node
volumes, and the browser dashboard. It is meant for a portfolio or interview
demo. All three replicas run on one host, so it demonstrates node/container
failure and recovery—not tolerance of the entire host failing.

## What is deployed

```text
browser → Caddy dashboard/proxy → node-1, node-2, node-3 HTTP APIs
                                  ↕ private Docker network
                              Raft gRPC replication
```

Only Caddy publishes host ports. The node HTTP and gRPC ports stay inside the
Docker network. Each node gets a distinct named volume at `/var/lib/kv`, which
contains its Raft stable state, WAL/log, and snapshots.

## Prerequisites

- A Linux/macOS laptop or a Linux VM with Docker Engine and Docker Compose v2.
- For a public HTTPS demo: a domain name with an A/AAAA record pointed to the
  VM, plus firewall rules allowing inbound TCP 80 and 443.
- Do **not** expose the Raft gRPC ports (`9090`) to the public Internet. This
  educational build does not yet have mTLS or authentication.

## Run locally

From the repository root:

```bash
cp .env.demo.example .env.demo
set -a
. ./.env.demo
set +a
docker compose --env-file .env.demo -f compose.demo.yaml up --build -d
docker compose --env-file .env.demo -f compose.demo.yaml ps
```

Open `http://localhost:8088` in a browser. Wait a few seconds for election;
one node card becomes the observed leader.

Useful checks:

```bash
curl http://localhost:8088/api/node-1/leader
curl http://localhost:8088/api/node-2/leader
curl http://localhost:8088/api/node-3/leader
docker compose --env-file .env.demo -f compose.demo.yaml logs -f node-1 node-2 node-3
```

## Interview demo script

1. In the dashboard, `SET demo-key = hello raft`.
2. Click **Read all nodes** and show that every node has the value.
3. Note the observed leader, then stop it in a terminal. Replace `node-2` with
   whichever node is currently leader:

   ```bash
   docker compose --env-file .env.demo -f compose.demo.yaml stop node-2
   ```

4. The dashboard should show that node as unreachable, then show a new leader
   in a higher term.
5. Write a second value through the new leader.
6. Restart the stopped node and show it becomes reachable and catches up:

   ```bash
   docker compose --env-file .env.demo -f compose.demo.yaml start node-2
   ```

7. To demonstrate durable restart rather than only a stop/start:

   ```bash
   docker compose --env-file .env.demo -f compose.demo.yaml restart node-2
   ```

The dashboard intentionally has no browser button for stopping containers. A
public UI must not expose shell/container control.

## Deploy on a public VM with HTTPS

1. Install Docker Engine and the Compose plugin on the VM.
2. Clone this repository onto the VM.
3. Point a DNS A/AAAA record, for example `raft-demo.example.com`, at the VM.
4. Create `.env.demo` in the repository root:

   ```dotenv
   DEMO_DOMAIN=raft-demo.example.com
   HTTP_PORT=80
   HTTPS_PORT=443
   ```

5. Allow TCP 80 and 443 through the VM firewall/security group. Do not allow
   8080, 9090, or Docker's private subnet from the public Internet.
6. Start the stack:

   ```bash
   docker compose --env-file .env.demo -f compose.demo.yaml up --build -d
   docker compose --env-file .env.demo -f compose.demo.yaml ps
   ```

7. Open `https://raft-demo.example.com`. Caddy requests and renews the public
   certificate when the DNS and ports are correct.

For a change after deployment, rebuild/recreate the stack:

```bash
docker compose --env-file .env.demo -f compose.demo.yaml up --build -d
```

## Data safety and reset

Named volumes keep each node's files when containers are stopped, recreated,
or restarted. Do not run `docker compose down -v` during a persistence demo: it
deletes those volumes and therefore erases the Raft state.

To intentionally reset the entire **demo** cluster:

```bash
docker compose --env-file .env.demo -f compose.demo.yaml down -v
```

Then start it again with `up --build -d`.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| No leader after startup | Wait one election timeout, then inspect `docker compose ... logs node-1 node-2 node-3`. |
| A node is unhealthy | Run `docker compose ... logs node-N`; confirm its config is mounted and its volume is writable. |
| The dashboard cannot reach a node | Check `docker compose ... ps`; only Caddy should expose public ports. |
| HTTPS does not issue | Confirm DNS resolves to the VM and TCP 80/443 are open before starting Caddy. |
| A write says leader changed | Refresh waits for the next leader poll; retry the write. The old leader was never allowed to claim success without a quorum. |

## What this demo does and does not prove

It proves election, quorum writes, process failure, durable recovery, and
eventually consistent local reads using the actual application binary. It does
not prove multi-host availability, authentication/TLS between Raft peers,
dynamic membership, or large chunked snapshots. State those limitations
directly when presenting the project.
