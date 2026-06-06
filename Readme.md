# CollabFlow

A **distributed real-time collaborative whiteboard** built on a custom RAFT consensus protocol — fault-tolerant, zero-downtime, and production-grade.

> Built from scratch to mimic how real distributed systems like **etcd** (used inside Kubernetes) maintain consistency under failures.

![Node.js](https://img.shields.io/badge/Node.js-339933?style=flat&logo=nodedotjs&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=flat&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat&logo=grafana&logoColor=white)

---

## What This Project Demonstrates

This is not a simple chat app or CRUD project. It implements real distributed systems concepts used in production infrastructure:

| Concept | Where It Appears |
|---|---|
| **RAFT Consensus** | Leader election, log replication, term management |
| **Fault Tolerance** | System stays alive when any replica dies |
| **Log Compaction** | Snapshots trim the log like etcd does in Kubernetes |
| **Concurrent Write Ordering** | Global logIndex stamps prevent canvas divergence |
| **Zero-Downtime Failover** | New leader elected in <800ms, clients never disconnect |
| **Observability** | Prometheus + Grafana dashboard shows live cluster state |
| **Containerization** | 6-container Docker Compose stack with hot reload |

---

## Architecture

```
Browser 1 ──┐
Browser 2 ──┼──[WebSocket]──► Gateway (port 8081)
Browser 3 ──┘                      │
                          ┌────────┼────────┐
                          ▼        ▼        ▼
                     Replica 1  Replica 2  Replica 3
                     (Leader)  (Follower) (Follower)
                          │        │        │
                          └────────┼────────┘
                                   ▼
                            Prometheus (port 9090)
                                   │
                            Grafana (port 3000)
```

### Components

| Service | Role |
|---|---|
| **Gateway** | Accepts WebSocket connections, discovers leader, broadcasts committed strokes |
| **Replica 1/2/3** | Run RAFT consensus — leader election, log replication, snapshots |
| **Prometheus** | Scrapes `/metrics` from all replicas every 5s |
| **Grafana** | Live dashboard showing leader, term, commit index, elections |

---

## Features Built

### 1. Mini-RAFT Consensus Engine
Custom implementation of the RAFT protocol across 3 replica nodes:
- **Leader election** with randomized timeouts (500–800ms)
- **Log replication** — strokes committed only after majority confirms
- **Term-based safety** — higher term always wins, stale leaders step down
- **Heartbeats** every 150ms to detect failures

### 2. Log Snapshots (Like etcd)
Every 100 commits, the leader takes a snapshot:
- Serializes entire canvas state into one object
- Trims the log — memory never grows unbounded
- Restarting replicas receive snapshot first, then only delta entries
- Same pattern used by etcd inside Kubernetes

### 3. Concurrent Write Ordering
Every stroke is stamped with a global `logIndex` by the leader:
- Guarantees all clients render strokes in identical order
- Prevents canvas divergence during simultaneous drawing
- Handles failover mid-draw without flickering

### 4. Prometheus + Grafana Monitoring
Live dashboard tracking:
- `raft_state` — which replica is leader (0=follower, 1=candidate, 2=leader)
- `raft_current_term` — term progression over time
- `raft_commit_index` — commit progress per replica
- `raft_elections_total` — election counter per replica
- `raft_strokes_committed_total` — strokes committed by each leader
- `raft_snapshot_index` — snapshot checkpoints
- `raft_log_length` — in-memory log size

---

## System Flow

```
User draws stroke
      ↓
Frontend sends WebSocket message to Gateway
      ↓
Gateway forwards to current Leader replica
      ↓
Leader appends to log + replicates to followers via /append-entries
      ↓
Majority (2/3) confirms → Leader commits
      ↓
Leader calls /broadcast on Gateway
      ↓
Gateway fans out to all connected browsers
      ↓
All canvases update in real time
```

### Failover Flow
```
Leader dies
      ↓
Followers miss heartbeat → election timeout fires (500-800ms)
      ↓
Candidates request votes → majority elects new leader
      ↓
Gateway discovers new leader via /status polling
      ↓
Drawing continues — clients never disconnect
Total downtime: < 800ms
```

### Catch-up Flow (Restarted Node)
```
Replica restarts with empty log
      ↓
Leader sends snapshot (canvas state at index N)
      ↓
Replica applies snapshot instantly
      ↓
Leader sends only entries after N
      ↓
Replica fully synced — joins cluster normally
```

---

## Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/)

### Run the stack
```bash
git clone https://github.com/Nandani2801/CollabFlow.git
cd CollabFlow
docker compose up --build -d
```

### Open in browser

| Service | URL |
|---|---|
| Drawing Board | `http://127.0.0.1:8081` |
| Grafana Dashboard | `http://127.0.0.1:3000` (admin/admin) |
| Prometheus | `http://127.0.0.1:9090` |
| Replica 1 status | `http://127.0.0.1:13001/status` |
| Replica 2 status | `http://127.0.0.1:13002/status` |
| Replica 3 status | `http://127.0.0.1:13003/status` |

---

## Demo Scenarios

### Failover Demo
```bash
# Open drawing board + Grafana side by side
# Draw some strokes, then kill the leader:
docker compose stop replica1

# Keep drawing — system stays live
# Watch Grafana: new leader elected in <800ms
# Bring it back:
docker compose start replica1
# Replica catches up via snapshot automatically
```

### Snapshot Demo
```bash
# Draw 100+ strokes to trigger snapshot
docker compose logs replica1
# Look for: 📸 Snapshot taken at index 99

# Kill and restart a replica
docker compose stop replica2
docker compose start replica2
docker compose logs replica2
# Look for: 📥 Applied snapshot up to index 99
```

### Concurrent Write Demo
```bash
# Open two browser tabs at http://127.0.0.1:8081
# Draw simultaneously in both tabs
# Both canvases stay identical — same stroke order guaranteed
```

---

## API Reference

### Replica Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/status` | RAFT state (role, term, leader, commitIndex, snapshotIndex) |
| POST | `/request-vote` | Vote request from candidate |
| POST | `/heartbeat` | Heartbeat from leader |
| POST | `/append-entries` | Log replication from leader |
| POST | `/stroke` | Submit stroke (leader only) |
| POST | `/clear` | Clear canvas (leader only) |
| GET | `/sync-log` | Fetch missing entries + snapshot |
| GET | `/metrics` | Prometheus metrics endpoint |

### Gateway Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/broadcast` | Fan out committed stroke to all WebSocket clients |
| POST | `/broadcast-clear` | Fan out clear event |
| GET | `/canvas-state` | Full canvas replay for new connections |

### WebSocket Events

| Direction | Type | Description |
|---|---|---|
| Browser → Gateway | `stroke` | New drawing stroke with clientId |
| Browser → Gateway | `clear` | Clear canvas |
| Gateway → Browser | `stroke` | Committed stroke broadcast |
| Gateway → Browser | `clear` | Clear broadcast |
| Gateway → Browser | `error` | No leader available |

---

## Environment Variables

| Variable | Example | Description |
|---|---|---|
| `REPLICA_ID` | `1` | Unique replica identifier |
| `PORT` | `3001` | Replica listen port |
| `PEERS` | `http://replica2:3002,http://replica3:3003` | Comma-separated peer URLs |
| `GATEWAY_URL` | `http://gateway:8080` | Gateway broadcast URL |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Consensus Engine | Node.js, Express, Axios |
| Gateway | Node.js, Express, ws |
| Frontend | HTML5 Canvas, WebSocket API |
| Metrics | prom-client |
| Monitoring | Prometheus, Grafana |
| Infrastructure | Docker, Docker Compose |

---

## Project Structure

```
CollabFlow/
├── docker-compose.yml        # 6-container stack
├── prometheus.yml            # Prometheus scrape config
├── README.md
├── gateway/
│   ├── server.js             # WebSocket server + leader routing
│   ├── package.json
│   └── Dockerfile
├── replica1/                 # RAFT node 1
│   ├── server.js             # Full RAFT implementation
│   ├── package.json
│   └── Dockerfile
├── replica2/                 # RAFT node 2 (identical logic)
├── replica3/                 # RAFT node 3 (identical logic)
└── frontend/
    └── index.html            # Canvas UI
```

---

## Real-World Relevance

| Production System | Concept Used | In This Project |
|---|---|---|
| **Kubernetes** | etcd runs RAFT for cluster state | Replicas run RAFT for stroke log |
| **CockroachDB** | RAFT-based distributed SQL | Same leader election + log replication |
| **Figma** | Real-time collaborative canvas | WebSocket broadcast + consistent state |
| **Kafka** | Log compaction | Snapshot + log trim every 100 commits |
| **Datadog/Grafana** | Metrics + alerting | Prometheus scraping + live dashboard |

---

## Author

**Nandani** — [github.com/Nandani2801](https://github.com/Nandani2801)

