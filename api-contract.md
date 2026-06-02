# API Contract — Distributed Real-Time Drawing Board
> **Version:** 1.0  |  **Status:** AGREED — do not change without team sign-off  
> **Last updated:** Week 1  
> **Purpose:** Single source of truth for all interfaces. Every Person codes against THIS document, not memory.

---

## Table of Contents

1. [Overview](#overview)
2. [Replica Endpoints](#replica-endpoints) (Person 1 implements)
   - [POST /request-vote](#post-request-vote)
   - [POST /append-entries](#post-append-entries)
   - [POST /heartbeat](#post-heartbeat)
   - [GET /sync-log](#get-sync-log)
   - [POST /stroke](#post-stroke)
   - [GET /status](#get-status)
3. [Gateway Endpoints](#gateway-endpoints) (Person 2 implements)
   - [POST /broadcast](#post-broadcast)
   - [GET /canvas-state](#get-canvas-state)
4. [WebSocket Messages](#websocket-messages)
   - [Browser → Gateway](#browser--gateway)
   - [Gateway → Browser](#gateway--browser)
5. [Shared Data Types](#shared-data-types)
6. [Error Codes](#error-codes)
7. [Sequence Diagrams (Text)](#sequence-diagrams)

---

## Overview

### Service URLs (inside Docker network)

| Service   | Internal URL              | External (debug) |
|-----------|---------------------------|------------------|
| Gateway   | `http://gateway:8080`     | `http://localhost:8080` |
| Replica 1 | `http://replica1:3001`    | `http://localhost:3001` |
| Replica 2 | `http://replica2:3002`    | `http://localhost:3002` |
| Replica 3 | `http://replica3:3003`    | `http://localhost:3003` |

### Who Calls What

```
Browser  ──[WebSocket]──►  Gateway  ──[HTTP POST /stroke]──►  Leader Replica
                              ▲                                      │
                              │                                      ▼
                    [HTTP POST /broadcast]              [HTTP POST /append-entries]
                              │                               Follower Replicas
                              └──────────────────────────────────────┘

Gateway  ──[HTTP GET /status]──►  Any Replica   (to discover who is leader)
Follower ──[HTTP GET /sync-log]──► Leader        (to catch up after restart)
```

### Content-Type
All HTTP endpoints accept and return `application/json`.

---

## Replica Endpoints

> **Owner:** Person 1  
> **Base URL:** `http://replica{N}:{PORT}`  
> These are the RAFT protocol RPCs. All replicas expose identical endpoints.

---

### POST /request-vote

**Called by:** A Candidate replica, sent to all peer replicas  
**Purpose:** Ask a peer for its vote in the current election term

#### Request Body

```json
{
  "term": 2,
  "candidateId": "1"
}
```

| Field         | Type   | Required | Description                              |
|---------------|--------|----------|------------------------------------------|
| `term`        | number | ✅        | The election term this candidate is running in |
| `candidateId` | string | ✅        | The REPLICA_ID of the candidate (`"1"`, `"2"`, or `"3"`) |

#### Response Body — Success (200)

```json
{
  "voteGranted": true,
  "term": 2
}
```

```json
{
  "voteGranted": false,
  "term": 3
}
```

| Field        | Type    | Description                                                    |
|--------------|---------|----------------------------------------------------------------|
| `voteGranted`| boolean | `true` if vote is granted, `false` otherwise                  |
| `term`       | number  | The responder's current term (candidate uses this to update itself) |

#### Vote Grant Rules

- ✅ Grant vote if: `term >= currentTerm` AND (`votedFor === null` OR `votedFor === candidateId`)
- ❌ Deny vote if: `term < currentTerm` (stale election)
- ❌ Deny vote if: already voted for a different candidate this term
- ⚠️ If `term > currentTerm`: update own term, revert to Follower, clear `votedFor`, THEN evaluate

#### Error Cases

| HTTP Status | Condition                  |
|-------------|----------------------------|
| 400         | Missing `term` or `candidateId` in body |

---

### POST /append-entries

**Called by:** The current Leader, sent to all Follower replicas  
**Purpose:** Replicate a new log entry (stroke) OR send empty heartbeat  
**Also used as:** Heartbeat when `entry` is omitted

#### Request Body — With Entry (log replication)

```json
{
  "term": 2,
  "leaderId": "2",
  "entry": {
    "index": 5,
    "term": 2,
    "stroke": {
      "x1": 120,
      "y1": 85,
      "x2": 145,
      "y2": 102,
      "color": "#e63946",
      "width": 4
    }
  },
  "leaderCommit": 4
}
```

#### Request Body — Empty Heartbeat

```json
{
  "term": 2,
  "leaderId": "2",
  "leaderCommit": 4
}
```

| Field          | Type   | Required | Description                                    |
|----------------|--------|----------|------------------------------------------------|
| `term`         | number | ✅        | Leader's current term                          |
| `leaderId`     | string | ✅        | Leader's REPLICA_ID                            |
| `entry`        | object | ❌        | Omit for heartbeat-only call                   |
| `entry.index`  | number | ✅ if entry | Position in the log (0-based)               |
| `entry.term`   | number | ✅ if entry | Term when this entry was created             |
| `entry.stroke` | object | ✅ if entry | The stroke data (see Stroke type below)      |
| `leaderCommit` | number | ✅        | Leader's current commitIndex                   |

#### Response Body — Success (200)

```json
{
  "success": true,
  "logLength": 6
}
```

#### Response Body — Rejected (200, not an HTTP error)

```json
{
  "success": false,
  "logLength": 3
}
```

| Field       | Type    | Description                                              |
|-------------|---------|----------------------------------------------------------|
| `success`   | boolean | Whether the follower accepted the entry                  |
| `logLength` | number  | Follower's current log length (leader uses for sync-log) |

#### Rejection Rules

- ❌ Reject if `term < currentTerm` (message is from a stale/old leader)
- ✅ Accept if `term >= currentTerm`: update own term, reset to Follower, reset election timer

---

### POST /heartbeat

**Called by:** The current Leader, sent to all Followers every 150ms  
**Purpose:** Suppress follower election timers — proves leader is alive

#### Request Body

```json
{
  "term": 2,
  "leaderId": "2"
}
```

| Field      | Type   | Required | Description                  |
|------------|--------|----------|------------------------------|
| `term`     | number | ✅        | Leader's current term        |
| `leaderId` | string | ✅        | Leader's REPLICA_ID          |

#### Response Body (200)

```json
{
  "ok": true,
  "term": 2
}
```

#### Follower Behaviour on Heartbeat

1. If `term >= currentTerm`: accept heartbeat, update `leaderId`, reset election timer
2. If `term < currentTerm`: ignore (stale leader — do NOT reset timer)
3. If `term > currentTerm`: update `currentTerm`, revert to Follower, clear `votedFor`

---

### GET /sync-log

**Called by:** A restarted Follower, directed at the current Leader  
**Purpose:** Download all missing committed log entries after a restart  
**How follower knows to call this:** When it receives an `/append-entries` and detects `entry.index > log.length`

#### Query Parameters

| Param  | Type   | Required | Description                                         |
|--------|--------|----------|-----------------------------------------------------|
| `from` | number | ✅        | Index to start from (follower's current log length) |

#### Example Request

```
GET /sync-log?from=3
```

#### Response Body (200)

```json
{
  "entries": [
    {
      "index": 3,
      "term": 1,
      "stroke": { "x1": 10, "y1": 20, "x2": 30, "y2": 40, "color": "#000000", "width": 3 }
    },
    {
      "index": 4,
      "term": 2,
      "stroke": { "x1": 50, "y1": 60, "x2": 70, "y2": 80, "color": "#ff0000", "width": 5 }
    }
  ],
  "commitIndex": 4
}
```

| Field         | Type    | Description                                             |
|---------------|---------|---------------------------------------------------------|
| `entries`     | array   | All committed log entries from `from` index onward      |
| `commitIndex` | number  | Leader's current commit index (follower should match this) |

#### Empty Response (already in sync)

```json
{
  "entries": [],
  "commitIndex": 4
}
```

---

### POST /stroke

**Called by:** Gateway (after receiving a stroke from a browser)  
**Purpose:** Submit a new drawing stroke to the cluster via the Leader  
**Important:** Only the Leader handles this. Followers reject it.

#### Request Body

```json
{
  "x1": 120,
  "y1": 85,
  "x2": 145,
  "y2": 102,
  "color": "#e63946",
  "width": 4
}
```

| Field   | Type   | Required | Description                       |
|---------|--------|----------|-----------------------------------|
| `x1`    | number | ✅        | Start X coordinate (canvas pixels) |
| `y1`    | number | ✅        | Start Y coordinate (canvas pixels) |
| `x2`    | number | ✅        | End X coordinate (canvas pixels)   |
| `y2`    | number | ✅        | End Y coordinate (canvas pixels)   |
| `color` | string | ✅        | Hex colour string e.g. `"#ff0000"` |
| `width` | number | ✅        | Brush size in pixels (1–20)        |

#### Response Body — Committed (200)

```json
{
  "ok": true,
  "index": 5,
  "commitIndex": 5
}
```

#### Response Body — Not Leader (403)

```json
{
  "error": "not_leader",
  "leaderId": "2",
  "message": "This replica is not the current leader. Route to leaderId."
}
```

> ⚠️ Gateway must handle the 403 and re-discover the leader via `/status`

#### Response Body — No Majority (503)

```json
{
  "error": "no_majority",
  "message": "Could not replicate to majority. Too many replicas offline."
}
```

---

### GET /status

**Called by:** Gateway (every 500ms, to discover who is leader)  
**Also useful for:** Manual debugging via `curl`  
**Purpose:** Return current RAFT state of this replica

#### Example Request

```
GET /status
```

#### Response Body (200)

```json
{
  "replicaId": "2",
  "state": "leader",
  "currentTerm": 3,
  "leaderId": "2",
  "logLength": 12,
  "commitIndex": 11,
  "peers": ["http://replica1:3001", "http://replica3:3003"]
}
```

| Field         | Type   | Description                                              |
|---------------|--------|----------------------------------------------------------|
| `replicaId`   | string | This replica's ID (`"1"`, `"2"`, or `"3"`)              |
| `state`       | string | `"follower"` \| `"candidate"` \| `"leader"`             |
| `currentTerm` | number | Monotonically increasing election term                   |
| `leaderId`    | string \| null | REPLICA_ID of current known leader (null during election) |
| `logLength`   | number | Number of entries in this replica's log                  |
| `commitIndex` | number | Index of last committed entry (-1 if none)               |
| `peers`       | array  | List of peer URLs this replica knows about               |

---

## Gateway Endpoints

> **Owner:** Person 2  
> **Base URL:** `http://gateway:8080`  

---

### POST /broadcast

**Called by:** The Leader replica (after committing a stroke)  
**Purpose:** Fan out a committed stroke to all connected browser WebSocket clients  
**When called:** Immediately after `commitIndex` is updated on the leader

#### Request Body

```json
{
  "x1": 120,
  "y1": 85,
  "x2": 145,
  "y2": 102,
  "color": "#e63946",
  "width": 4
}
```

> Same shape as the Stroke type — see [Shared Data Types](#shared-data-types)

#### Response Body (200)

```json
{
  "ok": true,
  "clientsNotified": 3
}
```

| Field             | Type   | Description                                      |
|-------------------|--------|--------------------------------------------------|
| `ok`              | boolean| Always true on success                           |
| `clientsNotified` | number | How many WebSocket clients received the broadcast |

#### Gateway Behaviour

1. Receive stroke payload
2. Loop over all entries in the `clients` Set
3. For each client: if `ws.readyState === WebSocket.OPEN`, call `ws.send(JSON.stringify({ type: 'stroke', stroke }))`
4. Skip closed/closing connections

---

### GET /canvas-state

**Called by:** Browser on page load  
**Purpose:** Return all committed strokes so a new user sees the full existing canvas  
**How:** Gateway fetches committed log from any healthy replica

#### Example Request

```
GET /canvas-state
```

#### Response Body (200)

```json
{
  "strokes": [
    { "x1": 10, "y1": 20, "x2": 30, "y2": 40, "color": "#000000", "width": 3 },
    { "x1": 50, "y1": 60, "x2": 70, "y2": 80, "color": "#e63946", "width": 5 },
    { "x1": 100, "y1": 110, "x2": 120, "y2": 130, "color": "#2a9d8f", "width": 2 }
  ],
  "count": 3
}
```

| Field     | Type   | Description                                      |
|-----------|--------|--------------------------------------------------|
| `strokes` | array  | All committed strokes in order (index 0 → N)     |
| `count`   | number | Total number of committed strokes                |

---

## WebSocket Messages

> **Transport:** `ws://localhost:8080`  
> All messages are JSON strings. Use `JSON.stringify()` to send, `JSON.parse()` to receive.

---

### Browser → Gateway

#### stroke — Send a new drawing stroke

Sent by browser every time the mouse moves while drawing.

```json
{
  "type": "stroke",
  "stroke": {
    "x1": 120,
    "y1": 85,
    "x2": 145,
    "y2": 102,
    "color": "#e63946",
    "width": 4
  }
}
```

| Field           | Type   | Required | Description                         |
|-----------------|--------|----------|-------------------------------------|
| `type`          | string | ✅        | Always `"stroke"` for this message  |
| `stroke.x1`     | number | ✅        | Start X (canvas pixels)             |
| `stroke.y1`     | number | ✅        | Start Y (canvas pixels)             |
| `stroke.x2`     | number | ✅        | End X (canvas pixels)               |
| `stroke.y2`     | number | ✅        | End Y (canvas pixels)               |
| `stroke.color`  | string | ✅        | Hex colour e.g. `"#e63946"`         |
| `stroke.width`  | number | ✅        | Brush thickness in pixels (1–20)    |

---

### Gateway → Browser

#### stroke — Broadcast a committed stroke to all clients

Sent by Gateway to ALL connected browsers when a stroke is committed by the cluster.

```json
{
  "type": "stroke",
  "stroke": {
    "x1": 120,
    "y1": 85,
    "x2": 145,
    "y2": 102,
    "color": "#e63946",
    "width": 4
  }
}
```

> ⚠️ Same schema as the outgoing stroke message. The browser's `ws.onmessage` handler draws this on the canvas — it does NOT send it back to the server (would cause infinite loop).

#### canvas_state — Full canvas on initial load

Sent once on connection open, contains all existing strokes for canvas replay.

```json
{
  "type": "canvas_state",
  "strokes": [
    { "x1": 10, "y1": 20, "x2": 30, "y2": 40, "color": "#000000", "width": 3 },
    { "x1": 50, "y1": 60, "x2": 70, "y2": 80, "color": "#e63946", "width": 5 }
  ]
}
```

#### error — Server-side error notification

Sent when the gateway cannot process a stroke (e.g. no leader available).

```json
{
  "type": "error",
  "code": "NO_LEADER",
  "message": "Election in progress. Please retry in a moment."
}
```

| Error Code    | When                                              |
|---------------|---------------------------------------------------|
| `NO_LEADER`   | Election is in progress, no leader yet            |
| `CLUSTER_DOWN`| All replicas are unreachable                      |
| `INVALID_MSG` | Received message with unknown/missing `type`      |

---

## Shared Data Types

### Stroke

Used in: `/stroke`, `/append-entries`, `/broadcast`, WebSocket messages, `/canvas-state`

```json
{
  "x1": 120,
  "y1": 85,
  "x2": 145,
  "y2": 102,
  "color": "#e63946",
  "width": 4
}
```

| Field   | Type   | Constraints         | Description                   |
|---------|--------|---------------------|-------------------------------|
| `x1`    | number | 0 ≤ x1 ≤ 900       | Start X (canvas is 900px wide) |
| `y1`    | number | 0 ≤ y1 ≤ 600       | Start Y (canvas is 600px tall) |
| `x2`    | number | 0 ≤ x2 ≤ 900       | End X                         |
| `y2`    | number | 0 ≤ y2 ≤ 600       | End Y                         |
| `color` | string | Valid CSS hex color  | e.g. `"#000000"` to `"#ffffff"` |
| `width` | number | 1 ≤ width ≤ 20     | Brush thickness in pixels     |

### LogEntry

Used internally in replica state. Wraps a Stroke with RAFT metadata.

```json
{
  "index": 5,
  "term": 2,
  "stroke": { "x1": 120, "y1": 85, "x2": 145, "y2": 102, "color": "#e63946", "width": 4 }
}
```

| Field    | Type   | Description                                              |
|----------|--------|----------------------------------------------------------|
| `index`  | number | Position in the log (0-based, monotonically increasing)  |
| `term`   | number | Election term when this entry was created by the leader  |
| `stroke` | Stroke | The actual drawing data                                  |

---

## Error Codes

### HTTP Status Codes Used

| Code | Meaning in This Project                                          |
|------|------------------------------------------------------------------|
| 200  | Success (also used for rejected votes/entries — check body)      |
| 400  | Bad request — missing required fields in body                    |
| 403  | Wrong target — e.g. sending stroke to a non-leader replica       |
| 503  | Unavailable — e.g. no majority reachable for commit              |

### Application Error Strings

| `error` value   | Endpoint      | Meaning                                           |
|-----------------|---------------|---------------------------------------------------|
| `"not_leader"`  | POST /stroke  | This replica is not the leader; redirect to `leaderId` |
| `"no_majority"` | POST /stroke  | Could not replicate to ≥2 replicas; too many offline |
| `"stale_term"`  | Any           | Sender's term is lower than receiver's; reject    |

---

## Sequence Diagrams

### Normal Stroke Flow

```
Browser          Gateway          Leader(R2)       Follower(R1)    Follower(R3)
   │                │                 │                 │               │
   │──WS stroke────►│                 │                 │               │
   │                │──POST /stroke──►│                 │               │
   │                │                 │──POST /append──►│               │
   │                │                 │──POST /append───────────────────►│
   │                │                 │◄── success ─────│               │
   │                │                 │◄── success ──────────────────────│
   │                │                 │  (majority=2/3, commit)          │
   │                │◄──POST /broadcast│                │               │
   │◄──WS stroke────│                 │                 │               │
```

### Leader Failover

```
Browser          Gateway          R1(dead)         R2               R3
   │                │                 │                │               │
   │──WS stroke────►│                 │                │               │
   │                │──POST /stroke──►│(timeout/fail)  │               │
   │                │   no response   │                │               │
   │                │──GET /status────────────────────►│  state=follower│
   │                │──GET /status────────────────────────────────────►│  state=follower
   │                │   (election in progress ~800ms)                   │
   │                │──GET /status────────────────────►│  state=leader  │
   │                │   currentLeader = R2             │               │
   │                │──POST /stroke───────────────────►│               │
   │◄──WS stroke────│                 │                │               │
```

### Restarted Node Catch-Up

```
Leader(R1)       Restarted Follower(R2)
   │                      │
   │──POST /heartbeat─────►│  (term=3, leaderId="1")
   │◄── ok ───────────────│  resetElectionTimer
   │                      │
   │──POST /append-entries►│  (entry.index=8, log.length=3 → MISMATCH)
   │◄── { success:false, logLength:3 }
   │                      │
   │──GET /sync-log?from=3►│  (leader pushes entries 3–8)
   │◄── { entries:[3,4,5,6,7,8], commitIndex:8 }
   │                      │  (follower bulk-appends all entries)
   │──POST /append-entries►│  (now in sync, normal replication resumes)
   │◄── { success:true }  │
```

---

## Change Log

| Version | Date   | Changed by | What changed                         |
|---------|--------|------------|--------------------------------------|
| 1.0     | Week 1 | All team   | Initial contract agreed and committed |

> **Rule:** Any change to this document requires all 3 teammates to agree before the code is updated. Add a new row to this table for every change.
