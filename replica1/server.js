const express = require('express');
const axios = require('axios');
const client = require('prom-client');

const app = express();
app.use(express.json());

const PORT = process.env.PORT;
const REPLICA_ID = process.env.REPLICA_ID;
const PEERS = process.env.PEERS.split(',');

let state = 'follower';
let currentTerm = 0;
let votedFor = null;
let log = [];
let commitIndex = -1;
let leaderId = null;
let leaderUrl = null;

let electionTimer;
let heartbeatInterval;

const SNAPSHOT_THRESHOLD = 100;
let snapshot = null;

// ✅ NEW: Prometheus metrics setup
const register = new client.Registry();
client.collectDefaultMetrics({ register });

// one metric per concept — labeled by replica ID
const termGauge = new client.Gauge({
  name: 'raft_current_term',
  help: 'Current RAFT term',
  labelNames: ['replica'],
  registers: [register]
});

const stateGauge = new client.Gauge({
  name: 'raft_state',
  help: 'Replica state: 0=follower 1=candidate 2=leader',
  labelNames: ['replica'],
  registers: [register]
});

const logLengthGauge = new client.Gauge({
  name: 'raft_log_length',
  help: 'Current in-memory log length',
  labelNames: ['replica'],
  registers: [register]
});

const commitGauge = new client.Gauge({
  name: 'raft_commit_index',
  help: 'Highest committed log index',
  labelNames: ['replica'],
  registers: [register]
});

const snapshotGauge = new client.Gauge({
  name: 'raft_snapshot_index',
  help: 'Last snapshot index (-1 if none)',
  labelNames: ['replica'],
  registers: [register]
});

const electionCounter = new client.Counter({
  name: 'raft_elections_total',
  help: 'Total elections started by this replica',
  labelNames: ['replica'],
  registers: [register]
});

const strokeCounter = new client.Counter({
  name: 'raft_strokes_committed_total',
  help: 'Total strokes committed by this replica as leader',
  labelNames: ['replica'],
  registers: [register]
});

// ✅ NEW: /metrics endpoint — Prometheus scrapes this every 5s
app.get('/metrics', async (req, res) => {
  // update gauges with latest values before responding
  stateGauge.set(
    { replica: REPLICA_ID },
    state === 'leader' ? 2 : state === 'candidate' ? 1 : 0
  );
  termGauge.set({ replica: REPLICA_ID }, currentTerm);
  logLengthGauge.set({ replica: REPLICA_ID }, log.length);
  commitGauge.set({ replica: REPLICA_ID }, commitIndex);
  snapshotGauge.set({ replica: REPLICA_ID }, snapshot ? snapshot.lastIncludedIndex : -1);

  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

function getLastLogIndex() {
  if (log.length === 0) return snapshot ? snapshot.lastIncludedIndex : -1;
  return log[log.length - 1].index;
}

function getNextLogIndex() {
  return getLastLogIndex() + 1;
}

async function getLeaderUrl() {
  if (leaderUrl) return leaderUrl;
  for (const peer of PEERS) {
    try {
      const res = await axios.get(peer + '/status', { timeout: 300 });
      if (res.data.state === 'leader') {
        leaderUrl = peer;
        return peer;
      }
    } catch {}
  }
  return null;
}

function takeSnapshot() {
  const base = snapshot ? snapshot.strokes : [];
  const newStrokes = log
    .filter(e => e.index <= commitIndex)
    .map(e => e.stroke);

  snapshot = {
    lastIncludedIndex: commitIndex,
    lastIncludedTerm: currentTerm,
    strokes: [...base, ...newStrokes]
  };

  log = log.filter(e => e.index > commitIndex);
  console.log(`[${REPLICA_ID}] 📸 Snapshot taken at index ${snapshot.lastIncludedIndex}, log trimmed to ${log.length} entries`);
}

function resetElectionTimer() {
  clearTimeout(electionTimer);
  const t = 500 + Math.random() * 300;
  electionTimer = setTimeout(startElection, t);
}

function stepDown(newTerm) {
  currentTerm = newTerm;
  state = 'follower';
  votedFor = null;
  clearInterval(heartbeatInterval);
  resetElectionTimer();
}

app.get('/status', (req, res) => {
  res.json({
    state, currentTerm, leaderId,
    logLength: log.length,
    commitIndex,
    snapshotIndex: snapshot ? snapshot.lastIncludedIndex : -1
  });
});

app.post('/request-vote', (req, res) => {
  const { term, candidateId } = req.body;
  if (term > currentTerm) stepDown(term);
  let voteGranted = false;
  if (term === currentTerm && (votedFor === null || votedFor === candidateId)) {
    voteGranted = true;
    votedFor = candidateId;
    resetElectionTimer();
  }
  res.json({ voteGranted, term: currentTerm });
});

async function startElection() {
  state = 'candidate';
  currentTerm += 1;
  votedFor = REPLICA_ID;

  // ✅ NEW: increment election counter
  electionCounter.inc({ replica: REPLICA_ID });
  console.log(`[${REPLICA_ID}] 🗳️ Starting election for term ${currentTerm}`);

  let votes = 1;
  for (const peer of PEERS) {
    try {
      const res = await axios.post(peer + '/request-vote',
        { term: currentTerm, candidateId: REPLICA_ID },
        { timeout: 300 }
      );
      if (res.data.voteGranted) votes++;
    } catch {}
  }

  if (votes >= 2) {
    state = 'leader';
    leaderUrl = null;
    console.log(`[${REPLICA_ID}] 👑 Became leader for term ${currentTerm}`);
    startHeartbeats();
  } else {
    state = 'follower';
    resetElectionTimer();
  }
}

function startHeartbeats() {
  heartbeatInterval = setInterval(() => {
    if (state !== 'leader') { clearInterval(heartbeatInterval); return; }
    for (const peer of PEERS) {
      axios.post(peer + '/heartbeat',
        { term: currentTerm, leaderId: REPLICA_ID },
        { timeout: 300 }
      ).catch(() => {});
    }
  }, 150);
}

app.post('/heartbeat', (req, res) => {
  const { term, leaderId: incomingLeaderId } = req.body;
  if (term >= currentTerm) {
    stepDown(term);
    leaderId = incomingLeaderId;
    leaderUrl = PEERS.find(p => p.includes(`replica${incomingLeaderId}:`));
  }
  res.json({});
});

app.post('/append-entries', async (req, res) => {
  const { term, entry, leaderCommit } = req.body;
  if (term < currentTerm) return res.json({ success: false, logLength: getLastLogIndex() + 1 });

  stepDown(term);

  if (entry) {
    const prevLogIndex = entry.index - 1;
    const snapshotLastIndex = snapshot ? snapshot.lastIncludedIndex : -1;
    const prevCoveredBySnapshot = prevLogIndex <= snapshotLastIndex;
    const prevInLog = log.find(e => e.index === prevLogIndex);
    const alreadyHaveEntry = log.find(e => e.index === entry.index);

    if (alreadyHaveEntry) {
      // already have it, skip
    } else if (prevLogIndex < 0 || prevCoveredBySnapshot || prevInLog) {
      log.push(entry);
    } else {
      const url = await getLeaderUrl();
      if (url) {
        try {
          const myFrom = snapshotLastIndex + 1;
          const syncRes = await axios.get(
            url + '/sync-log?from=' + myFrom,
            { timeout: 500 }
          );
          if (syncRes.data.snapshot) {
            snapshot = syncRes.data.snapshot;
            log = [];
            commitIndex = snapshot.lastIncludedIndex;
            leaderUrl = url;
            console.log(`[${REPLICA_ID}] 📥 Applied snapshot up to index ${commitIndex}`);
          }
          if (syncRes.data.entries && syncRes.data.entries.length > 0) {
            for (const e of syncRes.data.entries) {
              if (!log.find(l => l.index === e.index)) log.push(e);
            }
          }
        } catch {}
      }
      return res.json({ success: false, logLength: getLastLogIndex() + 1 });
    }
  }

  if (leaderCommit > commitIndex) commitIndex = leaderCommit;
  res.json({ success: true, logLength: getLastLogIndex() + 1 });
});

app.post('/stroke', async (req, res) => {
  if (state !== 'leader') return res.json({ error: 'not leader', leaderId });

  const logIndex = getNextLogIndex();
  const strokeWithIndex = {
    ...req.body,
    logIndex,
    clientId: req.body.clientId || 'unknown'
  };

  const entry = { term: currentTerm, index: logIndex, stroke: strokeWithIndex };
  log.push(entry);

  commitIndex = entry.index;
  axios.post('http://gateway:8080/broadcast', strokeWithIndex).catch(() => {});

  for (const peer of PEERS) {
    axios.post(peer + '/append-entries',
      { term: currentTerm, entry, leaderCommit: commitIndex },
      { timeout: 300 }
    ).catch(() => {});
  }

  // ✅ NEW: increment stroke counter
  strokeCounter.inc({ replica: REPLICA_ID });

  const snapshotBase = snapshot ? snapshot.lastIncludedIndex : -1;
  if (commitIndex - snapshotBase >= SNAPSHOT_THRESHOLD) {
    takeSnapshot();
  }

  res.json({});
});

app.post('/clear', async (req, res) => {
  if (state !== 'leader') return res.json({ error: 'not leader' });
  const logIndex = getNextLogIndex();
  const entry = { term: currentTerm, index: logIndex, stroke: null };
  log.push(entry);
  commitIndex = entry.index;

  axios.post('http://gateway:8080/broadcast-clear', {}).catch(() => {});

  for (const peer of PEERS) {
    axios.post(peer + '/append-entries',
      { term: currentTerm, entry, leaderCommit: commitIndex },
      { timeout: 300 }
    ).catch(() => {});
  }

  res.json({});
});

app.get('/sync-log', (req, res) => {
  const fromIndex = parseInt(req.query.from || 0);
  const snapshotLastIndex = snapshot ? snapshot.lastIncludedIndex : -1;

  if (fromIndex <= snapshotLastIndex) {
    console.log(`[${REPLICA_ID}] 📤 Sending snapshot to catching-up follower (from=${fromIndex})`);
    return res.json({ snapshot, entries: log, commitIndex });
  }

  const entries = log.filter(e => e.index >= fromIndex);
  res.json({ entries, commitIndex });
});

app.listen(PORT, () => {
  console.log(`[Replica ${REPLICA_ID}] started on port ${PORT}`);
  resetElectionTimer();
});