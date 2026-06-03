const express = require('express');
const axios = require('axios');

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

// ✅ get real last index accounting for snapshot offset
function getLastLogIndex() {
  if (log.length === 0) return snapshot ? snapshot.lastIncludedIndex : -1;
  return log[log.length - 1].index;
}

function getNextLogIndex() {
  return getLastLogIndex() + 1;
}

// ✅ NEW: if leaderUrl is null, find it from peers
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
    leaderUrl = null; // i am the leader now
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
    // ✅ always update leaderUrl on every heartbeat
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
      // safe to append
      log.push(entry);
    } else {
      // gap detected — need to catch up
      // ✅ FIX: use getLeaderUrl() instead of leaderUrl directly
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
            // ✅ FIX: set leaderUrl after successful sync
            leaderUrl = url;
            console.log(`[${REPLICA_ID}] 📥 Applied snapshot up to index ${commitIndex}`);
          }

          if (syncRes.data.entries && syncRes.data.entries.length > 0) {
            for (const e of syncRes.data.entries) {
              if (!log.find(l => l.index === e.index)) {
                log.push(e);
              }
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

  // broadcast immediately — no waiting
  commitIndex = entry.index;
  axios.post('http://gateway:8080/broadcast', strokeWithIndex).catch(() => {});

  // replicate to followers in background
  for (const peer of PEERS) {
    axios.post(peer + '/append-entries',
      { term: currentTerm, entry, leaderCommit: commitIndex },
      { timeout: 300 }
    ).catch(() => {});
  }

  // snapshot check
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