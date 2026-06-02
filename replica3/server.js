const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

const PORT = process.env.PORT;
let state = 'follower', currentTerm = 0, votedFor = null, log = [], commitIndex = -1, leaderId = null;
const REPLICA_ID = process.env.REPLICA_ID;
const PEERS = process.env.PEERS.split(',');

let electionTimer;
let heartbeatInterval;
let leaderUrl;

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
  res.json({ state, currentTerm, leaderId, logLength: log.length, commitIndex });
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
      const res = await axios.post(peer + '/request-vote', { term: currentTerm, candidateId: REPLICA_ID });
      if (res.data.voteGranted) votes++;
    } catch {}
  }
  if (votes >= 2) {
    state = 'leader';
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
      axios.post(peer + '/heartbeat', { term: currentTerm, leaderId: REPLICA_ID }).catch(() => {});
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
  if (term < currentTerm) return res.json({ success: false, logLength: log.length });

  stepDown(term);

  if (entry) {
    const prevLogIndex = entry.index - 1;
    if (prevLogIndex >= 0 && (log.length <= prevLogIndex || !log[prevLogIndex])) {
      if (leaderUrl) {
        try {
          const syncRes = await axios.get(leaderUrl + '/sync-log?from=' + log.length);
          log.push(...syncRes.data.entries);
        } catch {}
      }
      return res.json({ success: false, logLength: log.length });
    } else {
      if (log.length <= entry.index) log.push(entry);
    }
  }

  if (leaderCommit > commitIndex) commitIndex = leaderCommit;
  res.json({ success: true, logLength: log.length });
});

// ✅ CHANGED: attach logIndex and clientId to stroke before broadcasting
app.post('/stroke', async (req, res) => {
  if (state !== 'leader') return res.json({ error: 'not leader', leaderId });

  // Stamp the stroke with its global position in the log
  const logIndex = log.length;
  const strokeWithIndex = {
    ...req.body,
    logIndex,                              // global ordering number
    clientId: req.body.clientId || 'unknown'  // which user drew this
  };

  const entry = { term: currentTerm, index: logIndex, stroke: strokeWithIndex };
  log.push(entry);

  let confirmations = 1;
  for (const peer of PEERS) {
    try {
      const r = await axios.post(peer + '/append-entries', { term: currentTerm, entry, leaderCommit: commitIndex });
      if (r.data.success) confirmations++;
    } catch {}
  }

  if (confirmations >= 2) {
    commitIndex = entry.index;
    // ✅ CHANGED: broadcast the stamped stroke (includes logIndex)
    axios.post('http://gateway:8080/broadcast', strokeWithIndex);
  }
  res.json({});
});

app.post('/clear', async (req, res) => {
  if (state !== 'leader') return res.json({ error: 'not leader' });
  const entry = { term: currentTerm, index: log.length, stroke: null };
  log.push(entry);
  let confirmations = 1;
  for (const peer of PEERS) {
    try {
      const r = await axios.post(peer + '/append-entries', { term: currentTerm, entry, leaderCommit: commitIndex });
      if (r.data.success) confirmations++;
    } catch {}
  }
  if (confirmations >= 2) {
    commitIndex = entry.index;
    axios.post('http://gateway:8080/broadcast-clear', {});
  }
  res.json({});
});

app.get('/sync-log', (req, res) => {
  const fromIndex = parseInt(req.query.from || 0);
  res.json({ entries: log.slice(fromIndex), commitIndex });
});

app.listen(PORT, () => {
  console.log(`[Replica ${REPLICA_ID}] started on port ${PORT}`);
  resetElectionTimer();
});