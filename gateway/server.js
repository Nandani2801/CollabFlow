const express = require('express');
const axios = require('axios');
const WebSocket = require('ws');

const app = express();
app.use(express.json());

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  next();
});

const PORT = 8080;
const REPLICAS = [
  'http://replica1:3001',
  'http://replica2:3002',
  'http://replica3:3003'
];

let currentLeader = null;

async function discoverLeader() {
  for (const url of REPLICAS) {
    try {
      const res = await axios.get(`${url}/status`, { timeout: 500 });
      if (res.data.state === 'leader') {
        if (currentLeader !== url) {
          console.log(`Leader found: ${url}`);
        }
        currentLeader = url;
        return url;
      }
    } catch {}
  }
  currentLeader = null;
  return null;
}

// Discover leader every 500ms
setInterval(discoverLeader, 500);

const clients = new Set();
const wss = new WebSocket.Server({ noServer: true });

wss.on('connection', (ws) => {
  clients.add(ws);
  console.log(`Browser connected. Total clients: ${clients.size}`);

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`Browser disconnected. Total clients: ${clients.size}`);
  });

  ws.on('message', async (msg) => {
    const data = JSON.parse(msg);

    if (data.type === 'stroke') {
      if (!currentLeader) await discoverLeader();
      if (!currentLeader) {
        ws.send(JSON.stringify({ type: 'error', code: 'NO_LEADER' }));
        return;
      }
      try {
        await axios.post(`${currentLeader}/stroke`, data.stroke, { timeout: 1000 });
      } catch {
        currentLeader = null;
        await discoverLeader();
      }
    }

    if (data.type === 'clear') {
      if (!currentLeader) await discoverLeader();
      if (!currentLeader) {
        ws.send(JSON.stringify({ type: 'error', code: 'NO_LEADER' }));
        return;
      }
      try {
        await axios.post(`${currentLeader}/clear`, {}, { timeout: 1000 });
      } catch {
        currentLeader = null;
        await discoverLeader();
      }
    }
  });
});

app.post('/broadcast', (req, res) => {
  const stroke = req.body;
  let count = 0;
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type: 'stroke', stroke }));
      count++;
    }
  }
  console.log(`Broadcasted stroke to ${count} clients`);
  res.json({ ok: true, clientsNotified: count });
});

app.post('/broadcast-clear', (req, res) => {
  let count = 0;
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type: 'clear' }));
      count++;
    }
  }
  res.json({ ok: true, clientsNotified: count });
});

// ✅ FIXED: canvas-state correctly rebuilds full history from snapshot + log
app.get('/canvas-state', async (req, res) => {
  for (const url of REPLICAS) {
    try {
      // step 1 — check if this replica has a snapshot
      const statusRes = await axios.get(`${url}/status`, { timeout: 500 });
      const snapshotIndex = statusRes.data.snapshotIndex;

      let strokes = [];

      if (snapshotIndex >= 0) {
        // step 2a — replica has snapshot, fetch it
        const snapRes = await axios.get(`${url}/sync-log?from=0`, { timeout: 1000 });

        if (snapRes.data.snapshot && snapRes.data.snapshot.strokes) {
          // these are all strokes up to snapshotIndex
          strokes = [...snapRes.data.snapshot.strokes];
          console.log(`Canvas state: got ${strokes.length} strokes from snapshot`);
        }

        // step 3 — fetch log entries that came AFTER the snapshot
        const logRes = await axios.get(
          `${url}/sync-log?from=${snapshotIndex + 1}`,
          { timeout: 1000 }
        );

        if (logRes.data.entries && logRes.data.entries.length > 0) {
          const logStrokes = logRes.data.entries.map(e => e.stroke);
          strokes = [...strokes, ...logStrokes];
          console.log(`Canvas state: added ${logStrokes.length} strokes from log`);
        }

      } else {
        // step 2b — no snapshot yet, just get full log
        const logRes = await axios.get(`${url}/sync-log?from=0`, { timeout: 1000 });

        if (logRes.data.entries) {
          strokes = logRes.data.entries.map(e => e.stroke);
        }
      }

      console.log(`Canvas state total: ${strokes.length} strokes`);
      return res.json({ strokes, count: strokes.length });

    } catch {
      // replica offline, try next one
    }
  }
  res.json({ strokes: [], count: 0 });
});

const server = app.listen(PORT, () => {
  console.log(`Gateway running on port 8080 (external: 8081)`);
  discoverLeader();
});

server.on('upgrade', (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});