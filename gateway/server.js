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
        } catch (e) {
            // replica offline, try next
        }
    }
    currentLeader = null;
    return null;
}

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
            if (!currentLeader) {
                await discoverLeader();
            }
            if (!currentLeader) {
                ws.send(JSON.stringify({ type: 'error', code: 'NO_LEADER' }));
                return;
            }
            try {
                await axios.post(`${currentLeader}/stroke`, data.stroke);
            } catch (e) {
                // leader died, find new one
                currentLeader = null;
                await discoverLeader();
            }
        }

        if (data.type === 'clear') {
            if (!currentLeader) {
                await discoverLeader();
            }
            if (!currentLeader) {
                ws.send(JSON.stringify({ type: 'error', code: 'NO_LEADER' }));
                return;
            }
            try {
                await axios.post(`${currentLeader}/clear`);
            } catch (e) {
                // leader died, find new one
                currentLeader = null;
                await discoverLeader();
            }
        }
    });
});

// Called by leader after a stroke is committed
app.post('/broadcast', (req, res) => {
    const stroke = req.body;
    let clientsNotified = 0;

    for (const client of clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'stroke', stroke }));
            clientsNotified++;
        }
    }

    console.log(`Broadcasted stroke to ${clientsNotified} clients`);
    res.json({ ok: true, clientsNotified });
});

// Called by leader after a clear is committed
app.post('/broadcast-clear', (req, res) => {
    let clientsNotified = 0;

    for (const client of clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'clear' }));
            clientsNotified++;
        }
    }

    console.log(`Broadcasted clear to ${clientsNotified} clients`);
    res.json({ ok: true, clientsNotified });
});

// Called by new tabs on connect to replay committed log
// null stroke entries = clear markers, replayed in order
app.get('/canvas-state', async (req, res) => {
    for (const url of REPLICAS) {
        try {
            const result = await axios.get(`${url}/sync-log?from=0`, { timeout: 500 });
            // Return all entries including null (clear) markers — frontend replays them in order
            const strokes = result.data.entries.map(entry => entry.stroke);
            return res.json({ strokes, count: strokes.length });
        } catch (e) {
            // this replica is offline, try next one
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