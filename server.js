const WebSocket = require('ws');
const http = require('http');
const { v4: uuidv4 } = require('uuid');
const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('OK'); return; }
  res.writeHead(200); res.end('GhostChat');
});

const wss = new WebSocket.Server({ server });
const rooms = new Map();
const peerIds = new WeakMap();

wss.on('connection', (ws) => {
  const peerId = uuidv4();
  peerIds.set(ws, peerId);

  ws.on('message', (data) => {
    let msg; try { msg = JSON.parse(data); } catch { return; }
    const { type, roomId, payload } = msg;

    if (type === 'join') {
      if (!rooms.has(roomId)) rooms.set(roomId, new Set());
      const room = rooms.get(roomId);
      room.forEach(peer => {
        if (peer !== ws && peer.readyState === WebSocket.OPEN) {
          peer.send(JSON.stringify({ type: 'peer-joined', peerId }));
          ws.send(JSON.stringify({ type: 'peer-joined', peerId: peerIds.get(peer) }));
        }
      });
      room.add(ws);
      ws.roomId = roomId;
      ws.send(JSON.stringify({ type: 'joined', peerId, roomId, peerCount: room.size }));
    }

    if (['offer','answer','ice-candidate','message'].includes(type)) {
      const room = rooms.get(ws.roomId);
      if (!room) return;
      room.forEach(peer => {
        if (peer !== ws && peer.readyState === WebSocket.OPEN)
          peer.send(JSON.stringify({ type, payload, fromPeerId: peerId }));
      });
    }
  });

  ws.on('close', () => {
    const roomId = ws.roomId;
    if (!roomId || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    room.delete(ws);
    room.forEach(peer => {
      if (peer.readyState === WebSocket.OPEN)
        peer.send(JSON.stringify({ type: 'peer-left', peerId }));
    });
    if (room.size === 0) rooms.delete(roomId);
  });
});

server.listen(PORT, () => console.log('GhostChat server on port ' + PORT));
