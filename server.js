// OperalsMeet — signalling server + mediasoup SFU
require('dotenv').config();
const os = require('os');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const mediasoup = require('mediasoup');
const db = require('./db');

const PORT = Number(process.env.PORT || 3000);
const MAX_PEERS = Number(process.env.MAX_PEERS || 30);
const RTC_PORT = Number(process.env.RTC_PORT || 40000);
const LISTEN_IP = process.env.LISTEN_IP || '0.0.0.0';
const ANNOUNCED_IP = process.env.ANNOUNCED_IP || guessIp();
const NUM_WORKERS = Number(process.env.MEDIASOUP_WORKERS || Math.min(os.cpus().length, 4));
const FRONTEND_URL = process.env.FRONTEND_URL || '*';

function guessIp() {
  for (const list of Object.values(os.networkInterfaces()))
    for (const a of list || []) if (a.family === 'IPv4' && !a.internal) return a.address;
  return '127.0.0.1';
}

const mediaCodecs = [
  { kind: 'audio', mimeType: 'audio/opus', clockRate: 48000, channels: 2 },
  { kind: 'video', mimeType: 'video/VP8', clockRate: 90000, parameters: { 'x-google-start-bitrate': 1000 } },
  { kind: 'video', mimeType: 'video/H264', clockRate: 90000,
    parameters: { 'packetization-mode': 1, 'profile-level-id': '42e01f', 'level-asymmetry-allowed': 1 } },
];

// ------------------------------------------------------------------ mediasoup workers
const workers = [];
let nextWorker = 0;
async function startWorkers() {
  for (let i = 0; i < NUM_WORKERS; i++) {
    const worker = await mediasoup.createWorker({ logLevel: 'warn' });
    worker.on('died', () => { console.error('mediasoup worker died — exiting'); process.exit(1); });
    const webRtcServer = await worker.createWebRtcServer({
      listenInfos: [
        { protocol: 'udp', ip: LISTEN_IP, announcedAddress: ANNOUNCED_IP, port: RTC_PORT + i },
        { protocol: 'tcp', ip: LISTEN_IP, announcedAddress: ANNOUNCED_IP, port: RTC_PORT + i },
      ],
    });
    workers.push({ worker, webRtcServer });
  }
}

// ------------------------------------------------------------------ HTTP
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 5e6,
  pingTimeout: 20000,
  cors: { origin: FRONTEND_URL },
});

app.use((req, res, next) => { res.setHeader('Access-Control-Allow-Origin', FRONTEND_URL); next(); });
app.get('/config', (_req, res) => {
  let iceServers = [];
  if (process.env.ICE_SERVERS) { try { iceServers = JSON.parse(process.env.ICE_SERVERS); } catch { console.warn('ICE_SERVERS invalide'); } }
  res.json({ iceServers, maxPeers: MAX_PEERS });
});
app.get('/health', (_req, res) => res.json({ ok: true, rooms: rooms.size, peers: [...rooms.values()].reduce((n, r) => n + (r.peers?.size || 0), 0) }));

// ------------------------------------------------------------------ rooms
const rooms = new Map();
async function getRoom(id) {
  const r = rooms.get(id);
  if (r) return r instanceof Promise ? r : r;
  const p = (async () => {
    const w = workers[nextWorker++ % workers.length];
    const router = await w.worker.createRouter({ mediaCodecs });
    const room = {
      id, router, webRtcServer: w.webRtcServer, organizerId: null, peers: new Map(), closeTimer: null,
      doc: { open: false, html: null, title: `CR_Reunion_${id}`, version: 0, openedBy: null },
      chat: [],
    };
    rooms.set(id, room);
    await db.ensureMeeting(id).catch(() => {});
    return room;
  })();
  rooms.set(id, p);
  return p;
}
const peerPublic = (room, id, p) => ({
  id, name: p.name, cam: p.cam, mic: p.mic, hand: p.hand, screen: p.screen, organizer: id === room.organizerId,
  producers: [...p.producers.values()].map((pr) => ({ id: pr.id, kind: pr.kind })),
});

io.on('connection', (socket) => {
  let room = null;
  let peer = null;

  const on = (ev, fn) => socket.on(ev, async (data, ack) => {
    try { const r = await fn(data || {}); if (typeof ack === 'function') ack(r || {}); }
    catch (e) { console.warn(ev, e.message); if (typeof ack === 'function') ack({ error: e.message }); }
  });
  const need = () => { if (!room || !peer) throw new Error('Pas dans une réunion'); };

  on('join', async ({ room: roomId, name }) => {
    if (typeof roomId !== 'string' || !/^[\w-]{3,64}$/.test(roomId)) throw new Error('Code de réunion invalide');
    const r = await getRoom(roomId);
    if (r.peers.size >= MAX_PEERS) throw new Error(`La réunion est complète (${MAX_PEERS} participants max).`);
    clearTimeout(r.closeTimer);
    room = r;
    peer = { name: String(name || 'Invité').slice(0, 40), cam: true, mic: true, hand: false, screen: false,
      transports: new Map(), producers: new Map(), consumers: new Map() };
    const existing = [...r.peers.entries()].map(([id, p]) => peerPublic(r, id, p));
    if (!r.organizerId) {
      r.organizerId = socket.id;
      db.setOrganizerName(r.id, peer.name).catch(() => {});
    }
    r.peers.set(socket.id, peer);
    socket.join(r.id);
    socket.to(r.id).emit('peer-joined', peerPublic(r, socket.id, peer));
    const chatHistory = await db.getMessages(r.id).catch(() => r.chat.slice(-100));
    return { self: socket.id, organizerId: r.organizerId, rtpCapabilities: r.router.rtpCapabilities,
      peers: existing, doc: r.doc, chat: chatHistory, maxPeers: MAX_PEERS };
  });

  // ---- media (SFU) ----
  on('createTransport', async ({ direction }) => {
    need();
    const t = await room.router.createWebRtcTransport({
      webRtcServer: room.webRtcServer, enableUdp: true, enableTcp: true, preferUdp: true,
      initialAvailableOutgoingBitrate: 800000, appData: { direction },
    });
    if (direction === 'send') await t.setMaxIncomingBitrate(2500000).catch(() => {});
    t.on('dtlsstatechange', (s) => { if (s === 'closed' || s === 'failed') t.close(); });
    peer.transports.set(t.id, t);
    return { id: t.id, iceParameters: t.iceParameters, iceCandidates: t.iceCandidates, dtlsParameters: t.dtlsParameters };
  });

  on('connectTransport', async ({ transportId, dtlsParameters }) => {
    need();
    const t = peer.transports.get(transportId); if (!t) throw new Error('transport inconnu');
    await t.connect({ dtlsParameters });
  });

  on('produce', async ({ transportId, kind, rtpParameters, appData }) => {
    need();
    const t = peer.transports.get(transportId); if (!t) throw new Error('transport inconnu');
    const producer = await t.produce({ kind, rtpParameters, paused: !!appData?.paused, appData: { kind } });
    peer.producers.set(producer.id, producer);
    producer.on('transportclose', () => peer.producers.delete(producer.id));
    socket.to(room.id).emit('new-producer', { peerId: socket.id, producerId: producer.id, kind });
    return { id: producer.id };
  });

  on('closeProducer', ({ producerId }) => { need(); const p = peer.producers.get(producerId); p?.close(); peer.producers.delete(producerId); });
  on('pauseProducer', async ({ producerId }) => { need(); await peer.producers.get(producerId)?.pause(); });
  on('resumeProducer', async ({ producerId }) => { need(); await peer.producers.get(producerId)?.resume(); });

  on('consume', async ({ producerId, rtpCapabilities }) => {
    need();
    if (!room.router.canConsume({ producerId, rtpCapabilities })) throw new Error('Impossible de recevoir ce flux');
    const t = [...peer.transports.values()].find((x) => x.appData.direction === 'recv');
    if (!t) throw new Error('Pas de transport de réception');
    const consumer = await t.consume({ producerId, rtpCapabilities, paused: true });
    peer.consumers.set(consumer.id, consumer);
    const gone = () => { peer.consumers.delete(consumer.id); socket.emit('consumer-closed', { consumerId: consumer.id }); };
    consumer.on('transportclose', () => peer.consumers.delete(consumer.id));
    consumer.on('producerclose', gone);
    if (consumer.type === 'simulcast') await consumer.setPreferredLayers({ spatialLayer: 0, temporalLayer: 2 });
    return { id: consumer.id, producerId, kind: consumer.kind, rtpParameters: consumer.rtpParameters };
  });
  on('resumeConsumer', async ({ consumerId }) => { need(); await peer.consumers.get(consumerId)?.resume(); });
  on('pauseConsumer', async ({ consumerId }) => { need(); await peer.consumers.get(consumerId)?.pause(); });
  on('setLayers', async ({ consumerId, spatialLayer }) => {
    need(); const c = peer.consumers.get(consumerId);
    if (c?.type === 'simulcast') await c.setPreferredLayers({ spatialLayer: Math.max(0, Math.min(2, spatialLayer | 0)), temporalLayer: 2 });
  });

  on('state', (patch) => {
    need();
    for (const k of ['cam', 'mic', 'hand', 'screen']) if (k in patch) peer[k] = !!patch[k];
    socket.to(room.id).emit('peer-state', { id: socket.id, name: peer.name, cam: peer.cam, mic: peer.mic, hand: peer.hand, screen: peer.screen });
  });

  on('doc-open', ({ html }) => {
    need();
    room.doc.open = true; room.doc.openedBy = peer.name;
    if (!room.doc.html && typeof html === 'string') { room.doc.html = html; room.doc.version++; }
    io.to(room.id).emit('doc-opened', { by: peer.name, byId: socket.id, doc: room.doc });
    db.saveDocument(room.id, room.doc.title, room.doc.html, room.doc.version).catch(() => {});
  });
  on('doc-update', ({ html, title }) => {
    need(); if (!room.doc.open) return;
    if (typeof html === 'string' && html.length < 4e6) room.doc.html = html;
    if (typeof title === 'string') room.doc.title = title.slice(0, 80);
    room.doc.version++;
    socket.to(room.id).emit('doc-update', { html: room.doc.html, title: room.doc.title, version: room.doc.version });
    socket.emit('doc-saved', { version: room.doc.version });
    db.saveDocument(room.id, room.doc.title, room.doc.html, room.doc.version).catch(() => {});
  });
  on('doc-close', () => {
    need(); if (socket.id !== room.organizerId) throw new Error('Réservé à l\'organisateur');
    room.doc.open = false;
    io.to(room.id).emit('doc-closed', { by: peer.name });
  });

  on('chat', ({ text }) => {
    need(); if (typeof text !== 'string' || !text.trim()) return;
    const msg = { id: socket.id, name: peer.name, text: text.slice(0, 2000), ts: Date.now() };
    room.chat.push(msg); if (room.chat.length > 300) room.chat.shift();
    io.to(room.id).emit('chat', msg);
    db.saveMessage(room.id, peer.name, msg.text).catch(() => {});
  });

  on('end-for-all', () => { need(); if (socket.id === room.organizerId) socket.to(room.id).emit('ended'); });

  socket.on('disconnect', () => {
    if (!room || !peer) return;
    for (const t of peer.transports.values()) t.close();
    room.peers.delete(socket.id);
    socket.to(room.id).emit('peer-left', { id: socket.id });
    if (room.organizerId === socket.id) {
      room.organizerId = room.peers.keys().next().value || null;
      if (room.organizerId) io.to(room.id).emit('organizer', { id: room.organizerId });
    }
    if (room.peers.size === 0) {
      const r = room;
      db.closeMeeting(r.id).catch(() => {});
      r.closeTimer = setTimeout(() => { if (r.peers.size === 0) { r.router.close(); rooms.delete(r.id); } }, 10 * 60 * 1000);
    }
  });
});

startWorkers().then(() => db.setup()).then(() => {
  server.listen(PORT, () => {
    console.log(`OperalsMeet : http://localhost:${PORT}`);
    console.log(`Média : ${NUM_WORKERS} worker(s), UDP/TCP ${RTC_PORT}-${RTC_PORT + NUM_WORKERS - 1}, IP annoncée ${ANNOUNCED_IP}, ${MAX_PEERS} participants max`);
  });
}).catch((e) => { console.error('Démarrage impossible :', e); process.exit(1); });
