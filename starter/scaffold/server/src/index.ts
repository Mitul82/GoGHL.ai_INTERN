import crypto from 'node:crypto';
import { createServer } from 'node:http';
import express, { type NextFunction, type Request, type Response } from 'express';
import { WebSocketServer } from 'ws';
import { config } from './config.js';
import { getDb } from './db.js';
import { getRedis } from './redis.js';
import {
  attachWebSocket,
  enqueueReply,
  ensureIndexes,
  publishWorkspaceEvent,
  startOutboundWorker,
  updateOutboundStatus,
} from './store.js';
import type {
  ConversationDoc,
  WorkspaceRole,
  InboundMessageDoc,
  OutboundMessageDoc,
  SessionPayload,
} from './types.js';

const SESSION_COOKIE = 'sb_session';
const SESSION_SECRET = crypto
  .createHash('sha256')
  .update(`${config.graderToken}:${config.oidc.clientSecret}`)
  .digest('hex');

const app = express();

type RequestWithRawBody = Request & { rawBody?: Buffer; session?: SessionPayload };

app.use(
  express.json({
    verify: (req, _res, buf) => {
      (req as RequestWithRawBody).rawBody = buf;
    },
  })
);

// Malformed JSON -> always 400, never 5xx
app.use((err: unknown, _req: Request, res: Response, next: NextFunction) => {
  if (err instanceof SyntaxError && 'status' in err && (err as { status?: number }).status === 400) {
    res.status(400).json({ error: 'invalid_json' });
    return;
  }
  next(err);
});

function parseCookies(cookieHeader: string | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!cookieHeader) return out;
  for (const part of cookieHeader.split(';')) {
    const idx = part.indexOf('=');
    if (idx <= 0) continue;
    const key = part.slice(0, idx).trim();
    const val = part.slice(idx + 1).trim();
    try {
      out[key] = decodeURIComponent(val);
    } catch {
      out[key] = val;
    }
  }
  return out;
}

function signSession(payload: SessionPayload): string {
  const encoded = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig = crypto.createHmac('sha256', SESSION_SECRET).update(encoded).digest('base64url');
  return `${encoded}.${sig}`;
}

function verifySessionToken(token: string | undefined): SessionPayload | null {
  if (!token || typeof token !== 'string') return null;
  const parts = token.split('.');
  if (parts.length !== 2) return null;
  const [encoded, sig] = parts;
  const expected = crypto.createHmac('sha256', SESSION_SECRET).update(encoded).digest('base64url');
  try {
    const a = Buffer.from(sig);
    const b = Buffer.from(expected);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
    const parsed = JSON.parse(Buffer.from(encoded, 'base64url').toString('utf8')) as SessionPayload;
    if (!parsed.sub || !Array.isArray(parsed.workspaces)) return null;
    return parsed;
  } catch {
    return null;
  }
}

function readSession(req: Request): SessionPayload | null {
  const cookies = parseCookies(req.headers.cookie);
  return verifySessionToken(cookies[SESSION_COOKIE]);
}

function requireAuth(req: Request, res: Response, next: NextFunction): void {
  const session = readSession(req);
  if (!session) {
    res.status(401).json({ error: 'unauthorized' });
    return;
  }
  (req as RequestWithRawBody).session = session;
  next();
}

function requireWorkspaceMember(req: Request, res: Response, workspaceId: string): boolean {
  const session = (req as RequestWithRawBody).session;
  if (!session) {
    res.status(401).json({ error: 'unauthorized' });
    return false;
  }
  if (!session.workspaces.some((w) => w.id === workspaceId)) {
    res.status(403).json({ error: 'forbidden' });
    return false;
  }
  return true;
}

function requireGrader(req: Request, res: Response, next: NextFunction): void {
  const token = String(req.headers['x-grader-token'] || '');
  if (!config.graderToken || token !== config.graderToken) {
    res.status(401).json({ error: 'unauthorized' });
    return;
  }
  next();
}

app.get('/healthz', async (_req, res) => {
  try {
    await getRedis().ping();
    res.json({ ok: true });
  } catch {
    res.status(503).json({ ok: false });
  }
});

// OIDC Authorization Code + PKCE (S256)
app.get('/auth/login', async (_req, res) => {
  const verifier = crypto.randomBytes(32).toString('base64url');
  const challenge = crypto.createHash('sha256').update(verifier).digest('base64url');
  const state = crypto.randomBytes(16).toString('hex');

  await getRedis().set(`switchboard:pkce:${state}`, verifier, 'EX', 600);

  const authUrl = new URL(`${config.oidc.issuer.replace(/\/+$/, '')}/authorize`);
  authUrl.searchParams.set('response_type', 'code');
  authUrl.searchParams.set('client_id', config.oidc.clientId);
  authUrl.searchParams.set('redirect_uri', config.oidc.redirectUri);
  authUrl.searchParams.set('scope', 'openid profile email');
  authUrl.searchParams.set('state', state);
  authUrl.searchParams.set('code_challenge', challenge);
  authUrl.searchParams.set('code_challenge_method', 'S256');

  res.redirect(authUrl.toString());
});

app.get('/auth/callback', async (req, res) => {
  const code = String(req.query.code || '');
  const state = String(req.query.state || '');
  if (!code || !state) {
    res.status(400).send('Missing code or state');
    return;
  }

  const redis = getRedis();
  const verifier = await redis.get(`switchboard:pkce:${state}`);
  if (!verifier) {
    res.status(400).send('Invalid or expired state');
    return;
  }
  await redis.del(`switchboard:pkce:${state}`);

  const tokenRes = await fetch(`${config.oidc.issuer.replace(/\/+$/, '')}/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'authorization_code',
      code,
      client_id: config.oidc.clientId,
      client_secret: config.oidc.clientSecret,
      redirect_uri: config.oidc.redirectUri,
      code_verifier: verifier,
    }),
  });

  if (!tokenRes.ok) {
    res.status(502).send('Token exchange failed');
    return;
  }

  const tokenPayload = (await tokenRes.json()) as { id_token?: string; access_token?: string };
  const jwt = tokenPayload.id_token || tokenPayload.access_token || '';
  const jwtParts = jwt.split('.');
  if (jwtParts.length < 2) {
    res.status(502).send('Malformed JWT');
    return;
  }

  const claims = JSON.parse(Buffer.from(jwtParts[1], 'base64url').toString('utf8')) as {
    sub?: string;
    workspaces?: WorkspaceRole[];
  };

  if (!claims.sub) {
    res.status(502).send('Missing sub claim');
    return;
  }

  const session: SessionPayload = {
    sub: claims.sub,
    workspaces: Array.isArray(claims.workspaces) ? claims.workspaces : [],
  };

  res.cookie(SESSION_COOKIE, signSession(session), {
    httpOnly: true,
    sameSite: 'lax',
    path: '/',
  });

  res.redirect(`${config.webOrigin.replace(/\/+$/, '')}/`);
});

app.get('/api/me', requireAuth, (req, res) => {
  const session = (req as RequestWithRawBody).session!;
  res.json(session);
});

app.get('/api/conversations', requireAuth, async (req, res) => {
  const workspaceId = String(req.query.workspace || '');
  if (!workspaceId) {
    res.status(400).json({ error: 'workspace is required' });
    return;
  }
  if (!requireWorkspaceMember(req, res, workspaceId)) return;

  await ensureIndexes();
  const db = await getDb();
  const limit = Math.min(100, Math.max(1, Number(req.query.limit || '50') || 50));
  const offset = Math.max(0, Number(req.query.offset || '0') || 0);

  const conversations = await db
    .collection<ConversationDoc>('conversations')
    .find({ workspaceId })
    .sort({ lastActivityAt: -1 })
    .skip(offset)
    .limit(limit)
    .toArray();

  res.json({ conversations });
});

app.get('/api/conversations/:contact/messages', requireAuth, async (req, res) => {
  const workspaceId = String(req.query.workspace || '');
  const contact = decodeURIComponent(String(req.params.contact || ''));
  if (!workspaceId || !contact) {
    res.status(400).json({ error: 'workspace and contact are required' });
    return;
  }
  if (!requireWorkspaceMember(req, res, workspaceId)) return;

  await ensureIndexes();
  const db = await getDb();
  const [inbound, outbound] = await Promise.all([
    db
      .collection<InboundMessageDoc>('inbound_messages')
      .find({ workspaceId, contact })
      .sort({ sortOrder: 1 })
      .toArray(),
    db
      .collection<OutboundMessageDoc>('outbound_messages')
      .find({ workspaceId, contact })
      .sort({ createdAt: 1 })
      .toArray(),
  ]);

  res.json({ inbound, outbound });
});

async function claimConversation(
  workspaceId: string,
  contact: string,
  agent: string
): Promise<{ status: number; body: Record<string, unknown> }> {
  await ensureIndexes();
  const db = await getDb();
  const col = db.collection<ConversationDoc>('conversations');
  const now = new Date();

  await col.updateOne(
    { workspaceId, contact },
    {
      $setOnInsert: { workspaceId, contact, assignee: null, status: 'open', lastActivityAt: now },
    },
    { upsert: true }
  );

  const claimed = await col.findOneAndUpdate(
    {
      workspaceId,
      contact,
      assignee: { $in: [null, agent] },
    },
    {
      $set: { assignee: agent, status: 'open', lastActivityAt: now },
    },
    { returnDocument: 'after' }
  );

  if (!claimed) {
    const existing = await col.findOne({ workspaceId, contact });
    return {
      status: 409,
      body: { error: 'already_claimed', assignee: existing?.assignee ?? null },
    };
  }

  await publishWorkspaceEvent(workspaceId, 'conversation.assigned', {
    contact,
    assignee: agent,
  });

  return {
    status: 200,
    body: { contact, assignee: claimed.assignee, status: claimed.status },
  };
}

app.post('/api/conversations/:contact/claim', requireAuth, async (req, res) => {
  const session = (req as RequestWithRawBody).session!;
  const workspaceId = String(req.body?.workspace || req.query.workspace || '');
  const contact = decodeURIComponent(String(req.params.contact || ''));
  if (!workspaceId || !contact) {
    res.status(400).json({ error: 'workspace and contact are required' });
    return;
  }
  if (!requireWorkspaceMember(req, res, workspaceId)) return;

  const result = await claimConversation(workspaceId, contact, session.sub);
  res.status(result.status).json(result.body);
});

app.post('/api/conversations/:contact/status', requireAuth, async (req, res) => {
  const workspaceId = String(req.body?.workspace || '');
  const contact = decodeURIComponent(String(req.params.contact || ''));
  const status: 'open' | 'closed' = req.body?.status === 'closed' ? 'closed' : 'open';
  if (!workspaceId || !contact) {
    res.status(400).json({ error: 'workspace and contact are required' });
    return;
  }
  if (!requireWorkspaceMember(req, res, workspaceId)) return;

  await ensureIndexes();
  const db = await getDb();
  const now = new Date();
  await db.collection<ConversationDoc>('conversations').updateOne(
    { workspaceId, contact },
    {
      $setOnInsert: { workspaceId, contact, assignee: null },
      $set: { status, lastActivityAt: now },
    },
    { upsert: true }
  );

  res.json({ contact, status });
});

app.post('/api/replies', requireAuth, async (req, res) => {
  const workspaceId = String(req.body?.workspace || '');
  const contact = String(req.body?.contact || '');
  const text = String(req.body?.text || '');
  const clientRef = String(req.body?.client_ref || crypto.randomUUID());

  if (!workspaceId || !contact || !text) {
    res.status(400).json({ error: 'workspace, contact, and text are required' });
    return;
  }
  if (!requireWorkspaceMember(req, res, workspaceId)) return;

  await enqueueReply({ workspaceId, contact, text, clientRef });
  res.status(202).json({ accepted: true, client_ref: clientRef });
});

// Pigeon webhook signature verification: x-pigeon-signature: t=<sec>,v1=<hex> over "<t>.<raw body>"
function verifyPigeonSignature(req: RequestWithRawBody): boolean {
  const sigHeader = req.headers['x-pigeon-signature'];
  if (!sigHeader || typeof sigHeader !== 'string' || !req.rawBody) return false;

  let tPart = '';
  let v1Part = '';
  for (const piece of sigHeader.split(',')) {
    const trimmed = piece.trim();
    if (trimmed.startsWith('t=')) tPart = trimmed.slice(2);
    else if (trimmed.startsWith('v1=')) v1Part = trimmed.slice(3);
  }

  const tsSec = Number(tPart);
  if (!tPart || !v1Part || !Number.isFinite(tsSec)) return false;

  // Reject timestamps more than 5 minutes (300s) old
  const nowSec = Math.floor(Date.now() / 1000);
  if (Math.abs(nowSec - tsSec) > 300) return false;

  const expectedHex = crypto
    .createHmac('sha256', config.pigeon.webhookSecret)
    .update(`${tPart}.${req.rawBody.toString('utf8')}`)
    .digest('hex');

  try {
    const actualBuf = Buffer.from(v1Part, 'hex');
    const expectedBuf = Buffer.from(expectedHex, 'hex');
    if (actualBuf.length === 0 || actualBuf.length !== expectedBuf.length) return false;
    return crypto.timingSafeEqual(actualBuf, expectedBuf);
  } catch {
    return false;
  }
}

app.post('/webhooks/pigeon', async (req, res) => {
  const typedReq = req as RequestWithRawBody;
  if (!verifyPigeonSignature(typedReq)) {
    res.status(401).json({ error: 'invalid_signature' });
    return;
  }

  if (!req.body || typeof req.body !== 'object' || Array.isArray(req.body)) {
    res.status(400).json({ error: 'invalid_body' });
    return;
  }

  const body = req.body as Record<string, unknown>;
  const workspaceId = typeof body.workspace === 'string' ? body.workspace : '';
  const type = typeof body.type === 'string' ? body.type : '';
  const data =
    body.data && typeof body.data === 'object' && !Array.isArray(body.data)
      ? (body.data as Record<string, unknown>)
      : {};

  // Unknown workspace -> 2xx and ignore per PIGEON.md
  if (!workspaceId || !Object.prototype.hasOwnProperty.call(config.pigeon.apiKeys, workspaceId)) {
    res.status(200).json({ ok: true, ignored: 'unknown_workspace' });
    return;
  }

  await ensureIndexes();
  const db = await getDb();

  if (type === 'message.inbound') {
    const messageId = typeof data.message_id === 'string' ? data.message_id : '';
    const contact = typeof data.from === 'string' ? data.from : '';
    const text = typeof data.text === 'string' ? data.text : '';

    if (!messageId || !contact) {
      res.status(400).json({ error: 'missing_inbound_fields' });
      return;
    }

    const occurredAtParsed =
      typeof body.occurred_at === 'string' ? Date.parse(body.occurred_at) : NaN;
    const occurredAt = Number.isFinite(occurredAtParsed) ? new Date(occurredAtParsed) : new Date();
    const sortOrder = occurredAt.getTime();

    const inboundCol = db.collection<InboundMessageDoc>('inbound_messages');
    const existing = await inboundCol.findOne({ workspaceId, messageId });
    if (!existing) {
      try {
        await inboundCol.insertOne({
          workspaceId,
          contact,
          messageId,
          text,
          occurredAt,
          sortOrder,
        });
      } catch (err: unknown) {
        if ((err as { code?: number })?.code !== 11000) {
          throw err;
        }
      }

      await db.collection<ConversationDoc>('conversations').updateOne(
        { workspaceId, contact },
        {
          $setOnInsert: { workspaceId, contact, assignee: null, status: 'open' },
          $max: { lastActivityAt: occurredAt },
        },
        { upsert: true }
      );

      await publishWorkspaceEvent(workspaceId, 'message.inbound', {
        contact,
        message_id: messageId,
        text,
      });
    }

    res.status(200).json({ ok: true });
    return;
  }

  if (type === 'message.status') {
    const clientRef = typeof data.client_ref === 'string' ? data.client_ref : undefined;
    const pigeonMessageId = typeof data.message_id === 'string' ? data.message_id : undefined;
    const rawStatus = typeof data.status === 'string' ? data.status.toLowerCase() : '';

    if (rawStatus !== 'sent' && rawStatus !== 'delivered' && rawStatus !== 'failed') {
      res.status(200).json({ ok: true, ignored: 'unknown_status' });
      return;
    }

    await updateOutboundStatus({
      workspaceId,
      clientRef,
      pigeonMessageId,
      nextStatus: rawStatus,
      pigeonMsgIdToSave: pigeonMessageId,
    });

    res.status(200).json({ ok: true });
    return;
  }

  // Unknown event type -> 2xx per PIGEON.md
  res.status(200).json({ ok: true, ignored: 'unknown_event_type' });
});

// Grader contract endpoints
app.post('/__grader/reset', requireGrader, async (_req, res) => {
  await ensureIndexes();
  const db = await getDb();

  await Promise.all([
    db.collection('conversations').deleteMany({}),
    db.collection('inbound_messages').deleteMany({}),
    db.collection('outbound_messages').deleteMany({}),
    db.collection('events').deleteMany({}),
  ]);

  const redis = getRedis();
  const keys = await redis.keys('switchboard:*');
  const appKeys = keys.filter((k) => !k.startsWith('switchboard:pkce:'));
  if (appKeys.length > 0) {
    await redis.del(appKeys);
  }

  res.status(200).json({ ok: true });
});

app.get('/__grader/snapshot', requireGrader, async (_req, res) => {
  await ensureIndexes();
  const db = await getDb();

  const [conversations, inboundMessages, outboundMessages] = await Promise.all([
    db.collection<ConversationDoc>('conversations').find({}).toArray(),
    db.collection<InboundMessageDoc>('inbound_messages').find({}).sort({ sortOrder: 1, _id: 1 }).toArray(),
    db.collection<OutboundMessageDoc>('outbound_messages').find({}).toArray(),
  ]);

  const workspaces: Record<
    string,
    {
      contacts: Record<string, { inbound: string[]; assignee: string | null }>;
      outbound: Record<string, string>;
    }
  > = {};

  const getOrCreateWs = (wsId: string) => {
    if (!workspaces[wsId]) {
      workspaces[wsId] = { contacts: {}, outbound: {} };
    }
    return workspaces[wsId];
  };

  for (const conv of conversations) {
    const ws = getOrCreateWs(conv.workspaceId);
    if (!ws.contacts[conv.contact]) {
      ws.contacts[conv.contact] = { inbound: [], assignee: conv.assignee ?? null };
    } else {
      ws.contacts[conv.contact].assignee = conv.assignee ?? null;
    }
  }

  for (const msg of inboundMessages) {
    const ws = getOrCreateWs(msg.workspaceId);
    if (!ws.contacts[msg.contact]) {
      ws.contacts[msg.contact] = { inbound: [], assignee: null };
    }
    ws.contacts[msg.contact].inbound.push(msg.messageId);
  }

  for (const out of outboundMessages) {
    const ws = getOrCreateWs(out.workspaceId);
    ws.outbound[out.clientRef] = out.status;
  }

  res.status(200).json({ workspaces });
});

app.post('/__grader/reply', requireGrader, async (req, res) => {
  const workspace = String(req.body?.workspace || '');
  const contact = String(req.body?.contact || '');
  const text = String(req.body?.text || '');
  const clientRef = String(req.body?.client_ref || '');

  if (!workspace || !contact || !text || !clientRef) {
    res.status(400).json({ error: 'missing_fields' });
    return;
  }

  if (!Object.prototype.hasOwnProperty.call(config.pigeon.apiKeys, workspace)) {
    res.status(404).json({ error: 'unknown_workspace' });
    return;
  }

  await enqueueReply({ workspaceId: workspace, contact, text, clientRef });
  res.status(202).json({ accepted: true, client_ref: clientRef });
});

app.post('/__grader/claim', requireGrader, async (req, res) => {
  const workspace = String(req.body?.workspace || '');
  const contact = String(req.body?.contact || '');
  const agent = String(req.body?.agent || '');

  if (!workspace || !contact || !agent) {
    res.status(400).json({ error: 'missing_fields' });
    return;
  }

  const result = await claimConversation(workspace, contact, agent);
  res.status(result.status).json(result.body);
});

const server = createServer(app);
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (url.pathname !== '/ws') {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }

  const cookies = parseCookies(req.headers.cookie);
  const session = verifySessionToken(cookies[SESSION_COOKIE]);
  const workspaceId = url.searchParams.get('workspace') || '';
  const afterSeq = Math.max(0, Number(url.searchParams.get('after') || '0') || 0);

  if (!session) {
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
    socket.destroy();
    return;
  }

  if (!workspaceId || !session.workspaces.some((w) => w.id === workspaceId)) {
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    attachWebSocket(ws, workspaceId, afterSeq).catch(() => ws.close());
  });
});

async function start(): Promise<void> {
  await ensureIndexes();
  startOutboundWorker();
  server.listen(config.port, () => {
    console.log(`server on http://localhost:${config.port}`);
  });
}

process.on('unhandledRejection', (err) => {
  console.error('unhandledRejection:', err);
});

process.on('uncaughtException', (err) => {
  console.error('uncaughtException:', err);
});

start().catch((err) => {
  console.error('Failed to start server:', err);
  process.exit(1);
});