import crypto from 'node:crypto';
import { WebSocket } from 'ws';
import { config } from './config.js';
import { getDb } from './db.js';
import { getRedis } from './redis.js';
import type {
  ConversationDoc,
  EventDoc,
  InboundMessageDoc,
  OutboundMessageDoc,
} from './types.js';

const INSTANCE_ID = `${process.pid}-${crypto.randomUUID().slice(0, 8)}`;
const RATE_LIMIT_MAX = Number(process.env.PIGEON_RATE_LIMIT_MAX || '4');
const RATE_LIMIT_WINDOW_MS = Number(process.env.PIGEON_RATE_LIMIT_WINDOW_MS || '1000');

const STATUS_RANK: Record<OutboundMessageDoc['status'], number> = {
  failed: 0,
  queued: 1,
  sent: 2,
  delivered: 3,
};

let indexesEnsured = false;
let redisSubClient: ReturnType<typeof getRedis> | null = null;
const localSubscribers = new Map<string, Set<WebSocket>>();
const subscribedChannels = new Set<string>();

function wsChannel(workspaceId: string): string {
  return `switchboard:ws:${workspaceId}`;
}

function getSubscriber() {
  if (!redisSubClient) {
    redisSubClient = getRedis().duplicate();
    redisSubClient.on('message', (channel: string, rawMessage: string) => {
      const prefix = 'switchboard:ws:';
      if (!channel.startsWith(prefix)) return;
      const workspaceId = channel.slice(prefix.length);
      const sockets = localSubscribers.get(workspaceId);
      if (!sockets || sockets.size === 0) return;

      for (const ws of sockets) {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(rawMessage);
        }
      }
    });
  }
  return redisSubClient;
}

export async function ensureIndexes(): Promise<void> {
  if (indexesEnsured) return;
  const db = await getDb();

  await Promise.all([
    db.collection<ConversationDoc>('conversations').createIndex(
      { workspaceId: 1, contact: 1 },
      { unique: true }
    ),
    db.collection<ConversationDoc>('conversations').createIndex({
      workspaceId: 1,
      lastActivityAt: -1,
    }),
    db.collection<InboundMessageDoc>('inbound_messages').createIndex(
      { workspaceId: 1, messageId: 1 },
      { unique: true }
    ),
    db.collection<InboundMessageDoc>('inbound_messages').createIndex({
      workspaceId: 1,
      contact: 1,
      sortOrder: 1,
    }),
    db.collection<OutboundMessageDoc>('outbound_messages').createIndex(
      { workspaceId: 1, clientRef: 1 },
      { unique: true }
    ),
    db.collection<OutboundMessageDoc>('outbound_messages').createIndex({
      status: 1,
      nextAttemptAt: 1,
      lockExpiresAt: 1,
    }),
    db.collection<EventDoc>('events').createIndex(
      { workspaceId: 1, seq: 1 },
      { unique: true }
    ),
  ]);

  indexesEnsured = true;
}

const publishChains = new Map<string, Promise<unknown>>();

export function publishWorkspaceEvent(
  workspaceId: string,
  type: EventDoc['type'],
  payload: Record<string, unknown>
): Promise<{ seq: number; frame: Record<string, unknown> }> {
  const prev = publishChains.get(workspaceId) ?? Promise.resolve();
  const next = prev
    .catch(() => {})
    .then(async () => {
      await ensureIndexes();
      const db = await getDb();
      const redis = getRedis();

      const seq = await redis.incr(`switchboard:seq:${workspaceId}`);
      const frame = { seq, type, ...payload };

      await db.collection<EventDoc>('events').insertOne({
        workspaceId,
        seq,
        type,
        payload,
      });

      await redis.publish(wsChannel(workspaceId), JSON.stringify(frame));
      return { seq, frame };
    });

  publishChains.set(workspaceId, next);
  return next;
}

export async function attachWebSocket(
  ws: WebSocket,
  workspaceId: string,
  afterSeq: number
): Promise<void> {
  await ensureIndexes();
  const db = await getDb();
  const sub = getSubscriber();
  const channel = wsChannel(workspaceId);

  if (!subscribedChannels.has(channel)) {
    subscribedChannels.add(channel);
    await sub.subscribe(channel);
  }

  const buffer: string[] = [];
  let flushing = true;

  const onMessage = (ch: string, msg: string) => {
    if (ch !== channel) return;
    if (flushing) {
      buffer.push(msg);
    } else if (ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  };

  sub.on('message', onMessage);

  let set = localSubscribers.get(workspaceId);
  if (!set) {
    set = new Set();
    localSubscribers.set(workspaceId, set);
  }
  set.add(ws);

  const missed = await db
    .collection<EventDoc>('events')
    .find({ workspaceId, seq: { $gt: afterSeq } })
    .sort({ seq: 1 })
    .toArray();

  let lastSentSeq = afterSeq;
  for (const ev of missed) {
    if (ws.readyState !== WebSocket.OPEN) break;
    lastSentSeq = Math.max(lastSentSeq, ev.seq);
    ws.send(JSON.stringify({ seq: ev.seq, type: ev.type, ...(ev.payload || {}) }));
  }

  for (const raw of buffer) {
    if (ws.readyState !== WebSocket.OPEN) break;
    try {
      const parsed = JSON.parse(raw) as { seq?: number };
      if (typeof parsed.seq === 'number' && parsed.seq > lastSentSeq) {
        lastSentSeq = parsed.seq;
        ws.send(raw);
      }
    } catch {
      ws.send(raw);
    }
  }
  flushing = false;

  ws.on('close', () => {
    sub.off('message', onMessage);
    localSubscribers.get(workspaceId)?.delete(ws);
  });
}

async function getWorkspaceCooldownRemainingMs(workspaceId: string): Promise<number> {
  const redis = getRedis();
  const raw = await redis.get(`switchboard:pigeon_cooldown:${workspaceId}`);
  if (!raw) return 0;
  const remaining = Number(raw) - Date.now();
  return remaining > 0 ? remaining : 0;
}

async function setWorkspaceCooldown(workspaceId: string, ms: number): Promise<void> {
  const redis = getRedis();
  const clamped = Math.min(30_000, Math.max(250, ms));
  await redis.set(
    `switchboard:pigeon_cooldown:${workspaceId}`,
    String(Date.now() + clamped),
    'PX',
    clamped + 1000
  );
}

async function acquireWorkspaceRateSlot(workspaceId: string): Promise<boolean> {
  const cooldown = await getWorkspaceCooldownRemainingMs(workspaceId);
  if (cooldown > 0) return false;

  const redis = getRedis();
  const bucket = Math.floor(Date.now() / RATE_LIMIT_WINDOW_MS);
  const key = `switchboard:rl:${workspaceId}:${bucket}`;
  const count = await redis.incr(key);
  if (count === 1) {
    await redis.pexpire(key, RATE_LIMIT_WINDOW_MS * 2);
  }
  return count <= RATE_LIMIT_MAX;
}

export async function enqueueReply(params: {
  workspaceId: string;
  contact: string;
  text: string;
  clientRef: string;
}): Promise<void> {
  await ensureIndexes();
  const db = await getDb();
  const now = new Date();

  await db.collection<ConversationDoc>('conversations').updateOne(
    { workspaceId: params.workspaceId, contact: params.contact },
    {
      $setOnInsert: {
        workspaceId: params.workspaceId,
        contact: params.contact,
        assignee: null,
        status: 'open',
      },
      $set: { lastActivityAt: now },
    },
    { upsert: true }
  );

  await db.collection<OutboundMessageDoc>('outbound_messages').updateOne(
    { workspaceId: params.workspaceId, clientRef: params.clientRef },
    {
      $setOnInsert: {
        workspaceId: params.workspaceId,
        contact: params.contact,
        clientRef: params.clientRef,
        pigeonMessageId: null,
        text: params.text,
        status: 'queued',
        statusRank: STATUS_RANK.queued,
        attempts: 0,
        nextAttemptAt: now,
        lockedBy: null,
        lockExpiresAt: null,
        createdAt: now,
      },
    },
    { upsert: true }
  );
}

export async function updateOutboundStatus(params: {
  workspaceId: string;
  clientRef?: string;
  pigeonMessageId?: string;
  nextStatus: 'queued' | 'sent' | 'delivered' | 'failed';
  pigeonMsgIdToSave?: string;
}): Promise<boolean> {
  await ensureIndexes();
  const db = await getDb();
  const col = db.collection<OutboundMessageDoc>('outbound_messages');

  const query: Record<string, unknown> = { workspaceId: params.workspaceId };
  if (params.clientRef) {
    query.clientRef = params.clientRef;
  } else if (params.pigeonMessageId) {
    query.pigeonMessageId = params.pigeonMessageId;
  } else {
    return false;
  }

  const doc = await col.findOne(query);
  if (!doc) return false;

  const incomingRank = STATUS_RANK[params.nextStatus];
  let changed = false;
  const setFields: Record<string, unknown> = {
    lockedBy: null,
    lockExpiresAt: null,
  };

  if (params.pigeonMsgIdToSave && !doc.pigeonMessageId) {
    setFields.pigeonMessageId = params.pigeonMsgIdToSave;
    changed = true;
  }

  if (incomingRank > doc.statusRank) {
    setFields.status = params.nextStatus;
    setFields.statusRank = incomingRank;
    changed = true;
  }

  await col.updateOne({ _id: doc._id }, { $set: setFields });

  if (changed) {
    await publishWorkspaceEvent(params.workspaceId, 'message.status', {
      contact: doc.contact,
      client_ref: doc.clientRef,
      status: incomingRank > doc.statusRank ? params.nextStatus : doc.status,
    });
  }

  return true;
}

async function processOneOutboundJob(): Promise<boolean> {
  await ensureIndexes();
  const db = await getDb();
  const col = db.collection<OutboundMessageDoc>('outbound_messages');
  const now = new Date();
  const leaseUntil = new Date(now.getTime() + 25_000);

  const res = await col.findOneAndUpdate(
    {
      status: 'queued',
      nextAttemptAt: { $lte: now },
      $or: [{ lockExpiresAt: null }, { lockExpiresAt: { $lte: now } }],
    },
    {
      $set: { lockedBy: INSTANCE_ID, lockExpiresAt: leaseUntil },
      $inc: { attempts: 1 },
    },
    { sort: { createdAt: 1 }, returnDocument: 'after' }
  );

  const job = res;
  if (!job) return false;

  const cooldownMs = await getWorkspaceCooldownRemainingMs(job.workspaceId);
  if (cooldownMs > 0) {
    await col.updateOne(
      { _id: job._id },
      {
        $set: {
          lockedBy: null,
          lockExpiresAt: null,
          nextAttemptAt: new Date(Date.now() + cooldownMs),
        },
        $inc: { attempts: -1 },
      }
    );
    return false;
  }

  const slotOk = await acquireWorkspaceRateSlot(job.workspaceId);
  if (!slotOk) {
    await col.updateOne(
      { _id: job._id },
      {
        $set: {
          lockedBy: null,
          lockExpiresAt: null,
          nextAttemptAt: new Date(Date.now() + 250 + Math.floor(Math.random() * 250)),
        },
        $inc: { attempts: -1 },
      }
    );
    return false;
  }

  const apiKey = config.pigeon.apiKeys[job.workspaceId];
  if (!apiKey) {
    await updateOutboundStatus({
      workspaceId: job.workspaceId,
      clientRef: job.clientRef,
      nextStatus: 'failed',
    });
    return true;
  }

  try {
    const response = await fetch(`${config.pigeon.url.replace(/\/+$/, '')}/v1/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
        'Idempotency-Key': job.clientRef,
      },
      body: JSON.stringify({
        to: job.contact,
        text: job.text,
        client_ref: job.clientRef,
      }),
      signal: AbortSignal.timeout(5500),
    });

    if (response.ok) {
      let pigeonId: string | undefined;
      try {
        const json = (await response.json()) as { id?: string };
        if (json?.id) pigeonId = String(json.id);
      } catch {
        // ignore parse failure on 200
      }

      await updateOutboundStatus({
        workspaceId: job.workspaceId,
        clientRef: job.clientRef,
        nextStatus: 'sent',
        pigeonMsgIdToSave: pigeonId,
      });
      return true;
    }

    if (response.status === 429) {
      const retryAfterHeader = response.headers.get('retry-after');
      const retryAfterSec = retryAfterHeader ? Number(retryAfterHeader) : 1;
      const delayMs =
        Number.isFinite(retryAfterSec) && retryAfterSec > 0 ? retryAfterSec * 1000 : 1000;
      await setWorkspaceCooldown(job.workspaceId, delayMs);
      await col.updateOne(
        { _id: job._id },
        {
          $set: {
            lockedBy: null,
            lockExpiresAt: null,
            nextAttemptAt: new Date(Date.now() + delayMs + Math.floor(Math.random() * 200)),
          },
        }
      );
      return true;
    }

    if (response.status >= 400 && response.status < 500) {
      await updateOutboundStatus({
        workspaceId: job.workspaceId,
        clientRef: job.clientRef,
        nextStatus: 'failed',
      });
      return true;
    }

    const backoffMs =
      Math.min(6000, 350 * Math.pow(1.5, Math.min(job.attempts, 7))) +
      Math.floor(Math.random() * 200);
    await col.updateOne(
      { _id: job._id },
      {
        $set: {
          lockedBy: null,
          lockExpiresAt: null,
          nextAttemptAt: new Date(Date.now() + backoffMs),
        },
      }
    );
    return true;
  } catch {
    const backoffMs =
      Math.min(6000, 400 * Math.pow(1.5, Math.min(job.attempts, 7))) +
      Math.floor(Math.random() * 200);
    await col.updateOne(
      { _id: job._id },
      {
        $set: {
          lockedBy: null,
          lockExpiresAt: null,
          nextAttemptAt: new Date(Date.now() + backoffMs),
        },
      }
    );
    return true;
  }
}

let timer: ReturnType<typeof setInterval> | null = null;
let ticking = false;

export function startOutboundWorker(): void {
  if (timer) return;
  const runTick = async () => {
    if (ticking) return;
    ticking = true;
    try {
      for (let i = 0; i < 8; i++) {
        const worked = await processOneOutboundJob();
        if (!worked) break;
      }
    } finally {
      ticking = false;
    }
  };
  timer = setInterval(runTick, 100);
  runTick().catch(() => {});
}