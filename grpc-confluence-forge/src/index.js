import kvs, { WhereConditions } from '@forge/kvs';
import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';

/** Key prefix of queued events. */
export const PREFIX = 'evt:';
/** Header that carries the shared secret. */
export const SECRET_HEADER = 'x-pipestream-queue-secret';
/** Most events answered per take. */
export const MAX_TAKE = 100;
/** Days a queued event is kept when nobody collects it. */
export const TTL_DAYS = 7;
/** KVS page size for queries. */
const PAGE = 20;

/** The queue key for an event stored at {@code now}: ordered by time. */
export function eventKey(now = Date.now(), id = randomUUID()) {
  return `${PREFIX}${String(now).padStart(15, '0')}:${id}`;
}

/** Stores one product event in {@code store}. */
export async function store(event, target) {
  await target.set(eventKey(), event, { ttl: { value: TTL_DAYS, unit: 'DAYS' } });
}

/** Trigger: stores one product event in the queue. */
export async function onEvent(event) {
  await store(event, kvs);
}

/** Compares the request secret with the configured one in constant time. */
export function authorized(headers, secret) {
  if (!secret) {
    return false;
  }
  const given = header(headers, SECRET_HEADER);
  if (!given) {
    return false;
  }
  const a = createHash('sha256').update(given).digest();
  const b = createHash('sha256').update(secret).digest();
  return timingSafeEqual(a, b);
}

function header(headers, name) {
  for (const [key, value] of Object.entries(headers ?? {})) {
    if (key.toLowerCase() === name) {
      return Array.isArray(value) ? value[0] : value;
    }
  }
  return undefined;
}

function answer(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': ['application/json'] },
    body: JSON.stringify(body),
  };
}

/** Takes up to {@code max} of the oldest queued events without removing them. */
export async function take(max, store = kvs) {
  const limit = Math.max(1, Math.min(MAX_TAKE, Number.isInteger(max) ? max : MAX_TAKE));
  const events = [];
  let cursor;
  do {
    let query = store.query().where('key', WhereConditions.beginsWith(PREFIX))
      .limit(Math.min(PAGE, limit - events.length));
    if (cursor) {
      query = query.cursor(cursor);
    }
    const page = await query.getMany();
    for (const item of page.results) {
      events.push({ key: item.key, event: item.value });
    }
    cursor = page.nextCursor;
  } while (cursor && events.length < limit);
  return events;
}

/** Removes handled events; only queue keys are accepted. */
export async function ack(keys, store = kvs) {
  const valid = (Array.isArray(keys) ? keys : [])
    .filter((key) => typeof key === 'string' && key.startsWith(PREFIX));
  for (let i = 0; i < valid.length; i += PAGE) {
    await store.batchDelete(valid.slice(i, i + PAGE).map((key) => ({ key })));
  }
  return valid.length;
}

/** Web trigger: {"action":"take","max":n} or {"action":"ack","keys":[...]}. */
export async function queue(request) {
  return handle(request, kvs, process.env.QUEUE_SECRET);
}

/** Answers one queue request against {@code store} with {@code secret}. */
export async function handle(request, store, secret) {
  if (!authorized(request?.headers, secret)) {
    return answer(401, { error: 'unauthorized' });
  }
  let body;
  try {
    body = JSON.parse(request.body || '{}');
  } catch {
    return answer(400, { error: 'body is not JSON' });
  }
  if (body.action === 'take') {
    return answer(200, { events: await take(body.max, store) });
  }
  if (body.action === 'ack') {
    return answer(200, { acked: await ack(body.keys, store) });
  }
  return answer(400, { error: 'action must be take or ack' });
}
