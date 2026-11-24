import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ack, authorized, eventKey, handle, SECRET_HEADER, store as put } from './index.js';

/** In-memory stand-in for the KVS calls the app uses. */
function memoryStore() {
  const data = new Map();
  return {
    data,
    options: new Map(),
    async set(key, value, options) {
      data.set(key, value);
      this.options.set(key, options);
    },
    async batchDelete(items) {
      items.forEach(({ key }) => data.delete(key));
    },
    query(options) {
      const expiry = options?.metadataFields?.includes('EXPIRE_TIME');
      let prefix = '';
      let limit = 10;
      let start = 0;
      const builder = {
        where(_field, clause) {
          prefix = clause.condition === 'BEGINS_WITH' ? clause.values[0] : clause.value ?? '';
          return builder;
        },
        limit(n) {
          limit = n;
          return builder;
        },
        cursor(c) {
          start = Number(c);
          return builder;
        },
        async getMany() {
          const keys = [...data.keys()].filter((k) => k.startsWith(prefix)).sort();
          const slice = keys.slice(start, start + limit);
          const next = start + limit < keys.length ? String(start + limit) : undefined;
          const results = slice.map((key) => (expiry
            ? { key, value: data.get(key), expireTime: '2026-11-29T00:00:00Z' }
            : { key, value: data.get(key) }));
          return { results, nextCursor: next };
        },
      };
      return builder;
    },
  };
}

const request = (body, secret = 's3cret') => ({
  headers: { [SECRET_HEADER]: [secret] },
  body: JSON.stringify(body),
});

test('keys sort by time', () => {
  assert.ok(eventKey(5, 'b') < eventKey(10, 'a'));
});

test('refuses a missing or wrong secret', async () => {
  const store = memoryStore();
  assert.equal(authorized({}, 's3cret'), false);
  assert.equal(authorized({ [SECRET_HEADER]: ['x'] }, ''), false);
  assert.equal((await handle(request({ action: 'take' }, 'wrong'), store, 's3cret')).statusCode, 401);
});

test('take answers oldest first across pages and ack removes', async () => {
  const store = memoryStore();
  for (let i = 0; i < 45; i++) {
    await store.set(eventKey(1000 + i, 'id'), { eventType: 'avi:confluence:updated:page', n: i });
  }
  await store.set('other', {});
  const taken = await handle(request({ action: 'take', max: 30 }), store, 's3cret');
  assert.equal(taken.statusCode, 200);
  const events = JSON.parse(taken.body).events;
  assert.equal(events.length, 30);
  assert.equal(events[0].event.n, 0);
  assert.equal(events[29].event.n, 29);
  assert.equal(events[0].expireTime, '2026-11-29T00:00:00Z');
  const acked = await handle(request({ action: 'ack', keys: [...events.map((e) => e.key), 'other'] }),
    store, 's3cret');
  assert.equal(JSON.parse(acked.body).acked, 30);
  assert.equal(store.data.size, 16);
  assert.ok(store.data.has('other'));
});

test('store keeps the event under a queue key for 7 days, without its context token', async () => {
  const store = memoryStore();
  await put({ eventType: 'avi:confluence:created:page', contextToken: 'jwt' }, store);
  const [key] = store.data.keys();
  assert.ok(key.startsWith('evt:'));
  assert.deepEqual(store.data.get(key), { eventType: 'avi:confluence:created:page' });
  assert.deepEqual(store.options.get(key), { ttl: { value: 7, unit: 'DAYS' } });
});

test('rejects bad bodies and actions', async () => {
  const store = memoryStore();
  assert.equal((await handle({ headers: { [SECRET_HEADER]: 's3cret' }, body: '{' }, store, 's3cret'))
    .statusCode, 400);
  assert.equal((await handle(request({ action: 'drop' }), store, 's3cret')).statusCode, 400);
  assert.equal(await ack('nope', store), 0);
});
