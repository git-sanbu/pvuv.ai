import { build } from 'esbuild';
import assert from 'node:assert/strict';
import { test } from 'node:test';

const bundled = await build({
  stdin: {
    contents: `export * from './shared/queue';
      export { EVENT_COLUMNS } from './shared/events';
      export { default as consumer } from './workers/consumer/src/index';`,
    resolveDir: process.cwd(),
    loader: 'ts',
  },
  bundle: true, write: false, format: 'esm', platform: 'node',
});
const { packEvents, unpackEvents, QUEUE_PACK_BYTES, EVENT_COLUMNS, consumer } =
  await import('data:text/javascript;base64,' + Buffer.from(bundled.outputFiles[0].text).toString('base64'));

function row(id, extra = {}) {
  return {
    ...Object.fromEntries(EVENT_COLUMNS.map((key) => [key, null])),
    eid: String(id), site_id: 'test', visitor_id: 'visitor', session_id: 'session',
    event: 'pageview', ts: 1_794_500_000_000 + id, path: '/', hostname: 'example.com',
    bot_score: 0, bot_flags: 0, verdict: 'clean', had_interaction: 0,
    ...extra,
  };
}

test('ten events survive JSON transport as one message; singleton stays legacy', () => {
  const rows = Array.from({ length: 10 }, (_, i) => row(i));
  const messages = packEvents(rows);
  assert.equal(messages.length, 1);
  assert.deepEqual(unpackEvents(JSON.parse(JSON.stringify(messages[0]))), rows);
  assert.equal(packEvents([rows[0]])[0], rows[0]);
  assert.deepEqual(packEvents([]), []);
});

test('UTF-8 byte limits split messages without losing or reordering events', () => {
  const rows = Array.from({ length: 25 }, (_, i) => row(i, { props: '汉🙂'.repeat(1800) }));
  const messages = packEvents(rows);
  assert(messages.length > 1 && messages.length < rows.length);
  for (const message of messages) assert(Buffer.byteLength(JSON.stringify(message)) <= QUEUE_PACK_BYTES);
  assert.deepEqual(messages.flatMap(unpackEvents), rows);
  const large = row(99, { props: 'x'.repeat(70_000) });
  assert.deepEqual(packEvents([rows[0], large, rows[1]]).flatMap(unpackEvents), [rows[0], large, rows[1]]);
  assert.equal(packEvents([large])[0], large);
});

function database(failWrite = 0) {
  const writes = [];
  let attempts = 0;
  const db = {
    prepare(sql) {
      return {
        sql, values: [],
        bind(...values) { this.values = values; return this; },
        async all() { return { results: sql.startsWith('PRAGMA') ? EVENT_COLUMNS.map((name) => ({ name })) : [] }; },
        async run() { return { success: true }; },
      };
    },
    async batch(statements) {
      const events = statements.filter((s) => /INSERT OR IGNORE INTO events_/.test(s.sql));
      if (!events.length) return [];
      attempts++;
      if (attempts === failWrite) throw new Error('simulated D1 failure');
      writes.push(events.map((s) => s.values[0]));
      return statements.map(() => ({ success: true }));
    },
  };
  return { db, writes };
}

function message(body) {
  return { body, acknowledged: 0, retried: 0,
    ack() { this.acknowledged++; }, retry() { this.retried++; } };
}

test('mixed legacy and packed messages stay within 50 events per atomic write', async () => {
  const rows = Array.from({ length: 76 }, (_, i) => row(i));
  const messages = [message(rows[0]), ...packEvents(rows.slice(1)).map(message)];
  const { db, writes } = database();
  await consumer.queue({ messages }, { DB: db });
  assert.deepEqual(writes.flat(), rows.map((r) => r.eid));
  assert(writes.every((write) => write.length <= 50));
  assert(messages.every((m) => m.acknowledged === 1 && m.retried === 0));
});

test('a later D1 failure retries only its group; malformed packs are not acknowledged', async (t) => {
  const errors = t.mock.method(console, 'error', () => {});
  const rows = Array.from({ length: 100 }, (_, i) => row(i));
  const messages = packEvents(rows).map(message);
  const invalid = message({ format: 'pvuv-events-v1', events: [rows[0], null] });
  const { db, writes } = database(2);
  await consumer.queue({ messages: [...messages, invalid] }, { DB: db });
  assert.deepEqual(writes.flat(), rows.slice(0, 50).map((r) => r.eid));
  assert(messages.slice(0, 2).every((m) => m.acknowledged === 1 && m.retried === 0));
  assert(messages.slice(2).every((m) => m.acknowledged === 0 && m.retried === 1));
  assert.equal(invalid.acknowledged, 0);
  assert.equal(invalid.retried, 1);
  assert.equal(errors.mock.callCount(), 2);
  assert.throws(() => unpackEvents({ format: 'pvuv-events-v1', events: [] }));
});
