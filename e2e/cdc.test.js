/**
 * CDC (Change Data Capture) E2E Tests
 *
 * Tests real-time SSE and WebSocket subscriptions against the full stack:
 *   PostgreSQL (WAL) → excalibase-watcher → NATS → app → SSE / WebSocket
 *
 * Requires: docker compose up (postgres + nats + watcher + app)
 */

const EventSource = require('eventsource');
const WebSocket = require('ws');
const { API_URL, WS_URL, waitForApi, request, psql } = require('./client');

// Collect SSE events for a table, returns { events, close }
function subscribeSSE(table) {
  const events = [];
  const url = `${API_URL}/${table}/changes`;
  const es = new EventSource(url);

  for (const type of ['INSERT', 'UPDATE', 'DELETE']) {
    es.addEventListener(type, (e) => {
      events.push({ type, data: JSON.parse(e.data) });
    });
  }

  const close = () => es.close();
  return { events, close };
}

// Collect WebSocket messages for a table, returns { messages, close, ready }
function subscribeWS(table) {
  const messages = [];
  const ws = new WebSocket(`${WS_URL}/${table}/changes`);

  const ready = new Promise((resolve, reject) => {
    ws.on('open', resolve);
    ws.on('error', reject);
    setTimeout(() => reject(new Error('WebSocket open timeout')), 10000);
  });

  ws.on('message', (raw) => {
    messages.push(JSON.parse(raw.toString()));
  });

  const close = () => ws.close();
  return { messages, close, ready };
}

// Wait until predicate is true on an array, polling every 200ms
async function waitFor(arr, predicate, timeoutMs = 10000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate(arr)) return;
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms. Events: ${JSON.stringify(arr)}`);
}

// ─── Tests ───────────────────────────────────────────────────────────────────

beforeAll(async () => {
  await waitForApi();
}, 120000);

describe('SSE subscriptions', () => {
  let sse;
  let createdId;

  afterEach(() => {
    if (sse) sse.close();
  });

  afterAll(async () => {
    // Clean up all test data created by SSE tests
    try {
      psql("DELETE FROM customers WHERE email LIKE '%@e2e.test'");
    } catch (_) {}
  });

  test('receives INSERT event from API create', async () => {
    sse = subscribeSSE('customers');
    // Give SSE time to connect
    await new Promise((r) => setTimeout(r, 1000));

    const res = await request('POST', '/customers', {
      body: { name: 'SSE Test Insert', email: 'sse-insert@e2e.test', tier: 'bronze', is_active: true },
    });
    expect(res.status).toBe(201);
    createdId = res.body.id?.value || res.body.id;

    await waitFor(sse.events, (e) => e.some((ev) => ev.type === 'INSERT'));

    const insert = sse.events.find((e) => e.type === 'INSERT');
    expect(insert).toBeDefined();
    expect(insert.data.name).toBe('SSE Test Insert');
  });

  test('receives UPDATE event from API patch', async () => {
    // Ensure we have a row to update
    if (!createdId) {
      const res = await request('POST', '/customers', {
        body: { name: 'SSE Update Target', email: 'sse-update@e2e.test', tier: 'bronze', is_active: true },
      });
      createdId = res.body.id?.value || res.body.id;
    }

    sse = subscribeSSE('customers');
    await new Promise((r) => setTimeout(r, 1000));

    await request('PATCH', `/customers/${createdId}`, {
      body: { tier: 'gold' },
    });

    await waitFor(sse.events, (e) => e.some((ev) => ev.type === 'UPDATE'));

    const update = sse.events.find((e) => e.type === 'UPDATE');
    expect(update).toBeDefined();
    expect(update.data.new).toBeDefined();
    expect(update.data.new.tier).toBe('gold');
  });

  test('receives DELETE event from API delete', async () => {
    // Create a row specifically for deletion
    const res = await request('POST', '/customers', {
      body: { name: 'SSE Delete Target', email: 'sse-delete@e2e.test', tier: 'silver', is_active: true },
    });
    const deleteId = res.body.id?.value || res.body.id;

    sse = subscribeSSE('customers');
    await new Promise((r) => setTimeout(r, 1000));

    await request('DELETE', `/customers/${deleteId}`);

    await waitFor(sse.events, (e) => e.some((ev) => ev.type === 'DELETE'));

    const del = sse.events.find((e) => e.type === 'DELETE');
    expect(del).toBeDefined();
    expect(del.data.id).toBe(deleteId);
  });

  test('receives INSERT event from direct psql insert', async () => {
    sse = subscribeSSE('customers');
    await new Promise((r) => setTimeout(r, 1000));

    psql(
      "INSERT INTO customers(name, email, tier, is_active) VALUES ('PSQL Direct Insert', 'psql-direct@e2e.test', 'bronze', true)"
    );

    await waitFor(sse.events, (e) =>
      e.some((ev) => ev.type === 'INSERT' && ev.data.email === 'psql-direct@e2e.test')
    );

    const insert = sse.events.find(
      (e) => e.type === 'INSERT' && e.data.email === 'psql-direct@e2e.test'
    );
    expect(insert).toBeDefined();
    expect(insert.data.name).toBe('PSQL Direct Insert');

    // Clean up
    psql("DELETE FROM customers WHERE email = 'psql-direct@e2e.test'");
  });

  test('receives UPDATE event from direct psql update', async () => {
    // Create via psql
    psql(
      "INSERT INTO customers(name, email, tier, is_active) VALUES ('PSQL Update Target', 'psql-update@e2e.test', 'bronze', true)"
    );

    sse = subscribeSSE('customers');
    await new Promise((r) => setTimeout(r, 1000));

    psql("UPDATE customers SET tier = 'platinum' WHERE email = 'psql-update@e2e.test'");

    await waitFor(sse.events, (e) => e.some((ev) => ev.type === 'UPDATE'));

    const update = sse.events.find((e) => e.type === 'UPDATE');
    expect(update).toBeDefined();
    expect(update.data.new.tier).toBe('platinum');

    // Clean up
    psql("DELETE FROM customers WHERE email = 'psql-update@e2e.test'");
  });
});

describe('WebSocket subscriptions', () => {
  let ws;

  afterEach(() => {
    if (ws) ws.close();
  });

  test('receives INSERT event as JSON message', async () => {
    ws = subscribeWS('customers');
    await ws.ready;
    // Give subscription time to register
    await new Promise((r) => setTimeout(r, 1000));

    const res = await request('POST', '/customers', {
      body: { name: 'WS Test Insert', email: 'ws-insert@e2e.test', tier: 'bronze', is_active: true },
    });
    const insertId = res.body.id?.value || res.body.id;

    await waitFor(ws.messages, (m) => m.some((msg) => msg.event === 'INSERT'));

    const msg = ws.messages.find((m) => m.event === 'INSERT');
    expect(msg).toBeDefined();
    expect(msg.table).toBe('customers');
    expect(msg.schema).toBe('public');
    expect(msg.data).toBeDefined();
    expect(msg.timestamp).toBeDefined();

    // Clean up
    if (insertId) await request('DELETE', `/customers/${insertId}`);
  });

  test('receives UPDATE event with correct format', async () => {
    // Create a row first
    const res = await request('POST', '/customers', {
      body: { name: 'WS Update Target', email: 'ws-update@e2e.test', tier: 'bronze', is_active: true },
    });
    const updateId = res.body.id?.value || res.body.id;

    ws = subscribeWS('customers');
    await ws.ready;
    await new Promise((r) => setTimeout(r, 1000));

    await request('PATCH', `/customers/${updateId}`, {
      body: { tier: 'gold' },
    });

    await waitFor(ws.messages, (m) => m.some((msg) => msg.event === 'UPDATE'));

    const msg = ws.messages.find((m) => m.event === 'UPDATE');
    expect(msg).toBeDefined();
    expect(msg.event).toBe('UPDATE');
    expect(msg.table).toBe('customers');

    // Clean up
    if (updateId) await request('DELETE', `/customers/${updateId}`);
  });

  test('receives DELETE event', async () => {
    // Create then delete
    const res = await request('POST', '/customers', {
      body: { name: 'WS Delete Target', email: 'ws-delete@e2e.test', tier: 'silver', is_active: true },
    });
    const deleteId = res.body.id?.value || res.body.id;

    ws = subscribeWS('customers');
    await ws.ready;
    await new Promise((r) => setTimeout(r, 1000));

    await request('DELETE', `/customers/${deleteId}`);

    await waitFor(ws.messages, (m) => m.some((msg) => msg.event === 'DELETE'));

    const msg = ws.messages.find((m) => m.event === 'DELETE');
    expect(msg).toBeDefined();
    expect(msg.event).toBe('DELETE');
    expect(msg.table).toBe('customers');
  });

  test('receives event from direct psql insert', async () => {
    ws = subscribeWS('customers');
    await ws.ready;
    await new Promise((r) => setTimeout(r, 1000));

    psql(
      "INSERT INTO customers(name, email, tier, is_active) VALUES ('WS PSQL Insert', 'ws-psql@e2e.test', 'bronze', true)"
    );

    await waitFor(ws.messages, (m) =>
      m.some((msg) => msg.event === 'INSERT' && JSON.stringify(msg.data).includes('ws-psql@e2e.test'))
    );

    const msg = ws.messages.find(
      (m) => m.event === 'INSERT' && JSON.stringify(m.data).includes('ws-psql@e2e.test')
    );
    expect(msg).toBeDefined();

    // Clean up
    psql("DELETE FROM customers WHERE email = 'ws-psql@e2e.test'");
  });
});
