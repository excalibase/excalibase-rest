/**
 * Excalibase REST E2E test client utilities.
 *
 * Provides helpers for waiting on API readiness, making REST requests,
 * and executing psql commands against the test database.
 */

const API_URL = process.env.API_URL || 'http://localhost:20000/api/v1';
const WS_URL = process.env.WS_URL || 'ws://localhost:20000/ws';
const PG_CONTAINER = process.env.PG_CONTAINER || 'excalibase-rest-postgres';
const PG_USER = process.env.PG_USER || 'testuser';
const PG_DB = process.env.PG_DB || 'excalibase_rest';

/**
 * Wait for the REST API to become ready.
 */
async function waitForApi(url = API_URL, { maxRetries = 15, delayMs = 5000 } = {}) {
  for (let i = 1; i <= maxRetries; i++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
      if (res.status < 500) return;
    } catch (_) {
      // not ready yet
    }
    if (i < maxRetries) {
      console.log(`[waitForApi] Attempt ${i}/${maxRetries} — retrying in ${delayMs / 1000}s...`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  throw new Error(`API at ${url} not ready after ${maxRetries} attempts`);
}

/**
 * Make a REST API request.
 */
async function request(method, path, { body, headers = {} } = {}) {
  const url = `${API_URL}${path}`;
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json', ...headers },
    signal: AbortSignal.timeout(10000),
  };
  if (body !== undefined) {
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(url, opts);
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }
  return { status: res.status, body: json, text };
}

/**
 * Execute a SQL command via docker exec psql.
 */
async function psql(sql) {
  const { execSync } = require('child_process');
  const cmd = `docker exec ${PG_CONTAINER} psql -U ${PG_USER} -d ${PG_DB} -c "${sql.replace(/"/g, '\\"')}"`;
  return execSync(cmd, { encoding: 'utf-8' });
}

module.exports = { API_URL, WS_URL, waitForApi, request, psql };
