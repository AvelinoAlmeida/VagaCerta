'use strict';
// Shared plumbing. This file is an identical copy in every service (checked by scripts/check-lib.sh)
// so each service stays a self-contained Docker build context. Read it once, then ignore it.
//   - env helpers, one-line JSON logs (service, hostname, requestId)
//   - Express app with request-id, Prometheus metrics, /health, /ready, readiness gate
//   - graceful shutdown on SIGTERM/SIGINT
const os = require('node:os');
const crypto = require('node:crypto');
const { AsyncLocalStorage } = require('node:async_hooks');
const express = require('express');
const client = require('prom-client');

const hostname = os.hostname();
const als = new AsyncLocalStorage();
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };

const str = (name, def) => (process.env[name] !== undefined && process.env[name] !== '' ? process.env[name] : def);
const num = (name, def) => {
  const v = Number(str(name, def));
  return Number.isFinite(v) ? v : def;
};
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function makeLogger(service) {
  const min = LEVELS[str('LOG_LEVEL', 'info')] ?? LEVELS.info;
  const write = (level, msg, extra) => {
    if (LEVELS[level] < min) return;
    const requestId = als.getStore()?.requestId;
    console.log(JSON.stringify({ ts: new Date().toISOString(), level, service, hostname, requestId, msg, ...extra }));
  };
  return {
    debug: (m, e) => write('debug', m, e),
    info: (m, e) => write('info', m, e),
    warn: (m, e) => write('warn', m, e),
    error: (m, e) => write('error', m, e),
  };
}

// Every JSON response carries the hostname of the pod/container that served it.
const reply = (res, status, body) => res.status(status).json({ ...body, hostname });

// Retry an async function until it succeeds (used to wait for dependencies at startup).
async function retry(fn, { what, log, delayMs = 1000, tries = 300 }) {
  for (let i = 1; ; i++) {
    try {
      return await fn();
    } catch (err) {
      if (i >= tries) throw err;
      log.warn(`waiting for ${what}`, { attempt: i, err: err.message });
      await sleep(delayMs);
    }
  }
}

/**
 * createService builds the Express app and returns { app, start }.
 *  - routes are mounted by the caller on `app` (they are only reachable once ready)
 *  - init():     async, connects to dependencies (retried by the caller)
 *  - checks:     { name: async () => void } used by /ready (throw = not ready)
 *  - shutdown(): async, closes connections
 */
function createService({ name, port, log, init, checks = {}, shutdown = async () => {} }) {
  client.collectDefaultMetrics();
  const httpTotal = new client.Counter({
    name: 'http_requests_total',
    help: 'HTTP requests by method, route and status code',
    labelNames: ['service', 'method', 'route', 'code'],
  });
  const httpDuration = new client.Histogram({
    name: 'http_request_duration_seconds',
    help: 'HTTP request latency',
    labelNames: ['service', 'method', 'route', 'code'],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  });

  const state = { ready: false, stopping: false };
  const app = express();
  app.disable('x-powered-by');

  // Request id: reuse the caller's x-request-id (correlates logs across services) or create one.
  app.use((req, res, next) => {
    const requestId = req.get('x-request-id') || crypto.randomUUID();
    res.set('x-request-id', requestId);
    const t0 = performance.now();
    res.on('finish', () => {
      const route = req.route ? (req.baseUrl || '') + req.route.path : 'unmatched';
      const labels = { service: name, method: req.method, route, code: res.statusCode };
      const ms = performance.now() - t0;
      httpTotal.inc(labels);
      httpDuration.observe(labels, ms / 1000);
      if (route !== '/metrics' && route !== '/health') {
        log.info('request', { requestId, method: req.method, path: req.originalUrl, status: res.statusCode, ms: Math.round(ms) });
      }
    });
    als.run({ requestId }, next);
  });

  app.get('/health', (req, res) => reply(res, 200, { status: 'ok', service: name })); // liveness: no dependencies
  app.get('/metrics', async (req, res) => {
    res.set('content-type', client.register.contentType).send(await client.register.metrics());
  });
  app.get('/ready', async (req, res) => {
    if (!state.ready || state.stopping) return reply(res, 503, { status: 'not_ready', service: name });
    const results = {};
    let ok = true;
    for (const [dep, check] of Object.entries(checks)) {
      try {
        // Each check gets 500 ms: a hung dependency must not make the probe itself time out.
        await Promise.race([check(), sleep(500).then(() => { throw new Error('timeout (500 ms)'); })]);
        results[dep] = 'ok';
      } catch (err) {
        results[dep] = `error: ${err.message}`;
        ok = false;
      }
    }
    reply(res, ok ? 200 : 503, { status: ok ? 'ready' : 'not_ready', service: name, checks: results });
  });
  // Business routes (mounted later by the caller) are gated until init() has finished.
  app.use((req, res, next) => (state.ready ? next() : reply(res, 503, { error: 'starting' })));

  function start(mountRoutes) {
    mountRoutes(app);
    app.use((req, res) => reply(res, 404, { error: 'not_found' }));
    // eslint-disable-next-line no-unused-vars
    app.use((err, req, res, next) => {
      const status = err.status || err.statusCode || 500;
      if (status >= 500) log.error('unhandled error', { err: err.message });
      reply(res, status, { error: status >= 500 ? 'internal_error' : 'bad_request' });
    });

    const server = app.listen(port, () => log.info('listening', { port }));
    init()
      .then(() => {
        state.ready = true;
        log.info('ready');
      })
      .catch((err) => {
        log.error('init failed', { err: err.message });
        process.exit(1);
      });

    const stop = async (signal) => {
      if (state.stopping) return;
      state.stopping = true;
      log.info('shutting down', { signal });
      const force = setTimeout(() => process.exit(1), 10000);
      force.unref();
      // Stop accepting connections and let in-flight requests finish before closing the dependencies.
      const closed = new Promise((resolve) => server.close(resolve));
      const sweep = setInterval(() => server.closeIdleConnections(), 100);
      await closed;
      clearInterval(sweep);
      try {
        await shutdown();
      } catch (err) {
        log.warn('error during shutdown', { err: err.message });
      }
      log.info('bye');
      process.exit(0);
    };
    process.on('SIGTERM', () => stop('SIGTERM'));
    process.on('SIGINT', () => stop('SIGINT'));
    return server;
  }

  return { app, start, client };
}

module.exports = { hostname, str, num, sleep, makeLogger, reply, retry, createService, client };
