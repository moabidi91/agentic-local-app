#!/usr/bin/env node
/**
 * smoke.mjs — end-to-end check of the front/backend contract against a running server.
 *
 * Plain Node (>= 20), ESM, no dependency. It starts nothing: point it at a server that is
 * already up. One line per check, a non-zero exit as soon as one of them fails.
 *
 *   node smoke.mjs [baseUrl] [--allow-reset] [--timeout-ms=90000]
 *
 * Default base URL: http://127.0.0.1:8765/api/v1
 *
 * It never sends and never prints a token: the credential route is exercised through its two
 * refusals (empty token, malformed body), which write nothing on the server.
 *
 * `POST /admin/reset-database` destroys data. It is only called when the server reports
 * `api.allow_destructive_admin = false` (the call is then a harmless 403) or when --allow-reset
 * is given explicitly; otherwise the check is skipped and says so.
 *
 * Reference: docs/contracts/front-backend-v1.md
 */

const DEFAULT_BASE_URL = 'http://127.0.0.1:8765/api/v1';

const argv = process.argv.slice(2);
const flags = new Set(argv.filter((a) => a.startsWith('--')));
const timeoutArg = argv.find((a) => a.startsWith('--timeout-ms='));
const BASE = (argv.find((a) => !a.startsWith('--')) ?? DEFAULT_BASE_URL).replace(/\/+$/, '');
const ALLOW_RESET = flags.has('--allow-reset');
const SETTLE_TIMEOUT_MS = timeoutArg ? Number(timeoutArg.split('=')[1]) : 90_000;

let passed = 0;
let failed = 0;
let skipped = 0;
let step = 0;

function line(mark, name, detail) {
  step += 1;
  const index = String(step).padStart(2, '0');
  console.log(`${mark} ${index}  ${name}${detail ? ` — ${detail}` : ''}`);
}

function pass(name, detail) {
  passed += 1;
  line('ok  ', name, detail);
}

function fail(name, detail) {
  failed += 1;
  line('FAIL', name, detail);
}

function skip(name, detail) {
  skipped += 1;
  line('skip', name, detail);
}

function check(name, condition, detail) {
  if (condition) {
    pass(name, detail);
  } else {
    fail(name, detail);
  }
  return condition;
}

// ------------------------------------------------------------------------------------------------
// HTTP helpers
// ------------------------------------------------------------------------------------------------

async function request(path, { method = 'GET', body, accept = 'application/json' } = {}) {
  const init = { method, headers: { Accept: accept } };
  if (body !== undefined) {
    init.headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(body);
  }
  const response = await fetch(`${BASE}${path}`, init);
  const raw = response.status === 204 ? '' : await response.text();
  let parsed = null;
  if (raw !== '') {
    try {
      parsed = JSON.parse(raw);
    } catch {
      parsed = null;
    }
  }
  return { status: response.status, body: parsed, raw };
}

function errorCode(result) {
  return result.body && result.body.error ? result.body.error.error_code : '<no envelope>';
}

function isRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Reads an SSE stream until `max` frames or `timeoutMs`, whichever comes first. */
async function readSse(path, { max = 5, timeoutMs = 10_000 } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const frames = [];
  try {
    const response = await fetch(`${BASE}${path}`, {
      headers: { Accept: 'text/event-stream' },
      signal: controller.signal,
    });
    if (!response.ok || !response.body) {
      return { status: response.status, frames };
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (frames.length < max) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
      let cut = buffer.indexOf('\n\n');
      while (cut !== -1) {
        const block = buffer.slice(0, cut);
        buffer = buffer.slice(cut + 2);
        const frame = parseFrame(block);
        if (frame !== null) {
          frames.push(frame);
        }
        cut = buffer.indexOf('\n\n');
      }
    }
    await reader.cancel().catch(() => {});
    return { status: response.status, frames };
  } catch (error) {
    if (error.name !== 'AbortError') {
      throw error;
    }
    return { status: 200, frames };
  } finally {
    clearTimeout(timer);
    controller.abort();
  }
}

function parseFrame(block) {
  const frame = { id: null, event: null, data: null };
  let seen = false;
  for (const raw of block.split('\n')) {
    if (raw.startsWith(':') || raw === '') {
      continue;
    }
    const colon = raw.indexOf(':');
    const field = colon === -1 ? raw : raw.slice(0, colon);
    const value = colon === -1 ? '' : raw.slice(colon + 1).replace(/^ /, '');
    if (field === 'id') {
      frame.id = value;
      seen = true;
    } else if (field === 'event') {
      frame.event = value;
      seen = true;
    } else if (field === 'data') {
      frame.data = frame.data === null ? value : `${frame.data}\n${value}`;
      seen = true;
    }
  }
  if (!seen) {
    return null;
  }
  if (frame.data !== null) {
    try {
      frame.parsed = JSON.parse(frame.data);
    } catch {
      frame.parsed = null;
    }
  }
  return frame;
}

// ------------------------------------------------------------------------------------------------
// the run
// ------------------------------------------------------------------------------------------------

async function main() {
  console.log(`contract smoke check — ${BASE}`);
  console.log('');

  // ---- §3.1 reachability and identity ---------------------------------------------------------
  let health;
  try {
    health = await request('/health');
  } catch (error) {
    fail('server reachable', `${error.message} (is "uv run agentic-app serve" running?)`);
    return finish();
  }
  if (
    !check(
      'GET /health',
      health.status === 200 && isRecord(health.body) && health.body.status === 'ok',
      `status=${health.status}`,
    )
  ) {
    return finish();
  }

  const whoami = await request('/whoami');
  check(
    'GET /whoami → {user_id, source, host}',
    whoami.status === 200 &&
      isRecord(whoami.body) &&
      typeof whoami.body.user_id === 'string' &&
      typeof whoami.body.source === 'string' &&
      'host' in whoami.body,
    `user_id=${whoami.body?.user_id ?? '?'} source=${whoami.body?.source ?? '?'}`,
  );

  // ---- §3.2 model catalogue -------------------------------------------------------------------
  const models = await request('/models');
  const catalogue = Array.isArray(models.body?.models) ? models.body.models : [];
  const active = models.body?.active;
  check(
    'GET /models → {active, models[]}, active first',
    models.status === 200 &&
      typeof active === 'string' &&
      catalogue.length > 0 &&
      catalogue[0].name === active &&
      catalogue[0].active === true,
    `active=${active} profiles=${catalogue.length}`,
  );
  check(
    'GET /models carries no secret (no token, no URL)',
    !/https?:\/\//.test(models.raw) && !/token|secret|api_key/i.test(models.raw),
    'no url and no credential-looking key in the payload',
  );
  check(
    'ModelOption fields present (name, provider, codec, requires_credentials)',
    catalogue.every(
      (m) =>
        typeof m.name === 'string' &&
        typeof m.provider === 'string' &&
        typeof m.codec === 'string' &&
        typeof m.requires_credentials === 'boolean',
    ),
    `requires_credentials=${catalogue[0]?.requires_credentials}`,
  );

  // ---- §3.10 credentials: the two refusals, which write nothing --------------------------------
  const blank = await request('/credentials', { method: 'POST', body: { token: '   ' } });
  const blankRefused = blank.status === 400 && errorCode(blank) === 'CREDENTIALS_EMPTY';
  const notConfigured = blank.status === 409 && errorCode(blank) === 'CREDENTIALS_NOT_CONFIGURED';
  check(
    'POST /credentials with a blank token → refused, nothing written',
    blankRefused || notConfigured,
    `${blank.status} ${errorCode(blank)}`,
  );
  const malformed = await request('/credentials', { method: 'POST', body: { tok: 'x' } });
  check(
    'POST /credentials with a misspelled field → 422, body never echoed',
    malformed.status === 422 && !malformed.raw.includes('"x"'),
    `${malformed.status} ${errorCode(malformed)}`,
  );

  // ---- §4 error envelope ----------------------------------------------------------------------
  const ghost = await request('/sessions/does-not-exist');
  check(
    'unknown session → 404 NOT_FOUND in the uniform envelope',
    ghost.status === 404 &&
      errorCode(ghost) === 'NOT_FOUND' &&
      typeof ghost.body?.error?.error_type === 'string' &&
      typeof ghost.body?.error?.retryable === 'boolean',
    `error_type=${ghost.body?.error?.error_type}`,
  );
  const nowhere = await request('/nothing-here');
  check(
    'unknown route → 404 HTTP_404 in the same envelope',
    nowhere.status === 404 && errorCode(nowhere) === 'HTTP_404',
    `${nowhere.status} ${errorCode(nowhere)}`,
  );

  // ---- §3.4 create a session ------------------------------------------------------------------
  const created = await request('/sessions', {
    method: 'POST',
    body: {
      goal: 'Contract smoke check',
      user_message: 'Run the contract smoke check of the desktop console.',
    },
  });
  const sid = created.body?.session_id;
  if (
    !check(
      'POST /sessions → 201 SessionRecord',
      created.status === 201 && typeof sid === 'string' && created.body?.status === 'RUNNING',
      `session_id=${sid ?? '?'} status=${created.body?.status ?? '?'}`,
    )
  ) {
    return finish();
  }
  const session = encodeURIComponent(sid);

  const badSpace = await request('/sessions', {
    method: 'POST',
    body: { goal: 'g', user_message: 'm', working_space: './relative' },
  });
  check(
    'POST /sessions with a relative working_space → 400 WORKING_SPACE_INVALID',
    badSpace.status === 400 &&
      errorCode(badSpace) === 'WORKING_SPACE_INVALID' &&
      badSpace.body?.error?.details?.reason === 'not_absolute',
    `reason=${badSpace.body?.error?.details?.reason}`,
  );

  // ---- §7.3 the send button is blocked while the loop runs -------------------------------------
  const busy = await request(`/sessions/${session}/messages`, {
    method: 'POST',
    body: { user_message: 'while it runs' },
  });
  check(
    'POST /sessions/{sid}/messages while RUNNING → 409 SESSION_BUSY',
    busy.status === 409 && errorCode(busy) === 'SESSION_BUSY',
    `${busy.status} ${errorCode(busy)}`,
  );

  // ---- §5 the live stream ---------------------------------------------------------------------
  const stream = await readSse(`/sessions/${session}/events?last_event_id=0`, {
    max: 3,
    timeoutMs: 20_000,
  });
  const wellFormed = stream.frames.filter(
    (f) => typeof f.event === 'string' && f.parsed && typeof f.parsed.event_type === 'string',
  );
  check(
    'GET /sessions/{sid}/events (SSE) → frames with id, event and canonical data',
    wellFormed.length > 0 &&
      wellFormed.every((f) => f.event === f.parsed.event_type && typeof f.id === 'string'),
    `${wellFormed.length} frame(s): ${wellFormed.map((f) => f.event).join(', ')}`,
  );
  check(
    'SSE frame identifiers follow the audit sequence',
    wellFormed.every((f) => /^\d+(\.\d+)?$/.test(f.id) && typeof f.parsed.sequence === 'number'),
    `ids=${wellFormed.map((f) => f.id).join(',')}`,
  );

  // ---- §3.9 pause is readable and says "not paused" --------------------------------------------
  const pause = await request(`/sessions/${session}/pause`);
  check(
    'GET /sessions/{sid}/pause on a live session → 404 NOT_PAUSED',
    (pause.status === 404 && errorCode(pause) === 'NOT_PAUSED') ||
      (pause.status === 200 && typeof pause.body?.reason === 'string'),
    pause.status === 200 ? `paused: ${pause.body.reason}` : `${pause.status} ${errorCode(pause)}`,
  );

  // ---- the session settles --------------------------------------------------------------------
  const settled = await waitUntilSettled(session, SETTLE_TIMEOUT_MS);
  check(
    'the session leaves RUNNING within the timeout',
    settled.status !== null && settled.status !== 'RUNNING',
    `status=${settled.status ?? 'timeout'} after ${settled.waitedMs} ms`,
  );
  if (settled.status === 'FAILED') {
    const failures = await request(`/sessions/${session}/failures`);
    const last = Array.isArray(failures.body) ? failures.body[failures.body.length - 1] : null;
    fail(
      'the session reached COMPLETED',
      `FAILED: ${last?.error_type ?? '?'} ${last?.error_code ?? '?'} ` +
        '(is the model reachable? "uv run agentic-app mock-server" in another terminal)',
    );
  } else {
    check(
      'the session reached a terminal state the front can render',
      ['COMPLETED', 'READY', 'PAUSED'].includes(settled.status),
      `status=${settled.status}`,
    );
  }

  // ---- §3.5 snapshot: everything SessionSnapshot needs -----------------------------------------
  const snapshot = await request(`/sessions/${session}/snapshot`);
  const snap = snapshot.body;
  check(
    'GET /sessions/{sid}/snapshot → session, conversation, cycle, plan, tasks',
    snapshot.status === 200 &&
      isRecord(snap?.session) &&
      'conversation' in snap &&
      'cycle' in snap &&
      'plan' in snap &&
      Array.isArray(snap.tasks),
    `tasks=${snap?.tasks?.length ?? 0}`,
  );
  const budget = snap?.session?.session_budget;
  check(
    'snapshot carries the six budget counters the front shows',
    isRecord(budget) &&
      ['max_cycles', 'max_plans', 'max_total_duration_ms'].every((k) => typeof budget[k] === 'number') &&
      ['consumed_cycles', 'consumed_plans', 'consumed_duration_ms'].every(
        (k) => typeof budget[k] === 'number',
      ),
    `cycles ${budget?.consumed_cycles}/${budget?.max_cycles}, plans ${budget?.consumed_plans}/${budget?.max_plans}`,
  );

  // ---- §3.8 the chat ---------------------------------------------------------------------------
  const chat = await request(`/sessions/${session}/chat`);
  const turns = Array.isArray(chat.body?.messages) ? chat.body.messages : [];
  check(
    'GET /sessions/{sid}/chat → turns with id, role, text, created_at, message_type',
    chat.status === 200 &&
      turns.length > 0 &&
      turns.every(
        (t) =>
          typeof t.id === 'string' &&
          ['user', 'assistant', 'system'].includes(t.role) &&
          typeof t.text === 'string' &&
          typeof t.created_at === 'string' &&
          typeof t.message_type === 'string',
      ),
    `${turns.length} turn(s), roles: ${[...new Set(turns.map((t) => t.role))].join(', ')}`,
  );
  const visible = await request(`/sessions/${session}/chat?include_system=false`);
  const userTurns = Array.isArray(visible.body?.messages) ? visible.body.messages : [];
  check(
    'GET /sessions/{sid}/chat?include_system=false → only user and model turns',
    visible.status === 200 && userTurns.every((t) => t.role !== 'system'),
    `${userTurns.length} of ${turns.length} turn(s) kept`,
  );

  // ---- §3.6 a follow-up the settled session accepts ---------------------------------------------
  const followUp = await request(`/sessions/${session}/messages`, {
    method: 'POST',
    body: { user_message: 'One more question for the contract check.' },
  });
  check(
    'POST /sessions/{sid}/messages once the loop let go → 202',
    followUp.status === 202 && followUp.body?.status === 'RUNNING',
    `${followUp.status} ${followUp.status === 202 ? 'RUNNING' : errorCode(followUp)}`,
  );

  // ---- §3.7 interruption -----------------------------------------------------------------------
  const interrupt = await request(`/sessions/${session}/interrupt`, { method: 'POST' });
  check(
    'POST /sessions/{sid}/interrupt → 200 InterruptionReport',
    interrupt.status === 200 && isRecord(interrupt.body),
    `keys: ${Object.keys(interrupt.body ?? {}).slice(0, 4).join(', ')}`,
  );
  const afterStop = await request(`/sessions/${session}`);
  check(
    'the interrupted session is READY again (the stop button always works)',
    afterStop.status === 200 && afterStop.body?.status === 'READY',
    `status=${afterStop.body?.status}`,
  );

  // ---- §3.12 audit chain ------------------------------------------------------------------------
  const audit = await request(`/sessions/${session}/audit?limit=5`);
  check(
    'GET /sessions/{sid}/audit → chained entries with their hashes',
    audit.status === 200 &&
      Array.isArray(audit.body?.items) &&
      audit.body.items.length > 0 &&
      audit.body.items.every(
        (e) => typeof e.event_hash === 'string' && typeof e.previous_event_hash === 'string',
      ),
    `${audit.body?.items?.length ?? 0} entr(y|ies)`,
  );
  const verify = await request(`/sessions/${session}/audit/verify`);
  check(
    'GET /sessions/{sid}/audit/verify → the chain is valid',
    verify.status === 200 && verify.body?.valid === true,
    `valid=${verify.body?.valid}`,
  );

  // ---- §3.11 the three administration views -----------------------------------------------------
  const adminSessions = await request('/admin/sessions?limit=5');
  check(
    'GET /admin/sessions → {items, limit, offset, next_offset}, newest first',
    adminSessions.status === 200 &&
      Array.isArray(adminSessions.body?.items) &&
      adminSessions.body.items.length > 0 &&
      ['limit', 'offset', 'next_offset'].every((k) => k in adminSessions.body) &&
      adminSessions.body.items.every(
        (s) =>
          typeof s.session_id === 'string' &&
          typeof s.status === 'string' &&
          typeof s.user_id === 'string' &&
          typeof s.created_at === 'string' &&
          typeof s.updated_at === 'string',
      ),
    `${adminSessions.body?.items?.length ?? 0} session(s)`,
  );
  const adminEvents = await request('/admin/events?limit=5');
  check(
    'GET /admin/events → flat rows, hashes left to the audit view',
    adminEvents.status === 200 &&
      Array.isArray(adminEvents.body?.items) &&
      adminEvents.body.items.length > 0 &&
      adminEvents.body.items.every(
        (e) =>
          typeof e.event_id === 'string' &&
          typeof e.event_type === 'string' &&
          typeof e.timestamp === 'string' &&
          !('event_hash' in e),
      ),
    `${adminEvents.body?.items?.length ?? 0} event(s)`,
  );
  const adminAudit = await request('/admin/audit?limit=5');
  check(
    'GET /admin/audit → the chain of every session, hashes included',
    adminAudit.status === 200 &&
      Array.isArray(adminAudit.body?.items) &&
      adminAudit.body.items.length > 0 &&
      adminAudit.body.items.every(
        (e) => typeof e.event_hash === 'string' && typeof e.previous_event_hash === 'string',
      ),
    `${adminAudit.body?.items?.length ?? 0} entr(y|ies)`,
  );

  // ---- §3.13 the destructive route, and its gate -----------------------------------------------
  const config = await request('/config');
  const destructive = config.body?.api?.allow_destructive_admin;
  if (destructive === false) {
    const reset = await request('/admin/reset-database', { method: 'POST' });
    check(
      'POST /admin/reset-database is gated → 403 ADMIN_DISABLED, nothing touched',
      reset.status === 403 &&
        errorCode(reset) === 'ADMIN_DISABLED' &&
        reset.body?.error?.details?.setting === 'api.allow_destructive_admin',
      `${reset.status} ${errorCode(reset)}`,
    );
  } else if (ALLOW_RESET) {
    const reset = await request('/admin/reset-database', { method: 'POST' });
    const empty = await request('/admin/sessions');
    check(
      'POST /admin/reset-database → 204 and an empty store',
      reset.status === 204 && (empty.body?.items?.length ?? -1) === 0,
      `${reset.status}, ${empty.body?.items?.length ?? '?'} session(s) left`,
    );
  } else {
    skip(
      'POST /admin/reset-database',
      'api.allow_destructive_admin is true — pass --allow-reset to empty the store on purpose',
    );
  }

  return finish();
}

async function waitUntilSettled(session, timeoutMs) {
  const started = Date.now();
  let status = null;
  while (Date.now() - started < timeoutMs) {
    const response = await request(`/sessions/${session}`);
    status = response.body?.status ?? null;
    if (status !== null && status !== 'RUNNING' && status !== 'INTERRUPTING') {
      break;
    }
    await sleep(500);
  }
  return { status, waitedMs: Date.now() - started };
}

function finish() {
  console.log('');
  console.log(
    `${passed} passed, ${failed} failed${skipped > 0 ? `, ${skipped} skipped` : ''} — ${BASE}`,
  );
  process.exitCode = failed > 0 ? 1 : 0;
}

main().catch((error) => {
  fail('unexpected error', error && error.stack ? error.stack.split('\n')[0] : String(error));
  finish();
  process.exitCode = 1;
});
