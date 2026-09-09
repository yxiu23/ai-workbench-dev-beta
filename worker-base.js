/**
 * AI Media Workbench — Cloudflare Worker Backend (Task Center)
 *
 * Single-file ES module Worker providing:
 *   1. R2 file upload (backward compatible with existing frontend)
 *   2. Task Center: submit / list / get / cancel / delete single tasks
 *   3. Batch Queue: priority field + cron-driven concurrency-limited dispatch
 *   4. Pipeline (TTS → DigitalHuman → Package): step advancement on cron
 *   5. Chain (long-video segments): frontend-driven advancement, worker stores state
 *   6. Cron handler: polls running tasks, processes queued tasks, advances pipelines
 *   7. URL extraction that mirrors the frontend `extractUrl` exactly
 *   8. Speedx (速创) API forwarding with key held server-side only
 *
 * Endpoints (all authed via ?token=XXX unless noted):
 *   GET  /                              Health check (public)
 *   POST /upload                        R2 upload — FormData with "file" field
 *   POST /task                          Submit {kind:"single"|"pipeline"|"chain", ...}
 *   GET  /tasks                         Active + recent 20 completed tasks
 *   GET  /task/<id>                     Single task record
 *   POST /task/<id>/cancel              Mark canceled, move to recent
 *   DELETE /task/<id>                   Hard delete from KV + indexes
 *   POST /task/<id>/retry               Retry failed pipeline/chain/single
 *   POST /chain/<id>/advance            Frontend-driven: set tailFrameUrl, advance to next segment
 *   GET  /userdata?key=<k>&token=xxx   Read R2-backed JSON value (history sync)
 *   PUT  /userdata?key=<k>&token=xxx   Write R2-backed JSON value (body = raw JSON)
 *   POST /archive?token=xxx&url=...&ext=png   Download upstream result → store to R2 results/
 *
 * Environment:
 *   AUTH_TOKEN      (required)  Shared secret compared to ?token=
 *   SPEEDX_KEY      (required)  速创 API key, sent verbatim as Authorization header
 *   LIMIT_DAILY     (optional)  Daily submission cap, default 200
 *   PUBLIC_BASE_URL (optional)  Public URL prefix for R2 objects (custom domain)
 *
 * Bindings:
 *   TASKS     KV namespace — task records (key task:<id>) + index keys (idx:active, idx:recent)
 *   AI_BUCKET R2 bucket (existing)     — file uploads stored under uploads/<ts>-<rand>.<ext>
 */

// ============================================================
// Constants
// ============================================================

/** Speedx API base URL. */
const SPEEDX_BASE = 'https://api.wuyinkeji.com';

/** Speedx detail endpoint (GET with ?id=). */
const SPEEDX_DETAIL_PATH = '/api/async/detail';

/** KV key holding the JSON array of active task IDs (queued or running). */
const ACTIVE_INDEX_KEY = 'idx:active';

/** KV key holding the JSON array of recently completed task IDs (max RECENT_MAX). */
const RECENT_INDEX_KEY = 'idx:recent';

/** Maximum number of IDs retained in idx:recent. */
const RECENT_MAX = 50;

/** Number of recently completed tasks returned by GET /tasks. */
const RECENT_RETURN = 20;

/** Per-modelType concurrency caps applied when processing queued tasks. */
const CONCURRENCY = { image: 2, video: 1, audio: 2 };

/** Auto-retry delay (seconds) — queued task is skipped until notBefore elapses. */
const RETRY_DELAY_SEC = 60;

/** Maximum submit attempts (initial + retries) before permanent failure. */
const MAX_ATTEMPTS = 2;

/** Running task is treated as timed out after this many seconds since createdAt. */
const STALE_RUNNING_SEC = 1800;

/** Minimum seconds between two consecutive detail polls of the same task. */
const POLL_MIN_GAP_SEC = 3;

/** KV TTL for task records (30 days). */
const TASK_TTL_SEC = 86400 * 30;

/** KV TTL for daily counter keys (~25h to span any timezone's "today"). */
const COUNTER_TTL_SEC = 90000;

/** Top-level keys searched by extractUrl, in priority order. Mirrors frontend. */
const URL_KEYS = [
  'url', 'video_url', 'image_url', 'audio_url', 'result', 'result_url',
  'demo_audio', 'file_url', 'download_url', 'output_url',
  'video', 'image', 'audio', 'media_url', 'output',
];

/** Regex matching http(s) URLs, used by isUrl. */
const URL_RE = /^https?:\/\/\S+/i;

/**
 * Regex matching OSS / aliyuncs hosts served over HTTP — fixUrl rewrites them to HTTPS.
 * Captures the host so the rewrite preserves it. Mirrors frontend fixUrl exactly.
 */
const HTTP_OSS_RE = /^http:\/\/(wywxopenai\.oss[^/]+|[^/]*aliyuncs\.com)\//i;

// ============================================================
// Response & utility helpers
// ============================================================

/**
 * Build a JSON Response with no-store cache headers.
 * @param {number} status  HTTP status code.
 * @param {unknown} body   JSON-serializable body.
 * @returns {Response}
 */
function jsonResponse(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'access-control-allow-headers': 'Content-Type, Authorization',
    },
  });
}

/**
 * Generate a random short ID with a descriptive prefix.
 * Uses crypto.getRandomValues for non-deterministic 16 hex chars.
 * @param {string} prefix  Short prefix like "tc", "pl", "ch", "r2".
 * @returns {string} e.g. "tc_3f9a2b1c4d5e6f7a"
 */
function genId(prefix) {
  const bytes = crypto.getRandomValues(new Uint8Array(8));
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
  return `${prefix}_${hex}`;
}

/** Current time as integer seconds (matches JS Date.now()/1000). */
function nowSec() {
  return Math.floor(Date.now() / 1000);
}

/**
 * Parse a value as JSON if it is a string; return non-strings unchanged.
 * Mirrors the inline IIFE in frontend extractUrl.
 * @param {unknown} v
 * @returns {unknown}
 */
function tryParseJson(v) {
  if (typeof v !== 'string') return v;
  try {
    return JSON.parse(v);
  } catch {
    return v;
  }
}

/**
 * Test whether a value is a usable URL string.
 * Mirrors frontend `isUrl`: string, http(s) prefix, length under 4000 chars.
 * @param {unknown} s
 * @returns {boolean}
 */
function isUrl(s) {
  return typeof s === 'string' && URL_RE.test(s) && s.length < 4000;
}

/**
 * Rewrite HTTP OSS / aliyuncs URLs to HTTPS to avoid browser mixed-content blocking.
 * Mirrors frontend fixUrl() exactly — do not diverge.
 * @param {string|unknown} url
 * @returns {string|unknown}
 */
function fixUrl(url) {
  if (typeof url !== 'string') return url;
  return url.replace(HTTP_OSS_RE, 'https://$1/');
}

/**
 * Recursively search an arbitrary nested structure for the first URL-like string.
 * Prefers values whose key matches /url|file|output|result|media|video|image|audio/i.
 * Bounded at depth 8 to prevent runaway recursion.
 * Mirrors frontend _deepFindUrl exactly.
 * @param {unknown} obj
 * @param {number} [depth=0]
 * @returns {string|null}
 */
function deepFindUrl(obj, depth = 0) {
  if (!obj || depth > 8) return null;
  if (typeof obj === 'string') return isUrl(obj) ? obj : null;
  if (Array.isArray(obj)) {
    for (const it of obj) {
      const u = deepFindUrl(it, depth + 1);
      if (u) return u;
    }
    return null;
  }
  if (typeof obj === 'object') {
    for (const [k, v] of Object.entries(obj)) {
      if (/url|file|output|result|media|video|image|audio/i.test(k) && isUrl(v)) return v;
      const u = deepFindUrl(v, depth + 1);
      if (u) return u;
    }
  }
  return null;
}

/**
 * Extract a result URL from a Speedx detail response.
 *
 * Strategy mirrors frontend extractUrl exactly — divergence causes
 * "frontend shows success / worker reports failure" splits:
 *   1. Try common top-level keys (url, video_url, image_url, ...)
 *   2. Try nested obj.data (often a JSON string with the real payload)
 *   3. Try obj.outputs (array or object)
 *   4. Deep search the whole object
 *
 * All returned URLs are run through fixUrl for HTTP→HTTPS normalization.
 * @param {unknown} data  The full Speedx response body.
 * @returns {string|null}
 */
function extractUrl(data) {
  if (!data) return null;
  const obj = tryParseJson(data);

  for (const k of URL_KEYS) {
    const v = obj?.[k];
    if (isUrl(v)) return fixUrl(v);
    if (Array.isArray(v)) {
      const u = v.find(isUrl);
      if (u) return fixUrl(u);
    }
  }

  if (obj?.data) {
    const nested = tryParseJson(obj.data);
    for (const k of URL_KEYS) {
      const v = nested?.[k];
      if (isUrl(v)) return fixUrl(v);
      if (Array.isArray(v)) {
        const u = v.find(isUrl);
        if (u) return fixUrl(u);
      }
    }
    if (isUrl(nested)) return fixUrl(nested);
  }

  if (obj?.outputs) {
    if (Array.isArray(obj.outputs)) {
      for (const it of obj.outputs) {
        if (isUrl(it)) return fixUrl(it);
        if (typeof it === 'object' && it !== null) {
          const u = extractUrl(it);
          if (u) return fixUrl(u);
        }
      }
    } else if (typeof obj.outputs === 'object' && obj.outputs !== null) {
      const u = extractUrl(obj.outputs);
      if (u) return fixUrl(u);
    }
  }

  return fixUrl(deepFindUrl(obj));
}

/**
 * Map Speedx detail status (0/1/2/3) to internal task status.
 *   0 init, 1 running  → "running"
 *   2 succeeded        → "succeeded"
 *   3 failed           → "failed"
 * @param {number} s
 * @returns {"running"|"succeeded"|"failed"}
 */
function mapApiStatus(s) {
  if (s === 2) return 'succeeded';
  if (s === 3) return 'failed';
  return 'running';
}

/**
 * Decide whether an upstream failure is retryable.
 * Retryable: network errors (status 0), HTTP 429, HTTP 5xx,
 * or error messages mentioning timeout/network/fetch/tcp/dns.
 * @param {number} status   HTTP status from Speedx, or 0 for network error.
 * @param {string} [errMsg] Upstream error message.
 * @returns {boolean}
 */
function isRetryableError(status, errMsg) {
  if (status === 0) return true;
  if (status === 429) return true;
  if (status >= 500 && status < 600) return true;
  if (errMsg && /timeout|network|fetch|tcp|dns/i.test(errMsg)) return true;
  return false;
}

// ============================================================
// Auth
// ============================================================

/**
 * Validate ?token= against env.AUTH_TOKEN.
 * @param {URL} url
 * @param {{ AUTH_TOKEN?: string }} env
 * @returns {{ ok: true } | { ok: false, response: Response }}
 */
function requireAuth(url, env) {
  if (!env.AUTH_TOKEN) {
    return { ok: false, response: jsonResponse(500, { error: 'AUTH_TOKEN not configured' }) };
  }
  const token = url.searchParams.get('token');
  if (!token || token !== env.AUTH_TOKEN) {
    return { ok: false, response: jsonResponse(401, { error: 'Unauthorized' }) };
  }
  return { ok: true };
}

// ============================================================
// Speedx API forwarding
// ============================================================

/**
 * Forward a task body to a Speedx async endpoint.
 * The Authorization header carries SPEEDX_KEY verbatim (no Bearer prefix).
 * @param {{ SPEEDX_KEY: string }} env
 * @param {string} endpoint  Path beginning with /api/async/
 * @param {unknown} body     JSON-serializable request body.
 * @returns {Promise<{ status: number, ok: boolean, data: unknown|null, networkError: boolean }>}
 */
async function submitToSpeedx(env, endpoint, body) {
  try {
    const resp = await fetch(`${SPEEDX_BASE}${endpoint}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: env.SPEEDX_KEY,
      },
      body: JSON.stringify(body),
    });
    const text = await resp.text();
    let data = null;
    if (text) {
      try { data = JSON.parse(text); } catch { data = null; }
    }
    return { status: resp.status, ok: resp.ok, data, networkError: false };
  } catch (e) {
    return { status: 0, ok: false, data: null, networkError: true, error: e.message };
  }
}

/**
 * Query Speedx detail API for a task's current status & result.
 * @param {{ SPEEDX_KEY: string }} env
 * @param {string} apiId  The id returned by the original submit call.
 * @returns {Promise<{ status: number, ok: boolean, data: unknown|null }>}
 */
async function getDetailFromSpeedx(env, apiId) {
  try {
    const resp = await fetch(
      `${SPEEDX_BASE}${SPEEDX_DETAIL_PATH}?id=${encodeURIComponent(apiId)}`,
      {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          Authorization: env.SPEEDX_KEY,
        },
      },
    );
    const text = await resp.text();
    let data = null;
    if (text) {
      try { data = JSON.parse(text); } catch { data = null; }
    }
    return { status: resp.status, ok: resp.ok, data };
  } catch (e) {
    return { status: 0, ok: false, data: null };
  }
}

// ============================================================
// KV index helpers
// ============================================================

/**
 * Read a JSON-array index key from KV. Returns [] on missing/invalid.
 * @param {{ TASKS: KVNamespace }} env
 * @param {string} key
 * @returns {Promise<string[]>}
 */
async function getIndex(env, key) {
  const raw = await env.TASKS.get(key);
  if (!raw) return [];
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

/**
 * Overwrite an index key with a JSON array.
 * @param {{ TASKS: KVNamespace }} env
 * @param {string} key
 * @param {string[]} arr
 */
async function putIndex(env, key, arr) {
  await env.TASKS.put(key, JSON.stringify(arr));
}

/**
 * Append an ID to an index key (dedup, no ordering guarantees).
 * @param {{ TASKS: KVNamespace }} env
 * @param {string} key
 * @param {string} id
 */
async function addToIndex(env, key, id) {
  const arr = await getIndex(env, key);
  if (!arr.includes(id)) {
    arr.push(id);
    await putIndex(env, key, arr);
  }
}

/**
 * Remove an ID from an index key (no-op if absent).
 * @param {{ TASKS: KVnamespace }} env
 * @param {string} key
 * @param {string} id
 */
async function removeFromIndex(env, key, id) {
  const arr = await getIndex(env, key);
  const i = arr.indexOf(id);
  if (i >= 0) {
    arr.splice(i, 1);
    await putIndex(env, key, arr);
  }
}

/**
 * Move an ID from idx:active to idx:recent (most-recent-first, capped at RECENT_MAX).
 * @param {{ TASKS: KVNamespace }} env
 * @param {string} id
 */
async function moveToRecent(env, id) {
  await removeFromIndex(env, ACTIVE_INDEX_KEY, id);
  const recent = await getIndex(env, RECENT_INDEX_KEY);
  if (!recent.includes(id)) {
    recent.unshift(id);
    if (recent.length > RECENT_MAX) recent.length = RECENT_MAX;
    await putIndex(env, RECENT_INDEX_KEY, recent);
  }
}

// ============================================================
// Daily rate limit
// ============================================================

/**
 * Check whether the daily submission limit has been reached.
 * Uses a single global counter (single-tenant AUTH_TOKEN model).
 * @param {{ TASKS: KVNamespace, LIMIT_DAILY?: string }} env
 * @returns {Promise<{ ok: boolean, limit: number, count: number, key?: string }>}
 */
async function checkDailyLimit(env) {
  const limit = parseInt(env.LIMIT_DAILY || '200', 10) || 200;
  const today = new Date().toISOString().slice(0, 10);
  const key = `cnt:${today}`;
  const raw = await env.TASKS.get(key);
  const count = raw ? parseInt(raw, 10) || 0 : 0;
  if (count >= limit) return { ok: false, limit, count };
  return { ok: true, limit, count, key };
}

/**
 * Increment the daily counter, with ~25h TTL so it self-expires.
 * @param {{ TASKS: KVNamespace }} env
 * @param {string} key  The counter key returned by checkDailyLimit.
 */
async function incrDailyCount(env, key) {
  const raw = await env.TASKS.get(key);
  const count = raw ? parseInt(raw, 10) || 0 : 0;
  await env.TASKS.put(key, String(count + 1), { expirationTtl: COUNTER_TTL_SEC });
}

// ============================================================
// Task record read/write
// ============================================================

/**
 * Read a task record (single / pipeline / chain) by id.
 * @param {{ TASKS: KVNamespace }} env
 * @param {string} id
 * @returns {Promise<object|null>}
 */
async function getTask(env, id) {
  const raw = await env.TASKS.get(`task:${id}`);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch { return null; }
}

/**
 * Persist a task record. Sets updatedAt on every write for conflict detection.
 * @param {{ TASKS: KVNamespace }} env
 * @param {object} task
 */
async function putTask(env, task) {
  task.updatedAt = nowSec();
  await env.TASKS.put(`task:${task.id}`, JSON.stringify(task), {
    expirationTtl: TASK_TTL_SEC,
  });
}

/**
 * Optimistic update: re-read the task and only persist if updatedAt is unchanged
 * since the caller read it. Used by the cron to avoid clobbering concurrent
 * API-driven writes (cancel/delete) — KV is last-write-wins.
 * @param {{ TASKS: KVNamespace }} env
 * @param {object} task   The task as the caller currently sees it.
 * @returns {Promise<boolean>} true if the write succeeded.
 */
async function putTaskIfFresh(env, task) {
  const fresh = await getTask(env, task.id);
  if (fresh && fresh.updatedAt > task.updatedAt) {
    return false; // a newer write happened; skip
  }
  await putTask(env, task);
  return true;
}

// ============================================================
// Task submission — single / pipeline / chain
// ============================================================

/**
 * Submit a single task.
 *
 * Behavior:
 *   - Validates minimal structure (endpoint, body, modelType).
 *   - Enforces daily rate limit.
 *   - Stores the task as "queued" and adds to idx:active.
 *   - If no `priority` field was supplied → forward to Speedx immediately
 *     (Phase 2 "立即转发" semantics). On retryable failure, leaves as queued
 *     with notBefore = now + RETRY_DELAY_SEC for the cron to retry.
 *   - If `priority` was supplied → leave queued; the cron dispatches under
 *     concurrency caps (Phase 3 batch queue).
 *
 * @param {object} env
 * @param {{
 *   model?: string,
 *   modelType: "image"|"video"|"audio",
 *   endpoint: string,
 *   body: object,
 *   prompt?: string,
 *   cost?: number,
 *   priority?: number,
 * }} payload
 * @returns {Promise<{ ok: true, task: object } | { ok: false, status: number, error: string }>}
 */
async function submitSingleTask(env, payload) {
  if (!payload || typeof payload !== 'object') {
    return { ok: false, status: 400, error: 'Invalid payload' };
  }
  if (!payload.endpoint || typeof payload.endpoint !== 'string' || !payload.endpoint.startsWith('/api/')) {
    return { ok: false, status: 400, error: 'endpoint must be a string starting with /api/' };
  }
  if (!payload.body || typeof payload.body !== 'object') {
    return { ok: false, status: 400, error: 'body must be an object' };
  }
  if (!['image', 'video', 'audio'].includes(payload.modelType)) {
    return { ok: false, status: 400, error: 'modelType must be one of image|video|audio' };
  }

  const limitCheck = await checkDailyLimit(env);
  if (!limitCheck.ok) {
    return {
      ok: false,
      status: 429,
      error: `Daily limit reached (${limitCheck.count}/${limitCheck.limit})`,
    };
  }

  const now = nowSec();
  const hasPriority = payload.priority !== undefined && payload.priority !== null;

  /** @type {object} */
  const task = {
    id: genId('tc'),
    kind: 'single',
    model: payload.model || '',
    modelType: payload.modelType,
    endpoint: payload.endpoint,
    body: payload.body,
    prompt: payload.prompt || '',
    status: 'queued',
    apiId: null,
    resultUrl: null,
    error: null,
    createdAt: now,
    updatedAt: now,
    finishedAt: 0,
    attempts: 0,
    cost: typeof payload.cost === 'number' ? payload.cost : 0,
    priority: hasPriority ? Number(payload.priority) : 0,
    notBefore: 0,
  };

  await putTask(env, task);
  await addToIndex(env, ACTIVE_INDEX_KEY, task.id);
  await incrDailyCount(env, limitCheck.key);

  // Priority-queued tasks are dispatched by the cron under concurrency caps.
  if (hasPriority) {
    return { ok: true, task };
  }

  // Phase 2: forward immediately so the user sees "running" right away.
  await forwardTaskToSpeedx(env, task);
  return { ok: true, task };
}

/**
 * Forward a queued task to Speedx and update its state.
 *
 * On success: status=running, apiId set.
 * On retryable failure (429/5xx/network) with attempts<MAX_ATTEMPTS:
 *   attempts++, notBefore = now + RETRY_DELAY_SEC, status stays queued.
 * On non-retryable failure or attempts exhausted: status=failed, moved to recent.
 *
 * @param {object} env
 * @param {object} task  Mutated in place and persisted.
 */
async function forwardTaskToSpeedx(env, task) {
  const resp = await submitToSpeedx(env, task.endpoint, task.body);

  if (resp.ok && resp.data && resp.data.data && resp.data.data.id) {
    task.apiId = resp.data.data.id;
    task.status = 'running';
    task.error = null;
    await putTask(env, task);
    return;
  }

  const msg = (resp.data && resp.data.msg) || (resp.networkError ? 'network error' : `upstream ${resp.status}`);

  if (isRetryableError(resp.status, msg) && task.attempts < MAX_ATTEMPTS) {
    task.attempts += 1;
    task.notBefore = nowSec() + RETRY_DELAY_SEC;
    task.error = msg;
    await putTask(env, task);
    return;
  }

  task.status = 'failed';
  task.error = msg;
  task.finishedAt = nowSec();
  await putTask(env, task);
  await moveToRecent(env, task.id);
}

/**
 * Submit a pipeline (multi-step workflow). Step 1 is dispatched immediately;
 * subsequent steps are dispatched by the cron when the prior step succeeds.
 *
 * Payload shape:
 *   {
 *     kind: "pipeline",
 *     prompt?: string,
 *     cost?: number,
 *     steps: [
 *       { type, model?, modelType, endpoint, body, needs: [] },
 *       { type, model?, modelType, endpoint, body, needs: ["audioUrl"] },
 *       ...
 *     ]
 *   }
 *
 * The `needs` array on step N names the body keys that should be filled from
 * step N-1's resultUrl before submission. e.g. step 2 needs ["audioUrl"]
 * means step 1's audio URL is written to step2.body.audioUrl.
 *
 * @param {object} env
 * @param {object} payload
 * @returns {Promise<{ ok: true, task: object } | { ok: false, status: number, error: string }>}
 */
async function submitPipeline(env, payload) {
  if (!Array.isArray(payload?.steps) || payload.steps.length === 0) {
    return { ok: false, status: 400, error: 'Pipeline requires non-empty steps array' };
  }
  const limitCheck = await checkDailyLimit(env);
  if (!limitCheck.ok) {
    return {
      ok: false,
      status: 429,
      error: `Daily limit reached (${limitCheck.count}/${limitCheck.limit})`,
    };
  }

  const now = nowSec();
  /** @type {object} */
  const pipeline = {
    id: genId('pl'),
    kind: 'pipeline',
    steps: payload.steps.map((s, i) => ({
      seq: i + 1,
      type: s.type || '',
      model: s.model || s.type || '',
      modelType: s.modelType,
      endpoint: s.endpoint,
      body: s.body || {},
      needs: Array.isArray(s.needs) ? s.needs : [],
      status: 'pending',
      taskId: null,
      resultUrl: null,
      error: null,
    })),
    current: 1,
    status: 'running',
    failedStep: null,
    createdAt: now,
    updatedAt: now,
    finishedAt: 0,
    prompt: payload.prompt || '',
    cost: typeof payload.cost === 'number' ? payload.cost : 0,
  };

  await putTask(env, pipeline);
  await addToIndex(env, ACTIVE_INDEX_KEY, pipeline.id);
  await incrDailyCount(env, limitCheck.key);

  // Dispatch step 1 immediately (best-effort). On failure, the cron will retry.
  await startPipelineStep(env, pipeline, 1);
  return { ok: true, task: pipeline };
}

/**
 * Start a pipeline step by creating a child single task with priority=10
 * (so it skips immediate forward and goes through the cron queue).
 *
 * Fills the step's `needs` body keys from the prior step's resultUrl.
 * On submit failure (rate limit), marks the pipeline as failed with failedStep.
 *
 * @param {object} env
 * @param {object} pipeline  Mutated in place.
 * @param {number} seq       1-based step index.
 */
async function startPipelineStep(env, pipeline, seq) {
  const step = pipeline.steps[seq - 1];
  if (!step) return;

  if (seq > 1) {
    const prev = pipeline.steps[seq - 2];
    if (prev && prev.resultUrl) {
      for (const need of step.needs) {
        step.body[need] = prev.resultUrl;
      }
    }
  }

  const childResp = await submitSingleTask(env, {
    model: step.model,
    modelType: step.modelType,
    endpoint: step.endpoint,
    body: step.body,
    prompt: step.body?.prompt || '',
    cost: 0,
    priority: 10,
  });

  if (!childResp.ok) {
    step.status = 'failed';
    step.error = childResp.error;
    pipeline.status = 'failed';
    pipeline.failedStep = seq;
    pipeline.finishedAt = nowSec();
    await putTask(env, pipeline);
    await moveToRecent(env, pipeline.id);
    return;
  }

  step.taskId = childResp.task.id;
  step.status = 'running';
  pipeline.current = seq;
  await putTask(env, pipeline);
}

/**
 * Submit a chain (long-video segments). Segment 1 is dispatched immediately.
 * Subsequent segments are dispatched by the frontend calling
 * POST /chain/<id>/advance with the prior segment's tailFrameUrl.
 *
 * Payload shape:
 *   {
 *     kind: "chain",
 *     segments: [{ prompt: "..." }, ...],
 *     stylePrefix?: string,
 *     refImages?: string[],
 *     segmentSeconds?: number,
 *     endpoint?: string,        // default /api/async/video_wan_3.0
 *     model?: string,           // default "wan3"
 *     modelType?: string,       // default "video"
 *     cost?: number,
 *   }
 *
 * @param {object} env
 * @param {object} payload
 * @returns {Promise<{ ok: true, task: object } | { ok: false, status: number, error: string }>}
 */
async function submitChain(env, payload) {
  if (!Array.isArray(payload?.segments) || payload.segments.length === 0) {
    return { ok: false, status: 400, error: 'Chain requires non-empty segments array' };
  }
  const limitCheck = await checkDailyLimit(env);
  if (!limitCheck.ok) {
    return {
      ok: false,
      status: 429,
      error: `Daily limit reached (${limitCheck.count}/${limitCheck.limit})`,
    };
  }

  const now = nowSec();
  /** @type {object} */
  const chain = {
    id: genId('ch'),
    kind: 'chain',
    segments: payload.segments.map((s, i) => ({
      seq: i + 1,
      taskId: null,
      status: 'pending',
      resultUrl: null,
      tailFrameUrl: null,
      prompt: s.prompt || '',
    })),
    stylePrefix: payload.stylePrefix || '',
    refImages: Array.isArray(payload.refImages) ? payload.refImages : [],
    segmentSeconds: payload.segmentSeconds || 30,
    endpoint: payload.endpoint || '/api/async/video_wan_3.0',
    model: payload.model || 'wan3',
    modelType: payload.modelType || 'video',
    current: 1,
    status: 'running',
    createdAt: now,
    updatedAt: now,
    finishedAt: 0,
    cost: typeof payload.cost === 'number' ? payload.cost : 0,
  };

  await putTask(env, chain);
  await addToIndex(env, ACTIVE_INDEX_KEY, chain.id);
  await incrDailyCount(env, limitCheck.key);

  await startChainSegment(env, chain, 1);
  return { ok: true, task: chain };
}

/**
 * Start a chain segment by creating a child single task.
 *
 * The body is built from:
 *   - prompt = stylePrefix + " " + segment.prompt
 *   - duration = segmentSeconds
 *   - first_frame = previous segment's tailFrameUrl (if present)
 *   - images = chain.refImages joined with comma (if present)
 *
 * @param {object} env
 * @param {object} chain  Mutated in place.
 * @param {number} seq    1-based segment index.
 */
async function startChainSegment(env, chain, seq) {
  const seg = chain.segments[seq - 1];
  if (!seg) return;

  /** @type {object} */
  const body = {
    prompt: (chain.stylePrefix ? chain.stylePrefix + ' ' : '') + seg.prompt,
    duration: String(chain.segmentSeconds),
  };
  if (seq > 1 && chain.segments[seq - 2].tailFrameUrl) {
    body.first_frame = chain.segments[seq - 2].tailFrameUrl;
  }
  if (chain.refImages && chain.refImages.length > 0) {
    body.images = chain.refImages.join(',');
  }

  const childResp = await submitSingleTask(env, {
    model: chain.model,
    modelType: chain.modelType,
    endpoint: chain.endpoint,
    body,
    prompt: body.prompt,
    cost: 0,
    priority: 10,
  });

  if (!childResp.ok) {
    seg.status = 'failed';
    seg.error = childResp.error;
    chain.status = 'failed';
    chain.finishedAt = nowSec();
    await putTask(env, chain);
    await moveToRecent(env, chain.id);
    return;
  }

  seg.taskId = childResp.task.id;
  seg.status = 'running';
  chain.current = seq;
  await putTask(env, chain);
}

// ============================================================
// HTTP route handlers
// ============================================================

/**
 * Handle R2 file upload. Backward compatible with existing frontend:
 * FormData with "file" field → returns { url, fileUrl, publicUrl }.
 *
 * @param {Request} request
 * @param {object} env
 * @returns {Promise<Response>}
 */
async function handleUpload(request, env, dir) {
  if (!env.AI_BUCKET) {
    return jsonResponse(500, { error: 'AI_BUCKET not bound' });
  }
  const ct = request.headers.get('content-type') || '';
  // V26.9.11: sanitize dir param — only allow alphanumeric (defined here for all paths)
  const safeDir = (dir || 'uploads').replace(/[^a-zA-Z0-9_-]/g, '') || 'uploads';

  if (ct.startsWith('multipart/form-data')) {
    // Clone the request so we can fall back to manual binary parsing if formData()
    // misbehaves (some legacy Workers runtimes returned File fields as string).
    const requestClone = request.clone();

    // Primary path: Workers' formData() returns File objects (Blob subclass) for
    // file fields — this is fully binary-safe and the runtime handles multipart
    // parsing internally without any text-decode of the body.
    try {
      const formData = await request.formData();
      const file = formData.get('file');
      if (file && typeof file !== 'string' && typeof file.stream === 'function') {
        const fileName = file.name || 'file';
        const fileType = file.type || 'application/octet-stream';
        const ext = fileName.split('.').pop().toLowerCase() || 'bin';
        const key = `${safeDir}/${nowSec()}-${genId('r2')}.${ext}`;
        await env.AI_BUCKET.put(key, file.stream(), {
          httpMetadata: { contentType: fileType },
        });
        const base = env.PUBLIC_BASE_URL ? String(env.PUBLIC_BASE_URL).replace(/\/$/, '') : '';
        const publicUrl = base ? `${base}/${key}` : `r2://${key}`;
        return jsonResponse(200, { url: publicUrl, fileUrl: publicUrl, publicUrl });
      }
      // file is missing or returned as string → fall through to binary-safe parser.
    } catch {
      // formData() threw → fall through to binary-safe parser.
    }

    // Fallback: binary-safe manual multipart parser.
    // CRITICAL: Never use TextDecoder on the request body — that decodes binary
    // image bytes as UTF-8 text and replaces every invalid byte (e.g. PNG's 0x89
    // magic byte) with U+FFFD, irrecoverably corrupting the file. We operate
    // entirely on Uint8Array: headers are decoded as latin1 (lossless for all
    // 256 byte values), and the file body is sliced directly as bytes.
    return await parseMultipartBinary(requestClone, env, ct, safeDir);
  }

  // Raw body upload (no multipart) — treat entire body as the file.
  const file = await request.arrayBuffer();
  if (!file || file.byteLength === 0) {
    return jsonResponse(400, { error: 'Empty request body' });
  }
  const dispo = request.headers.get('content-disposition') || '';
  const m = /filename="?([^";\s]+)"?/i.exec(dispo);
  const fileName = m ? m[1] : 'file';
  const fileType = request.headers.get('content-type') || 'application/octet-stream';
  const ext = fileName.split('.').pop().toLowerCase() || 'bin';
  const key = `${safeDir}/${nowSec()}-${genId('r2')}.${ext}`;
  await env.AI_BUCKET.put(key, file, {
    httpMetadata: { contentType: fileType },
  });
  const base = env.PUBLIC_BASE_URL ? String(env.PUBLIC_BASE_URL).replace(/\/$/, '') : '';
  const publicUrl = base ? `${base}/${key}` : `r2://${key}`;
  return jsonResponse(200, { url: publicUrl, fileUrl: publicUrl, publicUrl });
}

/**
 * Binary-safe multipart/form-data parser — fallback for environments where
 * request.formData() is unavailable or returns File fields as strings.
 *
 * Operates entirely on Uint8Array. Headers are decoded as latin1 (lossless for
 * byte values 0–255); the file body is sliced directly as bytes and never
 * passes through TextDecoder/TextEncoder (which would corrupt binary data).
 *
 * @param {Request} request   A fresh/unconsumed request (use request.clone()).
 * @param {object} env
 * @param {string} ct         The original Content-Type header value.
 * @returns {Promise<Response>}
 */
async function parseMultipartBinary(request, env, ct, safeDir) {
  const boundary = /boundary=(?:"([^"]+)"|([^;]+))/i.exec(ct);
  if (!boundary) return jsonResponse(400, { error: 'No boundary in content-type' });
  const bndStr = '--' + (boundary[1] || boundary[2]).trim();
  // Boundary strings are ASCII; TextEncoder is safe here (we never touch the body).
  const bndBytes = new TextEncoder().encode(bndStr);

  const rawBody = new Uint8Array(await request.arrayBuffer());

  // Locate every boundary occurrence in the byte stream.
  const positions = [];
  for (let i = 0; i + bndBytes.length <= rawBody.length; i++) {
    let match = true;
    for (let j = 0; j < bndBytes.length; j++) {
      if (rawBody[i + j] !== bndBytes[j]) { match = false; break; }
    }
    if (match) { positions.push(i); i += bndBytes.length - 1; }
  }
  if (positions.length < 2) {
    return jsonResponse(400, { error: 'Malformed multipart body' });
  }

  // Header/body separator: \r\n\r\n (0x0D 0x0A 0x0D 0x0A).
  const CRLFCRLF = [0x0D, 0x0A, 0x0D, 0x0A];
  const findSubarray = (arr, sub, start, end) => {
    for (let i = start; i + sub.length <= end; i++) {
      let match = true;
      for (let j = 0; j < sub.length; j++) {
        if (arr[i + j] !== sub[j]) { match = false; break; }
      }
      if (match) return i;
    }
    return -1;
  };

  // Decode a byte slice as latin1 (each byte → one char; lossless for all byte values).
  // Used ONLY for part headers, never for the file body.
  const decodeLatin1 = (arr, start, end) => {
    let s = '';
    for (let i = start; i < end; i++) s += String.fromCharCode(arr[i]);
    return s;
  };

  for (let p = 0; p + 1 < positions.length; p++) {
    // Part starts after the boundary marker and the trailing CRLF.
    let partStart = positions[p] + bndBytes.length;
    if (partStart + 1 < rawBody.length &&
        rawBody[partStart] === 0x0D && rawBody[partStart + 1] === 0x0A) {
      partStart += 2;
    }
    // Part ends at the next boundary, minus the trailing CRLF that precedes it.
    let partEnd = positions[p + 1];
    if (partEnd >= 2 &&
        rawBody[partEnd - 2] === 0x0D && rawBody[partEnd - 1] === 0x0A) {
      partEnd -= 2;
    }
    if (partEnd <= partStart) continue;

    const sepPos = findSubarray(rawBody, CRLFCRLF, partStart, partEnd);
    if (sepPos === -1) continue;

    const headerStr = decodeLatin1(rawBody, partStart, sepPos);
    const bodyStart = sepPos + CRLFCRLF.length;
    const bodyEnd = partEnd;

    const cdMatch = /Content-Disposition: form-data;[^]*?name="([^"]+)"(?:;\s*filename="([^"]*)")?/i.exec(headerStr);
    if (!cdMatch || cdMatch[1] !== 'file') continue;
    const fileName = cdMatch[2] || 'file';
    const ctMatch = /Content-Type:\s*([^\r\n]+)/i.exec(headerStr);
    const fileType = ctMatch ? ctMatch[1].trim() : 'application/octet-stream';

    // Slice body bytes directly — NO text-encoding conversion.
    // .slice() on Uint8Array returns a new Uint8Array copy; R2 accepts it as-is.
    const fileBytes = rawBody.slice(bodyStart, bodyEnd);

    const ext = fileName.split('.').pop().toLowerCase() || 'bin';
    const key = `${safeDir}/${nowSec()}-${genId('r2')}.${ext}`;
    await env.AI_BUCKET.put(key, fileBytes, {
      httpMetadata: { contentType: fileType },
    });
    const base = env.PUBLIC_BASE_URL ? String(env.PUBLIC_BASE_URL).replace(/\/$/, '') : '';
    const publicUrl = base ? `${base}/${key}` : `r2://${key}`;
    return jsonResponse(200, { url: publicUrl, fileUrl: publicUrl, publicUrl });
  }

  return jsonResponse(400, { error: 'No "file" field found in multipart data' });
}

/**
 * Handle GET /tasks — returns active tasks plus the most recent 20 completed.
 * @param {object} env
 * @returns {Promise<Response>}
 */
async function handleListTasks(env) {
  const activeIds = await getIndex(env, ACTIVE_INDEX_KEY);
  const recentIds = (await getIndex(env, RECENT_INDEX_KEY)).slice(0, RECENT_RETURN);

  const [active, recent] = await Promise.all([
    Promise.all(activeIds.map((id) => getTask(env, id))),
    Promise.all(recentIds.map((id) => getTask(env, id))),
  ]);

  return jsonResponse(200, {
    active: active.filter(Boolean),
    recent: recent.filter(Boolean),
  });
}

/**
 * Handle GET /task/<id>.
 * @param {object} env
 * @param {string} id
 * @returns {Promise<Response>}
 */
async function handleGetTask(env, id) {
  const task = await getTask(env, id);
  if (!task) return jsonResponse(404, { error: 'Task not found' });
  return jsonResponse(200, { task });
}

/**
 * Handle POST /task/<id>/cancel.
 * Marks the task (and any active child task) as canceled and moves to recent.
 * @param {object} env
 * @param {string} id
 * @returns {Promise<Response>}
 */
async function handleCancelTask(env, id) {
  const task = await getTask(env, id);
  if (!task) return jsonResponse(404, { error: 'Task not found' });

  task.status = 'canceled';
  task.finishedAt = nowSec();
  await putTask(env, task);

  // For pipelines / chains, cancel the currently-running child task as well.
  if (task.kind === 'pipeline') {
    const step = task.steps[task.current - 1];
    if (step && step.taskId) {
      const child = await getTask(env, step.taskId);
      if (child && (child.status === 'queued' || child.status === 'running')) {
        child.status = 'canceled';
        child.finishedAt = nowSec();
        await putTask(env, child);
        await moveToRecent(env, child.id);
      }
    }
  } else if (task.kind === 'chain') {
    const seg = task.segments[task.current - 1];
    if (seg && seg.taskId) {
      const child = await getTask(env, seg.taskId);
      if (child && (child.status === 'queued' || child.status === 'running')) {
        child.status = 'canceled';
        child.finishedAt = nowSec();
        await putTask(env, child);
        await moveToRecent(env, child.id);
      }
    }
  }

  await moveToRecent(env, id);
  return jsonResponse(200, { task });
}

/**
 * Handle DELETE /task/<id>.
 * Hard-deletes the task record and removes it from both index keys.
 * @param {object} env
 * @param {string} id
 * @returns {Promise<Response>}
 */
async function handleDeleteTask(env, id) {
  const task = await getTask(env, id);
  if (!task) return jsonResponse(404, { error: 'Task not found' });
  await env.TASKS.delete(`task:${id}`);
  await removeFromIndex(env, ACTIVE_INDEX_KEY, id);
  await removeFromIndex(env, RECENT_INDEX_KEY, id);
  return jsonResponse(200, { ok: true });
}

/**
 * Handle POST /task/<id>/retry.
 *   - single: re-queue the failed task with attempts reset.
 *   - pipeline: restart from failedStep (prior steps' resultUrls preserved).
 *   - chain: restart from ?seq=N (or current), resetting that segment and all later ones.
 * @param {object} env
 * @param {string} id
 * @param {URL} url
 * @returns {Promise<Response>}
 */
async function handleRetryTask(env, id, url) {
  const task = await getTask(env, id);
  if (!task) return jsonResponse(404, { error: 'Task not found' });

  if (task.kind === 'pipeline') {
    if (task.status !== 'failed' || !task.failedStep) {
      return jsonResponse(400, { error: 'Pipeline is not in a retryable state' });
    }
    const seq = task.failedStep;
    const step = task.steps[seq - 1];
    step.status = 'pending';
    step.taskId = null;
    step.resultUrl = null;
    step.error = null;
    task.status = 'running';
    task.failedStep = null;
    task.current = seq;
    task.finishedAt = 0;
    await putTask(env, task);
    await addToIndex(env, ACTIVE_INDEX_KEY, task.id);
    await startPipelineStep(env, task, seq);
    return jsonResponse(200, { task: await getTask(env, id) });
  }

  if (task.kind === 'chain') {
    const seqParam = url.searchParams.get('seq');
    const seq = seqParam ? parseInt(seqParam, 10) : task.current;
    if (!Number.isInteger(seq) || seq < 1 || seq > task.segments.length) {
      return jsonResponse(400, { error: 'Invalid seq parameter' });
    }
    for (let i = seq - 1; i < task.segments.length; i++) {
      task.segments[i].status = 'pending';
      task.segments[i].taskId = null;
      task.segments[i].resultUrl = null;
      task.segments[i].tailFrameUrl = null;
      task.segments[i].error = null;
    }
    task.current = seq;
    task.status = 'running';
    task.finishedAt = 0;
    await putTask(env, task);
    await addToIndex(env, ACTIVE_INDEX_KEY, task.id);
    await startChainSegment(env, task, seq);
    return jsonResponse(200, { task: await getTask(env, id) });
  }

  // single
  if (task.status !== 'failed') {
    return jsonResponse(400, { error: 'Task is not failed' });
  }
  task.status = 'queued';
  task.error = null;
  task.apiId = null;
  task.attempts = 0;
  task.notBefore = 0;
  task.finishedAt = 0;
  await putTask(env, task);
  await addToIndex(env, ACTIVE_INDEX_KEY, task.id);
  // If no priority, forward immediately (Phase 2 semantics).
  if (!task.priority) {
    await forwardTaskToSpeedx(env, task);
  }
  return jsonResponse(200, { task: await getTask(env, id) });
}

/**
 * Handle POST /chain/<id>/advance.
 * Frontend-driven chain advancement: client extracts the prior segment's tail
 * frame (browser canvas), uploads it to R2, then posts { tailFrameUrl } here.
 * The worker stores the tailFrameUrl and dispatches the next segment.
 *
 * No-op if the current segment is not yet succeeded, or if this is the last segment.
 * @param {object} env
 * @param {string} id
 * @param {Request} request
 * @returns {Promise<Response>}
 */
async function handleAdvanceChain(env, id, request) {
  const chain = await getTask(env, id);
  if (!chain || chain.kind !== 'chain') {
    return jsonResponse(404, { error: 'Chain not found' });
  }

  let body = {};
  try { body = await request.json(); } catch { body = {}; }

  const seg = chain.segments[chain.current - 1];
  if (!seg) return jsonResponse(400, { error: 'No current segment' });

  if (typeof body.tailFrameUrl === 'string' && body.tailFrameUrl) {
    seg.tailFrameUrl = fixUrl(body.tailFrameUrl);
  }

  const canAdvance =
    seg.status === 'succeeded' &&
    seg.tailFrameUrl &&
    chain.current < chain.segments.length;

  if (canAdvance) {
    // startChainSegment mutates chain and persists it.
    await startChainSegment(env, chain, chain.current + 1);
  } else {
    await putTask(env, chain);
  }

  return jsonResponse(200, { task: await getTask(env, id) });
}

/**
 * Handle POST /task — dispatch by payload.kind to single/pipeline/chain submit.
 * @param {Request} request
 * @param {object} env
 * @returns {Promise<Response>}
 */
async function handleSubmitTask(request, env) {
  let payload;
  try {
    payload = await request.json();
  } catch {
    return jsonResponse(400, { error: 'Invalid JSON body' });
  }

  let result;
  if (payload && payload.kind === 'pipeline') {
    result = await submitPipeline(env, payload);
  } else if (payload && payload.kind === 'chain') {
    result = await submitChain(env, payload);
  } else {
    result = await submitSingleTask(env, payload);
  }

  if (!result.ok) {
    return jsonResponse(result.status || 500, { error: result.error });
  }
  return jsonResponse(200, { task: result.task });
}

// ============================================================
// Userdata sync (历史记录同步) — R2-backed JSON key/value store
// ============================================================

/**
 * Allowed keys for /userdata — alphanumeric only, max 64 chars.
 * Prevents path traversal (no slashes, dots, or special chars).
 */
const USERDATA_KEY_RE = /^[a-zA-Z0-9]{1,64}$/;

/**
 * Handle GET /userdata?key=<key>&token=xxx
 * Reads R2 object "userdata/<key>.json" and returns the parsed JSON.
 * Returns { ok: true, data: null } when the object does not exist (treat
 * missing-key as empty history — the frontend distinguishes via `data`).
 * @param {object} env
 * @param {string} key
 * @returns {Promise<Response>}
 */
async function handleUserdataGet(env, key) {
  if (!env.AI_BUCKET) {
    return jsonResponse(500, { error: 'AI_BUCKET not bound' });
  }
  if (!USERDATA_KEY_RE.test(key)) {
    return jsonResponse(400, { error: 'Invalid key (alphanumeric, 1-64 chars)' });
  }
  const obj = await env.AI_BUCKET.get('userdata/' + key + '.json');
  if (!obj) {
    return jsonResponse(200, { ok: true, data: null });
  }
  const text = await obj.text();
  let data = null;
  try { data = JSON.parse(text); } catch { data = null; }
  return jsonResponse(200, { ok: true, data });
}

/**
 * Handle PUT /userdata?key=<key>&token=xxx
 * Writes the request body (raw JSON) to R2 object "userdata/<key>.json".
 * Body is validated as parseable JSON before storing — refuses garbage so
 * the GET path always returns valid JSON or null.
 * @param {Request} request
 * @param {object} env
 * @param {string} key
 * @returns {Promise<Response>}
 */
async function handleUserdataPut(request, env, key) {
  if (!env.AI_BUCKET) {
    return jsonResponse(500, { error: 'AI_BUCKET not bound' });
  }
  if (!USERDATA_KEY_RE.test(key)) {
    return jsonResponse(400, { error: 'Invalid key (alphanumeric, 1-64 chars)' });
  }
  const body = await request.text();
  try { JSON.parse(body); } catch {
    return jsonResponse(400, { error: 'Body must be valid JSON' });
  }
  await env.AI_BUCKET.put('userdata/' + key + '.json', body, {
    httpMetadata: { contentType: 'application/json; charset=utf-8' },
  });
  return jsonResponse(200, { ok: true });
}

// ============================================================
// Result archive (结果转存) — download upstream result → R2 results/
// ============================================================

/**
 * Allowed file extensions for /archive, mapped to their Content-Type.
 * Used both for validation and for setting R2 httpMetadata.
 */
const ARCHIVE_EXT_CT = {
  png: 'image/png',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  webp: 'image/webp',
  mp4: 'video/mp4',
  mp3: 'audio/mpeg',
  wav: 'audio/wav',
};

/** Download timeout for /archive subrequests (ms). */
const ARCHIVE_TIMEOUT_MS = 30000;

/**
 * Handle POST /archive?token=xxx&url=<encoded URL>&ext=png
 *
 * Downloads the upstream result URL (速创 / aliyuncs OSS) with a 30s
 * timeout, stores the response body to R2 under results/<ts>-<rand>.<ext>,
 * and returns the public R2 URL. On download failure returns 502 with
 * { ok: false, error }.
 * @param {Request} request
 * @param {object} env
 * @param {string} srcUrl
 * @param {string} ext
 * @returns {Promise<Response>}
 */
async function handleArchive(request, env, srcUrl, ext) {
  if (!env.AI_BUCKET) {
    return jsonResponse(500, { error: 'AI_BUCKET not bound' });
  }
  if (!isUrl(srcUrl)) {
    return jsonResponse(400, { error: 'Invalid url parameter' });
  }
  // V26.9.12: normalize URL to avoid "Invalid URL string" in Workers fetch
  try { srcUrl = new URL(srcUrl).href; } catch (e) {
    return jsonResponse(400, { ok: false, error: 'Invalid url parameter' });
  }
  const normExt = (ext || '').toLowerCase();
  if (!ARCHIVE_EXT_CT[normExt]) {
    return jsonResponse(400, {
      error: 'Invalid or unsupported ext (allowed: png/jpg/jpeg/webp/mp4/mp3/wav)',
    });
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), ARCHIVE_TIMEOUT_MS);
  let upstream;
  try {
    upstream = await globalThis.fetch(srcUrl, { signal: controller.signal });
  } catch (e) {
    return jsonResponse(502, {
      ok: false,
      error: 'Download failed: ' + (e?.message || 'network error'),
    });
  } finally {
    clearTimeout(timeoutId);
  }
  if (!upstream || !upstream.ok) {
    const status = upstream ? upstream.status : 0;
    return jsonResponse(502, { ok: false, error: 'Upstream returned status ' + status });
  }

  const filename = `${nowSec()}-${genId('r2')}.${normExt}`;
  const r2Key = `results/${filename}`;
  try {
    // Stream the upstream body directly into R2 — avoids buffering large
    // media files (video/audio) in Worker memory.
    await env.AI_BUCKET.put(r2Key, upstream.body, {
      httpMetadata: { contentType: ARCHIVE_EXT_CT[normExt] },
    });
  } catch (e) {
    return jsonResponse(502, {
      ok: false,
      error: 'R2 store failed: ' + (e?.message || 'unknown'),
    });
  }

  const base = env.PUBLIC_BASE_URL ? String(env.PUBLIC_BASE_URL).replace(/\/$/, '') : '';
  const publicUrl = base ? `${base}/${r2Key}` : `r2://${r2Key}`;
  return jsonResponse(200, { ok: true, url: publicUrl });
}

// ============================================================
// Main fetch router
// ============================================================

/**
 * Worker fetch entry point.
 * @param {Request} request
 * @param {object} env
 * @param {ExecutionContext} ctx
 * @returns {Promise<Response>}
 */
async function fetch(request, env, ctx) {
  const url = new URL(request.url);

  // CORS preflight — respond immediately with CORS headers, no auth.
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: {
        'access-control-allow-origin': '*',
        'access-control-allow-methods': 'GET, POST, PUT, DELETE, OPTIONS',
        'access-control-allow-headers': 'Content-Type, Authorization',
        'access-control-max-age': '86400',
      },
    });
  }

  // Public health check — no auth required.
  if (url.pathname === '/' || url.pathname === '/health') {
    return jsonResponse(200, {
      ok: true,
      service: 'ai-media-proxy',
      time: nowSec(),
    });
  }

  // All other endpoints require ?token= matching AUTH_TOKEN.
  const auth = requireAuth(url, env);
  if (!auth.ok) return auth.response;

  // R2 upload (graceful degradation: works even if TASKS KV is unbound).
  if (url.pathname === '/upload' && request.method === 'POST') {
    try {
      // V26.9.11: pass dir param for subdirectory (e.g. thumb/)
      const dir = url.searchParams.get('dir') || 'uploads';
      return await handleUpload(request, env, dir);
    } catch (e) {
      return jsonResponse(500, { error: 'Upload failed: ' + (e?.message || 'unknown') });
    }
  }

  // Task Center
  if (url.pathname === '/task' && request.method === 'POST') {
    return await handleSubmitTask(request, env);
  }

  if (url.pathname === '/tasks' && request.method === 'GET') {
    return await handleListTasks(env);
  }

  // /task/<id>
  const taskMatch = url.pathname.match(/^\/task\/([^/]+)$/);
  if (taskMatch) {
    const id = taskMatch[1];
    if (request.method === 'GET') return await handleGetTask(env, id);
    if (request.method === 'DELETE') return await handleDeleteTask(env, id);
  }

  // /task/<id>/cancel
  const cancelMatch = url.pathname.match(/^\/task\/([^/]+)\/cancel$/);
  if (cancelMatch && request.method === 'POST') {
    return await handleCancelTask(env, cancelMatch[1]);
  }

  // /task/<id>/retry
  const retryMatch = url.pathname.match(/^\/task\/([^/]+)\/retry$/);
  if (retryMatch && request.method === 'POST') {
    return await handleRetryTask(env, retryMatch[1], url);
  }

  // /chain/<id>/advance
  const advanceMatch = url.pathname.match(/^\/chain\/([^/]+)\/advance$/);
  if (advanceMatch && request.method === 'POST') {
    return await handleAdvanceChain(env, advanceMatch[1], request);
  }

  // Userdata sync — R2-backed JSON key/value store (history sync).
  if (url.pathname === '/userdata' && request.method === 'GET') {
    return await handleUserdataGet(env, url.searchParams.get('key') || '');
  }
  if (url.pathname === '/userdata' && request.method === 'PUT') {
    return await handleUserdataPut(request, env, url.searchParams.get('key') || '');
  }

  // Result archive — download upstream result and store to R2 results/.
  if (url.pathname === '/archive' && request.method === 'POST') {
    return await handleArchive(
      request,
      env,
      url.searchParams.get('url') || '',
      url.searchParams.get('ext') || '',
    );
  }

  return jsonResponse(404, { error: 'Not found', path: url.pathname });
}

// ============================================================
// Scheduled (cron) handler
// ============================================================

/**
 * Cron entry point. Runs every minute via wrangler.toml triggers.crons.
 * Delegates all work to processScheduled via controller.waitUntil.
 * @param {ScheduledController} controller
 * @param {object} env
 * @param {ExecutionContext} ctx
 */
async function scheduled(controller, env, ctx) {
  ctx.waitUntil(processScheduled(env));
}

/**
 * Main scheduled work — runs in four phases:
 *   1. Poll all "running" single tasks for completion (Speedx detail API).
 *   2. Advance pipelines (submit next step on success, fail on child failure).
 *   3. Update chains (mark segment succeeded/failed; advancement is frontend-driven).
 *   4. Process queued tasks under concurrency caps (priority-sorted, notBefore-gated).
 *
 * Each phase tolerates partial failure: a single task error doesn't abort the rest.
 * @param {object} env
 */
async function processScheduled(env) {
  if (!env.TASKS || !env.SPEEDX_KEY) return;

  const activeIds = await getIndex(env, ACTIVE_INDEX_KEY);
  const tasks = (await Promise.all(activeIds.map((id) => getTask(env, id)))).filter(Boolean);

  const singles = tasks.filter((t) => t.kind === 'single');
  const pipelines = tasks.filter((t) => t.kind === 'pipeline');
  const chains = tasks.filter((t) => t.kind === 'chain');

  // Phase 1: poll running single tasks.
  await Promise.all(singles.map((t) => pollSingleTask(env, t).catch(() => null)));

  // Phase 2: advance pipelines.
  for (const p of pipelines) {
    if (p.status !== 'running') continue;
    try { await advancePipeline(env, p); } catch { /* skip on error */ }
  }

  // Phase 3: update chains.
  for (const c of chains) {
    if (c.status !== 'running') continue;
    try { await updateChain(env, c); } catch { /* skip on error */ }
  }

  // Phase 4: re-read active list (polling may have completed some tasks)
  // and dispatch queued tasks under concurrency caps.
  const freshIds = await getIndex(env, ACTIVE_INDEX_KEY);
  const freshTasks = (await Promise.all(freshIds.map((id) => getTask(env, id)))).filter(Boolean);
  await processQueuedTasks(env, freshTasks);
}

/**
 * Poll a single running task: call Speedx detail, update status on completion.
 *
 * Stale-check: tasks running > STALE_RUNNING_SEC are marked failed (timeout).
 * Throttled: skips polling if updated < POLL_MIN_GAP_SEC ago.
 * Conflict-safe: uses putTaskIfFresh to avoid clobbering concurrent writes.
 *
 * @param {object} env
 * @param {object} task
 */
async function pollSingleTask(env, task) {
  if (task.status !== 'running' || !task.apiId) return;

  const elapsed = nowSec() - task.createdAt;
  if (elapsed > STALE_RUNNING_SEC) {
    task.status = 'failed';
    task.error = 'timeout';
    task.finishedAt = nowSec();
    if (await putTaskIfFresh(env, task)) {
      await moveToRecent(env, task.id);
    }
    return;
  }

  if (task.updatedAt && nowSec() - task.updatedAt < POLL_MIN_GAP_SEC) return;

  const detail = await getDetailFromSpeedx(env, task.apiId);
  if (!detail.ok || !detail.data) return;

  // The data field may be a JSON string or an object.
  const innerRaw = detail.data.data;
  const inner = tryParseJson(innerRaw);
  if (!inner || typeof inner !== 'object') return;

  const mapped = mapApiStatus(inner.status);
  if (mapped === 'running') return;

  if (mapped === 'succeeded') {
    const url = extractUrl(detail.data);
    task.status = 'succeeded';
    task.resultUrl = url;
    task.error = null;
    task.finishedAt = nowSec();
    if (await putTaskIfFresh(env, task)) {
      await moveToRecent(env, task.id);
    }
    return;
  }

  // mapped === 'failed'
  const msg = inner.message || detail.data.msg || 'upstream failed';
  if (isRetryableError(detail.status, msg) && task.attempts < MAX_ATTEMPTS) {
    task.attempts += 1;
    task.status = 'queued';
    task.apiId = null;
    task.notBefore = nowSec() + RETRY_DELAY_SEC;
    task.error = msg;
    await putTaskIfFresh(env, task);
    // stays in idx:active for cron to retry
  } else {
    task.status = 'failed';
    task.error = msg;
    task.finishedAt = nowSec();
    if (await putTaskIfFresh(env, task)) {
      await moveToRecent(env, task.id);
    }
  }
}

/**
 * Advance a pipeline based on the current step's child task status.
 *
 *   step pending  → start the step (creates child single task)
 *   step running  → no-op (pollSingleTask will update the child)
 *   step succeeded → set step.resultUrl, advance to next step (or mark pipeline done)
 *   step failed    → mark pipeline failed with failedStep
 *
 * @param {object} env
 * @param {object} pipeline
 */
async function advancePipeline(env, pipeline) {
  const step = pipeline.steps[pipeline.current - 1];
  if (!step) return;

  if (step.status === 'pending' && !step.taskId) {
    await startPipelineStep(env, pipeline, pipeline.current);
    return;
  }

  if (step.status === 'succeeded') {
    // Already advanced — no-op.
    return;
  }

  if (step.status === 'failed') {
    pipeline.status = 'failed';
    pipeline.failedStep = pipeline.current;
    pipeline.finishedAt = nowSec();
    await putTask(env, pipeline);
    await moveToRecent(env, pipeline.id);
    return;
  }

  if (!step.taskId) return;
  const child = await getTask(env, step.taskId);
  if (!child) return;

  if (child.status === 'succeeded') {
    step.status = 'succeeded';
    step.resultUrl = child.resultUrl;

    if (pipeline.current >= pipeline.steps.length) {
      pipeline.status = 'succeeded';
      pipeline.finishedAt = nowSec();
      await putTask(env, pipeline);
      await moveToRecent(env, pipeline.id);
      return;
    }

    // Fill next step's needs from this step's resultUrl.
    const nextStep = pipeline.steps[pipeline.current];
    for (const need of nextStep.needs) {
      nextStep.body[need] = step.resultUrl;
    }
    await putTask(env, pipeline);
    await startPipelineStep(env, pipeline, pipeline.current + 1);
    return;
  }

  if (child.status === 'failed') {
    step.status = 'failed';
    step.error = child.error;
    pipeline.status = 'failed';
    pipeline.failedStep = pipeline.current;
    pipeline.finishedAt = nowSec();
    await putTask(env, pipeline);
    await moveToRecent(env, pipeline.id);
  }
}

/**
 * Update a chain's current segment based on its child task status.
 *
 * Chain advancement to the next segment is frontend-driven
 * (POST /chain/<id>/advance with tailFrameUrl) — the cron only updates
 * segment status from the child task and marks the chain done/failed.
 *
 * @param {object} env
 * @param {object} chain
 */
async function updateChain(env, chain) {
  const seg = chain.segments[chain.current - 1];
  if (!seg) return;

  if (seg.status === 'succeeded') {
    // If this is the last segment, the chain is complete.
    if (chain.current >= chain.segments.length) {
      chain.status = 'succeeded';
      chain.finishedAt = nowSec();
      await putTask(env, chain);
      await moveToRecent(env, chain.id);
    }
    return;
  }

  if (seg.status === 'failed') {
    chain.status = 'failed';
    chain.finishedAt = nowSec();
    await putTask(env, chain);
    await moveToRecent(env, chain.id);
    return;
  }

  if (!seg.taskId) return;
  const child = await getTask(env, seg.taskId);
  if (!child) return;

  if (child.status === 'succeeded') {
    seg.status = 'succeeded';
    seg.resultUrl = child.resultUrl;
    await putTask(env, chain);
    // If this is the last segment, mark the chain done now.
    if (chain.current >= chain.segments.length) {
      chain.status = 'succeeded';
      chain.finishedAt = nowSec();
      await putTask(env, chain);
      await moveToRecent(env, chain.id);
    }
    return;
  }

  if (child.status === 'failed') {
    seg.status = 'failed';
    seg.error = child.error;
    chain.status = 'failed';
    chain.finishedAt = nowSec();
    await putTask(env, chain);
    await moveToRecent(env, chain.id);
  }
}

/**
 * Dispatch queued single tasks under per-modelType concurrency caps.
 *
 * Selection: kind=single, status=queued, notBefore<=now (or 0).
 * Ordering: priority desc (higher first), then createdAt asc (older first).
 *
 * For each task up to its type's concurrency cap, calls forwardTaskToSpeedx.
 * Failures during forward (retryable) leave the task queued with notBefore.
 *
 * @param {object} env
 * @param {object[]} tasks  All currently-active task records.
 */
async function processQueuedTasks(env, tasks) {
  const now = nowSec();

  // Count running single tasks per modelType.
  const runningByType = { image: 0, video: 0, audio: 0 };
  for (const t of tasks) {
    if (t.kind === 'single' && t.status === 'running') {
      runningByType[t.modelType] = (runningByType[t.modelType] || 0) + 1;
    }
  }

  const queued = tasks
    .filter((t) =>
      t.kind === 'single' &&
      t.status === 'queued' &&
      (!t.notBefore || t.notBefore <= now),
    )
    .sort((a, b) => (b.priority || 0) - (a.priority || 0) || a.createdAt - b.createdAt);

  for (const task of queued) {
    const cap = CONCURRENCY[task.modelType] || 1;
    if (runningByType[task.modelType] >= cap) continue;

    // Re-read the task to detect concurrent writes (e.g. user just canceled).
    const fresh = await getTask(env, task.id);
    if (!fresh || fresh.status !== 'queued') continue;
    if (fresh.notBefore && fresh.notBefore > now) continue;

    runningByType[task.modelType] += 1;
    await forwardTaskToSpeedx(env, fresh);

    // If forward left it running, the running count stays up. If it failed
    // and was moved to recent (non-retryable), decrement so the next queued
    // task of the same type can start this tick.
    const after = await getTask(env, fresh.id);
    if (!after || after.status !== 'running') {
      runningByType[task.modelType] -= 1;
    }
  }
}

// ============================================================
// Exports
// ============================================================

export default { fetch, scheduled };
