// worker.js — LinqBridge Worker (final, CommonJS)

// =========================
// Env & Config
// =========================
const API_BASE = process.env.API_BASE || "https://calm-rejoicing-linqbridge.up.railway.app";
const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET || "";
const HEADLESS = (/^(true|1|yes)$/i).test(process.env.HEADLESS || "true");
const SOFT_MODE = (/^(true|1|yes)$/i).test(process.env.SOFT_MODE || "true");  // default safe
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000", 10);

// Optionally load Playwright only when needed (saves cold-start in soft mode)
let chromium = null;

// =========================
// Small Utils
// =========================
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function logFetchError(where, err) {
  const code = err?.cause?.code || err?.code || "unknown";
  console.error(`[worker] ${where} fetch failed:`, code, err?.message || err);
}

function apiUrl(path) {
  return path.startsWith("/") ? `${API_BASE}${path}` : `${API_BASE}/${path}`;
}

async function apiGet(path) {
  const res = await fetch(apiUrl(path), {
    method: "GET",
    headers: { "x-worker-secret": WORKER_SHARED_SECRET },
    signal: AbortSignal.timeout ? AbortSignal.timeout(15000) : undefined,
  });
  const text = await res.text();
  try {
    const json = JSON.parse(text);
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`);
    return json;
  } catch (e) {
    throw new Error(`GET ${path} non-JSON or error ${res.status}: ${text}`);
  }
}

async function apiPost(path, body) {
  const res = await fetch(apiUrl(path), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-worker-secret": WORKER_SHARED_SECRET,
    },
    body: JSON.stringify(body || {}),
    signal: AbortSignal.timeout ? AbortSignal.timeout(20000) : undefined,
  });
  const text = await res.text();
  try {
    const json = JSON.parse(text);
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`);
    return json;
  } catch (e) {
    throw new Error(`POST ${path} non-JSON or error ${res.status}: ${text}`);
  }
}

async function apiPostNoSecret(path, body) {
  // Use this only for public endpoints if ever needed.
  const res = await fetch(apiUrl(path), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
    signal: AbortSignal.timeout ? AbortSignal.timeout(20000) : undefined,
  });
  const text = await res.text();
  try {
    const json = JSON.parse(text);
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`);
    return json;
  } catch (e) {
    throw new Error(`POST ${path} non-JSON or error ${res.status}: ${text}`);
  }
}

// =========================
/**
 * Process a single SEND_CONNECTION job.
 * In SOFT_MODE: we do not open a browser—just pretend success and return a result.
 * In REAL mode: launch Chromium (Playwright), set cookies, visit page, and return a result.
 */
// =========================
async function handleSendConnection(job) {
  const { payload } = job || {};
  if (!payload) throw new Error("Job has no payload");
  const profileUrl = payload.profileUrl;
  const note = payload.note || null;
  const cookieBundle = payload.cookieBundle || {};

  if (!profileUrl) throw new Error("payload.profileUrl required");

  // Soft mode: simulate success and return
  if (SOFT_MODE) {
    return {
      mode: "soft",
      profileUrl,
      noteUsed: note,
      message: "Soft mode success (no browser launched).",
      at: new Date().toISOString(),
    };
  }

  // Real mode: launch Playwright Chromium and visit the profile
  try {
    if (!chromium) {
      // Lazy-require to avoid ESM/require conflicts unless needed.
      ({ chromium } = require("playwright"));
    }

    const browser = await chromium.launch({
      headless: HEADLESS,
      args: [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-extensions",
        "--disable-background-networking",
      ],
    });

    const context = await browser.newContext({
      userAgent: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    });

    // Inject cookies if provided
    const cookiesToSet = [];
    if (cookieBundle.li_at) {
      cookiesToSet.push({ name: "li_at", value: cookieBundle.li_at, domain: ".linkedin.com", path: "/", httpOnly: true, secure: true });
    }
    if (cookieBundle.jsessionid) {
      // LinkedIn wraps JSESSIONID in quotes usually
      cookiesToSet.push({ name: "JSESSIONID", value: `"${cookieBundle.jsessionid}"`, domain: ".linkedin.com", path: "/", httpOnly: true, secure: true });
    }
    if (cookieBundle.bcookie) {
      cookiesToSet.push({ name: "bcookie", value: cookieBundle.bcookie, domain: ".linkedin.com", path: "/", httpOnly: false, secure: true });
    }
    if (cookiesToSet.length) {
      await context.addCookies(cookiesToSet);
    }

    const page = await context.newPage();
    page.setDefaultTimeout(20000);
    page.setDefaultNavigationTimeout(30000);

    // Go to the profile URL
    const resp = await page.goto(profileUrl, { waitUntil: "domcontentloaded" });
    const status = resp ? resp.status() : 0;

    // Heuristic: if we got a login wall or 403, surface it
    if (!resp || status >= 400) {
      const title = await page.title().catch(() => "");
      await browser.close();
      throw new Error(`Navigation failed (HTTP ${status}) title="${title}"`);
    }

    // NOTE: Implementing the actual click flow to send the invite is brittle and UI-dependent.
    // For now we report a "viewed" / "attempted" result. You can extend this to click
    // the “Connect” button and fill the note based on your UI selectors.
    await sleep(1500); // small human-ish pause

    const result = {
      mode: "real",
      profileUrl,
      noteUsed: note,
      httpStatus: status,
      pageTitle: await page.title().catch(() => null),
      message: "Visited profile in real mode.",
      at: new Date().toISOString(),
    };

    await browser.close();
    return result;
  } catch (e) {
    // Let the caller fail the job
    throw new Error(`REAL mode failed: ${e.message}`);
  }
}

// =========================
// Job Loop
// =========================
async function processOne() {
  // Get next job for this worker
  let next;
  try {
    next = await apiPost("/jobs/next", { types: ["SEND_CONNECTION"] });
  } catch (e) {
    logFetchError("jobs/next", e);
    return; // try again next tick
  }

  const job = next?.job;
  if (!job) {
    // no jobs; nothing to do
    return;
  }

  try {
    let result = null;

    switch (job.type) {
      case "SEND_CONNECTION":
        result = await handleSendConnection(job);
        break;

      default:
        result = { note: `Unhandled job type: ${job.type}` };
        break;
    }

    // Complete the job
    try {
      await apiPost(`/jobs/${job.id}/complete`, { result });
      console.log(`[worker] Job ${job.id} done:`, result?.message || result);
    } catch (e) {
      logFetchError(`jobs/${job.id}/complete`, e);
    }
  } catch (e) {
    console.error(`[worker] Job ${job.id} failed:`, e.message);
    try {
      await apiPost(`/jobs/${job.id}/fail`, {
        error: e.message,
        requeue: false,
        delayMs: 0,
      });
    } catch (e2) {
      logFetchError(`jobs/${job.id}/fail`, e2);
    }
  }
}

async function mainLoop() {
  console.log(`[worker] starting. API_BASE=${API_BASE} Headless: ${HEADLESS} Soft mode: ${SOFT_MODE}`);
  if (!WORKER_SHARED_SECRET) {
    console.error("[worker] ERROR: WORKER_SHARED_SECRET is empty. Set it on both backend and worker!");
  }

  // Quick connectivity self-test
  try {
    const stats = await apiGet("/jobs/stats");
    console.log("[worker] API OK. Stats:", stats?.counts || stats);
  } catch (e) {
    logFetchError("jobs/stats (startup)", e);
  }

  // Poll loop
  while (true) {
    try {
      await processOne();
    } catch (e) {
      // Catch-all — should not normally happen
      console.error("[worker] loop error:", e.message || e);
    }
    await sleep(POLL_INTERVAL_MS);
  }
}

// Start
mainLoop().catch(e => {
  console.error("[worker] fatal:", e);
  process.exitCode = 1;
});
