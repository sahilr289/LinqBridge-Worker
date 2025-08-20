// worker.cjs — LinqBridge Worker (final, CommonJS, hardened against HTTP 999)

// =========================
// Env & Config
// =========================
const API_BASE = process.env.API_BASE || "https://calm-rejoicing-linqbridge.up.railway.app";
const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET || "";
const HEADLESS = (/^(true|1|yes)$/i).test(process.env.HEADLESS || "true");
const SOFT_MODE = (/^(true|1|yes)$/i).test(process.env.SOFT_MODE || "true"); // default safe
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000", 10);

// Optionally load Playwright only when needed (saves cold-start in soft mode)
let chromium = null;

// =========================
// Small Utils
// =========================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
// Playwright helpers (anti-999 hardening)
// =========================

/**
 * Create a hardened browser context that looks like a real user:
 * - realistic UA/locale/timezone, sec-ch hints
 * - removes navigator.webdriver
 * - sets cookies (JSESSIONID quoted)
 * - blocks heavy/noisy resources
 */
async function createBrowserContext(cookieBundle, headless = true) {
  if (!chromium) {
    // Lazy-require to avoid ESM/require conflicts unless needed.
    ({ chromium } = require("playwright"));
  }

  // A recent, plausible desktop UA that matches Playwright’s Chromium
  const UA =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) " +
    "Chrome/124.0.0.0 Safari/537.36";

  const browser = await chromium.launch({
    headless,
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled",
      "--disable-features=IsolateOrigins,site-per-process",
      "--disable-gpu",
      "--disable-extensions",
      "--disable-background-networking",
    ],
  });

  const context = await browser.newContext({
    userAgent: UA,
    locale: "en-US",
    timezoneId: "America/Los_Angeles",
    colorScheme: "light",
    viewport: { width: 1366, height: 768 },
    deviceScaleFactor: 1,
    javaScriptEnabled: true,
    permissions: [],
    extraHTTPHeaders: {
      "accept-language": "en-US,en;q=0.9",
      "sec-ch-ua": '"Chromium";v="124", "Not:A-Brand";v="8"',
      "sec-ch-ua-platform": '"Windows"',
      "sec-ch-ua-mobile": "?0",
      "upgrade-insecure-requests": "1",
    },
  });

  // Remove obvious automation fingerprints
  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
  });

  // Set cookies (JSESSIONID must be quoted)
  const cookies = [];
  if (cookieBundle?.li_at) {
    cookies.push({
      name: "li_at",
      value: cookieBundle.li_at,
      domain: ".linkedin.com",
      path: "/",
      httpOnly: true,
      secure: true,
      sameSite: "Lax",
    });
  }
  if (cookieBundle?.jsessionid) {
    cookies.push({
      name: "JSESSIONID",
      value: `"${cookieBundle.jsessionid}"`,
      domain: ".linkedin.com",
      path: "/",
      httpOnly: true,
      secure: true,
      sameSite: "Lax",
    });
  }
  if (cookieBundle?.bcookie) {
    cookies.push({
      name: "bcookie",
      value: cookieBundle.bcookie,
      domain: ".linkedin.com",
      path: "/",
      httpOnly: false,
      secure: true,
      sameSite: "Lax",
    });
  }
  if (cookies.length) await context.addCookies(cookies);

  // Block heavy/noisy resources to reduce risk & speed up
  await context.route("**/*", (route) => {
    const req = route.request();
    const type = req.resourceType();
    if (type === "image" || type === "media" || type === "font") return route.abort();

    const url = req.url();
    if (/\.(png|jpg|jpeg|gif|webp|svg)(\?|$)/i.test(url)) return route.abort();
    if (/doubleclick|google-analytics|adservice|facebook|hotjar|segment/.test(url)) return route.abort();

    return route.continue();
  });

  const page = await context.newPage();
  page.setDefaultTimeout(20000);
  page.setDefaultNavigationTimeout(30000);

  return { browser, context, page };
}

/**
 * Navigate to a LinkedIn profile with retries and fallbacks to avoid 999/authwall:
 * - Appends a benign query param
 * - Falls back to m.linkedin.com profile if needed
 * - Soft refresh via /feed between attempts
 */
async function navigateLinkedInWithRetries(page, rawUrl, { attempts = 3 } = {}) {
  const addParam = (u) => {
    try {
      const url = new URL(u);
      url.searchParams.set("trk", "public_profile_nav");
      return url.toString();
    } catch {
      return rawUrl;
    }
  };

  const urlCandidates = [
    addParam(rawUrl),
    rawUrl.replace("www.linkedin.com/in/", "m.linkedin.com/in/"), // mobile fallback
  ];

  let lastErr;
  for (let i = 0; i < attempts; i++) {
    const target = urlCandidates[Math.min(i, urlCandidates.length - 1)];
    try {
      const resp = await page.goto(target, {
        waitUntil: "domcontentloaded",
        timeout: 25000,
      });

      const status = resp ? resp.status() : null;

      if (status && status >= 200 && status < 400) {
        const title = (await page.title().catch(() => ""))?.toLowerCase?.() || "";
        if (title.includes("sign in") || title.includes("authwall")) {
          throw new Error("Hit auth wall");
        }
        return; // success
      }

      throw new Error(`Nav bad status ${status || "none"}`);
    } catch (e) {
      lastErr = e;
      // jittered backoff
      await page.waitForTimeout(1200 + Math.random() * 800);
      // Soft refresh to reset server heuristics
      try {
        await page.goto("https://www.linkedin.com/feed/", {
          waitUntil: "domcontentloaded",
          timeout: 15000,
        });
        await page.waitForTimeout(800);
      } catch {}
    }
  }
  throw new Error(`Navigation failed after retries: ${lastErr?.message || lastErr}`);
}

// =========================
/**
 * Process a single SEND_CONNECTION job.
 * In SOFT_MODE: we do not open a browser—just pretend success and return a result.
 * In REAL mode: launch Chromium (Playwright), set cookies, visit page (hardened), and return a result.
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

  // Real mode: hardened context + navigation with retries/fallbacks
  let browser;
  let context;
  let page;

  try {
    ({ browser, context, page } = await createBrowserContext(cookieBundle, HEADLESS));

    // Optional warm-up to stabilize session before hitting profile
    try {
      await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 20000 });
      await page.waitForTimeout(1200);
    } catch {}

    await navigateLinkedInWithRetries(page, profileUrl, { attempts: 3 });

    await sleep(1500); // small human-ish pause

    const status = page.response()?.status?.() ?? null; // may be undefined if Playwright doesn't keep last resp
    const result = {
      mode: "real",
      profileUrl,
      noteUsed: note,
      httpStatus: status,
      pageTitle: await page.title().catch(() => null),
      message: "Visited profile in real mode.",
      at: new Date().toISOString(),
    };

    await browser.close().catch(() => {});
    return result;
  } catch (e) {
    try {
      await browser?.close();
    } catch {}
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
  console.log(
    `[worker] starting. API_BASE=${API_BASE} Headless: ${HEADLESS} Soft mode: ${SOFT_MODE}`
  );
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
mainLoop().catch((e) => {
  console.error("[worker] fatal:", e);
  process.exitCode = 1;
});
