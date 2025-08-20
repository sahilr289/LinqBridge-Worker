// worker.js — LinqBridge Worker (Playwright + Job Runner)
// Run with: node worker.js
// Env required:
//   API_BASE, WORKER_SHARED_SECRET
//   HEADLESS=true|false, SOFT_MODE=true|false
// Optional:
//   POLL_INTERVAL_MS=5000, JOB_TYPES=SEND_CONNECTION

const { chromium } = require("playwright"); // browsers must be installed in image
const fetch = require("node-fetch");        // Node18+ has fetch; keep for portability

// ---------- Config ----------
const API_BASE = process.env.API_BASE || "http://localhost:8080";
const WORKER_SECRET = process.env.WORKER_SHARED_SECRET || "";
if (!WORKER_SECRET) {
  console.error("[worker] WORKER_SHARED_SECRET is required.");
  process.exit(1);
}

const HEADLESS = /^true$/i.test(process.env.HEADLESS || "true");
const SOFT_MODE = /^true$/i.test(process.env.SOFT_MODE || "false");
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000", 10);
const JOB_TYPES = (process.env.JOB_TYPES || "SEND_CONNECTION")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);

// For LinkedIn automation
const DEFAULT_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36";

// ---------- HTTP helpers ----------
async function apiPost(path, body) {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-worker-secret": WORKER_SECRET,
    },
    body: JSON.stringify(body || {}),
  });
  const text = await res.text();
  try {
    return { ok: res.ok, status: res.status, data: JSON.parse(text) };
  } catch {
    return { ok: res.ok, status: res.status, data: { raw: text } };
  }
}

async function apiGet(path) {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, {
    method: "GET",
    headers: { "x-worker-secret": WORKER_SECRET },
  });
  const text = await res.text();
  try {
    return { ok: res.ok, status: res.status, data: JSON.parse(text) };
  } catch {
    return { ok: res.ok, status: res.status, data: { raw: text } };
  }
}

// ---------- Browser helpers ----------
async function newBrowserContextWithCookies(cookieBundle) {
  const browser = await chromium.launch({
    headless: HEADLESS,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext({
    userAgent: DEFAULT_UA,
    viewport: { width: 1280, height: 900 },
    javaScriptEnabled: true,
  });

  // Prepare cookies for .linkedin.com
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
    // Ensure no surrounding quotes for Playwright cookie value
    let val = cookieBundle.jsessionid;
    if (val.length >= 2 && val.startsWith('"') && val.endsWith('"')) {
      val = val.slice(1, -1);
    }
    cookies.push({
      name: "JSESSIONID",
      value: val,
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

  if (cookies.length) {
    await context.addCookies(cookies);
  }

  const page = await context.newPage();
  // Make initial hop to set cookie jar
  await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 45000 })
    .catch(() => {});
  return { browser, context, page };
}

function normalizeUrl(u) {
  if (!u) return null;
  if (/^https?:\/\//i.test(u)) return u;
  return `https://${u.replace(/^\/+/, "")}`;
}

async function gotoLinkedInProfile(page, rawUrl) {
  const url = normalizeUrl(rawUrl);
  if (!url) throw new Error("Invalid profileUrl");

  // Retry strategy: first domcontentloaded, then load fallback
  const tries = [
    { waitUntil: "domcontentloaded", timeout: 45000 },
    { waitUntil: "load", timeout: 45000 },
  ];

  let lastErr;
  for (const t of tries) {
    try {
      await page.goto(url, t);
      // Wait for the main column to confirm we’re on a profile-like page
      await page.waitForTimeout(800);
      // common containers on /in/ pages
      await Promise.race([
        page.locator("main").waitFor({ timeout: 8000 }),
        page.locator("[data-view-name]").waitFor({ timeout: 8000 }),
        page.locator("section").waitFor({ timeout: 8000 }),
      ]);
      return true;
    } catch (e) {
      lastErr = e;
      // sometimes ERR_ABORTED occurs; small wait then retry
      await page.waitForTimeout(800);
    }
  }
  throw lastErr || new Error("Could not open profile");
}

async function tryClickConnect(page) {
  // Try common “Connect” entry points
  const candidates = [
    // LinkedIn profile header “Connect”
    page.getByRole("button", { name: /connect/i }),
    page.locator('button:has-text("Connect")'),
    // Three-dots menu -> Connect
    page.locator('[aria-label*="More actions"]'),
    page.locator('div[role="menuitem"]:has-text("Connect")'),
  ];

  // 1) Direct Connect button present?
  const direct = await candidates[0].first();
  if (await direct.isVisible().catch(() => false)) {
    await direct.click({ delay: 50 });
  } else {
    // 2) Open kebab menu, then choose Connect
    const kebab = candidates[2];
    if (await kebab.isVisible().catch(() => false)) {
      await kebab.click({ delay: 50 });
      await page.waitForTimeout(200);
      const menuConnect = candidates[3];
      if (await menuConnect.isVisible().catch(() => false)) {
        await menuConnect.click({ delay: 50 });
      }
    }
  }

  // Now a dialog may appear: either with “Add a note” or straight “Send”
  return true;
}

async function maybeAddNoteAndSend(page, noteText) {
  // If user wants to add a note, click “Add a note” first
  if (noteText) {
    const addNote = page.getByRole("button", { name: /add a note/i });
    if (await addNote.isVisible().catch(() => false)) {
      await addNote.click({ delay: 50 });
    }
    // Type into note textarea/field
    const noteBox = page.locator('textarea[name="message"]')
      .or(page.locator('textarea[aria-label*="Add a note"]'))
      .or(page.locator('textarea'));
    if (await noteBox.first().isVisible().catch(() => false)) {
      await noteBox.first().fill(noteText.slice(0, 280), { timeout: 5000 }).catch(() => {});
    }
  }

  // Finally, click Send (unless soft mode)
  const sendBtn = page.getByRole("button", { name: /^send$/i })
    .or(page.locator('button:has-text("Send")'));

  if (SOFT_MODE) {
    // Don’t click send in soft mode
    return { clicked: false, reason: "soft_mode" };
  }

  if (await sendBtn.first().isVisible().catch(() => false)) {
    await sendBtn.first().click({ delay: 50 });
    // Give LI a breath to submit
    await page.waitForTimeout(1000);
    return { clicked: true };
  }

  // Some flows auto-send without extra dialog, treat as sent
  return { clicked: true, assumed: true };
}

// ---------- Job handlers ----------
async function handleSendConnection(job) {
  const { payload } = job;
  const profileUrl = payload?.profileUrl;
  const cookieBundle = payload?.cookieBundle || {};
  const noteRaw = payload?.note || null;

  if (!profileUrl || !cookieBundle.li_at) {
    throw new Error("Missing profileUrl or li_at in job payload");
  }

  const note = noteRaw
    ? noteRaw.replace(/\{\{first\}\}/gi, "").trim() // (Optional: you could personalize if you pass firstName)
    : null;

  const { browser, context, page } = await newBrowserContextWithCookies({
    li_at: cookieBundle.li_at,
    jsessionid: cookieBundle.jsessionid || null,
    bcookie: cookieBundle.bcookie || null,
  });

  try {
    await gotoLinkedInProfile(page, profileUrl);
    await page.waitForTimeout(800);

    // Try to click “Connect”
    await tryClickConnect(page);
    await page.waitForTimeout(400);

    // Add note and/or send
    const sendRes = await maybeAddNoteAndSend(page, note);

    // Small post-wait to let LinkedIn process
    await page.waitForTimeout(1200);

    return {
      ok: true,
      action: sendRes.clicked ? "connection_sent" : "no_send",
      details: sendRes,
    };
  } finally {
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

// ---------- Runner loop ----------
async function pollOnceAndProcess() {
  const next = await apiPost("/jobs/next", { types: JOB_TYPES });
  if (!next.ok) {
    console.error("[worker] jobs/next error", next.status, next.data);
    return;
  }
  const job = next.data.job;
  if (!job) {
    // Nothing to do
    return;
  }

  console.log(`[worker] got job ${job.id} type=${job.type}`);

  try {
    let result = null;
    switch (job.type) {
      case "SEND_CONNECTION":
        result = await handleSendConnection(job);
        break;
      default:
        throw new Error(`Unsupported job type: ${job.type}`);
    }

    await apiPost(`/jobs/${job.id}/complete`, { result });
    console.log(`[worker] Job ${job.id} completed:`, result);
  } catch (err) {
    console.error(`[worker] Job ${job.id} failed:`, err?.message || err);
    await apiPost(`/jobs/${job.id}/fail`, {
      error: String(err && err.stack ? err.stack : err),
      requeue: false,
    });
  }
}

let _stopped = false;
function startLoop() {
  console.log(`[worker] starting. Headless: ${HEADLESS} Soft mode: ${SOFT_MODE}`);
  const tick = async () => {
    if (_stopped) return;
    try {
      await pollOnceAndProcess();
    } catch (e) {
      console.error("[worker] loop error:", e?.message || e);
    } finally {
      if (!_stopped) setTimeout(tick, POLL_INTERVAL_MS);
    }
  };
  tick();
}

function setupShutdown() {
  const stop = async () => {
    if (_stopped) return;
    _stopped = true;
    console.log("[worker] shutting down…");
    process.exit(0);
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  process.on("uncaughtException", (e) => {
    console.error("[worker] uncaughtException:", e);
    stop();
  });
  process.on("unhandledRejection", (e) => {
    console.error("[worker] unhandledRejection:", e);
    stop();
  });
}

// ---------- Main ----------
setupShutdown();
startLoop();
