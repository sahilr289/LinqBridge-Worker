// worker.js — LinqBridge Worker (ESM, Playwright + Job Runner)
// Env required:
//   API_BASE, WORKER_SHARED_SECRET
//   HEADLESS=true|false, SOFT_MODE=true|false
// Optional:
//   POLL_INTERVAL_MS=5000, JOB_TYPES=SEND_CONNECTION

import { chromium } from "playwright";

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

// ---------- HTTP helpers (Node 18+ has global fetch) ----------
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
  // Initial hop to set cookie jar
  await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 45000 }).catch(() => {});
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

  const tries = [
    { waitUntil: "domcontentloaded", timeout: 45000 },
    { waitUntil: "load", timeout: 45000 },
  ];

  let lastErr;
  for (const t of tries) {
    try {
      await page.goto(url, t);
      await page.waitForTimeout(800);
      await Promise.race([
        page.locator("main").waitFor({ timeout: 8000 }),
        page.locator("[data-view-name]").waitFor({ timeout: 8000 }),
        page.locator("section").waitFor({ timeout: 8000 }),
      ]);
      return true;
    } catch (e) {
      lastErr = e;
      await page.waitForTimeout(800);
    }
  }
  throw lastErr || new Error("Could not open profile");
}

async function tryClickConnect(page) {
  // Try common “Connect” entry points
  const direct = page.getByRole("button", { name: /connect/i }).first();
  if (await direct.isVisible().catch(() => false)) {
    await direct.click({ delay: 50 });
  } else {
    const kebab = page.locator('[aria-label*="More actions"]').first();
    if (await kebab.isVisible().catch(() => false)) {
      await kebab.click({ delay: 50 });
      await page.waitForTimeout(200);
      const menuConnect = page.locator('div[role="menuitem"]:has-text("Connect")').first();
      if (await menuConnect.isVisible().catch(() => false)) {
        await menuConnect.click({ delay: 50 });
      }
    }
  }
  return true;
}

async function maybeAddNoteAndSend(page, noteText) {
  if (noteText) {
    const addNote = page.getByRole("button", { name: /add a note/i }).first();
    if (await addNote.isVisible().catch(() => false)) {
      await addNote.click({ delay: 50 });
    }
    const noteBox = page
      .locator('textarea[name="message"]')
      .or(page.locator('textarea[aria-label*="Add a note"]'))
      .or(page.locator("textarea"))
      .first();

    if (await noteBox.isVisible().catch(() => false)) {
      await noteBox.fill(noteText.slice(0, 280), { timeout: 5000 }).catch(() => {});
    }
  }

  const sendBtn = page.getByRole("button", { name: /^send$/i }).or(page.locator('button:has-text("Send")')).first();

  if (SOFT_MODE) {
    return { clicked: false, reason: "soft_mode" };
    }

  if (await sendBtn.isVisible().catch(() => false)) {
    await sendBtn.click({ delay: 50 });
    await page.waitForTimeout(1000);
    return { clicked: true };
  }

  // Sometimes LinkedIn auto-sends or the flow differs — assume success
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

  const note = noteRaw ? noteRaw.replace(/\{\{first\}\}/gi, "").trim() : null;

  const { browser, context, page } = await newBrowserContextWithCookies({
    li_at: cookieBundle.li_at,
    jsessionid: cookieBundle.jsessionid || null,
    bcookie: cookieBundle.bcookie || null,
  });

  try {
    await gotoLinkedInProfile(page, profileUrl);
    await page.waitForTimeout(800);

    await tryClickConnect(page);
    await page.waitForTimeout(400);

    const sendRes = await maybeAddNoteAndSend(page, note);
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
  if (!job) return;

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
