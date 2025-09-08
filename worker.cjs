// worker.cjs — LinqBridge Worker (FINAL: connects & messaging kept intact; adds caps+window+acceptance-scan)
//
// ✅ Everything that was working stays working.
// ➕ Additions (non-breaking):
//   - Per-user daily caps (default 15 invites / 15 messages) with in-worker counters (IST-based) and gentle requeue
//   - Active window (default 10:00–22:00 IST, Mon–Fri). Outside window, jobs are requeued until next window
//   - Optional “rest after cap” sleep window (default 12h) before picking more jobs for that user
//   - Backend metrics ping (/worker/metrics/incr) after successful send (best-effort; doesn’t block)
//   - New job types: ACCEPTANCE_SCAN (bulk) and CHECK_CONNECTED (single) to check 1st-degree acceptance via profile visit
//
// Notes:
//   * No changes to connect flow, throttling, or messaging selectors.
//   * If ENFORCE_DAILY_CAPS=false, worker will not gate—still reports metrics.
//   * Window & caps are environment-driven here to avoid JWT; backend counters are incremented for observability.
//   * If multiple workers run, in-worker caps are per-process; backend still records usage centrally.
//
// -----------------------------
// Existing code (unaltered core) + additive logic starts below
// -----------------------------

const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const { chromium } = require("playwright");

let fetchRef = global.fetch;
async function getFetch() {
  if (fetchRef) return fetchRef;
  try { fetchRef = (await import("node-fetch")).default; return fetchRef; } catch {}
  try { fetchRef = require("cross-fetch"); return fetchRef; } catch {}
  throw new Error("No fetch implementation available");
}

// ---------- Config ----------
const API_BASE = process.env.API_BASE || "http://localhost:8080";
const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET || "";

const HEADLESS = (/^(true|1|yes)$/i).test(process.env.HEADLESS || "false");
const SLOWMO_MS = parseInt(process.env.SLOWMO_MS || (HEADLESS ? "0" : "50"), 10);
const SOFT_MODE = (/^(true|1|yes)$/i).test(process.env.SOFT_MODE || "false");

const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000", 10);
const MAX_ACTIONS_PER_HOUR = parseInt(process.env.MAX_ACTIONS_PER_HOUR || "18", 10);
const MIN_GAP_MS = parseInt(process.env.MIN_GAP_MS || "60000", 10);
const COOLDOWN_AFTER_SENT_MS = parseInt(process.env.COOLDOWN_AFTER_SENT_MS || "90000", 10);
const COOLDOWN_AFTER_FAIL_MS = parseInt(process.env.COOLDOWN_AFTER_FAIL_MS || "600000", 10);

const MICRO_DELAY_MIN_MS = parseInt(process.env.MICRO_DELAY_MIN_MS || "400", 10);
const MICRO_DELAY_MAX_MS = parseInt(process.env.MICRO_DELAY_MAX_MS || "1200", 10);

const FEED_INITIAL_WAIT_MS = parseInt(process.env.FEED_INITIAL_WAIT_MS || "0", 10);
const FEED_SCROLL_MS = parseInt(process.env.FEED_SCROLL_MS || "0", 10);
const PROFILE_INITIAL_WAIT_MS = parseInt(process.env.PROFILE_INITIAL_WAIT_MS || "8000", 10);

const DEFAULT_TIMEOUT_MS = parseInt(process.env.DEFAULT_TIMEOUT_MS || "35000", 10);
const NAV_TIMEOUT_MS = parseInt(process.env.NAV_TIMEOUT_MS || "45000", 10);

const USE_PROFILE_MOBILE_FIRST = (/^(true|1|yes)$/i).test(process.env.USE_PROFILE_MOBILE_FIRST || "true");
const FORCE_NO_NOTES = (/^(true|1|yes)$/i).test(process.env.FORCE_NO_NOTES || "true");

const DEFAULT_STATE_PATH = process.env.STORAGE_STATE_PATH || "/app/auth-state.json";
const STATE_DIR = process.env.STATE_DIR || "/app/state";

const FORCE_RELOGIN = (/^(true|1|yes)$/i).test(process.env.FORCE_RELOGIN || "false");
const ALLOW_INTERACTIVE_LOGIN = (/^(true|1|yes)$/i).test(process.env.ALLOW_INTERACTIVE_LOGIN || "true");
const INTERACTIVE_LOGIN_TIMEOUT_MS = parseInt(process.env.INTERACTIVE_LOGIN_TIMEOUT_MS || "300000", 10);

const PROXY_SERVER = process.env.PROXY_SERVER || "";
const PROXY_USERNAME = process.env.PROXY_USERNAME || "";
const PROXY_PASSWORD = process.env.PROXY_PASSWORD || "";

// ---------- NEW: Window & Caps (env-driven; IST-based) ----------
const ENFORCE_DAILY_CAPS = (/^(true|1|yes)$/i).test(process.env.ENFORCE_DAILY_CAPS || "true");
const MAX_INVITES_PER_DAY   = parseInt(process.env.MAX_INVITES_PER_DAY   || "15", 10);
const MAX_MESSAGES_PER_DAY  = parseInt(process.env.MAX_MESSAGES_PER_DAY  || "15", 10);
const START_TIME_IST        = process.env.START_TIME_IST || "10:00"; // HH:MM
const ACTIVE_WINDOW_HOURS   = parseInt(process.env.ACTIVE_WINDOW_HOURS || "12", 10);
const ACTIVE_DAYS           = (process.env.ACTIVE_DAYS || "Mon,Tue,Wed,Thu,Fri").split(",").map(s=>s.trim());
const REST_AFTER_CAP_HOURS  = parseInt(process.env.REST_AFTER_CAP_HOURS || "12", 10);
const REQUEUE_OUTSIDE_WINDOW_MS = parseInt(process.env.REQUEUE_OUTSIDE_WINDOW_MS || String(30*60*1000), 10); // 30m default

// ---------- Utils ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();
const within = (min, max) => min + Math.floor(Math.random() * (max - min + 1));
async function microDelay() { await sleep(within(MICRO_DELAY_MIN_MS, MICRO_DELAY_MAX_MS)); }
async function sprout(label = "") { const n = within(1, 3); for (let i = 0; i < n; i++) await microDelay(); if (label) console.log(`[sprout] ${label} x${n}`); }
const sanitizeUserId = (s) => (String(s || "default").toLowerCase().replace(/[^a-z0-9]+/g, "_"));
const statePathForUser = (userId) => path.join(STATE_DIR, `${sanitizeUserId(userId)}.json`);

function logFetchError(where, err) {
  const code = err?.cause?.code || err?.code || "unknown";
  console.error("[worker]", `${where} fetch failed:`, code, err?.message || err);
}
function apiUrl(p) { return p.startsWith("/") ? `${API_BASE}${p}` : `${API_BASE}/${p}`; }

async function apiGet(p) {
  const fetch = await getFetch();
  const res = await fetch(apiUrl(p), { method: "GET", headers: { "x-worker-secret": WORKER_SHARED_SECRET }, signal: AbortSignal.timeout ? AbortSignal.timeout(15000) : undefined });
  const text = await res.text();
  try { const json = JSON.parse(text); if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`); return json; }
  catch { throw new Error(`GET ${p} non-JSON or error ${res.status}: ${text}`); }
}
async function apiPost(p, body) {
  const fetch = await getFetch();
  const res = await fetch(apiUrl(p), { method: "POST", headers: { "Content-Type": "application/json", "x-worker-secret": WORKER_SHARED_SECRET }, body: JSON.stringify(body || {}), signal: AbortSignal.timeout ? AbortSignal.timeout(25000) : undefined });
  const text = await res.text();
  try { const json = JSON.parse(text); if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`); return json; }
  catch { throw new Error(`POST ${p} non-JSON or error ${res.status}: ${text}`); }
}

// ---------- NEW: IST helpers, window & counters ----------
const IST_OFFSET_MIN = 330; // +05:30
function nowIST() {
  const d = new Date();
  const utcMs = d.getTime() + d.getTimezoneOffset()*60000;
  return new Date(utcMs + IST_OFFSET_MIN*60000);
}
function istYMD(d = nowIST()) {
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,"0");
  const day = String(d.getDate()).padStart(2,"0");
  return `${y}-${m}-${day}`;
}
function parseHHMM(hhmm = "10:00") {
  const [h, m] = (hhmm||"10:00").split(":").map(x=>parseInt(x,10));
  return {h: Number.isFinite(h)?h:10, m: Number.isFinite(m)?m:0};
}
const weekdayNames = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
function istWindowForToday() {
  const now = nowIST();
  const ymd = istYMD(now);
  const {h,m} = parseHHMM(START_TIME_IST);
  // Build Date in IST by backing out offset to UTC
  const utcStartMs = Date.UTC(now.getFullYear(), now.getMonth(), now.getDate(), h - Math.floor(IST_OFFSET_MIN/60), m - (IST_OFFSET_MIN%60));
  const start = new Date(utcStartMs);
  const end = new Date(start.getTime() + (ACTIVE_WINDOW_HOURS||12)*3600_000);
  const isActive = now >= start && now <= end;
  const wd = weekdayNames[now.getDay()];
  const dayActive = ACTIVE_DAYS.includes(wd);
  return { ymd, start, end, isActive, dayActive, nowIST: now };
}
function nextActiveStartFrom(nowObj) {
  // Returns next IST Date when window opens again on an active day
  const {h,m} = parseHHMM(START_TIME_IST);
  let d = new Date(nowObj.nowIST.getTime());
  for (let i=0;i<8;i++) {
    const wd = weekdayNames[d.getDay()];
    if (ACTIVE_DAYS.includes(wd)) {
      const uStart = Date.UTC(d.getFullYear(), d.getMonth(), d.getDate(), h - Math.floor(IST_OFFSET_MIN/60), m - (IST_OFFSET_MIN%60));
      const candidate = new Date(uStart);
      if (candidate > nowObj.nowIST) return candidate;
    }
    d = new Date(d.getTime() + 24*3600_000);
  }
  // Fallback: tomorrow same time
  const tmr = new Date(nowObj.nowIST.getTime() + 24*3600_000);
  const uStart = Date.UTC(tmr.getFullYear(), tmr.getMonth(), tmr.getDate(), h - Math.floor(IST_OFFSET_MIN/60), m - (IST_OFFSET_MIN%60));
  return new Date(uStart);
}

// In-process counters per user/day (IST)
const userDaily = new Map(); // userId -> { ymd, invites, messages, sleepUntilMs? }

function getUserCounter(userId) {
  const key = sanitizeUserId(userId || "default");
  const today = istYMD();
  let obj = userDaily.get(key);
  if (!obj || obj.ymd !== today) {
    obj = { ymd: today, invites: 0, messages: 0, sleepUntilMs: 0 };
    userDaily.set(key, obj);
  }
  return obj;
}
async function postMetricIncrement(userId, kind, delta=1) {
  try { await apiPost("/worker/metrics/incr", { userId, kind, delta }); } catch (e) { logFetchError("metrics/incr", e); }
}

// Gate by window + caps; return {ok, requeueMs, reason}
function gateWindowAndCaps(userId, kind /* 'invite' | 'message' */) {
  if (!ENFORCE_DAILY_CAPS) return { ok: true };

  const wnd = istWindowForToday();
  if (!wnd.dayActive) {
    const next = nextActiveStartFrom(wnd);
    const ms = Math.max(1000, next.getTime() - wnd.nowIST.getTime());
    return { ok: false, requeueMs: ms, reason: "inactive_day" };
  }
  if (!wnd.isActive) {
    const ms = wnd.nowIST < wnd.start ? (wnd.start.getTime() - wnd.nowIST.getTime()) : REQUEUE_OUTSIDE_WINDOW_MS;
    return { ok: false, requeueMs: Math.max(1000, ms), reason: "outside_window" };
  }

  const ctr = getUserCounter(userId);
  if (ctr.sleepUntilMs && Date.now() < ctr.sleepUntilMs) {
    return { ok: false, requeueMs: Math.max(1000, ctr.sleepUntilMs - Date.now()), reason: "rest_after_cap" };
  }

  if (kind === "invite" && ctr.invites >= MAX_INVITES_PER_DAY) {
    ctr.sleepUntilMs = Date.now() + REST_AFTER_CAP_HOURS*3600_000;
    return { ok: false, requeueMs: Math.max(1000, ctr.sleepUntilMs - Date.now()), reason: "invites_cap_reached" };
  }
  if (kind === "message" && ctr.messages >= MAX_MESSAGES_PER_DAY) {
    ctr.sleepUntilMs = Date.now() + REST_AFTER_CAP_HOURS*3600_000;
    return { ok: false, requeueMs: Math.max(1000, ctr.sleepUntilMs - Date.now()), reason: "messages_cap_reached" };
  }
  return { ok: true };
}
function bumpCounter(userId, kind, by=1) {
  const ctr = getUserCounter(userId);
  if (kind === "invite") ctr.invites += by;
  if (kind === "message") ctr.messages += by;
}

// ---------- Page helpers ----------
async function anyVisible(...locs) {
  const checks = await Promise.all(locs.map(l => l.first().isVisible({ timeout: 800 }).catch(() => false)));
  return checks.some(Boolean);
}
async function waitFullLoad(page, timeout = 45000) {
  await page.waitForLoadState('domcontentloaded', { timeout }).catch(()=>{});
  try { await Promise.race([ page.waitForLoadState('networkidle', { timeout: Math.min(timeout, 12000) }), sleep(6000) ]); } catch {}
}
async function slowHumanScroll(page, totalMs = 6000) {
  const start = Date.now();
  while (Date.now() - start < totalMs) { try { await page.mouse.wheel(0, within(20, 55)); } catch {} await sleep(within(140, 260)); }
  await sleep(within(400, 900));
}
async function briefProfileScroll(page, totalMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < totalMs) { try { await page.mouse.wheel(0, within(80, 140)); } catch {} await sleep(within(120, 220)); }
}
function looksLikeAuthRedirect(url) { return /\/uas\/login/i.test(url) || /\/checkpoint\//i.test(url); }
async function isAuthWalledOrGuest(page) {
  try {
    const url = page.url() || "";
    if (looksLikeAuthRedirect(url)) return true;
    const title = (await page.title().catch(() => ""))?.toLowerCase?.() || "";
    if (title.includes("sign in") || title.includes("join linkedin") || title.includes("authwall")) return true;
    const hasLogin = await page.locator('a[href*="login"]').first().isVisible({ timeout: 600 }).catch(() => false);
    return !!hasLogin;
  } catch { return false; }
}
async function detectHardScreen(page) {
  try {
    const url = page.url() || "";
    const title = (await page.title().catch(()=> "")) || "";
    const bodyText = await page.locator("body").innerText().catch(()=>"");
    if (url.includes("/404") || /page not found/i.test(title) || /page not found/i.test(bodyText)) return "404";
    if (/429/.test(title) || /too many requests/i.test(bodyText) || /temporarily blocked/i.test(bodyText)) return "429";
    if (/captcha/i.test(title) || /verify/i.test(bodyText)) return "captcha";
  } catch {}
  return null;
}

// ---------- Per-domain throttle ----------
class DomainThrottle {
  constructor() { this.state = new Map(); }
  _get(domain) { if (!this.state.has(domain)) this.state.set(domain, { lastActionAt: 0, events: [], cooldownUntil: 0 }); return this.state.get(domain); }
  _pruneOld(events) { const cutoff = now() - 3600_000; while (events.length && events[0] < cutoff) events.shift(); }
  async reserve(domain, label = "action") {
    const st = this._get(domain);
    while (true) {
      this._pruneOld(st.events);
      const nowTs = now(); const waits = [];
      if (st.cooldownUntil && st.cooldownUntil > nowTs) waits.push(st.cooldownUntil - nowTs);
      if (st.events.length >= MAX_ACTIONS_PER_HOUR) waits.push((st.events[0] + 3600_000) - nowTs);
      const sinceLast = nowTs - (st.lastActionAt || 0);
      if (sinceLast < MIN_GAP_MS) waits.push(MIN_GAP_MS - sinceLast);
      if (waits.length) {
        const waitMs = Math.max(...waits) + within(1500, 3500);
        console.log(`[throttle] Waiting ${Math.ceil(waitMs/1000)}s before ${label} (used: ${st.events.length}/${MAX_ACTIONS_PER_HOUR})`);
        await sleep(waitMs); continue;
      }
      st.lastActionAt = nowTs; st.events.push(nowTs);
      console.log(`[throttle] Reserved slot for ${label}. Used this hour: ${st.events.length}/${MAX_ACTIONS_PER_HOUR}`);
      return;
    }
  }
  success(domain) { const st = this._get(domain); st.cooldownUntil = Math.max(st.cooldownUntil, now() + COOLDOWN_AFTER_SENT_MS); }
  failure(domain) { const st = this._get(domain); st.cooldownUntil = Math.max(st.cooldownUntil, now() + COOLDOWN_AFTER_FAIL_MS); }
}
const throttle = new DomainThrottle();

// ---------- Playwright boot ----------
async function createBrowserContext({ headless, userStatePath }) {
  await fsp.mkdir(path.dirname(userStatePath || DEFAULT_STATE_PATH), { recursive: true }).catch(()=>{});
  const launchOpts = {
    headless: !!headless,
    slowMo: SLOWMO_MS,
    args: [
      "--no-sandbox","--disable-dev-shm-usage","--disable-gpu",
      "--disable-features=IsolateOrigins,site-per-process",
      "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
      "--webrtc-stun-probe-trial=disabled",
    ],
  };
  if (PROXY_SERVER) launchOpts.proxy = { server: PROXY_SERVER, username: PROXY_USERNAME || undefined, password: PROXY_PASSWORD || undefined };
  const browser = await chromium.launch(launchOpts);
  const vw = 1280 + Math.floor(Math.random() * 192);
  const vh = 720 + Math.floor(Math.random() * 160);
  const storageStateOpt =
    (!FORCE_RELOGIN && userStatePath && fs.existsSync(userStatePath)) ? userStatePath :
    (!FORCE_RELOGIN && fs.existsSync(DEFAULT_STATE_PATH) ? DEFAULT_STATE_PATH : undefined);

  const context = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    locale: "en-US", timezoneId: "America/Los_Angeles",
    viewport: { width: vw, height: vh }, javaScriptEnabled: true,
    recordVideo: { dir: "/tmp/pw-video" }, storageState: storageStateOpt,
  });
  await context.setExtraHTTPHeaders({
    "accept-language": "en-US,en;q=0.9",
    "upgrade-insecure-requests": "1",
    "sec-ch-ua": '"Chromium";v="124", "Not:A-Brand";v="8"',
    "sec-ch-ua-platform": '"Windows"',
    "sec-ch-ua-mobile": "?0",
    referer: "https://www.google.com/",
  });
  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
    try {
      Object.defineProperty(navigator, "hardwareConcurrency", { get: () => 8 });
      Object.defineProperty(navigator, "language", { get: () => "en-US" });
      Object.defineProperty(navigator, "languages", { get: () => ["en-US","en"] });
      Object.defineProperty(navigator, "userAgent", { get: () =>
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36" });
    } catch {}
  });
  const page = await context.newPage();
  page.setDefaultTimeout(DEFAULT_TIMEOUT_MS);
  page.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
  try { await page.bringToFront(); } catch {}
  return { browser, context, page };
}
async function newPageInContext(context) {
  const p = await context.newPage();
  p.setDefaultTimeout(DEFAULT_TIMEOUT_MS);
  p.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
  try { await p.bringToFront(); } catch {}
  return p;
}
async function saveStorageState(context, outPath) {
  try {
    await fsp.mkdir(path.dirname(outPath), { recursive: true });
    await context.storageState({ path: outPath });
    console.log("[auth] storageState saved to", outPath);
  } catch (e) { console.log("[auth] storageState save failed:", e?.message || e); }
}

// ---------- Feed warmup ----------
async function feedWarmup(page) {
  try {
    if (!/linkedin\.com\/feed\/?$/i.test(page.url())) {
      console.log("[nav] → feed-desktop: https://www.linkedin.com/feed/");
      await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    }
    if (FEED_INITIAL_WAIT_MS > 0) { console.log(`[feed] initial wait ${FEED_INITIAL_WAIT_MS} ms`); await sleep(FEED_INITIAL_WAIT_MS); }
    console.log("[feed] waiting full load…"); await waitFullLoad(page, NAV_TIMEOUT_MS);
    console.log("[feed] slow human scroll…"); await slowHumanScroll(page, Math.max(3000, FEED_SCROLL_MS || 6000));
  } catch (e) { console.log("[feed] warmup error:", e?.message || e); }
}

// ---------- Auth ensure ----------
async function ensureAuthenticated(context, page, userStatePath) {
  try {
    const r1 = await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    const s1 = r1 ? r1.status() : null;
    console.log(`[nav] ✓ feed-desktop: status=${s1} final=${page.url()}`);
    if (s1 && s1 >= 200 && s1 < 400 && !(await isAuthWalledOrGuest(page))) {
      await saveStorageState(context, userStatePath || DEFAULT_STATE_PATH);
      return { ok: true, via: "desktop", url: page.url() };
    }
  } catch (e) { console.log("[nav] feed-desktop error:", e?.message || e); }
  try {
    const r2 = await page.goto("https://m.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    const s2 = r2 ? r2.status() : null;
    console.log(`[nav] ✓ feed-mobile: status=${s2} final=${page.url()}`);
    if (s2 && s2 >= 200 && s2 < 400 && !(await isAuthWalledOrGuest(page))) {
      await saveStorageState(context, userStatePath || DEFAULT_STATE_PATH);
      return { ok: true, via: "mobile", url: page.url() };
    }
  } catch (e) { console.log("[nav] feed-mobile error:", e?.message || e); }
  if (!ALLOW_INTERACTIVE_LOGIN) return { ok: false, reason: "guest_or_authwall", url: page.url() };
  try {
    console.log("[nav] → login: https://www.linkedin.com/login");
    const r = await page.goto("https://www.linkedin.com/login", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    console.log(`[nav] ✓ login: status=${r ? r.status() : "n/a"} final=${page.url()}`);
  } catch {}
  const deadline = Date.now() + INTERACTIVE_LOGIN_TIMEOUT_MS;
  while (Date.now() < deadline) {
    await sleep(1500);
    if (!(await isAuthWalledOrGuest(page))) {
      await saveStorageState(context, userStatePath || DEFAULT_STATE_PATH);
      return { ok: true, via: "interactive", url: page.url() };
    }
  }
  return { ok: false, reason: "interactive_timeout", url: page.url() };
}

// ---------- Relationship helpers ----------
async function getConnectionDegree(page) {
  try {
    const cands = [
      page.getByText(/^1st\b/i).first(),
      page.getByText(/^2nd\b/i).first(),
      page.getByText(/^3rd\b/i).first(),
      page.locator('[data-test-connection-badge]').first(),
    ];
    for (const l of cands) {
      const vis = await l.isVisible({ timeout: 800 }).catch(() => false);
      if (!vis) continue;
      const t = (await l.innerText().catch(()=>"")).trim();
      if (/^1st\b/i.test(t)) return "1st";
      if (/^2nd\b/i.test(t)) return "2nd";
      if (/^3rd\b/i.test(t)) return "3rd";
    }
  } catch {}
  return null;
}

// Broadened InMail/Open Profile recognition
async function looksLikeInMailOrOpenProfile(page) {
  try {
    const hasMessage = await anyVisible(
      page.getByRole("button", { name: /(^|\s)Message(\s|$)/i }),
      page.getByRole("link",   { name: /(^|\s)Message(\s|$)/i }),
      page.locator('button[aria-label^="Message"]'),
      page.locator('a[aria-label^="Message"]'),
      page.locator('button:has(span.artdeco-button__text:has-text("Message"))'),
      page.locator('a:has(span.artdeco-button__text:has-text("Message"))'),
      page.locator('button[aria-label="Message"]'),
      page.locator('a[data-control-name*="message"]'),
      page.locator('button:has-text("Message")'),
    );
    const hasConnect = await anyVisible(
      page.getByRole("button", { name: /^Connect$/i }),
      page.locator('button[aria-label="Connect"]'),
      page.locator('button[data-control-name="connect"]'),
      page.locator('button:has-text("Connect")')
    );
    const inmailText = await page.getByText(/InMail/i).first().isVisible({ timeout: 600 }).catch(() => false);
    const openText = await page.getByText(/Open Profile|Open to messages/i).first().isVisible({ timeout: 600 }).catch(() => false);
    return (inmailText || openText) && !hasConnect && hasMessage;
  } catch {}
  return false;
}

// ORIGINAL-ish: conservative detector (keeps connect flow untouched)
async function detectRelationshipStatus(page) {
  // Pending?
  const pending = await anyVisible( page.getByRole("button", { name: /Pending|Requested|Withdraw|Pending invitation/i }) );
  if (pending) return { status: "pending", reason: "Pending/Requested visible" };

  // Degree badge = connected
  const degree = await getConnectionDegree(page);
  const messageBtn = await anyVisible(
    page.getByRole("button", { name: /^Message$/i }),
    page.getByRole("link",   { name: /^Message$/i })
  );
  const connectBtn = await anyVisible(
    page.getByRole("button", { name: /^Connect$/i }),
    page.locator('button:has-text("Connect")')
  );

  if (degree === "1st") return { status: "connected", reason: 'Degree badge "1st"' };

  // Message visible alone ≠ connected; could be InMail/Open Profile
  if (messageBtn && !connectBtn) {
    const paid = await looksLikeInMailOrOpenProfile(page);
    if (paid) return { status: "not_connected", reason: "Message is InMail/Open Profile (not 1st)" };
    return { status: "not_connected", reason: "Message visible but not 1st-degree" };
  }

  if (connectBtn) return { status: "not_connected", reason: "Connect button visible" };

  const moreVisible = await anyVisible(
    page.getByRole("button", { name: /^More$/i }),
    page.getByRole("button", { name: /More actions/i })
  );
  if (moreVisible) return { status: "not_connected", reason: "Connect may be under More" };

  return { status: "not_connected", reason: "Unable to confirm; will try menus" };
}

// ---------- Profile nav with authwall recovery ----------
function toMobileProfileUrl(u) { try { const url = new URL(u); url.hostname = "m.linkedin.com"; return url.toString(); } catch { return u; } }
function isProfileUrl(u) { try { return /https?:\/\/([^.]+\.)?linkedin\.com\/(m\/)?in\//i.test(u); } catch { return false; } }

async function navigateProfileClean(page, rawUrl) {
  const primary = String(rawUrl).replace("linkedin.com//", "linkedin.com/");
  const mobile = toMobileProfileUrl(primary);
  const tries = USE_PROFILE_MOBILE_FIRST ? [mobile, primary] : [primary, mobile];
  for (let i = 0; i < tries.length; i++) {
    const u = tries[i];
    try {
      console.log(`[nav] → profile-${i === 0 ? "first" : "fallback"}: ${u}`);
      const resp = await page.goto(u, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
      const status = resp ? resp.status() : null;
      const finalUrl = page.url();
      console.log(`[nav] final after goto: ${finalUrl}`);
      const hard = await detectHardScreen(page);
      if (status === 429 || hard === "429") return { authed: false, status: 429, usedUrl: u, finalUrl, error: "rate_limited" };
      if (hard === "404") return { authed: false, status: status || 404, usedUrl: u, finalUrl, error: "not_found" };
      const isAuthwall = /\/authwall/i.test(finalUrl);
      if (isAuthwall) {
        console.log("[nav] authwall detected → re-auth then retry once");
        try {
          await page.goto("https://www.linkedin.com/login", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
          await sleep(1500);
          await page.goto(u, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
        } catch (e) { console.log("[nav] authwall re-auth failed:", e?.message || e); }
        const final2 = page.url();
        const hard2 = await detectHardScreen(page);
        const stillAuthwall = /\/authwall/i.test(final2);
        if (!stillAuthwall && !hard2 && isProfileUrl(final2)) return { authed: true, status: 200, usedUrl: u, finalUrl: final2 };
      } else {
        const authed = !(await isAuthWalledOrGuest(page));
        if (authed && status && status >= 200 && status < 400 && !hard && isProfileUrl(finalUrl)) {
          return { authed: true, status, usedUrl: u, finalUrl };
        }
      }
      if (i === tries.length - 1) {
        return { authed: false, status, usedUrl: u, finalUrl,
          error: /\/authwall/i.test(finalUrl) ? "authwall" : (isProfileUrl(finalUrl) ? "authwall_or_unknown" : "not_profile") };
      }
    } catch (e) {
      const finalUrl = page.url();
      if (i === tries.length - 1) {
        const msg = e?.message || "nav_failed";
        if (msg.toLowerCase().includes("too many redirects")) return { authed: false, status: null, usedUrl: tries[i], finalUrl, error: "redirect_loop" };
        return { authed: false, status: null, usedUrl: tries[i], finalUrl, error: msg };
      }
    }
  }
  return { authed: false, status: null, usedUrl: tries[0], finalUrl: page.url(), error: "unknown" };
}

// ---------- Open Connect (button-only + misclick recovery) ----------
async function openConnectDialog(page) {
  try { await page.evaluate(() => window.scrollTo(0, 0)); } catch {}
  await microDelay();
  // 1) More → Connect
  const more = [
    page.getByRole("button", { name: /^More$/i }),
    page.getByRole("button", { name: /More actions/i }),
    page.locator('button[aria-label="More actions"]'),
  ];
  for (const m of more) {
    try {
      const handle = m.first();
      if (await handle.isVisible({ timeout: 1200 }).catch(() => false)) {
        await sprout('open-more');
        const beforeUrl = page.url();
        await handle.click({ timeout: 4000 });
        await microDelay();
        if (page.url() !== beforeUrl && /\/search\/results\/people/i.test(page.url())) {
          console.log("[guard] misclick → people search from More; going back");
          await page.goBack({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS }).catch(()=>{});
          await microDelay();
        }
        const candidates = [
          page.getByRole("menuitem", { name: /^Connect$/i }).first(),
          page.locator('div[role="menuitem"]').filter({ hasText: /^\s*Connect\s*$/i }).first(),
          page.locator('span,div').filter({ hasText: /^\s*Connect\s*$/i }).locator('xpath=ancestor-or-self::*[@role="menuitem"]').first(),
        ];
        for (const item of candidates) {
          try {
            if (await item.isVisible({ timeout: 1200 }).catch(() => false)) {
              const t = (await item.innerText().catch(()=>"")).trim();
              if (/Sales\s*Navigator|View in Sales/i.test(t)) continue;
              await sprout('click-connect-menu');
              const beforeMenuUrl = page.url();
              await item.click({ timeout: 4000 });
              await microDelay();
              if (page.url() !== beforeMenuUrl && /\/search\/results\/people/i.test(page.url())) {
                console.log("[guard] misclick → people search from menu; going back");
                await page.goBack({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS }).catch(()=>{});
                await microDelay();
              }
              const ready = await Promise.race([
                page.getByRole("dialog").waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
                page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
                page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
              ]);
              if (ready) return { opened: true, via: "more_menu" };
            }
          } catch {}
        }
      }
    } catch {}
  }
  // 2) Primary "Connect" button
  const direct = [
    page.getByRole("button", { name: "Connect" }),
    page.locator('button[aria-label="Connect"]'),
    page.locator('button[data-control-name="connect"]'),
    page.locator('button').filter({ hasText: /^\s*Connect\s*$/i }),
  ];
  for (const h of direct) {
    try {
      const cand = h.first();
      if (!(await cand.isVisible({ timeout: 1200 }).catch(() => false))) continue;
      const isAnchor = await cand.evaluate(el => el.tagName.toLowerCase() === "a").catch(()=>false);
      if (isAnchor) continue;
      await sprout('connect-primary');
      const beforeUrl = page.url();
      await cand.click({ timeout: 4000 });
      await microDelay();
      if (page.url() !== beforeUrl && /\/search\/results\/people/i.test(page.url())) {
        console.log("[guard] misclick → people search; going back");
        await page.goBack({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS }).catch(()=>{});
        await microDelay();
      }
      const ready = await Promise.race([
        page.getByRole("dialog").waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
        page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
        page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
      ]);
      if (ready) return { opened: true, via: "primary" };
    } catch {}
  }
  // 3) Mobile fallback
  try {
    const mobileConnect = page.getByRole("button", { name: "Connect" }).first();
    if (await mobileConnect.isVisible({ timeout: 1200 }).catch(() => false)) {
      await sprout('mobile-connect');
      const beforeUrl = page.url();
      await mobileConnect.click({ timeout: 4000 });
      await microDelay();
      if (page.url() !== beforeUrl && /\/search\/results\/people/i.test(page.url())) {
        console.log("[guard] misclick → people search (mobile); going back");
        await page.goBack({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS }).catch(()=>{});
        await microDelay();
      }
      const ready = await Promise.race([
        page.getByRole("dialog").waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
        page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
        page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
      ]);
      if (ready) return { opened: true, via: "mobile_primary" };
    }
  } catch {}
  return { opened: false };
}

// ---------- Complete Connect (forced no note) ----------
async function completeConnectDialog(page, note) {
  const noteToUse = null; // force no notes
  const sendCandidates = [
    page.getByRole("button", { name: /^Send$/i }),
    page.locator('button[aria-label="Send now"]'),
    page.locator('button:has-text("Send")'),
  ];
  for (const s of sendCandidates) {
    try {
      const handle = s.first();
      if (await handle.isVisible({ timeout: 1500 }).catch(() => false)) {
        await sprout('send-invite');
        await handle.click({ timeout: 4000 });
        await microDelay();
        const closed = await Promise.race([
          page.getByRole("dialog").waitFor({ state: "detached", timeout: 5000 }).then(() => true).catch(() => false),
          page.locator('div:has-text("Invitation sent")').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
        ]);
        if (closed) return { sent: true, withNote: false };
      }
    } catch {}
  }
  try {
    const sendWithout = page.locator('button:has-text("Send without a note")').first();
    if (await sendWithout.isVisible({ timeout: 1200 }).catch(() => false)) {
      await sprout('send-without-note');
      await sendWithout.click({ timeout: 4000 });
      await microDelay();
      const closed = await page.getByRole("dialog").waitFor({ state: "detached", timeout: 5000 }).then(() => true).catch(() => false);
      if (closed) return { sent: true, withNote: false };
    }
  } catch {}
  return { sent: false, withNote: false };
}

// ---------- Connection flow (UNCHANGED) ----------
async function sendConnectionRequest(page, note) {
  const rs1 = await detectRelationshipStatus(page);
  if (rs1.status === "connected") return { actionTaken: "none", relationshipStatus: "connected", details: "Already 1st-degree" };
  if (rs1.status === "pending")   return { actionTaken: "none", relationshipStatus: "pending", details: "Invitation already pending" };

  let opened = await openConnectDialog(page);
  if (!opened.opened) {
    try { await page.mouse.wheel(0, within(600, 1000)); await sleep(within(600, 1000)); } catch {}
    opened = await openConnectDialog(page);
    if (!opened.opened) return { actionTaken: "unavailable", relationshipStatus: "not_connected", details: "Connect button not found" };
  }

  const completed = await completeConnectDialog(page, null);
  if (completed.sent) return { actionTaken: "sent_without_note", relationshipStatus: "pending", details: "Invitation sent" };

  const rs2 = await detectRelationshipStatus(page);
  if (rs2.status === "pending")   return { actionTaken: "sent_maybe", relationshipStatus: "pending", details: "Pending after dialog" };
  if (rs2.status === "connected") return { actionTaken: "none", relationshipStatus: "connected", details: "Connected" };

  return { actionTaken: "failed_to_send", relationshipStatus: "not_connected", details: "Unable to send invite" };
}

// ---------- Message Flow ----------
async function openMessageDialog(page) {
  try { await page.evaluate(() => window.scrollTo(0, 0)); } catch {}
  await microDelay();

  // broadened "Message" selectors (supports aria-label="Message <Name>")
  const direct = [
    page.getByRole("button", { name: /(^|\s)Message(\s|$)/i }),
    page.getByRole("link",   { name: /(^|\s)Message(\s|$)/i }),
    page.locator('button[aria-label^="Message"]'),
    page.locator('a[aria-label^="Message"]'),
    page.locator('button:has(span.artdeco-button__text:has-text("Message"))'),
    page.locator('a:has(span.artdeco-button__text:has-text("Message"))'),
    page.locator('button[aria-label="Message"]'),
    page.locator('a[data-control-name*="message"]'),
    page.locator('button:has-text("Message")'),
  ];
  for (const h of direct) {
    try {
      const handle = h.first();
      if (await handle.isVisible({ timeout: 1200 }).catch(() => false)) {
        await sprout('open-message');
        await handle.click({ timeout: 4000 });
        await microDelay();
        const ready = await Promise.race([
          page.getByRole("dialog").waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
          page.locator('.msg-overlay-conversation-bubble').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
          page.locator('[data-test-conversation-compose], .msg-form__contenteditable').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
        ]);
        if (ready) return { opened: true, via: "primary" };
      }
    } catch {}
  }

  // More ▸ Message
  const more = [
    page.getByRole("button", { name: /^More$/i }),
    page.getByRole("button", { name: /More actions/i }),
    page.locator('button[aria-label="More actions"]'),
  ];
  for (const m of more) {
    try {
      const handle = m.first();
      if (await handle.isVisible({ timeout: 1200 }).catch(() => false)) {
        await sprout('open-more-msg');
        await handle.click({ timeout: 4000 });
        await microDelay();
        const menuMsg = [
          page.getByRole("menuitem", { name: /^Message$/i }).first(),
          page.locator('div[role="menuitem"]:has-text("Message")').first(),
          page.locator('span:has-text("Message")').locator('xpath=ancestor-or-self::*[@role="menuitem"]').first(),
        ];
        for (const mi of menuMsg) {
          if (await mi.isVisible({ timeout: 1200 }).catch(() => false)) {
            await sprout('click-message-menu');
            await mi.click({ timeout: 4000 });
            await microDelay();
            const ready = await Promise.race([
              page.getByRole("dialog").waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
              page.locator('.msg-overlay-conversation-bubble').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
              page.locator('[data-test-conversation-compose], .msg-form__contenteditable').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
            ]);
            if (ready) return { opened: true, via: "more_menu" };
          }
        }
      }
    } catch {}
  }

  // Mobile fallback
  try {
    const mobileMsg = page.locator('button:has-text("Message"), a:has-text("Message")');
    if (await mobileMsg.first().isVisible({ timeout: 1200 }).catch(() => false)) {
      await sprout('mobile-message');
      await mobileMsg.first().click({ timeout: 4000 });
      await microDelay();
      const ready = await Promise.race([
        page.getByRole("dialog").waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
        page.locator('.msg-overlay-conversation-bubble').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
        page.locator('[data-test-conversation-compose], .msg-form__contenteditable').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
      ]);
      if (ready) return { opened: true, via: "mobile_primary" };
    }
  } catch {}
  return { opened: false };
}

async function typeIntoComposer(page, text) {
  const limited = String(text).slice(0, 3000);
  const editors = [
    page.locator('.msg-form__contenteditable[contenteditable="true"]'),
    page.locator('[role="textbox"][contenteditable="true"]'),
    page.locator('div[contenteditable="true"]'),
    page.getByRole("textbox"),
    page.locator('textarea'),
  ];
  for (const ed of editors) {
    try {
      const handle = ed.first();
      if (await handle.isVisible({ timeout: 1500 }).catch(() => false)) {
        await handle.click({ timeout: 3000 }).catch(()=>{});
        const tag = await handle.evaluate(el => el.tagName.toLowerCase()).catch(() => "");
        if (tag === "textarea" || tag === "input") { await handle.fill(limited, { timeout: 4000 }); }
        else { await handle.press("Mod+A").catch(()=>{}); await handle.type(limited, { delay: within(5, 25) }); }
        return true;
      }
    } catch {}
  }
  return false;
}

async function clickSendInComposer(page) {
  const candidates = [
    page.getByRole("button", { name: /^Send$/i }),
    page.locator('button[aria-label="Send now"]'),
    page.locator('button:has-text("Send")'),
  ];
  for (const s of candidates) {
    try {
      const handle = s.first();
      if (await handle.isVisible({ timeout: 1500 }).catch(() => false)) {
        await sprout('send-message');
        await handle.click({ timeout: 4000 });
        await microDelay();
        const sent = await Promise.race([
          page.locator('div:has-text("Message sent")').waitFor({ timeout: 4000 }).then(() => true).catch(() => false),
          page.locator('.msg-form__contenteditable[contenteditable="true"]').evaluate(el => (el.innerText || "").trim().length === 0).catch(() => false),
          sleep(1500).then(() => true),
        ]);
        if (sent) return true;
      }
    } catch {}
  }
  try {
    const editor = page.locator('.msg-form__contenteditable[contenteditable="true"], [role="textbox"][contenteditable="true"], textarea').first();
    if (await editor.isVisible({ timeout: 800 }).catch(() => false)) {
      await editor.press("Enter").catch(()=>{});
      await microDelay();
      const sentByEnter = await Promise.race([
        page.locator('div:has-text("Message sent")').waitFor({ timeout: 2000 }).then(() => true).catch(() => false),
        sleep(1500).then(() => true),
      ]);
      if (sentByEnter) return true;
      await editor.press("Control+Enter").catch(()=>{});
      await microDelay();
      const sentByCtrlEnter = await Promise.race([
        page.locator('div:has-text("Message sent")').waitFor({ timeout: 2000 }).then(() => true).catch(() => false),
        sleep(1500).then(() => true),
      ]);
      if (sentByCtrlEnter) return true;
    }
  } catch {}
  return false;
}

// *** FINAL FIX: robust messaging without affecting connect flow ***
async function sendMessageFlow(page, messageText) {
  // 1) Infer relationship (conservative)
  const rs = await detectRelationshipStatus(page);

  // 2) If not clearly connected, allow "best-effort" click when a real Message button exists and it's not InMail/Open Profile
  let allowClickAnyway = false;
  if (rs.status !== "connected") {
    const messageBtnVisible = await anyVisible(
      page.getByRole("button", { name: /(^|\s)Message(\s|$)/i }),
      page.getByRole("link",   { name: /(^|\s)Message(\s|$)/i }),
      page.locator('button[aria-label^="Message"]'),
      page.locator('a[aria-label^="Message"]'),
      page.locator('button:has(span.artdeco-button__text:has-text("Message"))'),
      page.locator('a:has(span.artdeco-button__text:has-text("Message"))'),
      page.locator('button[aria-label="Message"]'),
      page.locator('a[data-control-name*="message"]'),
      page.locator('button:has-text("Message")'),
    );
    const paidSurface = await looksLikeInMailOrOpenProfile(page);
    allowClickAnyway = messageBtnVisible && !paidSurface;
  }

  if (rs.status !== "connected" && !allowClickAnyway) {
    const paid = await looksLikeInMailOrOpenProfile(page);
    if (paid) return { actionTaken: "unavailable", relationshipStatus: "not_connected", details: "Messaging requires InMail/Open Profile (not 1st-degree)" };
    return { actionTaken: "unavailable", relationshipStatus: rs.status, details: "Message not available (not connected)" };
  }

  // 3) Open message UI
  const opened = await openMessageDialog(page);
  if (!opened.opened) return { actionTaken: "unavailable", relationshipStatus: rs.status === "connected" ? "connected" : "not_confirmed", details: "Message dialog not found" };

  // 4) Verify composer actually present (not upsell)
  const composerPresent = await anyVisible(
    page.locator('[data-test-conversation-compose]'),
    page.locator('.msg-overlay-conversation-bubble'),
    page.locator('.msg-form__contenteditable[contenteditable="true"]'),
    page.getByRole('textbox')
  );
  if (!composerPresent) return { actionTaken: "unavailable", relationshipStatus: "not_connected", details: "Opened non-message surface (likely InMail/upsell)" };

  // 5) Type & send
  await microDelay();
  const typed = await typeIntoComposer(page, messageText);
  if (!typed) return { actionTaken: "failed_to_type", relationshipStatus: "connected", details: "Could not type into composer" };

  await microDelay();
  const sent = await clickSendInComposer(page);
  if (sent) return { actionTaken: "sent", relationshipStatus: "connected", details: "Message sent" };
  return { actionTaken: "failed_to_send", relationshipStatus: "connected", details: "Failed to send message" };
}

// ---------- NEW: Acceptance scan helpers ----------
async function checkConnectedStatus(page, profileUrl) {
  const nav = await navigateProfileClean(page, profileUrl);
  if (!nav.authed) {
    return { ok: false, profileUrl, status: "nav_error", details: nav.error || "nav_failed", httpStatus: nav.status || null };
  }
  await waitFullLoad(page, NAV_TIMEOUT_MS);
  await briefProfileScroll(page, 1200);
  const rel = await detectRelationshipStatus(page);
  return { ok: true, profileUrl, status: rel.status || "unknown", reason: rel.reason || null };
}

// ---------- Job handlers ----------
async function handleAuthCheck(job) {
  const userId = job?.payload?.userId || "default";
  const userStatePath = statePathForUser(userId);
  if (SOFT_MODE) return { ok: true, via: "soft", message: "Soft auth ok" };
  let browser, context, page, video;
  try {
    ({ browser, context, page } = await createBrowserContext({ headless: HEADLESS, userStatePath }));
    video = page.video?.();
    const auth = await ensureAuthenticated(context, page, userStatePath);
    if (auth.ok) {
      await feedWarmup(page);
      await browser.close().catch(()=>{});
      if (video) { try { console.log("[video] saved:", await video.path()); } catch {} }
      return { ok: true, via: auth.via || "unknown", url: auth.url || null, message: "Authenticated and storageState saved." };
    } else {
      await browser.close().catch(()=>{});
      return { ok: false, reason: auth.reason || "guest_or_authwall", url: auth.url || null };
    }
  } catch (e) {
    try { await browser?.close(); } catch {}
    throw new Error(`AUTH_CHECK failed: ${e.message}`);
  }
}

async function handleSendConnection(job) {
  const p = job?.payload || {};
  const targetUrl = p.profileUrl || (p.publicIdentifier ? `https://www.linkedin.com/in/${encodeURIComponent(p.publicIdentifier)}/` : null);
  if (!targetUrl) throw new Error("payload.profileUrl or publicIdentifier required");
  const userId = p.userId || "default";

  // Window + caps gate
  const gate = gateWindowAndCaps(userId, "invite");
  if (!gate.ok) {
    console.log(`[gate] Requeue SEND_CONNECTION for ${userId}: ${gate.reason} requeueMs=${gate.requeueMs}`);
    throw { _softDefer: true, msg: `gate:${gate.reason}`, requeueMs: gate.requeueMs || REQUEUE_OUTSIDE_WINDOW_MS };
  }

  if (SOFT_MODE) {
    await throttle.reserve("linkedin.com", "SOFT send_connection");
    await microDelay();
    throttle.success("linkedin.com");
    bumpCounter(userId, "invite", 1);
    postMetricIncrement(userId, "invite", 1);
    return { mode: "soft", profileUrl: targetUrl, at: new Date().toISOString(), acceptanceCheckAfterHours: 24 };
  }

  await throttle.reserve("linkedin.com", "SEND_CONNECTION");
  const userStatePath = statePathForUser(userId);

  let browser, context, feedPage, profilePage, video;
  try {
    ({ browser, context, page: feedPage } = await createBrowserContext({ headless: HEADLESS, userStatePath }));
    video = feedPage.video?.();

    const auth = await ensureAuthenticated(context, feedPage, userStatePath);
    if (!auth.ok) {
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, actionTaken: "unavailable", details: "Not authenticated (authwall/guest)" };
    }

    await feedWarmup(feedPage);
    profilePage = await newPageInContext(context);

    const nav = await navigateProfileClean(profilePage, targetUrl);
    if (!nav.authed) {
      const details = nav.error || "Authwall/404/429 on profile nav.";
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl, httpStatus: nav.status, actionTaken: "unavailable", details };
    }

    if (!isProfileUrl(profilePage.url())) {
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, actionTaken: "unavailable", details: "Landed on non-profile page (redirected to feed)." };
    }

    await waitFullLoad(profilePage, NAV_TIMEOUT_MS);
    await briefProfileScroll(profilePage, 2000);
    await sleep(PROFILE_INITIAL_WAIT_MS);

    const hard = await detectHardScreen(profilePage);
    if (hard === "404" || hard === "429" || hard === "captcha") {
      const details = hard === "404" ? "Public profile URL returned 404." : hard === "429" ? "Hit LinkedIn 429 (rate-limited)." : "Encountered verification/captcha.";
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, actionTaken: hard === "404" ? "page_not_found" : "rate_limited", details };
    }

    const outcome = await sendConnectionRequest(profilePage, null);

    if (!isProfileUrl(profilePage.url())) {
      try { await profilePage.goBack({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS }).catch(()=>{}); } catch {}
      if (!isProfileUrl(profilePage.url())) {
        try { await profilePage.close().catch(()=>{}); } catch {}
        await browser.close().catch(()=>{});
        throttle.failure("linkedin.com");
        return { mode: "real", profileUrl: targetUrl, actionTaken: "unavailable", details: "Redirected off profile during attempt." };
      }
    }

    await sleep(3000);
    try { await profilePage.close().catch(()=>{}); } catch {}
    await browser.close().catch(()=>{});

    if (outcome.actionTaken?.startsWith("sent")) {
      throttle.success("linkedin.com");
      bumpCounter(userId, "invite", 1);
      postMetricIncrement(userId, "invite", 1);
    } else if (outcome.actionTaken === "failed_to_send" || outcome.actionTaken === "unavailable") {
      throttle.failure("linkedin.com");
    } else throttle.success("linkedin.com");

    return {
      mode: "real", profileUrl: targetUrl,
      actionTaken: outcome.actionTaken,
      relationshipStatus: outcome.relationshipStatus || "unknown",
      details: outcome.details, at: new Date().toISOString(),
      acceptanceCheckAfterHours: 24
    };
  } catch (e) {
    try { await profilePage?.close()?.catch(()=>{}); } catch {}
    try { await browser?.close(); } catch {}
    throttle.failure("linkedin.com");
    if (e && e._softDefer) throw e; // bubble up for requeue
    throw new Error(`SEND_CONNECTION failed: ${e.message}`);
  }
}

async function handleSendMessage(job) {
  const p = job?.payload || {};
  const targetUrl = p.profileUrl || (p.publicIdentifier ? `https://www.linkedin.com/in/${encodeURIComponent(p.publicIdentifier)}/` : null);
  if (!targetUrl) throw new Error("payload.profileUrl or publicIdentifier required");

  const messageText = p.message;
  if (!messageText) throw new Error("payload.message required");

  const userId = p.userId || "default";

  // Window + caps gate
  const gate = gateWindowAndCaps(userId, "message");
  if (!gate.ok) {
    console.log(`[gate] Requeue SEND_MESSAGE for ${userId}: ${gate.reason} requeueMs=${gate.requeueMs}`);
    throw { _softDefer: true, msg: `gate:${gate.reason}`, requeueMs: gate.requeueMs || REQUEUE_OUTSIDE_WINDOW_MS };
  }

  if (SOFT_MODE) {
    await throttle.reserve("linkedin.com", "SOFT send_message");
    await microDelay();
    throttle.success("linkedin.com");
    bumpCounter(userId, "message", 1);
    postMetricIncrement(userId, "message", 1);
    return { mode: "soft", profileUrl: targetUrl, messageUsed: messageText, at: new Date().toISOString() };
  }

  await throttle.reserve("linkedin.com", "SEND_MESSAGE");
  const userStatePath = statePathForUser(userId);

  let browser, context, feedPage, profilePage, video;
  try {
    ({ browser, context, page: feedPage } = await createBrowserContext({ headless: HEADLESS, userStatePath }));
    video = feedPage.video?.();

    const auth = await ensureAuthenticated(context, feedPage, userStatePath);
    if (!auth.ok) {
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, actionTaken: "unavailable", details: "Not authenticated (authwall/guest)" };
    }

    await feedWarmup(feedPage);
    profilePage = await newPageInContext(context);

    const nav = await navigateProfileClean(profilePage, targetUrl);
    if (!nav.authed) {
      const details = nav.error || "Authwall/404/429 on profile nav.";
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl, httpStatus: nav.status, actionTaken: "unavailable", details };
    }

    await waitFullLoad(profilePage, NAV_TIMEOUT_MS);
    await briefProfileScroll(profilePage, 2000);
    await sleep(PROFILE_INITIAL_WAIT_MS);

    const hard = await detectHardScreen(profilePage);
    if (hard === "404" || hard === "429" || hard === "captcha") {
      const details = hard === "404" ? "Public profile URL returned 404." : hard === "429" ? "Hit LinkedIn 429 (rate-limited)." : "Encountered verification/captcha.";
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, actionTaken: hard === "404" ? "page_not_found" : "rate_limited", details };
    }

    const outcome = await sendMessageFlow(profilePage, messageText);

    await sleep(1200);
    try { await profilePage.close().catch(()=>{}); } catch {}
    await browser.close().catch(()=>{});

    if (outcome.actionTaken === "sent") {
      throttle.success("linkedin.com");
      bumpCounter(userId, "message", 1);
      postMetricIncrement(userId, "message", 1);
    } else if (outcome.actionTaken?.startsWith("failed") || outcome.actionTaken === "unavailable") {
      throttle.failure("linkedin.com");
    } else throttle.success("linkedin.com");

    return {
      mode: "real", profileUrl: targetUrl,
      actionTaken: outcome.actionTaken,
      relationshipStatus: outcome.relationshipStatus || "unknown",
      details: outcome.details, at: new Date().toISOString(),
    };
  } catch (e) {
    try { await profilePage?.close()?.catch(()=>{}); } catch {}
    try { await browser?.close(); } catch {}
    throttle.failure("linkedin.com");
    if (e && e._softDefer) throw e; // bubble up to requeue
    throw new Error(`SEND_MESSAGE failed: ${e.message}`);
  }
}

// ---------- NEW: Acceptance scan handlers ----------
async function handleAcceptanceScan(job) {
  const p = job?.payload || {};
  const userId = p.userId || "default";
  const profiles = Array.isArray(p.profiles) ? p.profiles.slice(0, 50) : null; // optional list
  const windowHours = Number.isFinite(+p.windowHours) ? +p.windowHours : 72;

  if (SOFT_MODE) {
    return { mode: "soft", scanned: profiles ? profiles.length : 0, windowHours, at: new Date().toISOString() };
  }

  const userStatePath = statePathForUser(userId);
  let browser, context, feedPage, checkPage;
  try {
    ({ browser, context, page: feedPage } = await createBrowserContext({ headless: HEADLESS, userStatePath }));
    const auth = await ensureAuthenticated(context, feedPage, userStatePath);
    if (!auth.ok) {
      await browser.close().catch(()=>{});
      return { ok: false, reason: "auth_failed" };
    }
    await feedWarmup(feedPage);
    checkPage = await newPageInContext(context);

    const results = [];
    if (profiles && profiles.length) {
      for (const u of profiles) {
        await throttle.reserve("linkedin.com", "ACCEPTANCE_CHECK");
        const r = await checkConnectedStatus(checkPage, u);
        results.push(r);
        if (r.ok && r.status === "connected") {
          // (No side effects here; backend will move lead to Journey Navigators & enqueue message)
        }
        throttle.success("linkedin.com");
        await sleep(within(1200, 2500));
      }
    } else {
      // If no explicit profiles passed, we just noop-success — backend can use this as a timer tick.
    }

    try { await checkPage.close().catch(()=>{}); } catch {}
    await browser.close().catch(()=>{});
    return { ok: true, scanned: results.length, windowHours, results };
  } catch (e) {
    try { await checkPage?.close()?.catch(()=>{}); } catch {}
    try { await browser?.close(); } catch {}
    throw new Error(`ACCEPTANCE_SCAN failed: ${e.message}`);
  }
}

async function handleCheckConnected(job) {
  const p = job?.payload || {};
  const userId = p.userId || "default";
  const profileUrl = p.profileUrl;
  if (!profileUrl) throw new Error("payload.profileUrl required");

  if (SOFT_MODE) {
    return { mode: "soft", profileUrl, status: "unknown", at: new Date().toISOString() };
  }

  const userStatePath = statePathForUser(userId);
  let browser, context, page;
  try {
    ({ browser, context, page } = await createBrowserContext({ headless: HEADLESS, userStatePath }));
    const auth = await ensureAuthenticated(context, page, userStatePath);
    if (!auth.ok) {
      await browser.close().catch(()=>{});
      return { ok: false, profileUrl, status: "auth_failed" };
    }
    await feedWarmup(page);
    const result = await checkConnectedStatus(page, profileUrl);
    await browser.close().catch(()=>{});
    return result;
  } catch (e) {
    try { await browser?.close(); } catch {}
    throw new Error(`CHECK_CONNECTED failed: ${e.message}`);
  }
}

// ---------- Job loop ----------
async function processOne() {
  let next;
  try { next = await apiPost("/jobs/next", { types: ["AUTH_CHECK", "SEND_CONNECTION", "SEND_MESSAGE", "ACCEPTANCE_SCAN", "CHECK_CONNECTED"] }); }
  catch (e) { logFetchError("jobs/next", e); return; }
  const job = next?.job; if (!job) return;

  try {
    let result = null;
    switch (job.type) {
      case "AUTH_CHECK":       result = await handleAuthCheck(job); break;
      case "SEND_CONNECTION":  result = await handleSendConnection(job); break;
      case "SEND_MESSAGE":     result = await handleSendMessage(job); break;
      case "ACCEPTANCE_SCAN":  result = await handleAcceptanceScan(job); break;
      case "CHECK_CONNECTED":  result = await handleCheckConnected(job); break;
      default: result = { note: `Unhandled job type: ${job.type}` }; break;
    }
    try {
      await apiPost(`/jobs/${job.id}/complete`, { result });
      console.log("[worker] Job", job.id, "done:", result?.message || result?.details || result?.actionTaken || result?.note || "ok");
    } catch (e) { logFetchError(`jobs/${job.id}/complete`, e); }
  } catch (e) {
    const soft = e && e._softDefer;
    console.error("[worker] Job", job?.id, soft ? "soft-defer:" : "failed:", soft ? e.msg : e.message);
    try {
      await apiPost(`/jobs/${job.id}/fail`, {
        error: soft ? e.msg : e.message,
        requeue: !!soft,
        delayMs: soft ? (e.requeueMs || REQUEUE_OUTSIDE_WINDOW_MS) : 0
      });
    } catch (e2) { logFetchError(`jobs/${job.id}/fail`, e2); }
  }
}

async function mainLoop() {
  console.log("[worker] starting.", `API_BASE=${API_BASE}`, `Headless: ${HEADLESS}`, `SlowMo: ${SLOWMO_MS}ms`, `Soft mode: ${SOFT_MODE}`);
  console.log("[worker] window:", `${START_TIME_IST} + ${ACTIVE_WINDOW_HOURS}h`, "days:", ACTIVE_DAYS.join(","));
  console.log("[worker] caps:", `invites=${MAX_INVITES_PER_DAY} messages=${MAX_MESSAGES_PER_DAY} enforce=${ENFORCE_DAILY_CAPS}`);
  if (!WORKER_SHARED_SECRET) console.error("[worker] ERROR: WORKER_SHARED_SECRET is empty. Set it on both backend and worker!");
  try { const stats = await apiGet("/jobs/stats"); console.log("[worker] API OK. Stats:", stats?.counts || stats); }
  catch (e) { logFetchError("jobs/stats (startup)", e); }
  while (true) {
    try { await processOne(); } catch (e) { console.error("[worker] loop error:", e.message || e); }
    await sleep(POLL_INTERVAL_MS);
  }
}
mainLoop().catch((e) => { console.error("[worker] fatal:", e); process.exitCode = 1; });
