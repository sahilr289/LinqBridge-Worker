// worker.cjs — LinqBridge Worker (FINAL robust, sequential, misclick-guarded)
//
// What’s included (high-level):
// - Strict single-thread flow: FEED → slow human scroll → then open PROFILE tab
// - Auth & storageState per user (email)
// - Authwall recovery (login nudge + retry)
// - Mobile-first profile hop (env flag, fixed URL builder)
// - Degree-aware relationship detection (1st/2nd/3rd)
// - InMail/Open Profile recognition
// - Connect selection filtered (button-only, no anchors; no "View in Sales Navigator")
// - Sends plain invites by default (no note) — FORCE_NO_NOTES default true
// - Message flow only when truly 1st-degree
// - Per-domain throttle + gentle pacing
// - Optional HTTPS proxy
// - Safer fetch import, Mac/Win-friendly keybinds (Mod)
// - Misclick goBack recovery if a click navigates to people search
//
// ENV (tune as needed):
// API_BASE=https://your-backend.example.com
// WORKER_SHARED_SECRET=...            (must match backend)
// HEADLESS=false|true
// SLOWMO_MS=50
// SOFT_MODE=false                     (if true, simulate without browser)
// POLL_INTERVAL_MS=5000
// MAX_ACTIONS_PER_HOUR=18
// MIN_GAP_MS=60000
// COOLDOWN_AFTER_SENT_MS=90000
// COOLDOWN_AFTER_FAIL_MS=600000
// MICRO_DELAY_MIN_MS=400
// MICRO_DELAY_MAX_MS=1200
// FEED_INITIAL_WAIT_MS=0              (full-load wait is enforced separately)
// FEED_SCROLL_MS=0                    (we still do a 3–6s slow scroll even if 0)
// PROFILE_INITIAL_WAIT_MS=8000
// DEFAULT_TIMEOUT_MS=35000
// NAV_TIMEOUT_MS=45000
// USE_PROFILE_MOBILE_FIRST=true|false
// FORCE_NO_NOTES=true|false           (default true)
// STORAGE_STATE_PATH=/app/auth-state.json
// STATE_DIR=/app/state
// FORCE_RELOGIN=false
// ALLOW_INTERACTIVE_LOGIN=true
// INTERACTIVE_LOGIN_TIMEOUT_MS=300000
// PROXY_SERVER=http://user:pass@host:port   (optional)
// PROXY_USERNAME=...
// PROXY_PASSWORD=...

const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const { chromium } = require("playwright");

let fetchRef = global.fetch;
async function getFetch() {
  if (fetchRef) return fetchRef;
  try { fetchRef = (await import('node-fetch')).default; return fetchRef; } catch {}
  try { fetchRef = require('cross-fetch'); return fetchRef; } catch {}
  throw new Error('No fetch implementation available');
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
  console.error(`[worker] ${where} fetch failed:`, code, err?.message || err);
}
function apiUrl(p) { return p.startsWith("/") ? `${API_BASE}${p}` : `${API_BASE}/${p}`; }

async function apiGet(p) {
  const fetch = await getFetch();
  const res = await fetch(apiUrl(p), {
    method: "GET",
    headers: { "x-worker-secret": WORKER_SHARED_SECRET },
    signal: AbortSignal.timeout ? AbortSignal.timeout(15000) : undefined,
  });
  const text = await res.text();
  try {
    const json = JSON.parse(text);
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`);
    return json;
  } catch {
    throw new Error(`GET ${p} non-JSON or error ${res.status}: ${text}`);
  }
}
async function apiPost(p, body) {
  const fetch = await getFetch();
  const res = await fetch(apiUrl(p), {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-worker-secret": WORKER_SHARED_SECRET },
    body: JSON.stringify(body || {}),
    signal: AbortSignal.timeout ? AbortSignal.timeout(25000) : undefined,
  });
  const text = await res.text();
  try {
    const json = JSON.parse(text);
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`);
    return json;
  } catch {
    throw new Error(`POST ${p} non-JSON or error ${res.status}: ${text}`);
  }
}

async function anyVisible(...locs) {
  const checks = await Promise.all(locs.map(l => l.first().isVisible({ timeout: 800 }).catch(() => false)));
  return checks.some(Boolean);
}

async function waitFullLoad(page, timeout = 45000) {
  await page.waitForLoadState('domcontentloaded', { timeout }).catch(()=>{});
  try {
    await Promise.race([
      page.waitForLoadState('networkidle', { timeout: Math.min(timeout, 12000) }),
      sleep(6000)
    ]);
  } catch {}
}

async function slowHumanScroll(page, totalMs = 6000) {
  const start = Date.now();
  let y = 0;
  while (Date.now() - start < totalMs) {
    const step = within(20, 55);
    y += step;
    try { await page.mouse.wheel(0, step); } catch {}
    await sleep(within(140, 260));
  }
  await sleep(within(400, 900));
}

async function briefProfileScroll(page, totalMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < totalMs) {
    try { await page.mouse.wheel(0, within(80, 140)); } catch {}
    await sleep(within(120, 220));
  }
}

function looksLikeAuthRedirect(url) {
  return /\/uas\/login/i.test(url) || /\/checkpoint\//i.test(url);
}
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
  _get(domain) {
    if (!this.state.has(domain)) this.state.set(domain, { lastActionAt: 0, events: [], cooldownUntil: 0 });
    return this.state.get(domain);
  }
  _pruneOld(events) {
    const cutoff = now() - 3600_000;
    while (events.length && events[0] < cutoff) events.shift();
  }
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
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-features=IsolateOrigins,site-per-process",
      "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
      "--webrtc-stun-probe-trial=disabled",
    ],
  };
  if (PROXY_SERVER) {
    launchOpts.proxy = {
      server: PROXY_SERVER,
      username: PROXY_USERNAME || undefined,
      password: PROXY_PASSWORD || undefined,
    };
  }
  const browser = await chromium.launch(launchOpts);

  const vw = 1280 + Math.floor(Math.random() * 192);
  const vh = 720 + Math.floor(Math.random() * 160);

  const storageStateOpt =
    (!FORCE_RELOGIN && userStatePath && fs.existsSync(userStatePath)) ? userStatePath :
    (!FORCE_RELOGIN && fs.existsSync(DEFAULT_STATE_PATH) ? DEFAULT_STATE_PATH : undefined);

  const context = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    locale: "en-US",
    timezoneId: "America/Los_Angeles",
    viewport: { width: vw, height: vh },
    javaScriptEnabled: true,
    recordVideo: { dir: "/tmp/pw-video" },
    storageState: storageStateOpt,
  });

  await context.setExtraHTTPHeaders({
    "accept-language": "en-US,en;q=0.9",
    "upgrade-insecure-requests": "1",
    "sec-ch-ua": '"Chromium";v="124", "Not:A-Brand";v="8"',
    "sec-ch-ua-platform": '"Windows"',
    "sec-ch-ua-mobile": "?0",
    "referer": "https://www.google.com/"
  });

  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
    try {
      Object.defineProperty(navigator, "hardwareConcurrency", { get: () => 8 });
      Object.defineProperty(navigator, "language", { get: () => "en-US" });
      Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
      Object.defineProperty(navigator, "userAgent", { get: () =>
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
      });
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
  } catch (e) {
    console.log("[auth] storageState save failed:", e?.message || e);
  }
}

// ---------- Feed warmup ----------
async function feedWarmup(page) {
  try {
    if (!/linkedin\.com\/feed\/?$/i.test(page.url())) {
      console.log("[nav] → feed-desktop: https://www.linkedin.com/feed/");
      await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    }
    if (FEED_INITIAL_WAIT_MS > 0) {
      console.log(`[feed] initial wait ${FEED_INITIAL_WAIT_MS} ms`);
      await sleep(FEED_INITIAL_WAIT_MS);
    }
    console.log("[feed] waiting full load…");
    await waitFullLoad(page, NAV_TIMEOUT_MS);
    console.log("[feed] slow human scroll…");
    await slowHumanScroll(page, Math.max(3000, FEED_SCROLL_MS || 6000));
  } catch (e) {
    console.log("[feed] warmup error:", e?.message || e);
  }
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
  } catch (e) {
    console.log("[nav] feed-desktop error:", e?.message || e);
  }

  try {
    const r2 = await page.goto("https://m.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    const s2 = r2 ? r2.status() : null;
    console.log(`[nav] ✓ feed-mobile: status=${s2} final=${page.url()}`);
    if (s2 && s2 >= 200 && s2 < 400 && !(await isAuthWalledOrGuest(page))) {
      await saveStorageState(context, userStatePath || DEFAULT_STATE_PATH);
      return { ok: true, via: "mobile", url: page.url() };
    }
  } catch (e) {
    console.log("[nav] feed-mobile error:", e?.message || e);
  }

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
async function looksLikeInMailOrOpenProfile(page) {
  try {
    const hasMessage = await anyVisible(
      page.getByRole("button", { name: /^Message$/i }),
      page.getByRole("link",   { name: /^Message$/i })
    );
    const hasConnect = await anyVisible(
      page.getByRole("button", { name: /^Connect$/i }),
      page.locator('button:has-text("Connect")')
    );
    const inmailText = await page.getByText(/InMail/i).first().isVisible({ timeout: 600 }).catch(() => false);
    const openText   = await page.getByText(/Open Profile|Open to messages/i).first().isVisible({ timeout: 600 }).catch(() => false);
    return (inmailText || openText) && !hasConnect && hasMessage;
  } catch {}
  return false;
}
async function detectRelationshipStatus(page) {
  const pending = await anyVisible(
    page.getByRole("button", { name: /Pending|Requested|Withdraw|Pending invitation/i })
  );
  if (pending) return { status: "pending", reason: "Pending/Requested visible" };

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
function toMobileProfileUrl(u) {
  try {
    const url = new URL(u);
    url.hostname = "m.linkedin.com";
    return url.toString();
  } catch { return u; }
}
function isProfileUrl(u) {
  try { return /https?:\/\/([^.]+\.)?linkedin\.com\/(m\/)?in\//i.test(u); } catch { return false; }
}
async function navigateProfileClean(page, rawUrl) {
  const primary = String(rawUrl).replace("linkedin.com//", "linkedin.com/");
  const mobile  = toMobileProfileUrl(primary);
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
        } catch (e) {
          console.log("[nav] authwall re-auth failed:", e?.message || e);
        }
        const final2 = page.url();
        const hard2 = await detectHardScreen(page);
        const stillAuthwall = /\/authwall/i.test(final2);
        if (!stillAuthwall && !hard2 && isProfileUrl(final2)) {
          return { authed: true, status: 200, usedUrl: u, finalUrl: final2 };
        }
      } else {
        const authed = !(await isAuthWalledOrGuest(page));
        if (authed && status && status >= 200 && status < 400 && !hard && isProfileUrl(finalUrl)) {
          return { authed: true, status, usedUrl: u, finalUrl };
        }
      }

      if (i === tries.length - 1) {
        return { authed: false, status, usedUrl: u, finalUrl, error: isAuthwall ? "authwall" : (isProfileUrl(finalUrl) ? "authwall_or_unknown" : "not_profile") };
      }
    } catch (e) {
      const finalUrl = page.url();
      if (i === tries.length - 1) {
        const msg = e?.message || "nav_failed";
        if (msg.toLowerCase().includes("too many redirects")) {
          return { authed: false, status: null, usedUrl: tries[i], finalUrl, error: "redirect_loop" };
        }
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

  // 1) Try "More → Connect" first (less likely to be confused with links)
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

        // If menu button navigated (rare), guard.
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

              // Misclick recovery after menu item
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

  // 2) Try primary "Connect" button (button-only + exact text)
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

      // Skip anchors entirely (belt & suspenders)
      const isAnchor = await cand.evaluate(el => el.tagName.toLowerCase() === "a").catch(()=>false);
      if (isAnchor) continue;

      await sprout('connect-primary');
      const beforeUrl = page.url();
      await cand.click({ timeout: 4000 });
      await microDelay();

      // Misclick recovery if it navigated to people search
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

  // 3) Mobile fallback: button-only, exact name
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

async function sendConnectionRequest(page, note) {
  const rs1 = await detectRelationshipStatus(page);
  if (rs1.status === "connected") return { actionTaken: "none", relationshipStatus: "connected", details: "Already 1st-degree" };
  if (rs1.status === "pending")   return { actionTaken: "none", relationshipStatus: "pending",   details: "Invitation already pending" };

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

  const direct = [
    page.getByRole("button", { name: /^Message$/i }),
    page.getByRole("link",   { name: /^Message$/i }),
    page.locator('button[aria-label="Message"]'),
    page.locator('a[data-control-name*="message"]'),
    page.locator('button:has-text("Message")'),
  ];
  for (const h of direct) {
    try {
      const handle = h.first();
      if (await handle.isVisible({ timeout: 1200 }).catch(() => false)) {
        await sprout('open-message');
        await handle.click({ timeout: 4000 }); await microDelay();
        const ready = await Promise.race([
          page.getByRole("dialog").waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
          page.locator('.msg-overlay-conversation-bubble').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
          page.locator('[data-test-conversation-compose], .msg-form__contenteditable').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
        ]);
        if (ready) return { opened: true, via: "primary" };
      }
    } catch {}
  }

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
        await handle.click({ timeout: 4000 }); await microDelay();
        const menuMsg = [
          page.getByRole("menuitem", { name: /^Message$/i }).first(),
          page.locator('div[role="menuitem"]:has-text("Message")').first(),
          page.locator('span:has-text("Message")').locator('xpath=ancestor-or-self::*[@role="menuitem"]').first(),
        ];
        for (const mi of menuMsg) {
          if (await mi.isVisible({ timeout: 1200 }).catch(() => false)) {
            await sprout('click-message-menu');
            await mi.click({ timeout: 4000 }); await microDelay();
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
      await mobileMsg.first().click({ timeout: 4000 }); await microDelay();
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
        if (tag === "textarea" || tag === "input") {
          await handle.fill(limited, { timeout: 4000 });
        } else {
          await handle.press("Mod+A").catch(()=>{});
          await handle.type(limited, { delay: within(5, 25) });
        }
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
        await handle.click({ timeout: 4000 }); await microDelay();
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
async function sendMessageFlow(page, messageText) {
  const rs = await detectRelationshipStatus(page);
  if (rs.status !== "connected") {
    const paid = await looksLikeInMailOrOpenProfile(page);
    if (paid) {
      return { actionTaken: "unavailable", relationshipStatus: "not_connected", details: "Messaging requires InMail/Open Profile (not 1st-degree)" };
    }
    return { actionTaken: "unavailable", relationshipStatus: rs.status, details: "Message not available (not connected)" };
  }
  const opened = await openMessageDialog(page);
  if (!opened.opened) return { actionTaken: "unavailable", relationshipStatus: "connected", details: "Message dialog not found" };
  await microDelay();
  const typed = await typeIntoComposer(page, messageText);
  if (!typed) return { actionTaken: "failed_to_type", relationshipStatus: "connected", details: "Could not type into composer" };
  await microDelay();
  const sent = await clickSendInComposer(page);
  if (sent) return { actionTaken: "sent", relationshipStatus: "connected", details: "Message sent" };
  return { actionTaken: "failed_to_send", relationshipStatus: "connected", details: "Failed to send message" };
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

  if (SOFT_MODE) { await throttle.reserve("linkedin.com", "SOFT send_connection"); await microDelay(); throttle.success("linkedin.com"); return { mode: "soft", profileUrl: targetUrl, at: new Date().toISOString() }; }

  await throttle.reserve("linkedin.com", "SEND_CONNECTION");

  const userId = p.userId || "default";
  const userStatePath = statePathForUser(userId);

  let browser, context, feedPage, profilePage, video;
  try {
    ({ browser, context, page: feedPage } = await createBrowserContext({ headless: HEADLESS, userStatePath }));
    video = feedPage.video?.();

    // 1) auth on FEED, single tab only
    const auth = await ensureAuthenticated(context, feedPage, userStatePath);
    if (!auth.ok) {
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, actionTaken: "unavailable", details: "Not authenticated (authwall/guest)" };
    }

    // 2) Full load + slow human scroll
    await feedWarmup(feedPage);

    // 3) Open PROFILE tab only now
    profilePage = await newPageInContext(context);

    // 4) Navigate to profile
    const nav = await navigateProfileClean(profilePage, targetUrl);
    if (!nav.authed) {
      const details = nav.error || "Authwall/404/429 on profile nav.";
      console.log(`[nav] failed. finalUrl=${nav.finalUrl} error=${details}`);
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl, httpStatus: nav.status, actionTaken: "unavailable", details };
    }

    // extra guard: ensure we're actually on a profile page, not feed
    if (!isProfileUrl(profilePage.url())) {
      console.log(`[nav] landed on non-profile page (${profilePage.url()}). Aborting SEND_CONNECTION.`);
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, actionTaken: "unavailable", details: "Landed on non-profile page (redirected to feed)." };
    }

    // 5) brief profile scroll (~2s) then try connect
    await waitFullLoad(profilePage, NAV_TIMEOUT_MS);
    await briefProfileScroll(profilePage, 2000);

    await sleep(PROFILE_INITIAL_WAIT_MS);
    const hard = await detectHardScreen(profilePage);
    if (hard === "404" || hard === "429" || hard === "captcha") {
      const details = hard === "404" ? "Public profile URL returned 404."
                    : hard === "429" ? "Hit LinkedIn 429 (rate-limited)."
                    : "Encountered verification/captcha.";
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, actionTaken: hard === "404" ? "page_not_found" : "rate_limited", details };
    }

    // 6) send connection WITHOUT note
    const outcome = await sendConnectionRequest(profilePage, null);

    // verify still on a profile
    if (!isProfileUrl(profilePage.url())) {
      console.log(`[guard] non-profile after attempt: ${profilePage.url()}`);
      try { await profilePage.goBack({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS }).catch(()=>{}); } catch {}
      if (!isProfileUrl(profilePage.url())) {
        try { await profilePage.close().catch(()=>{}); } catch {}
        await browser.close().catch(()=>{});
        throttle.failure("linkedin.com");
        return { mode: "real", profileUrl: targetUrl, actionTaken: "unavailable", details: "Redirected off profile during attempt." };
      }
    }

    // 7) linger a bit
    await sleep(3000);

    try { await profilePage.close().catch(()=>{}); } catch {}
    await browser.close().catch(()=>{});

    if (outcome.actionTaken?.startsWith("sent")) throttle.success("linkedin.com");
    else if (outcome.actionTaken === "failed_to_send" || outcome.actionTaken === "unavailable") throttle.failure("linkedin.com");
    else throttle.success("linkedin.com");

    return {
      mode: "real",
      profileUrl: targetUrl,
      actionTaken: outcome.actionTaken,
      relationshipStatus: outcome.relationshipStatus || "unknown",
      details: outcome.details,
      at: new Date().toISOString()
    };
  } catch (e) {
    try { await profilePage?.close()?.catch(()=>{}); } catch {}
    try { await browser?.close(); } catch {}
    throttle.failure("linkedin.com");
    throw new Error(`SEND_CONNECTION failed: ${e.message}`);
  }
}

async function handleSendMessage(job) {
  const p = job?.payload || {};
  const targetUrl = p.profileUrl || (p.publicIdentifier ? `https://www.linkedin.com/in/${encodeURIComponent(p.publicIdentifier)}/` : null);
  if (!targetUrl) throw new Error("payload.profileUrl or publicIdentifier required");
  const messageText = p.message;
  if (!messageText) throw new Error("payload.message required");

  if (SOFT_MODE) { await throttle.reserve("linkedin.com", "SOFT send_message"); await microDelay(); throttle.success("linkedin.com"); return { mode: "soft", profileUrl: targetUrl, messageUsed: messageText, at: new Date().toISOString() }; }

  await throttle.reserve("linkedin.com", "SEND_MESSAGE");

  const userId = p.userId || "default";
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
      const details = hard === "404" ? "Public profile URL returned 404."
                    : hard === "429" ? "Hit LinkedIn 429 (rate-limited)."
                    : "Encountered verification/captcha.";
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { mode: "real", profileUrl: targetUrl, actionTaken: hard === "404" ? "page_not_found" : "rate_limited", details };
    }

    const outcome = await sendMessageFlow(profilePage, messageText);
    await sleep(1200);
    try { await profilePage.close().catch(()=>{}); } catch {}
    await browser.close().catch(()=>{});

    if (outcome.actionTaken === "sent") throttle.success("linkedin.com");
    else if (outcome.actionTaken?.startsWith("failed") || outcome.actionTaken === "unavailable") throttle.failure("linkedin.com");
    else throttle.success("linkedin.com");

    return {
      mode: "real",
      profileUrl: targetUrl,
      actionTaken: outcome.actionTaken,
      relationshipStatus: outcome.relationshipStatus || "unknown",
      details: outcome.details,
      at: new Date().toISOString()
    };
  } catch (e) {
    try { await profilePage?.close()?.catch(()=>{}); } catch {}
    try { await browser?.close(); } catch {}
    throttle.failure("linkedin.com");
    throw new Error(`SEND_MESSAGE failed: ${e.message}`);
  }
}

// ---------- Job loop ----------
async function processOne() {
  let next;
  try {
    next = await apiPost("/jobs/next", { types: ["AUTH_CHECK", "SEND_CONNECTION", "SEND_MESSAGE"] });
  } catch (e) {
    logFetchError("jobs/next", e);
    return;
  }
  const job = next?.job;
  if (!job) return;

  try {
    let result = null;
    switch (job.type) {
      case "AUTH_CHECK":      result = await handleAuthCheck(job); break;
      case "SEND_CONNECTION": result = await handleSendConnection(job); break;
      case "SEND_MESSAGE":    result = await handleSendMessage(job); break;
      default: result = { note: `Unhandled job type: ${job.type}` }; break;
    }
    try {
      await apiPost(`/jobs/${job.id}/complete`, { result });
      console.log(`[worker] Job ${job.id} done:`, result?.message || result?.details || result?.actionTaken || result?.note || "ok");
    } catch (e) {
      logFetchError(`jobs/${job.id}/complete`, e);
    }
  } catch (e) {
    console.error(`[worker] Job ${job?.id} failed:`, e.message);
    try {
      await apiPost(`/jobs/${job.id}/fail`, { error: e.message, requeue: false, delayMs: 0 });
    } catch (e2) {
      logFetchError(`jobs/${job.id}/fail`, e2);
    }
  }
}

async function mainLoop() {
  console.log(`[worker] starting. API_BASE=${API_BASE} Headless: ${HEADLESS} SlowMo: ${SLOWMO_MS}ms Soft mode: ${SOFT_MODE}`);
  if (!WORKER_SHARED_SECRET) console.error("[worker] ERROR: WORKER_SHARED_SECRET is empty. Set it on both backend and worker!");

  try {
    const stats = await apiGet("/jobs/stats");
    console.log("[worker] API OK. Stats:", stats?.counts || stats);
  } catch (e) {
    logFetchError("jobs/stats (startup)", e);
  }

  while (true) {
    try { await processOne(); }
    catch (e) { console.error("[worker] loop error:", e.message || e); }
    await sleep(POLL_INTERVAL_MS);
  }
}

mainLoop().catch((e) => { console.error("[worker] fatal:", e); process.exitCode = 1; });
