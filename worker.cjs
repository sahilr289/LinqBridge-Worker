// worker.cjs — LinqBridge Worker (FINAL: strict auth, creds+TOTP login, overlay/details fallback, 429/404 guard)

"use strict";

const path = require("path");
const fs = require("fs");
let chromium = null; // lazy import
const { authenticator } = require("otplib");

// -------------------------
// Optional health server
// -------------------------
if (process.env.ENABLE_HEALTH === "true") {
  try {
    const http = require("http");
    const HEALTH_PORT = parseInt(process.env.HEALTH_PORT || "3001", 10);
    http
      .createServer((req, res) => {
        if (req.url === "/" || req.url === "/health") {
          res.writeHead(200, { "content-type": "text/plain" });
          res.end("OK\n");
        } else { res.writeHead(404); res.end(); }
      })
      .listen(HEALTH_PORT, () => console.log(`[health] listening on :${HEALTH_PORT}`));
  } catch (e) {
    console.log("[health] server not started:", e?.message || e);
  }
}

// =========================
// Env & Config
// =========================
const API_BASE = process.env.API_BASE || "https://YOUR-BACKEND.example.com";
const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET || "";

// Browser execution
const HEADLESS = (/^(true|1|yes)$/i).test(process.env.HEADLESS || "false");
const SLOWMO_MS = parseInt(process.env.SLOWMO_MS || (HEADLESS ? "0" : "50"), 10);
const SOFT_MODE = (/^(true|1|yes)$/i).test(process.env.SOFT_MODE || "false");

// Polling loop for jobs
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000", 10);

// Human pacing (conservative)
const MAX_ACTIONS_PER_HOUR = parseInt(process.env.MAX_ACTIONS_PER_HOUR || "18", 10);
const MIN_GAP_MS = parseInt(process.env.MIN_GAP_MS || "60000", 10);
const COOLDOWN_AFTER_SENT_MS = parseInt(process.env.COOLDOWN_AFTER_SENT_MS || "90000", 10);
const COOLDOWN_AFTER_FAIL_MS = parseInt(process.env.COOLDOWN_AFTER_FAIL_MS || "600000", 10);

// Micro delays
const MICRO_DELAY_MIN_MS = parseInt(process.env.MICRO_DELAY_MIN_MS || "400", 10);
const MICRO_DELAY_MAX_MS = parseInt(process.env.MICRO_DELAY_MAX_MS || "1200", 10);

// Per spec
const FEED_INITIAL_WAIT_MS = parseInt(process.env.FEED_INITIAL_WAIT_MS || "12000", 10);
const FEED_SCROLL_MS = parseInt(process.env.FEED_SCROLL_MS || "6000", 10);
const PROFILE_INITIAL_WAIT_MS = parseInt(process.env.PROFILE_INITIAL_WAIT_MS || "10000", 10);

// Timeouts
const DEFAULT_TIMEOUT_MS = parseInt(process.env.DEFAULT_TIMEOUT_MS || "35000", 10);
const NAV_TIMEOUT_MS = parseInt(process.env.NAV_TIMEOUT_MS || "45000", 10);

// Interactive login allowed?
const ALLOW_INTERACTIVE_LOGIN = (/^(true|1|yes)$/i).test(process.env.ALLOW_INTERACTIVE_LOGIN || "false"); // default false

// Session persistence
const DEFAULT_STATE_PATH = process.env.STORAGE_STATE_PATH || "/app/auth-state.json";
const STATE_DIR = process.env.STATE_DIR || "/app/state";
const FORCE_RELOGIN = (/^(true|1|yes)$/i).test(process.env.FORCE_RELOGIN || "false");

// Proxy (global)
const PROXY_SERVER = process.env.PROXY_SERVER || "";
const PROXY_USERNAME = process.env.PROXY_USERNAME || "";
const PROXY_PASSWORD = process.env.PROXY_PASSWORD || "";

// =========================
// Small utils
// =========================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();
const within = (min, max) => min + Math.floor(Math.random() * (max - min + 1));
async function microDelay() { await sleep(within(MICRO_DELAY_MIN_MS, MICRO_DELAY_MAX_MS)); }

const sanitizeUserId = (s) => (String(s || "default").toLowerCase().replace(/[^a-z0-9]+/g, "_"));
const statePathForUser = (userId) => path.join(STATE_DIR, `${sanitizeUserId(userId)}.json`);

function logFetchError(where, err) {
  const code = err?.cause?.code || err?.code || "unknown";
  console.error(`[worker] ${where} fetch failed:`, code, err?.message || err);
}

function apiUrl(p) { return p.startsWith("/") ? `${API_BASE}${p}` : `${API_BASE}/${p}`; }

async function apiGet(p) {
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

// =========================
/** Per-domain throttle */
// =========================
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

// =========================
// Playwright helpers
// =========================
async function createBrowserContext({ cookieBundle, headless, userStatePath }) {
  if (!chromium) ({ chromium } = require("playwright"));
  try { await fs.promises.mkdir(path.dirname(userStatePath || DEFAULT_STATE_PATH), { recursive: true }); } catch {}

  const launchOpts = {
    headless: !!headless,
    slowMo: SLOWMO_MS,
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-features=IsolateOrigins,site-per-process",
      "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
      "--webrtc-stun-probe-trial=disabled"
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

  const storageStateOpt = (!FORCE_RELOGIN && userStatePath && fs.existsSync(userStatePath))
    ? userStatePath
    : (!FORCE_RELOGIN && fs.existsSync(DEFAULT_STATE_PATH) ? DEFAULT_STATE_PATH : undefined);

  const context = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    locale: "en-US",
    timezoneId: "America/Los_Angeles",
    colorScheme: "light",
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
      const originalQuery = navigator.permissions?.query?.bind(navigator.permissions);
      if (originalQuery) {
        navigator.permissions.query = (p) =>
          p?.name === "notifications" ? Promise.resolve({ state: "denied" }) : originalQuery(p);
      }
    } catch {}
  });

  const page = await context.newPage();
  page.setDefaultTimeout(DEFAULT_TIMEOUT_MS);
  page.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
  try { await page.bringToFront(); } catch {}

  // Optional cookie bundle (augments storageState)
  if (cookieBundle && (cookieBundle.li_at || cookieBundle.jsessionid || cookieBundle.bcookie)) {
    const expand = (cookie) => ([
      { ...cookie, domain: "linkedin.com" },
      { ...cookie, domain: "www.linkedin.com" },
      { ...cookie, domain: "m.linkedin.com" },
    ]);
    const cookies = [];
    if (cookieBundle.li_at) {
      cookies.push(...expand({ name: "li_at", value: cookieBundle.li_at, path: "/", httpOnly: true, secure: true, sameSite: "None" }));
    }
    if (cookieBundle.jsessionid) {
      cookies.push(...expand({ name: "JSESSIONID", value: `"${cookieBundle.jsessionid}"`, path: "/", httpOnly: true, secure: true, sameSite: "None" }));
    }
    if (cookieBundle.bcookie) {
      cookies.push(...expand({ name: "bcookie", value: cookieBundle.bcookie, path: "/", httpOnly: false, secure: true, sameSite: "None" }));
    }
    try { await context.addCookies(cookies); } catch {}
  }

  return { browser, context, page };
}

async function newPageInContext(context) {
  const p = await context.newPage();
  p.setDefaultTimeout(DEFAULT_TIMEOUT_MS);
  p.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
  try { await p.bringToFront(); } catch {}
  return p;
}

async function cookieDiag(context) {
  const all = await context.cookies("https://www.linkedin.com");
  const pick = (n) => all.find(c => c.name === n);
  const js = pick("JSESSIONID");
  return {
    has_li_at: !!pick("li_at"),
    has_JSESSIONID: !!js,
    JSESSIONID_quoted: js ? /^".*"$/.test(js.value || "") : null,
    has_bcookie: !!pick("bcookie"),
  };
}

async function saveStorageState(context, outPath) {
  try {
    await fs.promises.mkdir(path.dirname(outPath), { recursive: true }).catch(() => {});
    await context.storageState({ path: outPath });
    console.log("[auth] storageState saved to", outPath);
  } catch (e) {
    console.log("[auth] storageState save failed:", e?.message || e);
  }
}

function looksLikeAuthRedirect(url) {
  return /\/uas\/login/i.test(url) || /\/checkpoint\//i.test(url) || /\/login(?:[/?#]|$)/i.test(url);
}

async function isAuthWalledOrGuest(page) {
  try {
    const url = page.url() || "";
    if (looksLikeAuthRedirect(url)) return true;
    const title = (await page.title().catch(() => ""))?.toLowerCase?.() || "";
    if (title.includes("sign in") || title.includes("join linkedin") || title.includes("authwall")) return true;
    const body = await page.locator("body").innerText().catch(() => "");
    if (/sign in/i.test(body) || /authwall/i.test(body)) return true;
    const hasLogin = await page.locator('a[href*="/login"]').first().isVisible({ timeout: 600 }).catch(() => false);
    if (hasLogin) return true;
    return false;
  } catch { return true; }
}

async function ensureAuthenticated(context, page, userStatePath) {
  const before = await cookieDiag(context);

  // Desktop feed
  try {
    const r = await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    const s = r ? r.status() : null;
    console.log(`[nav] ✓ feed-desktop: status=${s} final=${page.url()}`);
    if (s && s >= 200 && s < 400 && !(await isAuthWalledOrGuest(page))) {
      await saveStorageState(context, userStatePath || DEFAULT_STATE_PATH);
      return { ok: true, via: "desktop", status: s, url: page.url(), diag: before };
    }
  } catch (e) {
    console.log("[nav] feed-desktop error:", e?.message || e);
  }

  // Mobile feed
  try {
    const r = await page.goto("https://m.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    const s = r ? r.status() : null;
    console.log(`[nav] ✓ feed-mobile: status=${s} final=${page.url()}`);
    if (s && s >= 200 && s < 400 && !(await isAuthWalledOrGuest(page))) {
      await saveStorageState(context, userStatePath || DEFAULT_STATE_PATH);
      return { ok: true, via: "mobile", status: s, url: page.url(), diag: before };
    }
  } catch (e) {
    console.log("[nav] feed-mobile error:", e?.message || e);
  }

  if (!ALLOW_INTERACTIVE_LOGIN) {
    return { ok: false, reason: "guest_or_authwall", url: page.url(), diag: before };
  }

  // Interactive login (disabled by default)
  try {
    console.log("[nav] → login: https://www.linkedin.com/login");
    const r = await page.goto("https://www.linkedin.com/login", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    console.log(`[nav] ✓ login: status=${r ? r.status() : "n/a"} final=${page.url()}`);
  } catch {}
  const deadline = Date.now() + 300000; // 5 min
  while (Date.now() < deadline) {
    await sleep(1500);
    const guest = await isAuthWalledOrGuest(page);
    if (!guest && !looksLikeAuthRedirect(page.url())) {
      await saveStorageState(context, userStatePath || DEFAULT_STATE_PATH);
      return { ok: true, via: "interactive", url: page.url(), diag: before };
    }
  }
  return { ok: false, reason: "interactive_timeout", url: page.url(), diag: before };
}

// ========= URL helpers =========
function extractInSlug(u) {
  const m = String(u || "").match(/linkedin\.com\/in\/([^\/?#]+)/i);
  return m ? decodeURIComponent(m[1]) : null;
}
function looksLikeUrnSlug(slug) {
  return /^AC[ow]/.test(String(slug || ""));
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

// open helper
async function tryOpen(page, url) {
  try {
    const resp = await page.goto(url, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    const status = resp ? resp.status() : null;
    const hard = await detectHardScreen(page);
    const authed = !(await isAuthWalledOrGuest(page));
    return { status, hard, authed, finalUrl: page.url() };
  } catch (e) {
    return { status: null, hard: null, authed: false, finalUrl: page.url(), error: e?.message || "nav_failed" };
  }
}

// Clean navigation with fallbacks
async function navigateProfileClean(page, rawUrl) {
  const slug = extractInSlug(rawUrl);

  // 1) primary
  {
    console.log(`[nav] → profile-primary: ${rawUrl}`);
    const r = await tryOpen(page, rawUrl);
    if (r.hard === "429") return { authed: false, status: 429, usedUrl: rawUrl, finalUrl: r.finalUrl, error: "rate_limited" };
    if (r.hard === "404") return { authed: false, status: r.status || 404, usedUrl: rawUrl, finalUrl: r.finalUrl, error: "not_found" };
    if (r.authed && r.status && r.status >= 200 && r.status < 400 && !r.hard) {
      console.log(`[nav] ✓ profile: status=${r.status} final=${r.finalUrl}`);
      return { authed: true, status: r.status, usedUrl: rawUrl, finalUrl: r.finalUrl };
    }
  }

  // 2) contact-info overlay
  if (slug) {
    const overlay = `https://www.linkedin.com/in/${encodeURIComponent(slug)}/overlay/contact-info/`;
    console.log(`[nav] → contact-info overlay: ${overlay}`);
    const r2 = await tryOpen(page, overlay);
    if (!["404","429","captcha"].includes(r2.hard)) {
      // small settle
      await sleep(1200);
      console.log("[nav] ↩ retry main profile after overlay");
      const r2b = await tryOpen(page, `https://www.linkedin.com/in/${encodeURIComponent(slug)}/`);
      if (r2b.authed && r2b.status && r2b.status >= 200 && r2b.status < 400 && !r2b.hard) {
        console.log(`[nav] ✓ profile after overlay: status=${r2b.status} final=${r2b.finalUrl}`);
        return { authed: true, status: r2b.status, usedUrl: overlay, finalUrl: r2b.finalUrl };
      }
    }
  }

  // 3) details/experience probe + retry
  if (slug) {
    const details = `https://www.linkedin.com/in/${encodeURIComponent(slug)}/details/experience/`;
    console.log(`[nav] → details/experience: ${details}`);
    const r3 = await tryOpen(page, details);
    if (r3.authed && r3.status && r3.status >= 200 && r3.status < 400 && !r3.hard) {
      await sleep(1200);
      console.log("[nav] ↩ retry main profile after details");
      const r4 = await tryOpen(page, `https://www.linkedin.com/in/${encodeURIComponent(slug)}/`);
      if (r4.authed && r4.status && r4.status >= 200 && r4.status < 400 && !r4.hard) {
        console.log(`[nav] ✓ profile after details: status=${r4.status} final=${r4.finalUrl}`);
        return { authed: true, status: r4.status, usedUrl: details, finalUrl: r4.finalUrl };
      }
    }
  }

  return { authed: false, status: null, usedUrl: rawUrl, finalUrl: page.url(), error: "authwall_or_unknown" };
}

// Human scroll helper
async function humanScroll(page, durationMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < durationMs) {
    try { await page.mouse.wheel(0, within(120, 320)); } catch {}
    await sleep(within(120, 220));
  }
}

// FEED warmup
async function feedWarmup(page) {
  try {
    if (!/linkedin\.com\/feed\/?$/i.test(page.url())) {
      console.log("[nav] → feed-desktop: https://www.linkedin.com/feed/");
      await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    }
    console.log(`[feed] warmup: waiting ${FEED_INITIAL_WAIT_MS} ms, then scrolling ${FEED_SCROLL_MS} ms`);
    await sleep(FEED_INITIAL_WAIT_MS);
    await humanScroll(page, FEED_SCROLL_MS);
  } catch (e) {
    console.log("[feed] warmup error:", e?.message || e);
  }
}

// Expand helpers
async function scrollAndExpand(page, sectionLocator) {
  try {
    await sectionLocator.scrollIntoViewIfNeeded().catch(()=>{});
    await sleep(300 + Math.random()*400);
    const seeMore = sectionLocator.getByRole("button", { name: /see more/i }).first();
    const exists = await seeMore.isVisible({ timeout: 1200 }).catch(() => false);
    if (exists) {
      await seeMore.click({ timeout: 4000 }).catch(()=>{});
      await sleep(500 + Math.random()*700);
    }
  } catch {}
}
async function safeInnerText(loc) {
  try {
    if (await loc.first().isVisible({ timeout: 1800 }).catch(()=>false)) {
      const txt = await loc.first().evaluate(el => (el.innerText || "").trim()).catch(()=> "");
      return (txt || "").trim();
    }
  } catch {}
  return "";
}

// Scrape About + current role
async function scrapeAboutAndCurrentRole(page) {
  await sleep(PROFILE_INITIAL_WAIT_MS);

  const aboutSection = page.locator('section[id="about"], section:has(h2:has-text("About"))').first();
  await scrollAndExpand(page, aboutSection);
  const aboutText = (await safeInnerText(
    aboutSection.locator('.inline-show-more-text, .display-flex, .pv-shared-text-with-see-more, [data-test="about-section"]').first()
  )) || (await safeInnerText(aboutSection));

  const expSection = page.locator('section[id="experience"], section:has(h2:has-text("Experience"))').first();
  await expSection.scrollIntoViewIfNeeded().catch(()=>{});
  await sleep(500 + Math.random()*700);
  const firstItem = expSection.locator('li.pvs-list__paged-list-item, li').first();
  await scrollAndExpand(page, firstItem);
  const currentRoleText =
    (await safeInnerText(firstItem.locator('.inline-show-more-text, .pvs-list__outer-container, [data-test="experience-item"]').first())) ||
    (await safeInnerText(firstItem));

  const clip = (s, n=4000) => (s && s.length > n ? s.slice(0, n) : s) || "";
  return { about: clip(aboutText), currentRole: clip(currentRoleText) };
}

// Relationship & Connect flow
async function detectRelationshipStatus(page) {
  const checks = [
    { type: "connected",    loc: page.getByRole("button", { name: /^Message$/i }) },
    { type: "connected",    loc: page.getByRole("link",   { name: /^Message$/i }) },
    { type: "pending",      loc: page.getByRole("button", { name: /Pending|Requested|Withdraw|Pending invitation/i }) },
    { type: "pending",      loc: page.locator('text=/Pending invitation/i') },
    { type: "can_connect",  loc: page.getByRole("button", { name: /^Connect$/i }) },
    { type: "can_connect",  loc: page.locator('button:has-text("Connect"), a:has-text("Connect")') },
  ];
  for (const c of checks) {
    try {
      const visible = await c.loc.first().isVisible({ timeout: 1200 }).catch(() => false);
      if (visible) {
        if (c.type === "connected")    return { status: "connected",    reason: "Message CTA visible" };
        if (c.type === "pending")      return { status: "pending",      reason: "Pending/Requested visible" };
        if (c.type === "can_connect")  return { status: "not_connected" };
      }
    } catch {}
  }
  return { status: "not_connected", reason: "Connect may be under More menu or mobile UI" };
}

async function openConnectDialog(page) {
  await microDelay();
  const directButtons = [
    page.getByRole("button", { name: /^Connect$/i }),
    page.locator('button:has-text("Connect")'),
    page.getByRole("link",   { name: /^Connect$/i }),
  ];
  for (const b of directButtons) {
    try {
      if (await b.first().isVisible({ timeout: 1000 }).catch(() => false)) {
        await b.first().click({ timeout: 4000 });
        await microDelay();
        const dialogReady = await Promise.race([
          page.getByRole("dialog").waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
          page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
          page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
        ]);
        if (dialogReady) return { opened: true, via: "primary" };
      }
    } catch {}
  }
  const moreCandidates = [
    page.getByRole("button", { name: /^More$/i }),
    page.getByRole("button", { name: /More actions/i }),
    page.locator('button[aria-label="More actions"]'),
  ];
  for (const m of moreCandidates) {
    try {
      if (await m.first().isVisible({ timeout: 1200 }).catch(() => false)) {
        await m.first().click({ timeout: 4000 });
        await microDelay();
        const menuConnect = [
          page.getByRole("menuitem", { name: /^Connect$/i }),
          page.locator('div[role="menuitem"]:has-text("Connect")'),
          page.locator('span:has-text("Connect")'),
        ];
        for (const mi of menuConnect) {
          if (await mi.first().isVisible({ timeout: 1200 }).catch(() => false)) {
            await mi.first().click({ timeout: 4000 });
            await microDelay();
            const dialogReady = await Promise.race([
              page.getByRole("dialog").waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
              page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
              page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
            ]);
            if (dialogReady) return { opened: true, via: "more_menu" };
          }
        }
      }
    } catch {}
  }
  // Mobile fallback
  try {
    const mobileConnect = page.locator('button:has-text("Connect"), a:has-text("Connect")');
    if (await mobileConnect.first().isVisible({ timeout: 1200 }).catch(() => false)) {
      await mobileConnect.first().click({ timeout: 4000 });
      await microDelay();
      const dialogReady = await Promise.race([
        page.getByRole("dialog").waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
        page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
        page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
      ]);
      if (dialogReady) return { opened: true, via: "mobile_primary" };
    }
  } catch {}
  return { opened: false };
}

async function completeConnectDialog(page, note) {
  await microDelay();
  let addNoteClicked = false, filled = false;

  if (note) {
    const addNoteCandidates = [
      page.getByRole("button", { name: /^Add a note$/i }),
      page.locator('button:has-text("Add a note")'),
      page.locator('button[aria-label="Add a note"]'),
    ];
    for (const an of addNoteCandidates) {
      try {
        if (await an.first().isVisible({ timeout: 1200 }).catch(() => false)) {
          await an.first().click({ timeout: 4000 });
          addNoteClicked = true; await microDelay(); break;
        }
      } catch {}
    }
    const limited = String(note).slice(0, 300);
    const textareas = [
      page.locator('textarea[name="message"]'),
      page.locator('textarea#custom-message'),
      page.locator('textarea'),
      page.getByRole("textbox"),
    ];
    for (const ta of textareas) {
      try {
        if (await ta.first().isVisible({ timeout: 1200 }).catch(() => false)) {
          await ta.first().click({ timeout: 3000 }).catch(()=>{});
          await ta.first().fill(limited, { timeout: 4000 });
          filled = true; await microDelay(); break;
        }
      } catch {}
    }
  }

  const sendCandidates = [
    page.getByRole("button", { name: /^Send$/i }),
    page.locator('button:has-text("Send")'),
    page.locator('button[aria-label="Send now"]'),
  ];
  for (const s of sendCandidates) {
    try {
      if (await s.first().isVisible({ timeout: 1500 }).catch(() => false)) {
        await s.first().click({ timeout: 4000 });
        await microDelay();
        const closed = await Promise.race([
          page.getByRole("dialog").waitFor({ state: "detached", timeout: 5000 }).then(() => true).catch(() => false),
          page.locator('div:has-text("Invitation sent")').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
        ]);
        if (closed) return { sent: true, addNoteClicked, filled };
      }
    } catch {}
  }
  try {
    const sendWithout = page.locator('button:has-text("Send without a note")');
    if (await sendWithout.first().isVisible({ timeout: 1200 }).catch(() => false)) {
      await sendWithout.first().click({ timeout: 4000 });
      await microDelay();
      const closed = await page.getByRole("dialog").waitFor({ state: "detached", timeout: 5000 }).then(() => true).catch(() => false);
      if (closed) return { sent: true, addNoteClicked, filled: false };
    }
  } catch {}
  return { sent: false, addNoteClicked, filled };
}

async function sendConnectionRequest(page, note) {
  const rs1 = await detectRelationshipStatus(page);
  if (rs1.status === "connected") return { actionTaken: "none", relationshipStatus: "connected", details: rs1.reason || "Already connected" };
  if (rs1.status === "pending")   return { actionTaken: "none", relationshipStatus: "pending",   details: rs1.reason || "Invitation already pending" };

  let opened = await openConnectDialog(page);
  if (!opened.opened) {
    try { await page.mouse.wheel(0, within(600, 1000)); await sleep(within(600, 1000)); } catch {}
    opened = await openConnectDialog(page);
    if (!opened.opened) return { actionTaken: "unavailable", relationshipStatus: "not_connected", details: "Connect button not found" };
  }

  const completed = await completeConnectDialog(page, note);
  if (completed.sent) return { actionTaken: "sent", relationshipStatus: "pending", details: "Invitation sent", addNoteClicked: completed.addNoteClicked, noteFilled: completed.filled };

  const rs2 = await detectRelationshipStatus(page);
  if (rs2.status === "pending")   return { actionTaken: "sent_maybe", relationshipStatus: "pending", details: rs2.reason || "Pending after dialog" };
  if (rs2.status === "connected") return { actionTaken: "none", relationshipStatus: "connected", details: rs2.reason || "Connected" };
  return { actionTaken: "failed_to_send", relationshipStatus: "not_connected", details: "Unable to send invite" };
}

// =========================
// Message flow (optional, kept for future)
// =========================
async function openMessageDialog(page) { /* omitted for brevity; same pattern as connect */ return { opened: false }; }

// =========================
// Creds/TOTP login
// =========================
function genTotp(base32Secret) {
  if (!base32Secret) throw new Error("TOTP secret missing");
  authenticator.options = { window: 1 };
  return authenticator.generate(base32Secret);
}

async function loginWithCredentials(page, { username, password, totpSecret }) {
  await page.goto("https://www.linkedin.com/login", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });

  const userSel = 'input#username, input[name="session_key"]';
  const passSel = 'input#password, input[name="session_password"]';
  await page.locator(userSel).first().fill(username, { timeout: 12000 });
  await page.locator(passSel).first().fill(password, { timeout: 12000 });

  await Promise.all([
    page.locator('button[type="submit"], button:has-text("Sign in"), button:has-text("Log in")').first().click(),
    page.waitForLoadState("domcontentloaded", { timeout: 20000 }).catch(() => {})
  ]);

  const codeInput = page.locator('input[name*="pin"], input[aria-label*="verification"], input[type="tel"]').first();
  const need2FA = await codeInput.isVisible({ timeout: 5000 }).catch(() => false);

  if (need2FA) {
    if (!totpSecret) throw new Error("2FA required but no TOTP secret stored");
    const code = genTotp(totpSecret);
    await codeInput.fill(code);
    await page.locator('button:has-text("Submit"), button:has-text("Verify"), button[type="submit"]').first().click();
    await page.waitForLoadState("domcontentloaded", { timeout: 20000 }).catch(() => {});
  }

  const guest = await isAuthWalledOrGuest(page);
  if (guest) throw new Error("Login failed; still at auth wall after credentials/2FA");
}

async function fetchUserSecrets(userEmail) {
  const resp = await apiGet(`/secrets/for-user?email=${encodeURIComponent(userEmail)}`);
  if (!resp?.ok) throw new Error(resp?.error || "secrets_fetch_failed");
  return resp;
}

// =========================
// Job handlers
// =========================
async function handleAuthCheck(job) {
  const { payload } = job || {};
  const userId = payload?.userId || "default";
  const userStatePath = statePathForUser(userId);

  let browser, context, page, videoHandle;
  try {
    ({ browser, context, page } = await createBrowserContext({ headless: HEADLESS, cookieBundle: null, userStatePath }));
    videoHandle = page.video?.();

    // If no li_at, attempt credential login
    const diag0 = await cookieDiag(context);
    console.log("[auth] cookieDiag before auth:", diag0);
    if (!diag0.has_li_at) {
      try {
        const secrets = await fetchUserSecrets(userId);
        await loginWithCredentials(page, {
          username: secrets.li_username,
          password: secrets.li_password,
          totpSecret: secrets.totp_secret || null,
        });
        await saveStorageState(context, userStatePath || DEFAULT_STATE_PATH);
      } catch (e) {
        await browser.close().catch(() => {});
        return { ok: false, via: "creds", message: "Login failed: " + e.message, at: new Date().toISOString() };
      }
    }

    // Verify by feed/m-feed
    const auth = await ensureAuthenticated(context, page, userStatePath);
    let result;
    if (auth.ok) {
      await feedWarmup(page);
      result = {
        ok: true, via: auth.via || "unknown", url: auth.url || null, diag: auth.diag || null,
        message: "Authenticated and storageState saved.", at: new Date().toISOString(), statePath: userStatePath,
      };
    } else {
      result = { ok: false, via: "verify", message: `Auth failed: ${auth.reason || "unknown"}`, at: new Date().toISOString() };
    }

    await browser.close().catch(() => {});
    if (videoHandle) { try { console.log("[video] saved:", await videoHandle.path()); } catch {} }
    return result;
  } catch (e) {
    try { await browser?.close(); } catch {}
    throw new Error(`AUTH_CHECK failed: ${e.message}`);
  }
}

async function handleSendConnection(job) {
  const { payload } = job || {};
  if (!payload) throw new Error("Job has no payload");
  let targetUrl = payload.profileUrl || null;
  if (!targetUrl && payload.publicIdentifier) {
    targetUrl = `https://www.linkedin.com/in/${encodeURIComponent(payload.publicIdentifier)}/`;
  }
  if (!targetUrl) throw new Error("payload.profileUrl or publicIdentifier required");

  // Upfront bad-slug guard
  const slug = extractInSlug(targetUrl);
  if (slug && looksLikeUrnSlug(slug)) {
    throttle.failure("linkedin.com");
    return {
      mode: "real",
      profileUrl: targetUrl,
      actionTaken: "invalid_profile_url",
      relationshipStatus: "unknown",
      details: "Provided /in/ URL looks like an internal URN (ACo/ACw). Needs a real public slug.",
      at: new Date().toISOString(),
    };
  }

  const note = payload.note || null;
  const cookieBundle = payload.cookieBundle || {};
  const userId = payload.userId || "default";
  const userStatePath = statePathForUser(userId);

  if (SOFT_MODE) {
    await throttle.reserve("linkedin.com", "SOFT send_connection"); await microDelay(); throttle.success("linkedin.com");
    return { mode: "soft", profileUrl: targetUrl, noteUsed: note, message: "Soft mode success (no browser).", at: new Date().toISOString() };
  }

  await throttle.reserve("linkedin.com", "SEND_CONNECTION");

  let browser, context, feedPage, profilePage, videoHandle;
  try {
    ({ browser, context, page: feedPage } = await createBrowserContext({ headless: HEADLESS, cookieBundle, userStatePath }));
    videoHandle = feedPage.video?.();

    // If no li_at, credential login
    const diag0 = await cookieDiag(context);
    console.log("[auth] cookieDiag before auth:", diag0);
    if (!diag0.has_li_at) {
      const secrets = await fetchUserSecrets(userId).catch(() => null);
      if (!secrets?.li_username || !secrets?.li_password) {
        await browser.close().catch(()=>{});
        throttle.failure("linkedin.com");
        return {
          mode: "real", profileUrl: targetUrl, actionTaken: "unavailable", relationshipStatus: "unknown",
          details: "No li_at cookie and no stored LinkedIn credentials. Add them in dashboard.",
          at: new Date().toISOString(),
        };
      }
      try {
        await loginWithCredentials(feedPage, {
          username: secrets.li_username,
          password: secrets.li_password,
          totpSecret: secrets.totp_secret || null,
        });
        await saveStorageState(context, userStatePath);
      } catch (e) {
        await browser.close().catch(()=>{});
        throttle.failure("linkedin.com");
        return {
          mode: "real", profileUrl: targetUrl, actionTaken: "unavailable", relationshipStatus: "unknown",
          details: "Credential login failed: " + e.message, at: new Date().toISOString(),
        };
      }
    }

    // AUTH via feed
    const auth = await ensureAuthenticated(context, feedPage, userStatePath);
    if (!auth.ok) {
      // one more attempt with creds if available
      const secrets = await fetchUserSecrets(userId).catch(() => null);
      if (secrets?.li_username && secrets?.li_password) {
        try {
          await loginWithCredentials(feedPage, {
            username: secrets.li_username,
            password: secrets.li_password,
            totpSecret: secrets.totp_secret || null,
          });
          await saveStorageState(context, userStatePath);
        } catch (e) {
          await browser.close().catch(()=>{});
          throttle.failure("linkedin.com");
          return {
            mode: "real", profileUrl: targetUrl, actionTaken: "unavailable", relationshipStatus: "unknown",
            details: "Credential login failed: " + e.message, at: new Date().toISOString(),
          };
        }
      } else {
        await browser.close().catch(()=>{});
        throttle.failure("linkedin.com");
        return {
          mode: "real", profileUrl: targetUrl, actionTaken: "unavailable", relationshipStatus: "unknown",
          details: "Not authenticated and no stored credentials.", at: new Date().toISOString(),
        };
      }
    }

    // Feed warmup
    await feedWarmup(feedPage);

    // TAB 2 profile
    profilePage = await newPageInContext(context);

    const nav = await navigateProfileClean(profilePage, targetUrl);
    if (!nav.authed) {
      const result = {
        mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl || profilePage.url(),
        httpStatus: nav.status, relationshipStatus: "not_connected", actionTaken: "unavailable",
        details: nav.error || "Authwall/404/429 on profile nav.", at: new Date().toISOString(),
      };
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(() => {});
      throttle.failure("linkedin.com");
      return result;
    }

    // Scrape
    const { about, currentRole } = await scrapeAboutAndCurrentRole(profilePage);

    // Connect
    await microDelay();
    const connectOutcome = await sendConnectionRequest(profilePage, note);

    await sleep(2000);
    try { await profilePage.close().catch(()=>{}); } catch {}

    const result = {
      mode: "real",
      profileUrl: targetUrl,
      usedUrl: nav.usedUrl,
      finalUrl: nav.finalUrl || profilePage?.url?.() || "",
      noteUsed: note,
      httpStatus: nav.status,
      relationshipStatus: connectOutcome.relationshipStatus,
      actionTaken: connectOutcome.actionTaken,
      details: connectOutcome.details,
      scraped: {
        aboutLength: about ? about.length : 0,
        currentRoleLength: currentRole ? currentRole.length : 0,
        about,
        currentRole,
      },
      at: new Date().toISOString(),
    };

    await browser.close().catch(() => {});

    if (connectOutcome.actionTaken === "sent" || connectOutcome.actionTaken === "sent_maybe") throttle.success("linkedin.com");
    else if (connectOutcome.actionTaken === "failed_to_send" || connectOutcome.actionTaken === "unavailable") throttle.failure("linkedin.com");
    else throttle.success("linkedin.com");

    return result;
  } catch (e) {
    try { await profilePage?.close()?.catch(()=>{}); } catch {}
    try { await browser?.close(); } catch {}
    throttle.failure("linkedin.com");
    throw new Error(`SEND_CONNECTION failed: ${e.message}`);
  }
}

// =========================
// Job loop
// =========================
async function processOne() {
  let next;
  try {
    next = await apiPost("/jobs/next", { types: ["AUTH_CHECK", "SEND_CONNECTION"] });
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
      default: result = { note: `Unhandled job type: ${job.type}` }; break;
    }

    try {
      await apiPost(`/jobs/${job.id}/complete`, { result });
      console.log(`[worker] Job ${job.id} done:`, result?.message || result?.details || result?.actionTaken || result?.note || "ok");
    } catch (e) {
      logFetchError(`jobs/${job.id}/complete`, e);
    }
  } catch (e) {
    console.error(`[worker] Job ${job.id} failed:`, e.message);
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
