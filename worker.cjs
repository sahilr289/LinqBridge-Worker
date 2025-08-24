// worker.cjs — LinqBridge Worker (with fix for refresh loop)
//
// What this version does (per your asks):
// 1) After login, TAB 1 (feed): wait ~10s, then scroll ~5s. No refresh loops.
// 2) TAB 2 (profile): open once (mobile fallback only if needed), wait ~10s, then scrape, then connect.
// 3) Scraping expands "See more" where present.
// 4) Connect is tried directly and under "More".
// 5) Upfront guard rejects /in/ACo… or /in/ACw… slugs (prevents 404 churn).
// 6) Post-nav 429/404/captcha detection bails cleanly and applies cooldown.
// 7) Conservative throttling and longer nav timeouts for slower networks.
// 8) Optional HTTPS proxy support (set PROXY_* envs). WebRTC UDP is disabled.
//
// ENV you may want to tune:
// HEADLESS, SLOWMO_MS, SOFT_MODE
// FEED_INITIAL_WAIT_MS, FEED_SCROLL_MS, PROFILE_INITIAL_WAIT_MS
// NAV_TIMEOUT_MS, DEFAULT_TIMEOUT_MS
// MAX_ACTIONS_PER_HOUR, MIN_GAP_MS, COOLDOWN_AFTER_*
// PROXY_SERVER, PROXY_USERNAME, PROXY_PASSWORD
// STORAGE_STATE_PATH, STATE_DIR, FORCE_RELOGIN, ALLOW_INTERACTIVE_LOGIN

const path = require("path");
const fs = require("fs");

// -------------------------
// Optional health server (ENABLE_HEALTH=true)
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

// Per your spec
const FEED_INITIAL_WAIT_MS = parseInt(process.env.FEED_INITIAL_WAIT_MS || "10000", 10);      // ~10s
const FEED_SCROLL_MS = parseInt(process.env.FEED_SCROLL_MS || "5000", 10);                  // ~5s
const PROFILE_INITIAL_WAIT_MS = parseInt(process.env.PROFILE_INITIAL_WAIT_MS || "10000", 10); // ~10s

// Timeouts
const DEFAULT_TIMEOUT_MS = parseInt(process.env.DEFAULT_TIMEOUT_MS || "35000", 10);
const NAV_TIMEOUT_MS = parseInt(process.env.NAV_TIMEOUT_MS || "45000", 10);

// Mobile fallback allowed?
const ALLOW_MOBILE_FALLBACK = (/^(true|1|yes)$/i).test(process.env.ALLOW_MOBILE_FALLBACK || "true");

// Session persistence / interactive login
const DEFAULT_STATE_PATH = process.env.STORAGE_STATE_PATH || "/app/auth-state.json";
const STATE_DIR = process.env.STATE_DIR || "/app/state";
const FORCE_RELOGIN = (/^(true|1|yes)$/i).test(process.env.FORCE_RELOGIN || "false");
const ALLOW_INTERACTIVE_LOGIN = (/^(true|1|yes)$/i).test(process.env.ALLOW_INTERACTIVE_LOGIN || "true");
const INTERACTIVE_LOGIN_TIMEOUT_MS = parseInt(process.env.INTERACTIVE_LOGIN_TIMEOUT_MS || "300000", 10); // 5 min

// Proxy (optional; must support HTTPS CONNECT for LinkedIn)
const PROXY_SERVER = process.env.PROXY_SERVER || ""; // e.g. http://USER:PASS@HOST:PORT
const PROXY_USERNAME = process.env.PROXY_USERNAME || "";
const PROXY_PASSWORD = process.env.PROXY_PASSWORD || "";

// Playwright (lazy import)
let chromium = null;

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
  constructor() { this.state = new Map(); } // domain -> { lastActionAt, events: number[], cooldownUntil }
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
    const expandDomains = (cookie) => ([
      { ...cookie, domain: "linkedin.com" },
      { ...cookie, domain: "www.linkedin.com" },
      { ...cookie, domain: "m.linkedin.com" },
    ]);
    const cookies = [];
    if (cookieBundle.li_at) {
      cookies.push(...expandDomains({ name: "li_at", value: cookieBundle.li_at, path: "/", httpOnly: true, secure: true, sameSite: "None" }));
    }
    if (cookieBundle.jsessionid) {
      cookies.push(...expandDomains({ name: "JSESSIONID", value: `"${cookieBundle.jsessionid}"`, path: "/", httpOnly: true, secure: true, sameSite: "None" }));
    }
    if (cookieBundle.bcookie) {
      cookies.push(...expandDomains({ name: "bcookie", value: cookieBundle.bcookie, path: "/", httpOnly: false, secure: true, sameSite: "None" }));
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

  // Mobile feed fallback
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

  // Interactive login (one pass)
  if (!ALLOW_INTERACTIVE_LOGIN) {
    return { ok: false, reason: "guest_or_authwall", url: page.url(), diag: before };
  }
  try {
    console.log("[nav] → login: https://www.linkedin.com/login");
    const r = await page.goto("https://www.linkedin.com/login", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    console.log(`[nav] ✓ login: status=${r ? r.status() : "n/a"} final=${page.url()}`);
  } catch {}
  const deadline = Date.now() + INTERACTIVE_LOGIN_TIMEOUT_MS;
  while (Date.now() < deadline) {
    await sleep(1500);
    const ok = !(await isAuthWalledOrGuest(page));
    if (ok) {
      await saveStorageState(context, userStatePath || DEFAULT_STATE_PATH);
      return { ok: true, via: "interactive", url: page.url(), diag: before };
    }
  }
  return { ok: false, reason: "interactive_timeout", url: page.url(), diag: before };
}

// ========= URL/slug + hard-screen helpers =========
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

// Clean, single-shot navigation (optional single fallback) — NO LOOPS
async function navigateProfileClean(page, rawUrl) {
  const tries = [rawUrl];
  if (ALLOW_MOBILE_FALLBACK && /linkedin\.com\/in\//i.test(rawUrl)) {
    tries.push(rawUrl.replace("www.linkedin.com", "m.linkedin.com").replace("linkedin.com/in/", "m.linkedin.com/in/"));
  }
  for (let i = 0; i < tries.length; i++) {
    const u = tries[i];
    try {
      console.log(`[nav] → profile-${i === 0 ? "primary" : "mobile"}: ${u}`);
      const resp = await page.goto(u, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
      const status = resp ? resp.status() : null;
      const authed = !(await isAuthWalledOrGuest(page));
      const finalUrl = page.url();
      const hard = await detectHardScreen(page);
      if (status === 429 || hard === "429") return { authed: false, status: 429, usedUrl: u, finalUrl, error: "rate_limited" };
      if (hard === "404") return { authed: false, status: status || 404, usedUrl: u, finalUrl, error: "not_found" };
      if (authed && status && status >= 200 && status < 400 && !hard) {
        console.log(`[nav] ✓ profile: status=${status} final=${finalUrl}`);
        return { authed: true, status, usedUrl: u, finalUrl };
      }
      // if first failed and we have fallback, loop; otherwise return
      if (i === tries.length - 1) {
        return { authed: false, status, usedUrl: u, finalUrl, error: "authwall_or_unknown" };
      }
    } catch (e) {
      const finalUrl = page.url();
      if (i === tries.length - 1) return { authed: false, status: null, usedUrl: u, finalUrl, error: e?.message || "nav_failed" };
    }
  }
  return { authed: false, status: null, usedUrl: tries[0], finalUrl: page.url(), error: "unknown" };
}

// Human scroll helper (~durationMs total)
async function humanScroll(page, durationMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < durationMs) {
    try { await page.mouse.wheel(0, within(120, 320)); } catch {}
    await sleep(within(120, 220));
  }
}

// FEED warmup on TAB 1 — no extra refreshes
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

// Scrape About + current role (patient)
async function scrapeAboutAndCurrentRole(page) {
  await sleep(PROFILE_INITIAL_WAIT_MS);

  // ABOUT
  const aboutSection = page.locator('section[id="about"], section:has(h2:has-text("About"))').first();
  await scrollAndExpand(page, aboutSection);
  const aboutText = (await safeInnerText(
    aboutSection.locator('.inline-show-more-text, .display-flex, .pv-shared-text-with-see-more, [data-test="about-section"]').first()
  )) || (await safeInnerText(aboutSection));

  // EXPERIENCE / current role
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
// Message flow (optional usage)
// =========================
async function openMessageDialog(page) {
  const buttons = [
    page.getByRole("button", { name: /^Message$/i }),
    page.getByRole("link",   { name: /^Message$/i }),
    page.locator('button[aria-label="Message"]'),
  ];
  for (const btn of buttons) {
    try {
      if (await btn.first().isVisible({ timeout: 1500 }).catch(() => false)) {
        await btn.first().click({ timeout: 4000 }); await microDelay();
        const ready = await Promise.race([
          page.getByRole("dialog").waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
          page.locator('[data-test-conversation-compose], .msg-form__contenteditable').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
        ]);
        if (ready) return { opened: true };
      }
    } catch {}
  }
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
          await handle.press("Control+A").catch(()=>{});
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
      if (await s.first().isVisible({ timeout: 1500 }).catch(() => false)) {
        await s.first().click({ timeout: 4000 }); await microDelay();
        const closed = await page.getByRole("dialog").waitFor({ state: "detached", timeout: 5000 }).then(() => true).catch(() => false);
        if (closed) return true;
      }
    } catch {}
  }
  return false;
}

async function sendMessage(page, text) {
  let opened = await openMessageDialog(page);
  if (!opened.opened) return { actionTaken: "unavailable", details: "Message button not found" };

  const typed = await typeIntoComposer(page, text);
  if (!typed) return { actionTaken: "failed_to_type", details: "Failed to find message composer" };

  const sent = await clickSendInComposer(page);
  if (!sent) return { actionTaken: "failed_to_send", details: "Failed to find send button" };

  return { actionTaken: "sent", details: "Message sent successfully" };
}

// =========================
// Main job processing loop
// =========================
async function processJob(job, browser) {
  const { id, type, data } = job;
  const { user_id, profile_url, cookie_bundle, note } = data;
  const userStatePath = userStatePathForUser(user_id);
  const userLogPrefix = `[job:${id}] [user:${user_id}]`;

  console.log(`${userLogPrefix} Processing job: ${type} for ${profile_url || "feed"}`);

  // Create a new context and page for this job
  const { context, page } = await createBrowserContext({
    cookieBundle: cookie_bundle,
    headless: HEADLESS,
    userStatePath: userStatePath
  });

  try {
    // 1. Ensure we are authenticated and ready
    const authResult = await ensureAuthenticated(context, page, userStatePath);
    if (!authResult.ok) {
      console.error(`${userLogPrefix} Authentication failed: ${authResult.reason} - Bailing out.`);
      // IMPORTANT: update job status on backend and apply a long cooldown
      await apiPost(`/jobs/${id}/fail`, { error: `Authentication failed: ${authResult.reason}` });
      await sleep(COOLDOWN_AFTER_FAIL_MS);
      return;
    }
    console.log(`${userLogPrefix} Authentication successful via ${authResult.via}. Proceeding...`);

    // 2. Perform the feed warmup if it's the first time
    if (!profile_url) {
      await feedWarmup(page);
      console.log(`${userLogPrefix} Feed warmup complete.`);
      await apiPost(`/jobs/${id}/complete`, { status: "feed_warmed_up" });
    }
    
    // 3. Process a specific profile
    if (profile_url) {
        const slug = extractInSlug(profile_url);
        if (looksLikeUrnSlug(slug)) {
            console.error(`${userLogPrefix} Skipping invalid URN slug: ${slug}`);
            await apiPost(`/jobs/${id}/fail`, { error: "Invalid profile URL (URN slug)" });
            return;
        }

        const navResult = await navigateProfileClean(page, profile_url);
        if (!navResult.authed) {
          console.error(`${userLogPrefix} Profile navigation failed: ${navResult.error}`);
          await apiPost(`/jobs/${id}/fail`, { error: `Profile navigation failed: ${navResult.error}` });
          return;
        }
        
        // Scrape
        console.log(`${userLogPrefix} Scraping profile...`);
        const scrapedData = await scrapeAboutAndCurrentRole(page);
        console.log(`${userLogPrefix} Scraped data:`, scrapedData);
        await apiPost(`/jobs/${id}/update`, { data: scrapedData });

        // Connect
        if (type === "connect") {
            await throttle.reserve("linkedin.com", "connect");
            console.log(`${userLogPrefix} Attempting to send connection request...`);
            const connectResult = await sendConnectionRequest(page, note);
            console.log(`${userLogPrefix} Connection result:`, connectResult);
            if (connectResult.actionTaken === "sent" || connectResult.actionTaken === "sent_maybe") {
                await throttle.success("linkedin.com");
                await apiPost(`/jobs/${id}/complete`, { status: "sent", result: connectResult });
            } else {
                await throttle.failure("linkedin.com");
                await apiPost(`/jobs/${id}/fail`, { error: "Failed to send connection request", result: connectResult });
            }
        }
    }

  } catch (error) {
    console.error(`${userLogPrefix} An unhandled error occurred during job execution:`, error);
    await apiPost(`/jobs/${id}/fail`, { error: "Unhandled worker error", details: error?.message || "Unknown" }).catch(e => logFetchError("job-fail-post", e));
  } finally {
    try { await context.close(); } catch (e) { console.error(`${userLogPrefix} Failed to close context:`, e); }
  }
}

// =========================
// Main loop
// =========================
async function pollForJobs() {
  let browser = null;
  while (true) {
    try {
      console.log(`[main] Polling for jobs from ${API_BASE}...`);
      const jobs = await apiGet("/jobs/next");
      if (!jobs || jobs.length === 0) {
        console.log("[main] No jobs found. Sleeping.");
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      for (const job of jobs) {
        await processJob(job, browser);
      }
    } catch (e) {
      logFetchError("job-poll", e);
      console.log(`[main] Poll failed. Sleeping for ${POLL_INTERVAL_MS / 1000}s before retrying...`);
    } finally {
      if (browser) await browser.close();
    }
  }
}

// Start the main loop
pollForJobs().catch(e => {
  console.error("Fatal error in main loop:", e);
  process.exit(1);
});
