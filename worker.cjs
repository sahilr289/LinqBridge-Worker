// worker.cjs — LinqBridge Worker (public LinkedIn only, headed-ready)
// Flow: FEED -> 4s pause -> light scroll 2s -> open profile -> 4s pause ->
// scrape About (+ expand) -> scrape current role (+ expand) -> send connect -> 2s pause -> done

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
        } else {
          res.writeHead(404); res.end();
        }
      })
      .listen(HEALTH_PORT, () => console.log(`[health] listening on :${HEALTH_PORT}`));
  } catch (e) {
    console.log("[health] server not started:", e?.message || e);
  }
}

// =========================
// Env & Config
// =========================
const API_BASE = process.env.API_BASE || "https://calm-rejoicing-linqbridge.up.railway.app";
const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET || "";

// Headed by default so you can watch via noVNC
const HEADLESS = (/^(true|1|yes)$/i).test(process.env.HEADLESS || "false");
const SLOWMO_MS = parseInt(process.env.SLOWMO_MS || (HEADLESS ? "0" : "50"), 10);

const SOFT_MODE = (/^(true|1|yes)$/i).test(process.env.SOFT_MODE || "true");
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000", 10);

// Human pacing knobs
const MAX_ACTIONS_PER_HOUR = parseInt(process.env.MAX_ACTIONS_PER_HOUR || "35", 10);
const MIN_GAP_MS = parseInt(process.env.MIN_GAP_MS || "25000", 10);
const COOLDOWN_AFTER_SENT_MS = parseInt(process.env.COOLDOWN_AFTER_SENT_MS || "45000", 10);
const COOLDOWN_AFTER_FAIL_MS = parseInt(process.env.COOLDOWN_AFTER_FAIL_MS || "90000", 10);

const MICRO_DELAY_MIN_MS = parseInt(process.env.MICRO_DELAY_MIN_MS || "400", 10);
const MICRO_DELAY_MAX_MS = parseInt(process.env.MICRO_DELAY_MAX_MS || "1200", 10);

const THROTTLE_JITTER_MIN_MS = parseInt(process.env.THROTTLE_JITTER_MIN_MS || "1500", 10);
const THROTTLE_JITTER_MAX_MS = parseInt(process.env.THROTTLE_JITTER_MAX_MS || "3500", 10);

// Session persistence / interactive login
const path = require("path");
const fs = require("fs");
const STORAGE_DIR = process.env.STORAGE_DIR || "/app/state";
const STORAGE_STATE_PATH = process.env.STORAGE_STATE_PATH || "/app/auth-state.json";
const FORCE_RELOGIN = (/^(true|1|yes)$/i).test(process.env.FORCE_RELOGIN || "false");
const ALLOW_INTERACTIVE_LOGIN = (/^(true|1|yes)$/i).test(process.env.ALLOW_INTERACTIVE_LOGIN || "true");
const INTERACTIVE_LOGIN_TIMEOUT_MS = parseInt(process.env.INTERACTIVE_LOGIN_TIMEOUT_MS || "300000", 10); // 5 min

// Logging
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase(); // debug|info|silent
const LOG_CONSOLE_EVENTS = (/^(true|1|yes)$/i).test(process.env.LOG_CONSOLE_EVENTS || "false");

// Optional: log scraped text to backend connection_logs
const LOG_TO_BACKEND = (/^(true|1|yes)$/i).test(process.env.LOG_TO_BACKEND || "false");
// If you add the tiny endpoint in your backend (see note at bottom), keep default:
const LOG_INGEST_PATH = process.env.LOG_INGEST_PATH || "/api/connection/logs/ingest";

// Lazy import
let chromium = null;

// =========================
// Utils
// =========================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();
const within = (min, max) => min + Math.floor(Math.random() * (max - min + 1));
async function microDelay() { await sleep(within(MICRO_DELAY_MIN_MS, MICRO_DELAY_MAX_MS)); }

const log = {
  debug: (...a) => (LOG_LEVEL === "debug" ? console.log("[debug]", ...a) : undefined),
  info:  (...a) => (LOG_LEVEL !== "silent" ? console.log("[info]",  ...a) : undefined),
  warn:  (...a) => (LOG_LEVEL !== "silent" ? console.warn("[warn]",  ...a) : undefined),
  error: (...a) => console.error("[error]", ...a),
};

function logFetchError(where, err) {
  const code = err?.cause?.code || err?.code || "unknown";
  log.error(`${where} fetch failed:`, code, err?.message || err);
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
    signal: AbortSignal.timeout ? AbortSignal.timeout(20000) : undefined,
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

// Optional backend ingestion for logs visible in dashboard (see note at bottom)
async function emitConnLog(userEmail, level, event, details) {
  if (!LOG_TO_BACKEND) return;
  try {
    await apiPost(LOG_INGEST_PATH, {
      user_email: userEmail,
      level,
      event,
      details,
    });
  } catch (e) {
    log.warn("emitConnLog failed", e.message);
  }
}

// =========================
// Per-domain Throttle
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
        const waitMs = Math.max(...waits) + within(THROTTLE_JITTER_MIN_MS, THROTTLE_JITTER_MAX_MS);
        log.info(`[throttle] waiting ${Math.ceil(waitMs/1000)}s for ${label} on ${domain} (${st.events.length}/${MAX_ACTIONS_PER_HOUR})`);
        await sleep(waitMs); continue;
      }
      st.lastActionAt = nowTs; st.events.push(nowTs);
      log.debug(`[throttle] reserved for ${label} on ${domain}: used ${st.events.length}/${MAX_ACTIONS_PER_HOUR}`);
      return;
    }
  }
  success(domain) { const st = this._get(domain); st.cooldownUntil = Math.max(st.cooldownUntil, now() + COOLDOWN_AFTER_SENT_MS); }
  failure(domain) { const st = this._get(domain); st.cooldownUntil = Math.max(st.cooldownUntil, now() + COOLDOWN_AFTER_FAIL_MS); }
}
const throttle = new DomainThrottle();

// =========================
/** Playwright helpers (PUBLIC ONLY) */
// =========================
async function createBrowserContext(cookieBundle, headless = true, userScopedStatePath = null) {
  if (!chromium) ({ chromium } = require("playwright"));

  const browser = await chromium.launch({
    headless,
    slowMo: SLOWMO_MS,
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu"
    ],
  });

  const vw = 1280 + Math.floor(Math.random() * 192); // 1280–1471
  const vh = 720 + Math.floor(Math.random() * 160);  // 720–879

  // Use user-scoped state if present and not forcing re-login
  let statePath = (!FORCE_RELOGIN && userScopedStatePath && fs.existsSync(userScopedStatePath))
    ? userScopedStatePath
    : (!FORCE_RELOGIN && fs.existsSync(STORAGE_STATE_PATH) ? STORAGE_STATE_PATH : undefined);

  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit(537.36) Chrome/124.0.0.0 Safari/537.36",
    locale: "en-US",
    timezoneId: "America/Los_Angeles",
    colorScheme: "light",
    viewport: { width: vw, height: vh },
    deviceScaleFactor: 1,
    javaScriptEnabled: true,
    recordVideo: { dir: "/tmp/pw-video" },
    storageState: statePath,
  });

  await context.setExtraHTTPHeaders({
    "accept-language": "en-US,en;q=0.9",
  });

  if (LOG_CONSOLE_EVENTS) {
    context.on("page", p => {
      p.on("console", (msg) => { try { console.log("[page console]", msg.type(), msg.text()); } catch {} });
    });
  }

  // Light anti-detection shims
  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
    try {
      Object.defineProperty(navigator, "hardwareConcurrency", { get: () => 8 });
      Object.defineProperty(navigator, "language", { get: () => "en-US" });
      Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
      const originalQuery = navigator.permissions?.query?.bind(navigator.permissions);
      if (originalQuery) {
        navigator.permissions.query = (p) =>
          p?.name === "notifications" ? Promise.resolve({ state: "denied" }) : originalQuery(p);
      }
    } catch {}
  });

  // Apply cookie bundle (augments storageState if present)
  const expandDomains = (cookie) => ([
    { ...cookie, domain: "linkedin.com" },
    { ...cookie, domain: "www.linkedin.com" },
    { ...cookie, domain: "m.linkedin.com" },
  ]);
  let cookies = [];
  if (cookieBundle?.li_at) {
    cookies = cookies.concat(expandDomains({
      name: "li_at", value: cookieBundle.li_at, path: "/",
      httpOnly: true, secure: true, sameSite: "None",
    }));
  }
  if (cookieBundle?.jsessionid) {
    cookies = cookies.concat(expandDomains({
      name: "JSESSIONID", value: `"${cookieBundle.jsessionid}"`, path: "/",
      httpOnly: true, secure: true, sameSite: "None",
    }));
  }
  if (cookieBundle?.bcookie) {
    cookies = cookies.concat(expandDomains({
      name: "bcookie", value: cookieBundle.bcookie, path: "/",
      httpOnly: false, secure: true, sameSite: "None",
    }));
  }
  if (cookieBundle?.lang) {
    cookies = cookies.concat(expandDomains({
      name: "lang", value: cookieBundle.lang, path: "/",
      httpOnly: false, secure: true, sameSite: "None",
    }));
  }
  if (cookies.length) await context.addCookies(cookies);

  const page = await context.newPage();
  page.setDefaultTimeout(20000);
  page.setDefaultNavigationTimeout(30000);
  try { await page.bringToFront(); } catch {}

  return { browser, context, page };
}

async function isAuthWalledOrGuest(page) {
  try {
    const title = (await page.title().catch(() => ""))?.toLowerCase?.() || "";
    if (title.includes("sign in") || title.includes("join linkedin") || title.includes("authwall")) return true;
    const hasLogin = await page.locator('a[href*="login"]').first().isVisible({ timeout: 800 }).catch(() => false);
    return !!hasLogin;
  } catch { return false; }
}

async function ensureAuthenticated(context, page) {
  // Desktop feed
  try {
    const r = await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 25000 });
    const s = r ? r.status() : null;
    if (s >= 200 && s < 400 && !(await isAuthWalledOrGuest(page))) {
      await saveStorageState(context, STORAGE_STATE_PATH);
      return { ok: true, via: "desktop", status: s, url: page.url() };
    }
  } catch {}

  // Mobile feed
  try {
    const r = await page.goto("https://m.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 25000 });
    const s = r ? r.status() : null;
    if (s >= 200 && s < 400 && !(await isAuthWalledOrGuest(page))) {
      await saveStorageState(context, STORAGE_STATE_PATH);
      return { ok: true, via: "mobile", status: s, url: page.url() };
    }
  } catch {}

  return { ok: false, reason: "guest_or_authwall", url: page.url() };
}

async function interactiveLogin(context, page, userScopedStatePath = null) {
  if (!ALLOW_INTERACTIVE_LOGIN) return { ok: false, reason: "interactive_disabled" };
  log.info("[auth] interactive login: open noVNC and finish 2FA.");

  const deadline = Date.now() + INTERACTIVE_LOGIN_TIMEOUT_MS;
  try { await page.goto("https://www.linkedin.com/login", { waitUntil: "domcontentloaded", timeout: 30000 }); } catch {}

  while (Date.now() < deadline) {
    await sleep(1500);
    const ok = !(await isAuthWalledOrGuest(page));
    if (ok) {
      await saveStorageState(context, userScopedStatePath || STORAGE_STATE_PATH);
      log.info("[auth] interactive login success; session persisted.");
      return { ok: true, via: "interactive", url: page.url() };
    }
  }
  return { ok: false, reason: "interactive_timeout" };
}

async function saveStorageState(context, filePath) {
  try {
    await fs.promises.mkdir(path.dirname(filePath), { recursive: true }).catch(() => {});
    await context.storageState({ path: filePath });
    log.info("[auth] storageState saved to", filePath);
  } catch (e) {
    log.warn("[auth] storageState save failed:", e?.message || e);
  }
}

async function humanIdleScroll(page, totalMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < totalMs) {
    try {
      await page.mouse.wheel(0, within(120, 240));
      await sleep(within(250, 450));
    } catch {}
  }
}

async function navigateProfile(page, rawUrl) {
  const candidates = [rawUrl];
  if (rawUrl.includes("www.linkedin.com/in/")) {
    candidates.push(rawUrl.replace("www.linkedin.com/in/", "m.linkedin.com/in/"));
  }

  let lastStatus = null, usedUrl = null, finalUrl = null;
  for (const target of candidates) {
    try {
      const resp = await page.goto(target, { waitUntil: "domcontentloaded", timeout: 28000 });
      usedUrl = target;
      lastStatus = resp ? resp.status() : null;
      await sleep(within(700, 1200));

      const authed = !(await isAuthWalledOrGuest(page));
      finalUrl = page.url();
      if (lastStatus && lastStatus >= 200 && lastStatus < 400 && authed) {
        return { status: lastStatus, usedUrl, finalUrl, authed: true };
      }
    } catch {}
  }
  return { status: lastStatus, usedUrl, finalUrl, authed: false };
}

// =========================
// Scraping helpers
// =========================
async function openSeeMoreIn(scope) {
  const buttons = [
    scope.locator('button:has-text("See more")'),
    scope.locator('a:has-text("See more")'),
    scope.getByRole("button", { name: /See more/i }),
  ];
  for (const b of buttons) {
    try {
      if (await b.first().isVisible({ timeout: 800 }).catch(() => false)) {
        await b.first().click({ timeout: 2000 }).catch(() => {});
        await sleep(within(300, 600));
        return true;
      }
    } catch {}
  }
  return false;
}

async function extractVisibleText(scope) {
  try {
    return await scope.evaluate((el) => (el.innerText || "").trim());
  } catch {
    return "";
  }
}

async function findAboutSection(page) {
  const candidates = [
    'section:has(h2:has-text("About"))',
    'section:has(h3:has-text("About"))',
    'div:has(> h2:has-text("About"))',
    'div:has(> h3:has-text("About"))',
    // mobile variants are flatter
    'section:has-text("About")',
  ];
  for (const sel of candidates) {
    const loc = page.locator(sel).first();
    try {
      const vis = await loc.isVisible({ timeout: 800 }).catch(() => false);
      if (vis) return loc;
    } catch {}
  }
  return null;
}

async function findExperienceFirstItem(page) {
  // Experience section -> first entity
  const sectionCandidates = [
    'section:has(h2:has-text("Experience"))',
    'div:has(> h2:has-text("Experience"))',
    'section:has-text("Experience")',
  ];
  let section = null;
  for (const sel of sectionCandidates) {
    const loc = page.locator(sel).first();
    try {
      if (await loc.isVisible({ timeout: 800 }).catch(() => false)) { section = loc; break; }
    } catch {}
  }
  if (!section) return null;

  const firstItem = section.locator("li").first();
  try {
    if (await firstItem.isVisible({ timeout: 800 }).catch(() => false)) {
      return firstItem;
    }
  } catch {}
  return null;
}

async function scrapeAboutAndCurrentRole(page, { debug = false } = {}) {
  const result = { aboutText: "", currentRoleText: "", expandedAbout: false, expandedRole: false };

  // Scroll gently to load sections
  await humanIdleScroll(page, 1200);

  // ABOUT
  const about = await findAboutSection(page);
  if (about) {
    try {
      await about.scrollIntoViewIfNeeded().catch(() => {});
      await sleep(within(300, 600));
      let text = await extractVisibleText(about);
      if (text.length < 240) {
        const opened = await openSeeMoreIn(about);
        if (opened) {
          result.expandedAbout = true;
          await sleep(within(200, 400));
          text = await extractVisibleText(about);
        }
      }
      // strip heading "About"
      result.aboutText = text.replace(/^About\s*/i, "").trim();
    } catch {}
  }

  // CURRENT ROLE (first experience item)
  const firstExp = await findExperienceFirstItem(page);
  if (firstExp) {
    try {
      await firstExp.scrollIntoViewIfNeeded().catch(() => {});
      await sleep(within(300, 600));
      let rtext = await extractVisibleText(firstExp);
      if (rtext.length < 240) {
        const opened = await openSeeMoreIn(firstExp);
        if (opened) {
          result.expandedRole = true;
          await sleep(within(200, 400));
          rtext = await extractVisibleText(firstExp);
        }
      }
      result.currentRoleText = rtext.trim();
    } catch {}
  }

  // Keep sizes reasonable for result payload
  if (result.aboutText.length > 4000) result.aboutText = result.aboutText.slice(0, 4000);
  if (result.currentRoleText.length > 4000) result.currentRoleText = result.currentRoleText.slice(0, 4000);

  if (debug) log.debug("[scrape] about len:", result.aboutText.length, "role len:", result.currentRoleText.length);

  return result;
}

// =========================
// Relationship + messaging flows
// =========================
async function detectRelationshipStatus(page) {
  const checks = [
    { type: "connected", loc: page.getByRole("button", { name: /^Message$/i }) },
    { type: "connected", loc: page.getByRole("link",   { name: /^Message$/i }) },
    { type: "pending",   loc: page.getByRole("button", { name: /Pending|Requested|Withdraw|Pending invitation/i }) },
    { type: "pending",   loc: page.locator('text=/Pending invitation/i') },
    { type: "can_connect", loc: page.getByRole("button", { name: /^Connect$/i }) },
    { type: "can_connect", loc: page.locator('button:has-text("Connect"), a:has-text("Connect")') },
  ];
  for (const c of checks) {
    try {
      const visible = await c.loc.first().isVisible({ timeout: 900 }).catch(() => false);
      if (visible) {
        if (c.type === "connected") return { status: "connected", reason: "Message CTA visible" };
        if (c.type === "pending")   return { status: "pending", reason: "Pending/Requested visible" };
        if (c.type === "can_connect") return { status: "not_connected" };
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
      if (await b.first().isVisible({ timeout: 700 }).catch(() => false)) {
        await b.first().click({ timeout: 4000 });
        await microDelay();
        const dialogReady = await Promise.race([
          page.getByRole("dialog").waitFor({ timeout: 2500 }).then(() => true).catch(() => false),
          page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 2500 }).then(() => true).catch(() => false),
          page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 2500 }).then(() => true).catch(() => false),
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
      if (await m.first().isVisible({ timeout: 900 }).catch(() => false)) {
        await m.first().click({ timeout: 4000 });
        await microDelay();
        const menuConnect = [
          page.getByRole("menuitem", { name: /^Connect$/i }),
          page.locator('div[role="menuitem"]:has-text("Connect")'),
          page.locator('span:has-text("Connect")'),
        ];
        for (const mi of menuConnect) {
          if (await mi.first().isVisible({ timeout: 900 }).catch(() => false)) {
            await mi.first().click({ timeout: 4000 });
            await microDelay();
            const dialogReady = await Promise.race([
              page.getByRole("dialog").waitFor({ timeout: 2500 }).then(() => true).catch(() => false),
              page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 2500 }).then(() => true).catch(() => false),
              page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 2500 }).then(() => true).catch(() => false),
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
    if (await mobileConnect.first().isVisible({ timeout: 900 }).catch(() => false)) {
      await mobileConnect.first().click({ timeout: 4000 });
      await microDelay();
      const dialogReady = await Promise.race([
        page.getByRole("dialog").waitFor({ timeout: 2500 }).then(() => true).catch(() => false),
        page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 2500 }).then(() => true).catch(() => false),
        page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 2500 }).then(() => true).catch(() => false),
      ]);
      if (dialogReady) return { opened: true, via: "mobile_primary" };
    }
  } catch {}
  return { opened: false };
}

async function completeConnectDialog(page, note) {
  await microDelay();
  const addNoteCandidates = [
    page.getByRole("button", { name: /^Add a note$/i }),
    page.locator('button:has-text("Add a note")'),
    page.locator('button[aria-label="Add a note"]'),
  ];
  let addNoteClicked = false, filled = false;
  if (note) {
    for (const an of addNoteCandidates) {
      try {
        if (await an.first().isVisible({ timeout: 800 }).catch(() => false)) {
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
        if (await ta.first().isVisible({ timeout: 900 }).catch(() => false)) {
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
      if (await s.first().isVisible({ timeout: 1000 }).catch(() => false)) {
        await s.first().click({ timeout: 4000 });
        await microDelay();
        const closed = await Promise.race([
          page.getByRole("dialog").waitFor({ state: "detached", timeout: 3500 }).then(() => true).catch(() => false),
          page.locator('div:has-text("Invitation sent")').waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
        ]);
        if (closed) return { sent: true, addNoteClicked, filled };
      }
    } catch {}
  }
  try {
    const sendWithout = page.locator('button:has-text("Send without a note")');
    if (await sendWithout.first().isVisible({ timeout: 800 }).catch(() => false)) {
      await sendWithout.first().click({ timeout: 4000 });
      await microDelay();
      const closed = await page.getByRole("dialog").waitFor({ state: "detached", timeout: 3500 }).then(() => true).catch(() => false);
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
    try { await page.mouse.wheel(0, within(600, 1000)); await sleep(within(400, 800)); } catch {}
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
// Job handlers
// =========================
async function handleAuthCheck(job) {
  const email = job?.payload?.userId || "unknown";
  const cookieBundle = job?.payload?.cookieBundle || {};
  const userScopedStatePath = path.join(STORAGE_DIR, (email || "default").replace(/[@]/g, "_") + ".json");

  await throttle.reserve("linkedin.com", "AUTH_CHECK");

  let browser, context, page, videoHandle;
  try {
    ({ browser, context, page } = await createBrowserContext(cookieBundle, HEADLESS, userScopedStatePath));
    videoHandle = page.video?.();
    await context.tracing.start({ screenshots: true, snapshots: false });

    let auth = await ensureAuthenticated(context, page);
    if (!auth.ok) {
      const inter = await interactiveLogin(context, page, userScopedStatePath);
      if (inter.ok) auth = await ensureAuthenticated(context, page);
    }
    if (!auth.ok) {
      await context.tracing.stop({ path: "/tmp/trace-failed.zip" }).catch(()=>{});
      await browser.close().catch(()=>{});
      throttle.failure("linkedin.com");
      return { ok: false, message: "Auth failed (guest/authwall). Finish login in viewer." };
    }

    // small idle to stabilise
    await sleep(1200);
    await saveStorageState(context, userScopedStatePath);

    await context.tracing.stop({ path: "/tmp/trace.zip" }).catch(()=>{});
    await browser.close().catch(()=>{});
    throttle.success("linkedin.com");
    return { ok: true, message: "Authenticated and storageState saved." };
  } catch (e) {
    try { await context?.tracing?.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
    try { await browser?.close(); } catch {}
    throttle.failure("linkedin.com");
    throw new Error(e.message || "AUTH_CHECK failed");
  }
}

async function handleSendConnection(job) {
  const { payload } = job || {};
  if (!payload) throw new Error("Job has no payload");

  const email = payload.userId || "unknown";
  let targetUrl = payload.profileUrl || null;
  if (!targetUrl && payload.publicIdentifier) {
    targetUrl = `https://www.linkedin.com/in/${encodeURIComponent(payload.publicIdentifier)}`;
  }
  const note = payload.note || null;
  const cookieBundle = payload.cookieBundle || {};
  const userScopedStatePath = path.join(STORAGE_DIR, (email || "default").replace(/[@]/g, "_") + ".json");

  if (!targetUrl) throw new Error("payload.profileUrl or publicIdentifier required");

  if (SOFT_MODE) {
    await throttle.reserve("linkedin.com", "SOFT send_connection"); await microDelay(); throttle.success("linkedin.com");
    return { mode: "soft", profileUrl: targetUrl, noteUsed: note, message: "Soft mode (no browser).", at: new Date().toISOString() };
  }

  await throttle.reserve("linkedin.com", "SEND_CONNECTION");

  let browser, context, page, videoHandle;
  try {
    ({ browser, context, page } = await createBrowserContext(cookieBundle, HEADLESS, userScopedStatePath));
    videoHandle = page.video?.();
    await context.tracing.start({ screenshots: true, snapshots: false });

    // --- AUTH PREFLIGHT (interactive if needed) ---
    let auth = await ensureAuthenticated(context, page);
    if (!auth.ok) {
      const inter = await interactiveLogin(context, page, userScopedStatePath);
      if (inter.ok) auth = await ensureAuthenticated(context, page);
    }
    if (!auth.ok) {
      const result = {
        mode: "real",
        profileUrl: targetUrl,
        step: "auth",
        details: "Not authenticated (guest/authwall). Complete 2FA or refresh cookies.",
        at: new Date().toISOString(),
      };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {});
      throttle.failure("linkedin.com");
      return result;
    }

    // --- HUMAN FEED PAUSE + LIGHT SCROLL ---
    try {
      await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 25000 });
      await sleep(4000);                       // wait 4s on feed
      await humanIdleScroll(page, 2000);       // light scroll ~2s
    } catch {}

    // --- OPEN PROFILE (no extra params), then 4s pause ---
    const nav = await navigateProfile(page, targetUrl);
    if (!nav.authed) {
      const result = {
        mode: "real",
        profileUrl: targetUrl,
        step: "nav_profile",
        details: "Authwall/999 or page not found while opening profile.",
        at: new Date().toISOString(),
      };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {});
      throttle.failure("linkedin.com");
      return result;
    }
    await sleep(4000); // wait 4s after profile loads

    // --- SCRAPE ABOUT + CURRENT ROLE ---
    const scraped = await scrapeAboutAndCurrentRole(page, { debug: LOG_LEVEL === "debug" });
    await emitConnLog(email, "info", "profile_scraped", {
      profileUrl: targetUrl,
      aboutLen: scraped.aboutText.length,
      roleLen: scraped.currentRoleText.length,
      expandedAbout: scraped.expandedAbout,
      expandedRole: scraped.expandedRole,
    });

    // --- SEND CONNECTION ---
    await humanIdleScroll(page, 600);
    const connectOutcome = await sendConnectionRequest(page, note);

    // --- linger 2s after sending (or attempt) ---
    await sleep(2000);

    const result = {
      mode: "real",
      profileUrl: targetUrl,
      usedUrl: nav.usedUrl,
      finalUrl: nav.finalUrl || page.url(),
      noteUsed: note,
      httpStatus: nav.status,
      pageTitle: await page.title().catch(() => null),
      relationshipStatus: connectOutcome.relationshipStatus,
      actionTaken: connectOutcome.actionTaken,
      details: connectOutcome.details,
      scrapedAbout: scraped.aboutText,
      scrapedCurrentRole: scraped.currentRoleText,
      expandedAbout: scraped.expandedAbout,
      expandedRole: scraped.expandedRole,
      at: new Date().toISOString(),
    };

    try { await context.tracing.stop({ path: "/tmp/trace.zip" }); } catch {}
    await browser.close().catch(() => {});
    if (connectOutcome.actionTaken === "sent" || connectOutcome.actionTaken === "sent_maybe") throttle.success("linkedin.com");
    else if (connectOutcome.actionTaken === "failed_to_send" || connectOutcome.actionTaken === "unavailable") throttle.failure("linkedin.com");
    else throttle.success("linkedin.com");

    return result;
  } catch (e) {
    try {
      await page?.screenshot?.({ path: "/tmp/last-error.png", fullPage: false }).catch(() => {});
      await context?.tracing?.stop({ path: "/tmp/trace-failed.zip" }).catch(() => {});
    } catch {}
    try { await browser?.close(); } catch {}
    throttle.failure("linkedin.com");
    throw new Error(`REAL mode failed: ${e.message}`);
  }
}

async function handleSendMessage(job) {
  const { payload } = job || {};
  if (!payload) throw new Error("Job has no payload");

  let targetUrl = payload.profileUrl || null;
  if (!targetUrl && payload.publicIdentifier) {
    targetUrl = `https://www.linkedin.com/in/${encodeURIComponent(payload.publicIdentifier)}`;
  }
  const messageText = payload.message;
  const cookieBundle = payload.cookieBundle || {};
  const email = payload.userId || "unknown";
  const userScopedStatePath = path.join(STORAGE_DIR, (email || "default").replace(/[@]/g, "_") + ".json");
  if (!messageText) throw new Error("payload.message required");
  if (!targetUrl) throw new Error("payload.profileUrl or publicIdentifier required");

  if (SOFT_MODE) {
    await throttle.reserve("linkedin.com", "SOFT send_message"); await microDelay(); throttle.success("linkedin.com");
    return { mode: "soft", profileUrl: targetUrl, messageUsed: messageText, message: "Soft mode (no browser).", at: new Date().toISOString() };
  }

  await throttle.reserve("linkedin.com", "SEND_MESSAGE");

  let browser, context, page, videoHandle;
  try {
    ({ browser, context, page } = await createBrowserContext(cookieBundle, HEADLESS, userScopedStatePath));
    videoHandle = page.video?.();
    await context.tracing.start({ screenshots: true, snapshots: false });

    // AUTH PREFLIGHT
    let auth = await ensureAuthenticated(context, page);
    if (!auth.ok) {
      const inter = await interactiveLogin(context, page, userScopedStatePath);
      if (inter.ok) auth = await ensureAuthenticated(context, page);
    }
    if (!auth.ok) {
      const result = { mode: "real", profileUrl: targetUrl, step: "auth", details: "Not authenticated (guest/authwall)." };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {});
      throttle.failure("linkedin.com");
      return result;
    }

    // HUMAN FEED STALL
    try {
      await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 25000 });
      await sleep(2500);
      await humanIdleScroll(page, 1500);
    } catch {}

    // OPEN PROFILE
    const nav = await navigateProfile(page, targetUrl);
    if (!nav.authed) {
      const result = { mode: "real", profileUrl: targetUrl, step: "nav_profile", details: "Authwall/999 on profile nav." };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {});
      throttle.failure("linkedin.com");
      return result;
    }

    // MESSAGE FLOW
    const outcome = await (async () => {
      // we could also scrape here if desired
      return await (await import('node:module').catch(()=>({}))) && sendMessageFlow(page, messageText);
    })() || await sendMessageFlow(page, messageText);

    await sleep(1000);

    const result = {
      mode: "real",
      profileUrl: targetUrl,
      usedUrl: nav.usedUrl,
      finalUrl: nav.finalUrl || page.url(),
      messageUsed: messageText,
      httpStatus: nav.status,
      pageTitle: await page.title().catch(() => null),
      relationshipStatus: outcome.relationshipStatus,
      actionTaken: outcome.actionTaken,
      details: outcome.details,
      at: new Date().toISOString(),
    };

    try { await context.tracing.stop({ path: "/tmp/trace.zip" }); } catch {}
    await browser.close().catch(() => {});
    if (outcome.actionTaken === "sent") throttle.success("linkedin.com");
    else if (outcome.actionTaken?.startsWith("failed") || outcome.actionTaken === "unavailable") throttle.failure("linkedin.com");
    else throttle.success("linkedin.com");

    return result;
  } catch (e) {
    try {
      await page?.screenshot?.({ path: "/tmp/last-error.png", fullPage: false }).catch(() => {});
      await context?.tracing?.stop({ path: "/tmp/trace-failed.zip" }).catch(() => {});
    } catch {}
    try { await browser?.close(); } catch {}
    throttle.failure("linkedin.com");
    throw new Error(`REAL mode failed: ${e.message}`);
  }
}

// =========================
// Job loop
// =========================
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
      log.info(`[worker] job ${job.id} done:`, result?.message || result?.details || result?.actionTaken || "ok");
    } catch (e) {
      logFetchError(`jobs/${job.id}/complete`, e);
    }
  } catch (e) {
    log.error(`[worker] job ${job?.id} failed:`, e.message);
    try {
      await apiPost(`/jobs/${job.id}/fail`, { error: e.message, requeue: false, delayMs: 0 });
    } catch (e2) {
      logFetchError(`jobs/${job.id}/fail`, e2);
    }
  }
}

async function mainLoop() {
  console.log(`[worker] starting. API_BASE=${API_BASE} Headless: ${HEADLESS} SlowMo: ${SLOWMO_MS}ms Soft mode: ${SOFT_MODE} LogLevel: ${LOG_LEVEL}`);
  if (!WORKER_SHARED_SECRET) console.error("[worker] ERROR: WORKER_SHARED_SECRET is empty. Set it on both backend and worker!");

  try {
    const stats = await apiGet("/jobs/stats");
    log.info("[worker] API OK. Stats:", stats?.counts || stats);
  } catch (e) {
    logFetchError("jobs/stats (startup)", e);
  }

  while (true) {
    try { await processOne(); }
    catch (e) { log.error("[worker] loop error:", e.message || e); }
    await sleep(POLL_INTERVAL_MS);
  }
}

mainLoop().catch((e) => { console.error("[worker] fatal:", e); process.exitCode = 1; });
