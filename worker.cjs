// worker.cjs — LinqBridge Worker (PUBLIC LINKEDIN ONLY, headed-ready)
// - Interactive login to personal LinkedIn only (no Sales Navigator).
// - Persists session (storageState) after you complete login/2FA once.
// - Human pre-wander on feed, profile snippet capture (About + current role), then Connect.
// - Anti-999 navigation, per-domain throttle, micro-delays.
// - Health server disabled by default (noVNC should use the platform port).

// -------------------------
// Optional health server (off by default; enable with ENABLE_HEALTH=true)
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

const SOFT_MODE = (/^(true|1|yes)$/i).test(process.env.SOFT_MODE || "true"); // safe default
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000", 10);

// Human pacing knobs
const MAX_ACTIONS_PER_HOUR = parseInt(process.env.MAX_ACTIONS_PER_HOUR || "35", 10);
const MIN_GAP_MS = parseInt(process.env.MIN_GAP_MS || "25000", 10);
const COOLDOWN_AFTER_SENT_MS = parseInt(process.env.COOLDOWN_AFTER_SENT_MS || "45000", 10);
const COOLDOWN_AFTER_FAIL_MS = parseInt(process.env.COOLDOWN_AFTER_FAIL_MS || "90000", 10);

const MICRO_DELAY_MIN_MS = parseInt(process.env.MICRO_DELAY_MIN_MS || "400", 10);
const MICRO_DELAY_MAX_MS = parseInt(process.env.MICRO_DELAY_MAX_MS || "1200", 10);

// New: explicit waits the user asked for
const FEED_AFTER_LOAD_WAIT_MS = parseInt(process.env.FEED_AFTER_LOAD_WAIT_MS || "4000", 10);
const FEED_WANDER_MS = parseInt(process.env.FEED_WANDER_MS || "2000", 10);
const PROFILE_AFTER_LOAD_WAIT_MS = parseInt(process.env.PROFILE_AFTER_LOAD_WAIT_MS || "4000", 10);
const POST_CONNECT_HOLD_MS = parseInt(process.env.POST_CONNECT_HOLD_MS || "2000", 10);

const THROTTLE_JITTER_MIN_MS = parseInt(process.env.THROTTLE_JITTER_MIN_MS || "1500", 10);
const THROTTLE_JITTER_MAX_MS = parseInt(process.env.THROTTLE_JITTER_MAX_MS || "3500", 10);

// Session persistence / interactive login
const path = require("path");
const fs = require("fs");
const STORAGE_STATE_DIR = process.env.STORAGE_STATE_DIR || "/app/state";
const STORAGE_STATE_PATH = process.env.STORAGE_STATE_PATH || path.join(STORAGE_STATE_DIR, "auth-state.json"); // default
const FORCE_RELOGIN = (/^(true|1|yes)$/i).test(process.env.FORCE_RELOGIN || "false");
const ALLOW_INTERACTIVE_LOGIN = (/^(true|1|yes)$/i).test(process.env.ALLOW_INTERACTIVE_LOGIN || "true");
const INTERACTIVE_LOGIN_TIMEOUT_MS = parseInt(process.env.INTERACTIVE_LOGIN_TIMEOUT_MS || "300000", 10); // 5 min

// Lazy import
let chromium = null;

// =========================
// Small Utils
// =========================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();
const within = (min, max) => min + Math.floor(Math.random() * (max - min + 1));
async function microDelay() { await sleep(within(MICRO_DELAY_MIN_MS, MICRO_DELAY_MAX_MS)); }

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
        console.log(`[throttle] Waiting ${Math.ceil(waitMs/1000)}s before ${label} on ${domain} (used this hour: ${st.events.length}/${MAX_ACTIONS_PER_HOUR})`);
        await sleep(waitMs); continue;
      }
      st.lastActionAt = nowTs; st.events.push(nowTs);
      console.log(`[throttle] Reserved slot for ${label} on ${domain}. Used this hour: ${st.events.length}/${MAX_ACTIONS_PER_HOUR}`);
      return;
    }
  }
  success(domain) { const st = this._get(domain); st.cooldownUntil = Math.max(st.cooldownUntil, now() + COOLDOWN_AFTER_SENT_MS); }
  failure(domain) { const st = this._get(domain); st.cooldownUntil = Math.max(st.cooldownUntil, now() + COOLDOWN_AFTER_FAIL_MS); }
}
const throttle = new DomainThrottle();

// =========================
/* Playwright helpers (PUBLIC ONLY) */
// =========================
async function createBrowserContext(cookieBundle, headless = true, userKey = "default") {
  if (!chromium) ({ chromium } = require("playwright"));

  // Per-user storage state file so each BDR keeps their own session
  await fs.promises.mkdir(STORAGE_STATE_DIR, { recursive: true }).catch(()=>{});
  const userSafe = String(userKey).replace(/[^a-z0-9_\-\.]/gi, "_");
  const userStatePath = path.join(STORAGE_STATE_DIR, `${userSafe}.json`);
  const storageStateOpt = (!FORCE_RELOGIN && fs.existsSync(userStatePath))
    ? userStatePath
    : ( (!FORCE_RELOGIN && fs.existsSync(STORAGE_STATE_PATH)) ? STORAGE_STATE_PATH : undefined );

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
    storageState: storageStateOpt,
  });

  await context.setExtraHTTPHeaders({
    "accept-language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Chromium";v="124", "Not:A-Brand";v="8"',
    "sec-ch-ua-platform": '"Windows"',
    "sec-ch-ua-mobile": "?0",
    "upgrade-insecure-requests": "1"
  });

  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
    try {
      Object.defineProperty(navigator, "hardwareConcurrency", { get: () => 8 });
      Object.defineProperty(navigator, "language", { get: () => "en-US" });
      Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
      Object.defineProperty(navigator, "userAgent", {
        get: () =>
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit(537.36) Chrome/124.0.0.0 Safari/537.36"
      });
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

  // Allow LinkedIn/LICDN; trim obvious 3P trackers
  await context.route("**/*", (route) => {
    const url = route.request().url();
    try {
      const host = new URL(url).hostname.replace(/^www\./, "");
      const isFirstParty = host.endsWith("linkedin.com") || host.endsWith("licdn.com");
      if (isFirstParty) return route.continue();
    } catch {}
    if (/doubleclick|googletagmanager|adservice|facebook|hotjar|segment|optimizely/i.test(url)) return route.abort();
    return route.continue();
  });

  const page = await context.newPage();
  page.setDefaultTimeout(20000);
  page.setDefaultNavigationTimeout(30000);
  page.on("console", (msg) => { try { console.log("[page console]", msg.type(), msg.text()); } catch {} });
  try { await page.bringToFront(); } catch {}

  // Helper to persist per-user state
  async function saveState() {
    try {
      await context.storageState({ path: userStatePath });
      console.log("[auth] storageState saved to", userStatePath);
    } catch (e) { console.log("[auth] storageState save failed:", e?.message || e); }
  }

  return { browser, context, page, saveState, userStatePath };
}

function withParams(u, extra = {}) {
  try {
    const url = new URL(u);
    if (!url.searchParams.get("trk")) url.searchParams.set("trk", "public_profile_nav");
    url.searchParams.set("original_referer", "https://www.google.com/");
    for (const [k, v] of Object.entries(extra)) url.searchParams.set(k, String(v));
    return url.toString();
  } catch { return u; }
}

async function isAuthWalledOrGuest(page) {
  try {
    const title = (await page.title().catch(() => ""))?.toLowerCase?.() || "";
    if (title.includes("sign in") || title.includes("join linkedin") || title.includes("authwall")) return true;
    const hasLogin = await page.locator('a[href*="login"]').first().isVisible({ timeout: 600 }).catch(() => false);
    return !!hasLogin;
  } catch { return false; }
}

async function navigateLinkedInWithRetries(page, rawUrl, { attempts = 4 } = {}) {
  const mobile   = rawUrl && rawUrl.includes("/in/") ? rawUrl.replace("www.linkedin.com/in/", "m.linkedin.com/in/") : rawUrl;
  const desktop1 = rawUrl ? withParams(rawUrl) : null;
  const desktop2 = rawUrl ? withParams(rawUrl, { lipi: "urn-li-pi-" + Math.random().toString(36).slice(2) }) : null;

  const candidates = [desktop1, desktop2, mobile].filter(Boolean);

  let lastErr, lastStatus = null, usedUrl = null, finalUrl = null;
  for (let i = 0; i < Math.min(attempts, candidates.length); i++) {
    const target = candidates[i];
    try {
      const resp = await page.goto(target, { waitUntil: "domcontentloaded", timeout: 28000 });
      usedUrl = target;
      lastStatus = resp ? resp.status() : null;

      // tiny human jitter
      try { await page.mouse.move(30 + Math.random()*100, 20 + Math.random()*80, { steps: 3 }); } catch {}
      await page.waitForTimeout(700 + Math.random() * 900);

      const authed = !(await isAuthWalledOrGuest(page));
      finalUrl = page.url();

      if (lastStatus && lastStatus >= 200 && lastStatus < 400 && authed) {
        try { await page.mouse.wheel(0, 200 + Math.floor(Math.random()*200)); } catch {}
        await page.waitForTimeout(400 + Math.random()*600);
        return { status: lastStatus, usedUrl, finalUrl, authed: true };
      }
      await page.waitForTimeout(900 + Math.random() * 1100);
    } catch (e) {
      lastErr = e;
      await page.waitForTimeout(900 + Math.random() * 1100);
    }
  }
  return { status: lastStatus, usedUrl, finalUrl, authed: false, error: lastErr?.message || "auth/999/guest" };
}

async function humanizePage(page) {
  try {
    const hops = 2 + Math.floor(Math.random()*3);
    for (let i = 0; i < hops; i++) {
      await page.waitForTimeout(300 + Math.random()*600);
      try { await page.mouse.wheel(0, 120 + Math.random()*180); } catch {}
    }
    for (let i = 0; i < 3; i++) {
      await page.waitForTimeout(150 + Math.random()*350);
      try { await page.mouse.move(200 + Math.random()*300, 150 + Math.random()*200, { steps: 2 + Math.floor(Math.random()*3) }); } catch {}
    }
  } catch {}
}

// Human “wander” on feed: wait, then scroll around for ~FEED_WANDER_MS
async function wanderOnFeed(page) {
  await page.waitForLoadState("domcontentloaded", { timeout: 20000 }).catch(()=>{});
  await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(()=>{});
  await page.waitForTimeout(FEED_AFTER_LOAD_WAIT_MS);
  const start = Date.now();
  while (Date.now() - start < FEED_WANDER_MS) {
    try { await page.mouse.wheel(0, within(150, 400)); } catch {}
    await page.waitForTimeout(within(200, 450));
  }
}

// -------------------------
// Auth diagnostics + storageState
// -------------------------
async function cookieDiag(context) {
  const all = await context.cookies("https://www.linkedin.com");
  const pick = (n) => all.find(c => c.name === n);
  const js = pick("JSESSIONID");
  return {
    has_li_at: !!pick("li_at"),
    has_JSESSIONID: !!js,
    JSESSIONID_quoted: js ? /^".*"$/.test(js.value || "") : null,
    has_bcookie: !!pick("bcookie"),
    sample: [...new Set(all.slice(0,20).map(c => `${c.domain}:${c.name}`))],
  };
}

async function saveStorageState(context, pathOut) {
  try {
    await fs.promises.mkdir(path.dirname(pathOut), { recursive: true }).catch(() => {});
    await context.storageState({ path: pathOut });
    console.log("[auth] storageState saved to", pathOut);
  } catch (e) {
    console.log("[auth] storageState save failed:", e?.message || e);
  }
}

async function ensureAuthenticated(context, page) {
  const before = await cookieDiag(context);

  // Desktop feed
  try {
    const r = await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 25000 });
    const s = r ? r.status() : null;
    if (s >= 200 && s < 400 && !(await isAuthWalledOrGuest(page))) {
      return { ok: true, via: "desktop", status: s, url: page.url(), diag: before };
    }
  } catch {}

  // Mobile feed
  try {
    const r = await page.goto("https://m.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 25000 });
    const s = r ? r.status() : null;
    if (s >= 200 && s < 400 && !(await isAuthWalledOrGuest(page))) {
      return { ok: true, via: "mobile", status: s, url: page.url(), diag: before };
    }
  } catch {}

  // Optional: API probe if CSRF available
  try {
    const js = (await context.cookies("https://www.linkedin.com")).find(c => c.name === "JSESSIONID");
    const csrf = js ? (js.value || "").replace(/^"|"$/g, "") : null;
    if (csrf) {
      const code = await page.evaluate(async (csrfToken) => {
        const r = await fetch("https://www.linkedin.com/voyager/api/me", {
          headers: {
            "csrf-token": csrfToken,
            "x-restli-protocol-version": "2.0.0",
            "accept": "application/json"
          },
          credentials: "include",
          method: "GET",
        }).catch(() => null);
        return r ? r.status : null;
      }, csrf);
      if (code && code >= 200 && code < 400) {
        return { ok: true, via: "voyager-api", status: code, url: page.url(), diag: before };
      }
    }
  } catch {}

  return { ok: false, reason: "guest_or_authwall", url: page.url(), diag: before };
}

// =========================
// Scraping helpers (About + Current role)
// =========================
async function waitProfileSettled(page) {
  await page.waitForLoadState("domcontentloaded", { timeout: 20000 }).catch(()=>{});
  await page.waitForLoadState("networkidle", { timeout: 12000 }).catch(()=>{});
  await page.waitForTimeout(PROFILE_AFTER_LOAD_WAIT_MS);
}

async function revealSeeMoreIfPresent(scope) {
  const candidates = [
    scope.getByRole("button", { name: /see more/i }),
    scope.locator('button:has-text("See more")'),
    scope.locator('button[aria-expanded="false"]'),
  ];
  for (const btn of candidates) {
    try {
      const h = btn.first();
      if (await h.isVisible({ timeout: 600 }).catch(()=>false)) {
        await h.click({ timeout: 3000 }).catch(()=>{});
        await microDelay();
        return true;
      }
    } catch {}
  }
  return false;
}

async function getAboutText(page) {
  // Desktop patterns
  const aboutSectionLocators = [
    page.locator('section[id="about"]'),
    page.locator('section:has(h2:has-text("About"))'),
    page.locator('div:has(h2:has-text("About"))').locator('..').filter({ has: page.locator('section') }).first(),
  ];
  for (const sec of aboutSectionLocators) {
    try {
      const visible = await sec.first().isVisible({ timeout: 1200 }).catch(()=>false);
      if (!visible) continue;

      await sec.scrollIntoViewIfNeeded().catch(()=>{});
      await microDelay();

      await revealSeeMoreIfPresent(sec);

      const textNode = [
        sec.locator('[class*="inline-show-more-text"]'),
        sec.locator('div[dir="ltr"]'),
        sec.locator('p'),
      ];
      for (const t of textNode) {
        const handle = t.first();
        const ok = await handle.isVisible({ timeout: 600 }).catch(()=>false);
        if (ok) {
          const txt = await handle.innerText().catch(()=>null);
          if (txt && txt.trim().length > 0) return txt.trim();
        }
      }
    } catch {}
  }
  // Mobile fallback
  try {
    const mHead = page.locator('h2:has-text("About")').first();
    if (await mHead.isVisible({ timeout: 800 }).catch(()=>false)) {
      const container = mHead.locator('..').locator('..'); // ascend to section container
      await container.scrollIntoViewIfNeeded().catch(()=>{});
      await microDelay();
      await revealSeeMoreIfPresent(container);
      const txt = await container.locator('p, div[dir="ltr"]').first().innerText().catch(()=>null);
      if (txt && txt.trim()) return txt.trim();
    }
  } catch {}
  return null;
}

async function getCurrentRoleText(page) {
  // Experience section
  const expSection = [
    page.locator('section[id="experience"]'),
    page.locator('section:has(h2:has-text("Experience"))'),
  ];
  for (const sec of expSection) {
    const s = sec.first();
    try {
      if (!(await s.isVisible({ timeout: 1500 }).catch(()=>false))) continue;
      await s.scrollIntoViewIfNeeded().catch(()=>{});
      await microDelay();

      // First item in experience list (typically current)
      const items = [
        s.locator('li').first(),
        s.locator('[data-view-name*="experience_item"]').first(),
      ];
      for (const it of items) {
        try {
          if (!(await it.isVisible({ timeout: 800 }).catch(()=>false))) continue;

          // Expand if needed
          await revealSeeMoreIfPresent(it);

          // Try known description containers
          const desc = [
            it.locator('div[dir="ltr"]'),
            it.locator('p'),
            it.locator('[class*="inline-show-more-text"]'),
          ];
          for (const d of desc) {
            const h = d.first();
            const ok = await h.isVisible({ timeout: 600 }).catch(()=>false);
            if (ok) {
              const txt = await h.innerText().catch(()=>null);
              if (txt && txt.trim().length > 0) return txt.trim();
            }
          }
        } catch {}
      }
    } catch {}
  }
  // Mobile fallback
  try {
    const mHead = page.locator('h2:has-text("Experience")').first();
    if (await mHead.isVisible({ timeout: 800 }).catch(()=>false)) {
      const container = mHead.locator('..').locator('..');
      await container.scrollIntoViewIfNeeded().catch(()=>{});
      await microDelay();
      const firstCard = container.locator('li, article, div').first();
      if (await firstCard.isVisible({ timeout: 800 }).catch(()=>false)) {
        await revealSeeMoreIfPresent(firstCard);
        const txt = await firstCard.locator('p, div[dir="ltr"]').first().innerText().catch(()=>null);
        if (txt && txt.trim()) return txt.trim();
      }
    }
  } catch {}
  return null;
}

// =========================
// Connect flow (re-ordered detection)
// =========================
async function detectRelationshipStatus(page) {
  // 1) Pending is definitive
  const pendingCand = [
    page.getByRole("button", { name: /Pending|Requested|Withdraw|Pending invitation/i }),
    page.locator('text=/Pending invitation/i'),
  ];
  for (const p of pendingCand) {
    if (await p.first().isVisible({ timeout: 800 }).catch(() => false)) {
      return { status: "pending", reason: "Pending/Requested visible" };
    }
  }

  // 2) Any obvious Connect action (primary)
  const connectPrimary = [
    page.getByRole("button", { name: /^Connect$/i }),
    page.getByRole("link",   { name: /^Connect$/i }),
    page.locator('button:has-text("Connect"), a:has-text("Connect")'),
  ];
  for (const c of connectPrimary) {
    if (await c.first().isVisible({ timeout: 800 }).catch(() => false)) {
      return { status: "not_connected", reason: "Connect visible (primary)" };
    }
  }

  // 2b) Connect under "More" menu
  const moreBtns = [
    page.getByRole("button", { name: /^More$/i }),
    page.getByRole("button", { name: /More actions/i }),
    page.locator('button[aria-label="More actions"]'),
  ];
  for (const mb of moreBtns) {
    if (await mb.first().isVisible({ timeout: 800 }).catch(() => false)) {
      await mb.first().click({ timeout: 3000 }).catch(() => {});
      await microDelay();
      const menuConnect = [
        page.getByRole("menuitem", { name: /^Connect$/i }),
        page.locator('div[role="menuitem"]:has-text("Connect")'),
        page.locator('span:has-text("Connect")'),
      ];
      for (const mi of menuConnect) {
        if (await mi.first().isVisible({ timeout: 800 }).catch(() => false)) {
          return { status: "not_connected", reason: "Connect under More" };
        }
      }
    }
  }

  // 3) First-degree indicators → truly connected
  const firstDegree = [
    page.locator('span:has-text("1st")'),
    page.locator('span:has-text("1st degree")'),
    page.locator('span:has-text("Connected")'),
  ];
  for (const fd of firstDegree) {
    if (await fd.first().isVisible({ timeout: 600 }).catch(() => false)) {
      return { status: "connected", reason: "1st/Connected badge" };
    }
  }

  // 4) Message CTA but no Connect → likely InMail/Open Profile. Treat as not_connected.
  const msgBtns = [
    page.getByRole("button", { name: /^Message$/i }),
    page.getByRole("link",   { name: /^Message$/i }),
    page.locator('button[aria-label="Message"]'),
  ];
  for (const m of msgBtns) {
    if (await m.first().isVisible({ timeout: 800 }).catch(() => false)) {
      return { status: "not_connected", reason: "Message CTA only (InMail/Open Profile)" };
    }
  }

  return { status: "not_connected", reason: "No Connect/Message/Pending detected" };
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
      if (await b.first().isVisible({ timeout: 600 }).catch(() => false)) {
        await b.first().click({ timeout: 4000 });
        await microDelay();
        const dialogReady = await Promise.race([
          page.getByRole("dialog").waitFor({ timeout: 3000 }).then(() => true).catch(() => false),
          page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 3000 }).then(() => true).catch(() => false),
          page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 3000 }).then(() => true).catch(() => false),
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
      if (await m.first().isVisible({ timeout: 800 }).catch(() => false)) {
        await m.first().click({ timeout: 4000 });
        await microDelay();
        const menuConnect = [
          page.getByRole("menuitem", { name: /^Connect$/i }),
          page.locator('div[role="menuitem"]:has-text("Connect")'),
          page.locator('span:has-text("Connect")'),
        ];
        for (const mi of menuConnect) {
          if (await mi.first().isVisible({ timeout: 800 }).catch(() => false)) {
            await mi.first().click({ timeout: 4000 });
            await microDelay();
            const dialogReady = await Promise.race([
              page.getByRole("dialog").waitFor({ timeout: 3000 }).then(() => true).catch(() => false),
              page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 3000 }).then(() => true).catch(() => false),
              page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 3000 }).then(() => true).catch(() => false),
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
    if (await mobileConnect.first().isVisible({ timeout: 800 }).catch(() => false)) {
      await mobileConnect.first().click({ timeout: 4000 });
      await microDelay();
      const dialogReady = await Promise.race([
        page.getByRole("dialog").waitFor({ timeout: 3000 }).then(() => true).catch(() => false),
        page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 3000 }).then(() => true).catch(() => false),
        page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 3000 }).then(() => true).catch(() => false),
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
        if (await ta.first().isVisible({ timeout: 800 }).catch(() => false)) {
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
          page.getByRole("dialog").waitFor({ state: "detached", timeout: 4000 }).then(() => true).catch(() => false),
          page.locator('div:has-text("Invitation sent")').waitFor({ timeout: 4000 }).then(() => true).catch(() => false),
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
      const closed = await page.getByRole("dialog").waitFor({ state: "detached", timeout: 4000 }).then(() => true).catch(() => false);
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
    try { await page.mouse.wheel(0, within(600, 1000)); await sleep(within(500, 900)); } catch {}
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
// Message flow (unchanged)
// =========================
async function openMessageDialog(page) {
  const buttons = [
    page.getByRole("button", { name: /^Message$/i }),
    page.getByRole("link",   { name: /^Message$/i }),
    page.locator('button[aria-label="Message"]'),
  ];
  for (const btn of buttons) {
    try {
      if (await btn.first().isVisible({ timeout: 1000 }).catch(() => false)) {
        await btn.first().click({ timeout: 4000 }); await microDelay();
        const ready = await Promise.race([
          page.getByRole("dialog").waitFor({ timeout: 4000 }).then(() => true).catch(() => false),
          page.locator('[data-test-conversation-compose], .msg-form__contenteditable').waitFor({ timeout: 4000 }).then(() => true).catch(() => false),
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
      if (await handle.isVisible({ timeout: 1200 }).catch(() => false)) {
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
      if (await s.first().isVisible({ timeout: 1200 }).catch(() => false)) {
        await s.first().click({ timeout: 4000 }); await microDelay();
        const sent = await Promise.race([
          page.locator('div:has-text("Message sent")').waitFor({ timeout: 3000 }).then(() => true).catch(() => false),
          page.locator('.msg-form__contenteditable[contenteditable="true"]').evaluate(el => (el.innerText || "").trim().length === 0).catch(() => false),
          sleep(1200).then(() => true),
        ]);
        if (sent) return true;
      }
    } catch {}
  }
  // Keystroke fallbacks
  try {
    const editor = page.locator('.msg-form__contenteditable[contenteditable="true"], [role="textbox"][contenteditable="true"], textarea').first();
    if (await editor.isVisible({ timeout: 600 }).catch(() => false)) {
      await editor.press("Enter").catch(()=>{});
      await microDelay();
      const sentByEnter = await Promise.race([
        page.locator('div:has-text("Message sent")').waitFor({ timeout: 1500 }).then(() => true).catch(() => false),
        sleep(1200).then(() => true),
      ]);
      if (sentByEnter) return true;

      await editor.press("Control+Enter").catch(()=>{});
      await microDelay();
      const sentByCtrlEnter = await Promise.race([
        page.locator('div:has-text("Message sent")').waitFor({ timeout: 1500 }).then(() => true).catch(() => false),
        sleep(1200).then(() => true),
      ]);
      if (sentByCtrlEnter) return true;
    }
  } catch {}

  return false;
}

async function sendMessageFlow(page, messageText) {
  const rs = await detectRelationshipStatus(page);
  if (rs.status !== "connected") {
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

// =========================
// AUTH_CHECK handler (for viewer sign-in/2FA & state persist)
// =========================
async function handleAuthCheck(job) {
  const userId = job?.payload?.userId || "default";
  const cookieBundle = job?.payload?.cookieBundle || {};
  if (SOFT_MODE) {
    return { mode: "soft", message: "Auth check soft OK", at: new Date().toISOString() };
  }

  let browser, context, page, saveState;
  try {
    ({ browser, context, page, saveState } = await createBrowserContext(cookieBundle, HEADLESS, userId));
    await context.tracing.start({ screenshots: true, snapshots: false });

    // Go to feed, let the user log in if needed, then wander a bit
    let auth = await ensureAuthenticated(context, page);
    if (!auth.ok) {
      // Drive to login and wait interactively
      if (ALLOW_INTERACTIVE_LOGIN) {
        try { await page.goto("https://www.linkedin.com/login", { waitUntil: "domcontentloaded", timeout: 30000 }); } catch {}
        const deadline = Date.now() + INTERACTIVE_LOGIN_TIMEOUT_MS;
        while (Date.now() < deadline) {
          await sleep(1500);
          const ok = !(await isAuthWalledOrGuest(page));
          if (ok) break;
        }
      }
      // Re-check
      auth = await ensureAuthenticated(context, page);
      if (!auth.ok) {
        await context.tracing.stop({ path: "/tmp/trace-auth-failed.zip" }).catch(()=>{});
        await browser.close().catch(()=>{});
        return { mode: "real", message: "Not authenticated yet", at: new Date().toISOString() };
      }
    }

    // Ensure we are on feed, wait 4s, wander ~2s
    try { await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 25000 }); } catch {}
    await wanderOnFeed(page);
    await saveState();

    await context.tracing.stop({ path: "/tmp/trace-auth.zip" }).catch(()=>{});
    await browser.close().catch(()=>{});
    const video = page.video?.();
    if (video) { try { console.log("[video] saved:", await video.path()); } catch {} }
    return { mode: "real", message: "Authenticated and storageState saved.", at: new Date().toISOString() };
  } catch (e) {
    try { await browser?.close(); } catch {}
    throw e;
  }
}

// =========================
// Job handlers (PUBLIC ONLY)
// =========================
async function handleSendConnection(job) {
  const { payload } = job || {};
  if (!payload) throw new Error("Job has no payload");

  let targetUrl = payload.profileUrl || null;
  if (!targetUrl && payload.publicIdentifier) {
    targetUrl = `https://www.linkedin.com/in/${encodeURIComponent(payload.publicIdentifier)}`;
  }
  const note = payload.note || null;
  const cookieBundle = payload.cookieBundle || {};
  const userId = payload.userId || payload.email || "default";

  if (!targetUrl) throw new Error("payload.profileUrl or publicIdentifier required");

  if (SOFT_MODE) {
    await throttle.reserve("linkedin.com", "SOFT send_connection"); await microDelay(); throttle.success("linkedin.com");
    return { mode: "soft", profileUrl: targetUrl, noteUsed: note, message: "Soft mode success (no browser).", at: new Date().toISOString() };
  }

  await throttle.reserve("linkedin.com", "SEND_CONNECTION");

  let browser, context, page, saveState, videoHandle;
  try {
    ({ browser, context, page, saveState } = await createBrowserContext(cookieBundle, HEADLESS, userId));
    videoHandle = page.video?.();
    await context.tracing.start({ screenshots: true, snapshots: false });

    // AUTH PREFLIGHT -> interactive public login if needed
    let auth = await ensureAuthenticated(context, page);
    if (!auth.ok) {
      const interStart = Date.now();
      if (ALLOW_INTERACTIVE_LOGIN) {
        try { await page.goto("https://www.linkedin.com/login", { waitUntil: "domcontentloaded", timeout: 30000 }); } catch {}
        while (Date.now() - interStart < INTERACTIVE_LOGIN_TIMEOUT_MS) {
          await sleep(1500);
          const ok = !(await isAuthWalledOrGuest(page));
          if (ok) break;
        }
      }
      auth = await ensureAuthenticated(context, page);
    }
    if (!auth.ok) {
      const result = {
        mode: "real",
        profileUrl: targetUrl,
        usedUrl: "preflight",
        finalUrl: auth.url,
        httpStatus: null,
        pageTitle: await page.title().catch(() => null),
        relationshipStatus: "not_connected",
        actionTaken: "unavailable",
        details: "Not authenticated (guest/authwall). Complete 2FA or refresh cookies.",
        authDiag: auth.diag,
        at: new Date().toISOString(),
      };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {});
      if (videoHandle) { try { console.log("[video] saved:", await videoHandle.path()); } catch {} }
      throttle.failure("linkedin.com");
      return result;
    }

    // Human: go to FEED first, wait & wander (4s + ~2s), THEN open the lead
    try { await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 25000 }); } catch {}
    await wanderOnFeed(page);

    // Navigate to public profile only
    const nav = await navigateLinkedInWithRetries(page, targetUrl, { attempts: 4 });
    if (!nav.authed) {
      const result = {
        mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl || page.url(),
        httpStatus: nav.status, pageTitle: await page.title().catch(() => null),
        relationshipStatus: "not_connected", actionTaken: "unavailable", details: "Authwall/999 on profile nav.",
        at: new Date().toISOString(),
      };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {});
      if (videoHandle) { try { console.log("[video] saved:", await videoHandle.path()); } catch {} }
      throttle.failure("linkedin.com");
      return result;
    }

    // Wait for profile to settle, then scrape About + Current role (expand if needed)
    await waitProfileSettled(page);
    const aboutText = await getAboutText(page);
    const currentRoleText = await getCurrentRoleText(page);

    // Send connection (after scraping)
    await humanizePage(page);
    const connectOutcome = await sendConnectionRequest(page, note);

    // Hold for a couple seconds to look human after sending
    await sleep(POST_CONNECT_HOLD_MS);

    // Save state for next runs
    await saveState();

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
      aboutText: aboutText || null,
      currentRoleText: currentRoleText || null,
      at: new Date().toISOString(),
    };

    try { await context.tracing.stop({ path: "/tmp/trace.zip" }); } catch {}
    await browser.close().catch(() => {});
    if (videoHandle) { try { console.log("[video] saved:", await videoHandle.path()); } catch {} }
    if (connectOutcome.actionTaken === "sent" || connectOutcome.actionTaken === "sent_maybe") throttle.success("linkedin.com");
    else if (connectOutcome.actionTaken === "failed_to_send" || connectOutcome.actionTaken === "unavailable") throttle.failure("linkedin.com");
    else throttle.success("linkedin.com");

    return result;
  } catch (e) {
    try {
      const title = await page?.title().catch(() => null);
      const html = await page?.content().catch(() => null);
      if (title) console.log("[debug] page.title:", title);
      if (html)  console.log("[debug] page.content length:", html.length);
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
  const userId = payload.userId || payload.email || "default";
  if (!messageText) throw new Error("payload.message required");
  if (!targetUrl) throw new Error("payload.profileUrl or publicIdentifier required");

  if (SOFT_MODE) {
    await throttle.reserve("linkedin.com", "SOFT send_message"); await microDelay(); throttle.success("linkedin.com");
    return { mode: "soft", profileUrl: targetUrl, messageUsed: messageText, message: "Soft mode success (no browser).", at: new Date().toISOString() };
  }

  await throttle.reserve("linkedin.com", "SEND_MESSAGE");

  let browser, context, page, saveState, videoHandle;
  try {
    ({ browser, context, page, saveState } = await createBrowserContext(cookieBundle, HEADLESS, userId));
    videoHandle = page.video?.();
    await context.tracing.start({ screenshots: true, snapshots: false });

    // AUTH PREFLIGHT -> interactive public login if needed
    let auth = await ensureAuthenticated(context, page);
    if (!auth.ok) {
      const interStart = Date.now();
      if (ALLOW_INTERACTIVE_LOGIN) {
        try { await page.goto("https://www.linkedin.com/login", { waitUntil: "domcontentloaded", timeout: 30000 }); } catch {}
        while (Date.now() - interStart < INTERACTIVE_LOGIN_TIMEOUT_MS) {
          await sleep(1500);
          const ok = !(await isAuthWalledOrGuest(page));
          if (ok) break;
        }
      }
      auth = await ensureAuthenticated(context, page);
    }
    if (!auth.ok) {
      const result = {
        mode: "real",
        profileUrl: targetUrl,
        usedUrl: "preflight",
        finalUrl: auth.url,
        httpStatus: null,
        pageTitle: await page.title().catch(() => null),
        relationshipStatus: "unknown",
        actionTaken: "unavailable",
        details: "Not authenticated (guest/authwall). Complete 2FA or refresh cookies.",
        authDiag: auth.diag,
        at: new Date().toISOString(),
      };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {});
      if (videoHandle) { try { console.log("[video] saved:", await videoHandle.path()); } catch {} }
      throttle.failure("linkedin.com");
      return result;
    }

    // Human: go to FEED first, wait & wander (4s + ~2s), THEN open the lead
    try { await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 25000 }); } catch {}
    await wanderOnFeed(page);

    // Navigate to public profile only
    const nav = await navigateLinkedInWithRetries(page, targetUrl, { attempts: 4 });
    if (!nav.authed) {
      const result = {
        mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl || page.url(),
        httpStatus: nav.status, pageTitle: await page.title().catch(() => null),
        relationshipStatus: "unknown", actionTaken: "unavailable", details: "Authwall/999 on profile nav.",
        at: new Date().toISOString(),
      };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {});
      if (videoHandle) { try { console.log("[video] saved:", await videoHandle.path()); } catch {} }
      throttle.failure("linkedin.com");
      return result;
    }

    await waitProfileSettled(page);
    await humanizePage(page);
    const outcome = await sendMessageFlow(page, messageText);
    await saveState();

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
    if (videoHandle) { try { console.log("[video] saved:", await videoHandle.path()); } catch {} }
    if (outcome.actionTaken === "sent") throttle.success("linkedin.com");
    else if (outcome.actionTaken?.startsWith("failed") || outcome.actionTaken === "unavailable") throttle.failure("linkedin.com");
    else throttle.success("linkedin.com");

    return result;
  } catch (e) {
    try {
      const title = await page?.title().catch(() => null);
      const html = await page?.content().catch(() => null);
      if (title) console.log("[debug] page.title:", title);
      if (html)  console.log("[debug] page.content length:", html.length);
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
      console.log(`[worker] Job ${job.id} done:`, result?.message || result?.details || result);
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
