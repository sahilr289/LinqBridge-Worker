// worker.cjs — LinqBridge Worker (two-tab via window.open, safe nav, 429/404 guard)

const path = require("path");
const fs = require("fs");

// ------------------------- Health (optional)
if (process.env.ENABLE_HEALTH === "true") {
  try {
    const http = require("http");
    const HEALTH_PORT = parseInt(process.env.HEALTH_PORT || "3001", 10);
    http.createServer((req, res) => {
      if (req.url === "/" || req.url === "/health") {
        res.writeHead(200, { "content-type": "text/plain" }); res.end("OK\n");
      } else { res.writeHead(404); res.end(); }
    }).listen(HEALTH_PORT, () => console.log(`[health] listening on :${HEALTH_PORT}`));
  } catch (e) { console.log("[health] server not started:", e?.message || e); }
}

// ========================= Env & Config
const API_BASE = process.env.API_BASE || "https://YOUR-BACKEND.example.com";
const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET || "";

const HEADLESS = (/^(true|1|yes)$/i).test(process.env.HEADLESS || "false");
const SLOWMO_MS = parseInt(process.env.SLOWMO_MS || (HEADLESS ? "0" : "50"), 10);
const SOFT_MODE = (/^(true|1|yes)$/i).test(process.env.SOFT_MODE || "false");
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000", 10);

// Throttle
const MAX_ACTIONS_PER_HOUR = parseInt(process.env.MAX_ACTIONS_PER_HOUR || "18", 10);
const MIN_GAP_MS = parseInt(process.env.MIN_GAP_MS || "60000", 10);
const COOLDOWN_AFTER_SENT_MS = parseInt(process.env.COOLDOWN_AFTER_SENT_MS || "90000", 10);
const COOLDOWN_AFTER_FAIL_MS = parseInt(process.env.COOLDOWN_AFTER_FAIL_MS || "600000", 10);

// Micro delays
const MICRO_DELAY_MIN_MS = parseInt(process.env.MICRO_DELAY_MIN_MS || "400", 10);
const MICRO_DELAY_MAX_MS = parseInt(process.env.MICRO_DELAY_MAX_MS || "1200", 10);

// Spec waits
const FEED_INITIAL_WAIT_MS = parseInt(process.env.FEED_INITIAL_WAIT_MS || "10000", 10);
const FEED_SCROLL_MS = parseInt(process.env.FEED_SCROLL_MS || "5000", 10);
const PROFILE_INITIAL_WAIT_MS = parseInt(process.env.PROFILE_INITIAL_WAIT_MS || "10000", 10);

// Fallbacks
const ALLOW_MOBILE_FALLBACK = (/^(true|1|yes)$/i).test(process.env.ALLOW_MOBILE_FALLBACK || "true");

// Session / login
const DEFAULT_STATE_PATH = process.env.STORAGE_STATE_PATH || "/app/auth-state.json";
const STATE_DIR = process.env.STATE_DIR || "/app/state";
const FORCE_RELOGIN = (/^(true|1|yes)$/i).test(process.env.FORCE_RELOGIN || "false");
const ALLOW_INTERACTIVE_LOGIN = (/^(true|1|yes)$/i).test(process.env.ALLOW_INTERACTIVE_LOGIN || "true");
const INTERACTIVE_LOGIN_TIMEOUT_MS = parseInt(process.env.INTERACTIVE_LOGIN_TIMEOUT_MS || "300000", 10);

// Proxy (optional)
const PROXY_SERVER = process.env.PROXY_SERVER || "";
const PROXY_USERNAME = process.env.PROXY_USERNAME || "";
const PROXY_PASSWORD = process.env.PROXY_PASSWORD || "";

let chromium = null;

// ========================= Utils
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
const apiUrl = (p) => (p.startsWith("/") ? `${API_BASE}${p}` : `${API_BASE}/${p}`);

async function apiGet(p) {
  const res = await fetch(apiUrl(p), { method: "GET", headers: { "x-worker-secret": WORKER_SHARED_SECRET }, signal: AbortSignal.timeout?.(15000) });
  const text = await res.text();
  try { const json = JSON.parse(text); if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`); return json; }
  catch { throw new Error(`GET ${p} non-JSON or error ${res.status}: ${text}`); }
}
async function apiPost(p, body) {
  const res = await fetch(apiUrl(p), {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-worker-secret": WORKER_SHARED_SECRET },
    body: JSON.stringify(body || {}),
    signal: AbortSignal.timeout?.(25000),
  });
  const text = await res.text();
  try { const json = JSON.parse(text); if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`); return json; }
  catch { throw new Error(`POST ${p} non-JSON or error ${res.status}: ${text}`); }
}

// ========================= Throttle
class DomainThrottle {
  constructor() { this.state = new Map(); }
  _get(domain) {
    if (!this.state.has(domain)) this.state.set(domain, { lastActionAt: 0, events: [], cooldownUntil: 0 });
    return this.state.get(domain);
  }
  _pruneOld(events) { const cutoff = now() - 3600_000; while (events.length && events[0] < cutoff) events.shift(); }
  async reserve(domain, label = "action") {
    const st = this._get(domain);
    while (true) {
      this._pruneOld(st.events);
      const waits = [];
      const ts = now();
      if (st.cooldownUntil > ts) waits.push(st.cooldownUntil - ts);
      if (st.events.length >= MAX_ACTIONS_PER_HOUR) waits.push((st.events[0] + 3600_000) - ts);
      const sinceLast = ts - (st.lastActionAt || 0);
      if (sinceLast < MIN_GAP_MS) waits.push(MIN_GAP_MS - sinceLast);
      if (waits.length) {
        const waitMs = Math.max(...waits) + within(1500, 3500);
        console.log(`[throttle] Waiting ${Math.ceil(waitMs/1000)}s before ${label} (used: ${st.events.length}/${MAX_ACTIONS_PER_HOUR})`);
        await sleep(waitMs); continue;
      }
      st.lastActionAt = ts; st.events.push(ts);
      console.log(`[throttle] Reserved slot for ${label}. Used this hour: ${st.events.length}/${MAX_ACTIONS_PER_HOUR}`);
      return;
    }
  }
  success(domain) { const st = this._get(domain); st.cooldownUntil = Math.max(st.cooldownUntil, now() + COOLDOWN_AFTER_SENT_MS); }
  failure(domain) { const st = this._get(domain); st.cooldownUntil = Math.max(st.cooldownUntil, now() + COOLDOWN_AFTER_FAIL_MS); }
}
const throttle = new DomainThrottle();

// ========================= Playwright
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

  // Extend timeouts to be generous
  context.setDefaultTimeout(45000);
  context.setDefaultNavigationTimeout(90000);

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
      Object.defineProperty(navigator, "userAgent", {
        get: () =>
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
  page.setDefaultTimeout(45000);
  page.setDefaultNavigationTimeout(90000);

  // Optional cookie bundle
  if (cookieBundle && (cookieBundle.li_at || cookieBundle.jsessionid || cookieBundle.bcookie)) {
    const expandDomains = (cookie) => ([
      { ...cookie, domain: "linkedin.com" },
      { ...cookie, domain: "www.linkedin.com" },
      { ...cookie, domain: "m.linkedin.com" },
    ]);
    const cookies = [];
    if (cookieBundle.li_at) cookies.push(...expandDomains({ name: "li_at", value: cookieBundle.li_at, path: "/", httpOnly: true, secure: true, sameSite: "None" }));
    if (cookieBundle.jsessionid) cookies.push(...expandDomains({ name: "JSESSIONID", value: `"${cookieBundle.jsessionid}"`, path: "/", httpOnly: true, secure: true, sameSite: "None" }));
    if (cookieBundle.bcookie) cookies.push(...expandDomains({ name: "bcookie", value: cookieBundle.bcookie, path: "/", httpOnly: false, secure: true, sameSite: "None" }));
    try { await context.addCookies(cookies); } catch {}
  }

  try { await page.bringToFront(); } catch {}
  return { browser, context, page };
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
  } catch (e) { console.log("[auth] storageState save failed:", e?.message || e); }
}

async function isAuthWalledOrGuest(page) {
  try {
    const title = (await page.title().catch(() => ""))?.toLowerCase?.() || "";
    if (title.includes("sign in") || title.includes("join linkedin") || title.includes("authwall")) return true;
    const hasLogin = await page.locator('a[href*="login"]').first().isVisible({ timeout: 600 }).catch(() => false);
    return !!hasLogin;
  } catch { return false; }
}

async function ensureAuthenticated(context, page, userStatePath) {
  const before = await cookieDiag(context);

  try {
    console.log("[nav] → feed-desktop: https://www.linkedin.com/feed/");
    const r = await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 60000 });
    const s = r ? r.status() : null;
    console.log("[nav] ✓ feed-desktop: status=%s final=%s", s, page.url());
    if (s && s >= 200 && s < 400 && !(await isAuthWalledOrGuest(page))) {
      await saveStorageState(context, userStatePath || DEFAULT_STATE_PATH);
      return { ok: true, via: "desktop", status: s, url: page.url(), diag: before };
    }
  } catch {}

  try {
    console.log("[nav] → feed-mobile: https://m.linkedin.com/feed/");
    const r = await page.goto("https://m.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 60000 });
    const s = r ? r.status() : null;
    console.log("[nav] ✓ feed-mobile: status=%s final=%s", s, page.url());
    if (s && s >= 200 && s < 400 && !(await isAuthWalledOrGuest(page))) {
      await saveStorageState(context, userStatePath || DEFAULT_STATE_PATH);
      return { ok: true, via: "mobile", status: s, url: page.url(), diag: before };
    }
  } catch {}

  if (!ALLOW_INTERACTIVE_LOGIN) return { ok: false, reason: "guest_or_authwall", url: page.url(), diag: before };

  try {
    console.log("[nav] → login: https://www.linkedin.com/login");
    const r = await page.goto("https://www.linkedin.com/login", { waitUntil: "domcontentloaded", timeout: 60000 });
    console.log("[nav] ✓ login: status=%s final=%s", r ? r.status() : null, page.url());
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

// ========== Helpers
function extractInSlug(u) {
  const m = String(u || "").match(/linkedin\.com\/in\/([^\/?#]+)/i);
  return m ? decodeURIComponent(m[1]) : null;
}
function looksLikeUrnSlug(slug) { return /^AC[ow]/.test(String(slug || "")); }

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

// Nav guard (detect repeated reloads of same top URL)
function attachNavGuard(page) {
  const ns = { lastTopUrl: "", countSameUrlReloads: 0, t0: 0 };
  page.on("framenavigated", (frame) => {
    if (!frame.parentFrame()) {
      const u = frame.url();
      if (!ns.t0) ns.t0 = Date.now();
      if (u === ns.lastTopUrl) ns.countSameUrlReloads++;
      else { ns.lastTopUrl = u; ns.countSameUrlReloads = 0; ns.t0 = Date.now(); }
      const age = Date.now() - ns.t0;
      if (age < 20000 && ns.countSameUrlReloads >= 3) console.log("[nav-guard] excessive reloads on", u);
    }
  });
  return ns;
}

// Robust navigation: treat either goto OR URL match as success
async function safeGoto(page, url, opts = {}) {
  const timeout = opts.timeout ?? 90000;
  const waitUntil = opts.waitUntil ?? "domcontentloaded";

  // Start both: goto + waitForURL (pattern created from URL)
  const slug = extractInSlug(url);
  const urlPattern = slug ? new RegExp(`linkedin\\.com\\/in\\/${slug.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}\\/?`) : /linkedin\.com\/in\//i;

  let resp = null, urlMatched = false;
  try {
    const race = await Promise.race([
      (async () => { const r = await page.goto(url, { waitUntil, timeout }); return { type: "goto", r }; })(),
      (async () => { await page.waitForURL(urlPattern, { timeout }); return { type: "url" }; })(),
    ]);
    if (race.type === "goto") { resp = race.r; }
    else { urlMatched = true; }
  } catch (e) {
    // If goto threw but URL already changed, accept
    const current = page.url() || "";
    if (urlPattern.test(current)) urlMatched = true; else throw e;
  }
  const status = resp ? resp.status() : null;
  return { status, urlMatched, finalUrl: page.url() };
}

// Use feed to open profile via window.open (more human; avoids some stalls)
async function openProfileTabViaFeed(context, feedPage, targetUrl) {
  let profilePage = null;
  try {
    const [p] = await Promise.all([
      context.waitForEvent("page", { timeout: 15000 }),
      feedPage.evaluate((u) => window.open(u, "_blank"), targetUrl),
    ]);
    profilePage = p;
    await profilePage.bringToFront().catch(()=>{});
    // Wait either for URL match or DOM ready
    await Promise.race([
      profilePage.waitForURL(/linkedin\.com\/in\//i, { timeout: 90000 }),
      profilePage.waitForLoadState("domcontentloaded", { timeout: 90000 }),
    ]);
    return profilePage;
  } catch {
    // Fallback: manual new tab + safeGoto
    profilePage = await context.newPage();
    await profilePage.bringToFront().catch(()=>{});
    await safeGoto(profilePage, targetUrl, { timeout: 90000, waitUntil: "domcontentloaded" });
    return profilePage;
  }
}

// Clean profile nav wrapper with mobile fallback (no loops)
async function navigateProfileClean(page, rawUrl) {
  try {
    const a = await safeGoto(page, rawUrl, { timeout: 90000, waitUntil: "domcontentloaded" });
    const authed = !(await isAuthWalledOrGuest(page));
    const hard = await detectHardScreen(page);
    if (a.status === 429 || hard === "429") return { authed: false, status: 429, usedUrl: rawUrl, finalUrl: a.finalUrl, error: "rate_limited" };
    if (hard === "404") return { authed: false, status: a.status || 404, usedUrl: rawUrl, finalUrl: a.finalUrl, error: "not_found" };
    if (authed && (a.urlMatched || (a.status && a.status >= 200 && a.status < 400)) && !hard) {
      return { authed: true, status: a.status || 200, usedUrl: rawUrl, finalUrl: a.finalUrl };
    }
  } catch (e) {
    // fall through
  }

  if (ALLOW_MOBILE_FALLBACK && /linkedin\.com\/in\//i.test(rawUrl)) {
    const murl = rawUrl.replace("www.linkedin.com", "m.linkedin.com").replace("linkedin.com/in/", "m.linkedin.com/in/");
    try {
      const b = await safeGoto(page, murl, { timeout: 90000, waitUntil: "domcontentloaded" });
      const authed2 = !(await isAuthWalledOrGuest(page));
      const hard2 = await detectHardScreen(page);
      if (b.status === 429 || hard2 === "429") return { authed: false, status: 429, usedUrl: murl, finalUrl: b.finalUrl, error: "rate_limited" };
      if (hard2 === "404") return { authed: false, status: b.status || 404, usedUrl: murl, finalUrl: b.finalUrl, error: "not_found" };
      if (authed2 && (b.urlMatched || (b.status && b.status >= 200 && b.status < 400)) && !hard2) {
        return { authed: true, status: b.status || 200, usedUrl: murl, finalUrl: b.finalUrl };
      }
    } catch (e2) {
      return { authed: false, status: null, usedUrl: murl, finalUrl: page.url(), error: e2?.message || "nav_failed" };
    }
  }

  return { authed: false, status: null, usedUrl: rawUrl, finalUrl: page.url(), error: "authwall_or_unknown" };
}

// Human scroll and feed warmup
async function humanScroll(page, durationMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < durationMs) {
    try { await page.mouse.wheel(0, within(120, 320)); } catch {}
    await sleep(within(120, 220));
  }
}
async function feedWarmup(page) {
  try {
    if (!/linkedin\.com\/feed\/?$/i.test(page.url())) {
      await safeGoto(page, "https://www.linkedin.com/feed/", { timeout: 60000, waitUntil: "domcontentloaded" });
    }
    console.log("[feed] warmup: waiting %d ms, then scrolling %d ms", FEED_INITIAL_WAIT_MS, FEED_SCROLL_MS);
    await sleep(FEED_INITIAL_WAIT_MS);
    await humanScroll(page, FEED_SCROLL_MS);
  } catch {}
}

// “See more” helper
async function scrollAndExpand(page, sectionLocator) {
  try {
    await sectionLocator.scrollIntoViewIfNeeded().catch(()=>{});
    await sleep(300 + Math.random()*400);
    const seeMore = sectionLocator.getByRole("button", { name: /see more/i }).first();
    const exists = await seeMore.isVisible({ timeout: 1200 }).catch(() => false);
    if (exists) { await seeMore.click({ timeout: 4000 }).catch(()=>{}); await sleep(500 + Math.random()*700); }
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

// Relationship & connect
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
      const visible = await c.loc.first().isVisible({ timeout: 1200 }).catch(() => false);
      if (visible) {
        if (c.type === "connected") return { status: "connected", reason: "Message CTA visible" };
        if (c.type === "pending")   return { status: "pending", reason: "Pending/Requested visible" };
        if (c.type === "can_connect") return { status: "not_connected" };
      }
    } catch {}
  }
  return { status: "not_connected", reason: "Connect may be under More" };
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

// ========================= Message flow
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
        if (tag === "textarea" || tag === "input") await handle.fill(limited, { timeout: 4000 });
        else { await handle.press("Control+A").catch(()=>{}); await handle.type(limited, { delay: within(5, 25) }); }
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

// ========================= Job handlers
async function handleAuthCheck(job) {
  const { payload } = job || {};
  const userId = payload?.userId || "default";
  const userStatePath = statePathForUser(userId);

  let browser, context, page, videoHandle;
  try {
    ({ browser, context, page } = await createBrowserContext({ headless: HEADLESS, cookieBundle: null, userStatePath }));
    videoHandle = page.video?.();
    await context.tracing.start({ screenshots: true, snapshots: false });

    const auth = await ensureAuthenticated(context, page, userStatePath);
    if (auth.ok) { await feedWarmup(page); }

    const result = {
      ok: !!auth.ok,
      via: auth.via || "unknown",
      url: auth.url || null,
      diag: auth.diag || null,
      message: auth.ok ? "Authenticated and storageState saved." : `Auth failed: ${auth.reason || "unknown"}`,
      at: new Date().toISOString(),
      statePath: userStatePath,
    };

    try { await context.tracing.stop({ path: "/tmp/trace-auth.zip" }); } catch {}
    await browser.close().catch(() => {});
    if (videoHandle) { try { console.log("[video] saved:", await videoHandle.path()); } catch {} }
    return result;
  } catch (e) {
    try { await context?.tracing?.stop({ path: "/tmp/trace-auth-failed.zip" }).catch(()=>{}); } catch {}
    try { await browser?.close(); } catch {}
    throw new Error(`AUTH_CHECK failed: ${e.message}`);
  }
}

async function handleSendConnection(job) {
  const { payload } = job || {};
  if (!payload) throw new Error("Job has no payload");
  let targetUrl = payload.profileUrl || (payload.publicIdentifier ? `https://www.linkedin.com/in/${encodeURIComponent(payload.publicIdentifier)}/` : null);
  if (!targetUrl) throw new Error("payload.profileUrl or publicIdentifier required");

  // Bad/URN slug guard
  const slug = extractInSlug(targetUrl);
  if (slug && looksLikeUrnSlug(slug)) {
    throttle.failure("linkedin.com");
    return { mode: "real", profileUrl: targetUrl, actionTaken: "invalid_profile_url", relationshipStatus: "unknown", details: "Looks like internal URN; use public slug.", at: new Date().toISOString() };
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
    await context.tracing.start({ screenshots: true, snapshots: false });

    const auth = await ensureAuthenticated(context, feedPage, userStatePath);
    if (!auth.ok) {
      const result = { mode: "real", profileUrl: targetUrl, usedUrl: "preflight", finalUrl: auth.url, httpStatus: null, relationshipStatus: "not_connected", actionTaken: "unavailable", details: "Not authenticated.", authDiag: auth.diag, at: new Date().toISOString() };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {}); throttle.failure("linkedin.com"); return result;
    }
    await feedWarmup(feedPage);

    // PROFILE TAB via window.open from feed
    profilePage = await openProfileTabViaFeed(context, feedPage, targetUrl);
    const navStability = attachNavGuard(profilePage);

    const nav = await navigateProfileClean(profilePage, targetUrl);
    if (!nav.authed) {
      const result = { mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl || profilePage.url(), httpStatus: nav.status, relationshipStatus: "not_connected", actionTaken: "unavailable", details: nav.error || "Authwall/404/429.", at: new Date().toISOString() };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {}); throttle.failure("linkedin.com"); return result;
    }

    await sleep(PROFILE_INITIAL_WAIT_MS);
    if (navStability.countSameUrlReloads >= 3) {
      const result = { mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: profilePage.url(), httpStatus: nav.status, relationshipStatus: "unknown", actionTaken: "auto_reload_detected", details: "Repeated reloads; backing off.", at: new Date().toISOString() };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {}); throttle.failure("linkedin.com"); return result;
    }

    const hard = await detectHardScreen(profilePage);
    if (hard === "404" || hard === "429" || hard === "captcha")) {
      const details = hard === "404" ? "Profile 404." : hard === "429" ? "Rate-limited." : "Verification/CAPTCHA.";
      const result = { mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: profilePage.url(), httpStatus: nav.status, relationshipStatus: "unknown", actionTaken: hard === "404" ? "page_not_found" : "rate_limited", details, at: new Date().toISOString() };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {}); throttle.failure("linkedin.com"); return result;
    }

    const { about, currentRole } = await scrapeAboutAndCurrentRole(profilePage);
    await microDelay();
    const connectOutcome = await sendConnectionRequest(profilePage, note);
    await sleep(2000);

    const result = {
      mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl || profilePage.url(),
      noteUsed: note, httpStatus: nav.status, relationshipStatus: connectOutcome.relationshipStatus,
      actionTaken: connectOutcome.actionTaken, details: connectOutcome.details,
      scraped: { aboutLength: about?.length || 0, currentRoleLength: currentRole?.length || 0, about, currentRole },
      at: new Date().toISOString(),
    };

    try { await context.tracing.stop({ path: "/tmp/trace.zip" }); } catch {}
    await browser.close().catch(() => {});

    if (["sent", "sent_maybe"].includes(connectOutcome.actionTaken)) throttle.success("linkedin.com");
    else if (["failed_to_send","unavailable"].includes(connectOutcome.actionTaken) || result.actionTaken === "auto_reload_detected") throttle.failure("linkedin.com");
    else throttle.success("linkedin.com");

    return result;
  } catch (e) {
    try { await profilePage?.screenshot?.({ path: "/tmp/last-error.png", fullPage: false }).catch(() => {}); await context?.tracing?.stop({ path: "/tmp/trace-failed.zip" }).catch(() => {}); } catch {}
    try { await context?.close(); await browser?.close(); } catch {}
    throttle.failure("linkedin.com");
    throw new Error(`SEND_CONNECTION failed: ${e.message}`);
  }
}

async function handleSendMessage(job) {
  const { payload } = job || {};
  if (!payload) throw new Error("Job has no payload");
  let targetUrl = payload.profileUrl || (payload.publicIdentifier ? `https://www.linkedin.com/in/${encodeURIComponent(payload.publicIdentifier)}/` : null);
  if (!targetUrl) throw new Error("payload.profileUrl or publicIdentifier required");

  const slug = extractInSlug(targetUrl);
  if (slug && looksLikeUrnSlug(slug)) {
    throttle.failure("linkedin.com");
    return { mode: "real", profileUrl: targetUrl, actionTaken: "invalid_profile_url", relationshipStatus: "unknown", details: "Looks like internal URN; use public slug.", at: new Date().toISOString() };
  }

  const messageText = payload.message;
  if (!messageText) throw new Error("payload.message required");

  const cookieBundle = payload.cookieBundle || {};
  const userId = payload.userId || "default";
  const userStatePath = statePathForUser(userId);

  if (SOFT_MODE) {
    await throttle.reserve("linkedin.com", "SOFT send_message"); await microDelay(); throttle.success("linkedin.com");
    return { mode: "soft", profileUrl: targetUrl, messageUsed: messageText, message: "Soft mode success (no browser).", at: new Date().toISOString() };
  }

  await throttle.reserve("linkedin.com", "SEND_MESSAGE");

  let browser, context, feedPage, profilePage, videoHandle;
  try {
    ({ browser, context, page: feedPage } = await createBrowserContext({ headless: HEADLESS, cookieBundle, userStatePath }));
    videoHandle = feedPage.video?.();
    await context.tracing.start({ screenshots: true, snapshots: false });

    const auth = await ensureAuthenticated(context, feedPage, userStatePath);
    if (!auth.ok) {
      const result = { mode: "real", profileUrl: targetUrl, usedUrl: "preflight", finalUrl: auth.url, httpStatus: null, relationshipStatus: "unknown", actionTaken: "unavailable", details: "Not authenticated.", authDiag: auth.diag, at: new Date().toISOString() };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {}); throttle.failure("linkedin.com"); return result;
    }
    await feedWarmup(feedPage);

    profilePage = await openProfileTabViaFeed(context, feedPage, targetUrl);
    const navStability = attachNavGuard(profilePage);

    const nav = await navigateProfileClean(profilePage, targetUrl);
    if (!nav.authed) {
      const result = { mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl || profilePage.url(), httpStatus: nav.status, relationshipStatus: "unknown", actionTaken: "unavailable", details: nav.error || "Authwall/404/429.", at: new Date().toISOString() };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {}); throttle.failure("linkedin.com"); return result;
    }

    await sleep(PROFILE_INITIAL_WAIT_MS);
    if (navStability.countSameUrlReloads >= 3) {
      const result = { mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: profilePage.url(), httpStatus: nav.status, relationshipStatus: "unknown", actionTaken: "auto_reload_detected", details: "Repeated reloads; backing off.", at: new Date().toISOString() };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {}); throttle.failure("linkedin.com"); return result;
    }

    const hard = await detectHardScreen(profilePage);
    if (hard === "404" || hard === "429" || hard === "captcha") {
      const details = hard === "404" ? "Profile 404." : hard === "429" ? "Rate-limited." : "Verification/CAPTCHA.";
      const result = { mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: profilePage.url(), httpStatus: nav.status, relationshipStatus: "unknown", actionTaken: hard === "404" ? "page_not_found" : "rate_limited", details, at: new Date().toISOString() };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {}); throttle.failure("linkedin.com"); return result;
    }

    const outcome = await sendMessageFlow(profilePage, messageText);
    await microDelay(); await sleep(1500);

    const result = {
      mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl || profilePage.url(),
      messageUsed: messageText, httpStatus: nav.status,
      relationshipStatus: outcome.relationshipStatus, actionTaken: outcome.actionTaken, details: outcome.details,
      at: new Date().toISOString(),
    };

    try { await context.tracing.stop({ path: "/tmp/trace.zip" }); } catch {}
    await browser.close().catch(() => {});

    if (outcome.actionTaken === "sent") throttle.success("linkedin.com");
    else if (outcome.actionTaken?.startsWith("failed") || outcome.actionTaken === "unavailable" || result.actionTaken === "auto_reload_detected") throttle.failure("linkedin.com");
    else throttle.success("linkedin.com");

    return result;
  } catch (e) {
    try { await profilePage?.screenshot?.({ path: "/tmp/last-error.png", fullPage: false }).catch(() => {}); await context?.tracing?.stop({ path: "/tmp/trace-failed.zip" }).catch(() => {}); } catch {}
    try { await context?.close(); await browser?.close(); } catch {}
    throttle.failure("linkedin.com");
    throw new Error(`SEND_MESSAGE failed: ${e.message}`);
  }
}

// ========================= Job loop
async function processOne() {
  let next;
  try { next = await apiPost("/jobs/next", { types: ["AUTH_CHECK", "SEND_CONNECTION", "SEND_MESSAGE"] }); }
  catch (e) { logFetchError("jobs/next", e); return; }

  const job = next?.job; if (!job) return;

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
    } catch (e) { logFetchError(`jobs/${job.id}/complete`, e); }
  } catch (e) {
    console.error(`[worker] Job ${job.id} failed:`, e.message);
    try { await apiPost(`/jobs/${job.id}/fail`, { error: e.message, requeue: false, delayMs: 0 }); }
    catch (e2) { logFetchError(`jobs/${job.id}/fail`, e2); }
  }
}

async function mainLoop() {
  console.log(`[worker] starting. API_BASE=${API_BASE} Headless: ${HEADLESS} SlowMo: ${SLOWMO_MS}ms Soft mode: ${SOFT_MODE}`);
  if (!WORKER_SHARED_SECRET) console.error("[worker] ERROR: WORKER_SHARED_SECRET is empty. Set it on both backend and worker!");

  try {
    const stats = await apiGet("/jobs/stats");
    console.log("[worker] API OK. Stats:", stats?.counts || stats);
  } catch (e) { logFetchError("jobs/stats (startup)", e); }

  while (true) {
    try { await processOne(); }
    catch (e) { console.error("[worker] loop error:", e.message || e); }
    await sleep(POLL_INTERVAL_MS);
  }
}
mainLoop().catch((e) => { console.error("[worker] fatal:", e); process.exitCode = 1; });
