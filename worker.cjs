// worker.cjs — LinqBridge Worker (final robust)
// - Feed redirect loop recovery
// - Authwall soft re-auth + mobile-first fallback
// - Degree-aware relationship detection (1st/2nd/3rd)
// - Avoid Sales Navigator mis-clicks; target only true "Connect"
// - Optional FORCE_NO_NOTES
// - Smarter message open (primary/More/mobile)
// - Same API/queue/throttle shape you already run

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

// Human pacing / throttling
const MAX_ACTIONS_PER_HOUR = parseInt(process.env.MAX_ACTIONS_PER_HOUR || "18", 10);
const MIN_GAP_MS = parseInt(process.env.MIN_GAP_MS || "60000", 10);
const COOLDOWN_AFTER_SENT_MS = parseInt(process.env.COOLDOWN_AFTER_SENT_MS || "90000", 10);
const COOLDOWN_AFTER_FAIL_MS = parseInt(process.env.COOLDOWN_AFTER_FAIL_MS || "600000", 10);

// Micro delays
const MICRO_DELAY_MIN_MS = parseInt(process.env.MICRO_DELAY_MIN_MS || "400", 10);
const MICRO_DELAY_MAX_MS = parseInt(process.env.MICRO_DELAY_MAX_MS || "1200", 10);

// Timings
const FEED_INITIAL_WAIT_MS = parseInt(process.env.FEED_INITIAL_WAIT_MS || "10000", 10);
const FEED_SCROLL_MS = parseInt(process.env.FEED_SCROLL_MS || "5000", 10);
const PROFILE_INITIAL_WAIT_MS = parseInt(process.env.PROFILE_INITIAL_WAIT_MS || "10000", 10);
const DEFAULT_TIMEOUT_MS = parseInt(process.env.DEFAULT_TIMEOUT_MS || "35000", 10);
const NAV_TIMEOUT_MS = parseInt(process.env.NAV_TIMEOUT_MS || "45000", 10);

// Fallback & toggles
const ALLOW_MOBILE_FALLBACK = (/^(true|1|yes)$/i).test(process.env.ALLOW_MOBILE_FALLBACK || "true");
const USE_PROFILE_MOBILE_FIRST = (/^(true|1|yes)$/i).test(process.env.USE_PROFILE_MOBILE_FIRST || "false");
const FORCE_NO_NOTES = (/^(true|1|yes)$/i).test(process.env.FORCE_NO_NOTES || "false");

// Session persistence / interactive login
const DEFAULT_STATE_PATH = process.env.STORAGE_STATE_PATH || "/app/auth-state.json";
const STATE_DIR = process.env.STATE_DIR || "/app/state";
const FORCE_RELOGIN = (/^(true|1|yes)$/i).test(process.env.FORCE_RELOGIN || "false");
const ALLOW_INTERACTIVE_LOGIN = (/^(true|1|yes)$/i).test(process.env.ALLOW_INTERACTIVE_LOGIN || "true");
const INTERACTIVE_LOGIN_TIMEOUT_MS = parseInt(process.env.INTERACTIVE_LOGIN_TIMEOUT_MS || "300000", 10); // 5 min

// Proxy (optional)
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

  // Optional cookie bundle (augment existing state)
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
    if (/\/authwall/i.test(url)) return "authwall";
  } catch {}
  return null;
}

// --- Redirect loop helpers & voyager check ---
function isRedirectLoopError(e) {
  const msg = (e?.message || "").toLowerCase();
  return msg.includes("too many redirects") || msg.includes("too_many_redirects");
}

async function testVoyagerMe(context) {
  try {
    const req = await context.request.get("https://www.linkedin.com/voyager/api/me", {
      headers: {
        "accept": "application/json",
        "csrf-token": "ajax:123456789",
        "x-restli-protocol-version": "2.0.0",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
      },
      timeout: 8000
    });
    if (req.ok()) return { ok: true };
    return { ok: false, status: req.status() };
  } catch {
    return { ok: false, status: 0 };
  }
}

// Clean, resilient feed warmup (or skip)
async function humanScroll(page, durationMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < durationMs) {
    try { await page.mouse.wheel(0, within(120, 320)); } catch {}
    await sleep(within(120, 220));
  }
}

async function feedWarmup(page) {
  try {
    // If session cookie/CSRF is valid (voyager OK), skip feed entirely
    const ok = await testVoyagerMe(page.context());
    if (ok.ok) {
      console.log("[feed] skipping warmup (voyager/me OK)");
      return;
    }

    // Desktop feed
    try {
      if (!/linkedin\.com\/feed\/?$/i.test(page.url())) {
        console.log("[nav] → feed-desktop: https://www.linkedin.com/feed/");
        await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
      }
      console.log(`[feed] warmup: waiting ${FEED_INITIAL_WAIT_MS} ms, then scrolling ${FEED_SCROLL_MS} ms`);
      await sleep(FEED_INITIAL_WAIT_MS);
      await humanScroll(page, FEED_SCROLL_MS);
      return;
    } catch (e) {
      if (!isRedirectLoopError(e)) throw e;
      console.log("[feed] desktop redirect loop detected, trying mobile …");
    }

    // Mobile feed
    try {
      await page.goto("https://m.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
      console.log(`[feed] (mobile) warmup: waiting ${FEED_INITIAL_WAIT_MS} ms, then scrolling ${FEED_SCROLL_MS} ms`);
      await sleep(FEED_INITIAL_WAIT_MS);
      await humanScroll(page, FEED_SCROLL_MS);
      return;
    } catch (e2) {
      if (!isRedirectLoopError(e2)) throw e2;
      console.log("[feed] mobile redirect loop too. Clearing cookies + reauth …");
    }

    try {
      await page.context().clearCookies();
      try { await page.goto("https://www.linkedin.com/m/logout", { waitUntil: "domcontentloaded", timeout: 15000 }); } catch {}
    } catch {}

  } catch (e) {
    console.log("[feed] warmup error:", e?.message || e);
  }
}

// Clean navigation to profile with authwall handling + mobile-first
async function navigateProfileClean(page, rawUrl) {
  const primary = rawUrl.replace("linkedin.com//", "linkedin.com/");
  const mobile  = primary
    .replace("www.linkedin.com", "m.linkedin.com")
    .replace("linkedin.com/in/", "m.linkedin.com/in/");

  const tries = USE_PROFILE_MOBILE_FIRST ? [mobile, primary] : [primary, mobile];

  for (let i = 0; i < tries.length; i++) {
    const u = tries[i];
    try {
      console.log(`[nav] → profile-${i === 0 ? "first" : "fallback"}: ${u}`);
      const resp = await page.goto(u, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
      const status = resp ? resp.status() : null;
      const finalUrl = page.url();

      const hard = await detectHardScreen(page);
      if (status === 429 || hard === "429") return { authed: false, status: 429, usedUrl: u, finalUrl, error: "rate_limited" };
      if (hard === "404") return { authed: false, status: status || 404, usedUrl: u, finalUrl, error: "not_found" };

      const isAuthwall = /\/authwall/i.test(finalUrl) ||
        (await page.locator('h1:has-text("Sign in")').first().isVisible({ timeout: 600 }).catch(() => false));

      if (isAuthwall) {
        console.log("[nav] authwall detected → soft re-auth & retry this URL once");
        try {
          await page.goto("https://www.linkedin.com/login", { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
          await sleep(1500);
          await page.goto(u, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
        } catch (e) {
          console.log("[nav] authwall re-auth attempt failed:", e?.message || e);
        }
        const final2 = page.url();
        const hard2 = await detectHardScreen(page);
        const stillAuthwall = /\/authwall/i.test(final2);
        if (!stillAuthwall && !hard2) {
          return { authed: true, status: 200, usedUrl: u, finalUrl: final2 };
        }
        // try the other variant (desktop/mobile)
      } else {
        const authed = !(await isAuthWalledOrGuest(page));
        if (authed && status && status >= 200 && status < 400 && !hard) {
          console.log(`[nav] ✓ profile: status=${status} final=${finalUrl}`);
          return { authed: true, status, usedUrl: u, finalUrl };
        }
      }

      if (i === tries.length - 1) {
        return { authed: false, status, usedUrl: u, finalUrl, error: isAuthwall ? "authwall" : "authwall_or_unknown" };
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

// Connection degree & InMail/OpenProfile signals
async function getConnectionDegree(page) {
  try {
    const locs = [
      page.locator('span:has-text("1st")').first(),
      page.locator('span:has-text("2nd")').first(),
      page.locator('span:has-text("3rd")').first(),
      page.locator('[data-test-connection-badge]').first(),
    ];
    for (const l of locs) {
      const vis = await l.isVisible({ timeout: 800 }).catch(() => false);
      if (!vis) continue;
      const t = (await l.innerText().catch(() => "")).trim();
      if (/^1st\b/i.test(t)) return "1st";
      if (/^2nd\b/i.test(t)) return "2nd";
      if (/^3rd\b/i.test(t)) return "3rd";
    }
  } catch {}
  return null;
}

async function looksLikeInMailOrOpenProfile(page) {
  try {
    const inmail = await page.locator('text=/InMail/i').first().isVisible({ timeout: 600 }).catch(() => false);
    const open   = await page.locator('text=/Open Profile|Open to messages/i').first().isVisible({ timeout: 600 }).catch(() => false);
    const hasConnect = await page.getByRole("button", { name: /^Connect$/i }).first().isVisible({ timeout: 600 }).catch(() => false);
    return (inmail || open) && !hasConnect;
  } catch {}
  return false;
}

// Relationship & Connect flow (degree-aware)
async function detectRelationshipStatus(page) {
  const pending = await page.getByRole("button", { name: /Pending|Requested|Withdraw|Pending invitation/i })
    .first().isVisible({ timeout: 800 }).catch(() => false);
  if (pending) return { status: "pending", reason: "Pending/Requested visible" };

  const degree = await getConnectionDegree(page);

  const messageBtn = await Promise.any([
    page.getByRole("button", { name: /^Message$/i }).first().isVisible({ timeout: 800 }),
    page.getByRole("link",   { name: /^Message$/i }).first().isVisible({ timeout: 800 })
  ]).catch(() => false);
  const connectBtn = await Promise.any([
    page.getByRole("button", { name: /^Connect$/i }).first().isVisible({ timeout: 800 }),
    page.locator('button:has-text("Connect")').first().isVisible({ timeout: 800 })
  ]).catch(() => false);

  if (degree === "1st") return { status: "connected", reason: 'Degree badge "1st"' };

  if (messageBtn && !connectBtn) {
    const paid = await looksLikeInMailOrOpenProfile(page);
    if (paid) return { status: "not_connected", reason: "Message is InMail/Open Profile (not 1st)" };
    return { status: "not_connected", reason: "Message visible but not 1st-degree" };
  }

  if (connectBtn) return { status: "not_connected", reason: "Connect button visible" };

  const moreVisible = await Promise.any([
    page.getByRole("button", { name: /^More$/i }).first().isVisible({ timeout: 800 }),
    page.getByRole("button", { name: /More actions/i }).first().isVisible({ timeout: 800 })
  ]).catch(() => false);

  if (moreVisible) return { status: "not_connected", reason: "Connect may be under More" };

  return { status: "not_connected", reason: "Unable to confirm; will try menus" };
}

async function openConnectDialog(page) {
  // stay near top where CTAs live
  try { await page.evaluate(() => window.scrollTo(0, 0)); } catch {}
  await microDelay();

  // 1) Direct Connect buttons/links (avoid Sales Navigator)
  const direct = [
    page.getByRole("button", { name: /^Connect$/i }),
    page.getByRole("link",   { name: /^Connect$/i }),
    page.locator('button[aria-label="Connect"]'),
    page.locator('button[data-control-name="connect"]'),
    page.locator('a[data-control-name="connect"]'),
    page.locator('button:has-text("Connect")').filter({ hasNotText: /Sales Navigator|Sales|View in/i })
  ];
  for (const h of direct) {
    try {
      if (await h.first().isVisible({ timeout: 1200 }).catch(() => false)) {
        await h.first().click({ timeout: 4000 });
        await microDelay();
        const ready = await Promise.race([
          page.getByRole("dialog").waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
          page.getByRole("button", { name: /^Add a note$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
          page.getByRole("button", { name: /^Send$/i }).waitFor({ timeout: 3500 }).then(() => true).catch(() => false),
        ]);
        if (ready) return { opened: true, via: "primary" };
      }
    } catch {}
  }

  // 2) More menu → Connect (EXCLUDE Sales Navigator)
  const more = [
    page.getByRole("button", { name: /^More$/i }),
    page.getByRole("button", { name: /More actions/i }),
    page.locator('button[aria-label="More actions"]'),
  ];
  for (const m of more) {
    try {
      if (await m.first().isVisible({ timeout: 1200 }).catch(() => false)) {
        await m.first().click({ timeout: 4000 });
        await microDelay();

        const candidates = [
          page.getByRole("menuitem", { name: /^Connect$/i }),
          page.locator('div[role="menuitem"]').filter({ hasText: /^\s*Connect\s*$/i }),
          page.locator('span,div').filter({ hasText: /^\s*Connect\s*$/i }).locator('xpath=ancestor-or-self::*[@role="menuitem"]')
        ];

        const isBadLabel = async (el) => {
          try {
            const t = (await el.innerText()).trim();
            return /Sales\s*Navigator|View in Sales/i.test(t);
          } catch { return false; }
        };

        for (const group of candidates) {
          try {
            const item = group.first();
            if (await item.isVisible({ timeout: 1200 }).catch(() => false)) {
              if (await isBadLabel(item)) continue;
              await item.click({ timeout: 4000 });
              await microDelay();
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

  // 3) Mobile fallback
  try {
    const mobileConnect = page.locator('button:has-text("Connect"), a:has-text("Connect")')
      .filter({ hasNotText: /Sales Navigator|Sales|View in/i });
    if (await mobileConnect.first().isVisible({ timeout: 1200 }).catch(() => false)) {
      await mobileConnect.first().click({ timeout: 4000 });
      await microDelay();
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

async function completeConnectDialog(page, note) {
  await microDelay();
  let addNoteClicked = false, filled = false;

  const wantNote = FORCE_NO_NOTES ? null : (note || null);

  if (wantNote) {
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
    const limited = String(wantNote).slice(0, 300);
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
// Message flow (robust)
// =========================
async function openMessageDialog(page) {
  try { await page.evaluate(() => window.scrollTo(0, 0)); } catch {}
  await microDelay();

  // 1) Direct “Message”
  const direct = [
    page.getByRole("button", { name: /^Message$/i }),
    page.getByRole("link",   { name: /^Message$/i }),
    page.locator('button[aria-label="Message"]'),
    page.locator('a[data-control-name*="message"]'),
    page.locator('button:has-text("Message")'),
  ];
  for (const h of direct) {
    try {
      if (await h.first().isVisible({ timeout: 1200 }).catch(() => false)) {
        await h.first().click({ timeout: 4000 }); await microDelay();
        const ready = await Promise.race([
          page.getByRole("dialog").waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
          page.locator('.msg-overlay-conversation-bubble').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
          page.locator('[data-test-conversation-compose], .msg-form__contenteditable').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
        ]);
        if (ready) return { opened: true, via: "primary" };
      }
    } catch {}
  }

  // 2) Under “More”
  const more = [
    page.getByRole("button", { name: /^More$/i }),
    page.getByRole("button", { name: /More actions/i }),
    page.locator('button[aria-label="More actions"]'),
  ];
  for (const m of more) {
    try {
      if (await m.first().isVisible({ timeout: 1200 }).catch(() => false)) {
        await m.first().click({ timeout: 4000 }); await microDelay();
        const menuMsg = [
          page.getByRole("menuitem", { name: /^Message$/i }),
          page.locator('div[role="menuitem"]:has-text("Message")'),
          page.locator('span:has-text("Message")'),
        ];
        for (const mi of menuMsg) {
          if (await mi.first().isVisible({ timeout: 1200 }).catch(() => false)) {
            await mi.first().click({ timeout: 4000 }); await microDelay();
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

  // 3) Mobile fallback
  try {
    const mobileMsg = page.locator('button:has-text("Message"), a:has-text("Message")');
    if (await mobileMsg.first().isVisible({ timeout: 1200 }).catch(() => false)) {
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
          try { await handle.press("Control+A"); } catch {}
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
      try { await editor.press("Enter"); } catch {}
      await microDelay();
      const sentByEnter = await Promise.race([
        page.locator('div:has-text("Message sent")').waitFor({ timeout: 2000 }).then(() => true).catch(() => false),
        sleep(1500).then(() => true),
      ]);
      if (sentByEnter) return true;
      try { await editor.press("Control+Enter"); } catch {}
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

    const auth = await ensureAuthenticated(context, page, userStatePath);
    if (auth.ok) {
      await feedWarmup(page);
    }

    const result = {
      ok: !!auth.ok,
      via: auth.via || "unknown",
      url: auth.url || null,
      diag: auth.diag || null,
      message: auth.ok ? "Authenticated and storageState saved." : `Auth failed: ${auth.reason || "unknown"}`,
      at: new Date().toISOString(),
      statePath: userStatePath,
    };

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

  const note = FORCE_NO_NOTES ? null : (payload.note || null);
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

    // AUTH on TAB 1 (feed)
    const auth = await ensureAuthenticated(context, feedPage, userStatePath);
    if (!auth.ok) {
      const result = {
        mode: "real", profileUrl: targetUrl, usedUrl: "preflight", finalUrl: auth.url,
        httpStatus: null, relationshipStatus: "not_connected", actionTaken: "unavailable",
        details: "Not authenticated (guest/authwall). Complete 2FA or refresh cookies.",
        authDiag: auth.diag, at: new Date().toISOString(),
      };
      await browser.close().catch(() => {});
      throttle.failure("linkedin.com");
      return result;
    }

    // Feed warmup (TAB 1) — do not throw on errors
    await feedWarmup(feedPage);

    // Open TAB 2 for profile work
    profilePage = await newPageInContext(context);

    // PROFILE nav (with authwall handling + mobile-first toggle)
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

    // Wait calmly on profile, then hard-screen check
    await sleep(PROFILE_INITIAL_WAIT_MS);
    const hard = await detectHardScreen(profilePage);
    if (hard === "404" || hard === "429" || hard === "captcha" || hard === "authwall") {
      const details = hard === "404" ? "Public profile URL returned 404."
                    : hard === "429" ? "Hit LinkedIn 429 (rate-limited)."
                    : hard === "captcha" ? "Encountered verification/captcha."
                    : "Hit LinkedIn authwall.";
      const result = {
        mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: profilePage.url(),
        httpStatus: nav.status, relationshipStatus: "unknown", actionTaken: hard === "404" ? "page_not_found" : "rate_limited",
        details, at: new Date().toISOString(),
      };
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(() => {});
      throttle.failure("linkedin.com");
      return result;
    }

    // SCRAPE sections (TAB 2) — optional
    const { about, currentRole } = await scrapeAboutAndCurrentRole(profilePage);

    // CONNECT (TAB 2)
    await microDelay();
    const connectOutcome = await sendConnectionRequest(profilePage, note);

    // linger 2s then close TAB 2
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

async function handleSendMessage(job) {
  const { payload } = job || {};
  if (!payload) throw new Error("Job has no payload");

  let targetUrl = payload.profileUrl || null;
  if (!targetUrl && payload.publicIdentifier) {
    targetUrl = `https://www.linkedin.com/in/${encodeURIComponent(payload.publicIdentifier)}/`;
  }
  if (!targetUrl) throw new Error("payload.profileUrl or publicIdentifier required");

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

    const auth = await ensureAuthenticated(context, feedPage, userStatePath);
    if (!auth.ok) {
      const result = {
        mode: "real", profileUrl: targetUrl, usedUrl: "preflight", finalUrl: auth.url,
        httpStatus: null, relationshipStatus: "unknown", actionTaken: "unavailable",
        details: "Not authenticated (guest/authwall). Complete 2FA or refresh cookies.",
        authDiag: auth.diag, at: new Date().toISOString(),
      };
      await browser.close().catch(() => {});
      throttle.failure("linkedin.com");
      return result;
    }

    await feedWarmup(feedPage);

    // TAB 2
    profilePage = await newPageInContext(context);

    const nav = await navigateProfileClean(profilePage, targetUrl);
    if (!nav.authed) {
      const result = {
        mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl || profilePage.url(),
        httpStatus: nav.status, relationshipStatus: "unknown", actionTaken: "unavailable",
        details: nav.error || "Authwall/404/429 on profile nav.", at: new Date().toISOString(),
      };
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(() => {});
      throttle.failure("linkedin.com");
      return result;
    }

    await sleep(PROFILE_INITIAL_WAIT_MS);
    const hard = await detectHardScreen(profilePage);
    if (hard === "404" || hard === "429" || hard === "captcha" || hard === "authwall") {
      const details = hard === "404" ? "Public profile URL returned 404."
                    : hard === "429" ? "Hit LinkedIn 429 (rate-limited)."
                    : hard === "captcha" ? "Encountered verification/captcha."
                    : "Hit LinkedIn authwall.";
      const result = {
        mode: "real", profileUrl: targetUrl, usedUrl: nav.usedUrl, finalUrl: profilePage.url(),
        httpStatus: nav.status, relationshipStatus: "unknown", actionTaken: hard === "404" ? "page_not_found" : "rate_limited",
        details, at: new Date().toISOString(),
      };
      try { await profilePage.close().catch(()=>{}); } catch {}
      await browser.close().catch(() => {});
      throttle.failure("linkedin.com");
      return result;
    }

    const outcome = await sendMessageFlow(profilePage, messageText);
    await microDelay();

    await sleep(1500);
    try { await profilePage.close().catch(()=>{}); } catch {}

    const result = {
      mode: "real",
      profileUrl: targetUrl,
      usedUrl: nav.usedUrl,
      finalUrl: nav.finalUrl || profilePage?.url?.() || "",
      messageUsed: messageText,
      httpStatus: nav.status,
      relationshipStatus: outcome.relationshipStatus,
      actionTaken: outcome.actionTaken,
      details: outcome.details,
      at: new Date().toISOString(),
    };

    await browser.close().catch(() => {});

    if (outcome.actionTaken === "sent") throttle.success("linkedin.com");
    else if (outcome.actionTaken?.startsWith("failed") || outcome.actionTaken === "unavailable") throttle.failure("linkedin.com");
    else throttle.success("linkedin.com");

    return result;
  } catch (e) {
    try { await profilePage?.close()?.catch(()=>{}); } catch {}
    try { await browser?.close(); } catch {}
    throttle.failure("linkedin.com");
    throw new Error(`SEND_MESSAGE failed: ${e.message}`);
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
