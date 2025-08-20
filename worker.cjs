// worker.cjs — LinqBridge Worker (FINAL)
// Hardened login, Sales Nav → public resolver, Connect + Message flows,
// human-like pacing & per-domain throttle, headless-friendly tracing & screenshots.

// =========================
// Env & Config
// =========================
const API_BASE = process.env.API_BASE || "https://calm-rejoicing-linqbridge.up.railway.app";
const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET || "";
const HEADLESS = (/^(true|1|yes)$/i).test(process.env.HEADLESS || "true");
const SOFT_MODE = (/^(true|1|yes)$/i).test(process.env.SOFT_MODE || "true"); // default safe
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000", 10);

// ---- Human pacing knobs (safe defaults) ----
const MAX_ACTIONS_PER_HOUR = parseInt(process.env.MAX_ACTIONS_PER_HOUR || "35", 10); // hourly cap (rolling window)
const MIN_GAP_MS = parseInt(process.env.MIN_GAP_MS || "25000", 10);                  // min gap between actions
const COOLDOWN_AFTER_SENT_MS = parseInt(process.env.COOLDOWN_AFTER_SENT_MS || "45000", 10);
const COOLDOWN_AFTER_FAIL_MS = parseInt(process.env.COOLDOWN_AFTER_FAIL_MS || "90000", 10);

// Micro-delays around actions for human-ness
const MICRO_DELAY_MIN_MS = parseInt(process.env.MICRO_DELAY_MIN_MS || "400", 10);
const MICRO_DELAY_MAX_MS = parseInt(process.env.MICRO_DELAY_MAX_MS || "1200", 10);

// Throttle jitter while waiting
const THROTTLE_JITTER_MIN_MS = parseInt(process.env.THROTTLE_JITTER_MIN_MS || "1500", 10);
const THROTTLE_JITTER_MAX_MS = parseInt(process.env.THROTTLE_JITTER_MAX_MS || "3500", 10);

// Optionally load Playwright only when needed (saves cold-start in soft mode)
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

function apiUrl(path) { return path.startsWith("/") ? `${API_BASE}${path}` : `${API_BASE}/${path}`; }

async function apiGet(path) {
  const res = await fetch(apiUrl(path), {
    method: "GET",
    headers: { "x-worker-secret": WORKER_SHARED_SECRET },
    signal: AbortSignal.timeout ? AbortSignal.timeout(15000) : undefined,
  });
  const text = await res.text();
  try {
    const json = JSON.parse(text);
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`);
    return json;
  } catch (e) {
    throw new Error(`GET ${path} non-JSON or error ${res.status}: ${text}`);
  }
}

async function apiPost(path, body) {
  const res = await fetch(apiUrl(path), {
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
  } catch (e) {
    throw new Error(`POST ${path} non-JSON or error ${res.status}: ${text}`);
  }
}

// =========================
// Per-domain Throttle (rolling 1-hour window + min gap + cooldowns)
// =========================
class DomainThrottle {
  constructor() { this.state = new Map(); } // domain -> { lastActionAt, events: number[], cooldownUntil }
  _get(domain) {
    if (!this.state.has(domain)) this.state.set(domain, { lastActionAt: 0, events: [], cooldownUntil: 0 });
    return this.state.get(domain);
  }
  _pruneOld(events) {
    const cutoff = now() - 3600_000; // 1 hour window
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
// Playwright helpers (anti-999 hardening + debug tracing)
// =========================
async function createBrowserContext(cookieBundle, headless = true) {
  if (!chromium) ({ chromium } = require("playwright"));

  const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
  const browser = await chromium.launch({
    headless,
    args: [
      "--no-sandbox", "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled",
      "--disable-features=IsolateOrigins,site-per-process",
      "--disable-gpu", "--disable-extensions", "--disable-background-networking",
    ],
  });

  const context = await browser.newContext({
    userAgent: UA, locale: "en-US", timezoneId: "America/Los_Angeles",
    colorScheme: "light", viewport: { width: 1366, height: 768 }, deviceScaleFactor: 1,
    javaScriptEnabled: true,
    extraHTTPHeaders: {
      "accept-language": "en-US,en;q=0.9",
      "sec-ch-ua": '"Chromium";v="124", "Not:A-Brand";v="8"',
      "sec-ch-ua-platform": '"Windows"',
      "sec-ch-ua-mobile": "?0",
      "upgrade-insecure-requests": "1",
    },
  });

  // Hide webdriver flag
  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
  });

  // Helper to map a cookie spec across host variants
  const expandDomains = (cookie) => ([
    { ...cookie, domain: ".linkedin.com" },
    { ...cookie, domain: "www.linkedin.com" },
    { ...cookie, domain: "m.linkedin.com" },
  ]);

  // Build cookie list (SameSite=None + Secure)
  let cookies = [];
  if (cookieBundle?.li_at) {
    cookies = cookies.concat(expandDomains({ name: "li_at", value: cookieBundle.li_at, path: "/", httpOnly: true, secure: true, sameSite: "None" }));
  }
  if (cookieBundle?.jsessionid) {
    cookies = cookies.concat(expandDomains({ name: "JSESSIONID", value: `"${cookieBundle.jsessionid}"`, path: "/", httpOnly: true, secure: true, sameSite: "None" }));
  }
  if (cookieBundle?.bcookie) {
    cookies = cookies.concat(expandDomains({ name: "bcookie", value: cookieBundle.bcookie, path: "/", httpOnly: false, secure: true, sameSite: "None" }));
  }
  if (cookieBundle?.lang) {
    cookies = cookies.concat(expandDomains({ name: "lang", value: cookieBundle.lang, path: "/", httpOnly: false, secure: true, sameSite: "None" }));
  }
  if (cookies.length) await context.addCookies(cookies);

  // Block heavy/noisy resources
  await context.route("**/*", (route) => {
    const req = route.request(); const type = req.resourceType();
    if (type === "image" || type === "media" || type === "font") return route.abort();
    const url = req.url();
    if (/\.(png|jpg|jpeg|gif|webp|svg)(\?|$)/i.test(url)) return route.abort();
    if (/doubleclick|google-analytics|adservice|facebook|hotjar|segment/.test(url)) return route.abort();
    return route.continue();
  });

  const page = await context.newPage();
  page.setDefaultTimeout(20000); page.setDefaultNavigationTimeout(30000);
  page.on("console", (msg) => { try { console.log("[page console]", msg.type(), msg.text()); } catch {} });

  return { browser, context, page };
}

// ---------- Navigation & Auth helpers ----------
async function isAuthWalledOrGuest(page) {
  try {
    const title = (await page.title().catch(() => ""))?.toLowerCase?.() || "";
    if (title.includes("sign in") || title.includes("join linkedin") || title.includes("authwall")) return true;
    // quick guest heuristics
    const hasGuest = await page.locator('a[href*="login"]').first().isVisible({ timeout: 500 }).catch(() => false);
    return !!hasGuest;
  } catch { return false; }
}

function withParams(u, extra = {}) {
  try {
    const url = new URL(u);
    if (!url.searchParams.get("trk")) url.searchParams.set("trk", "public_profile_nav");
    url.searchParams.set("original_referer", "https://www.google.com/");
    for (const [k, v] of Object.entries(extra)) url.searchParams.set(k, v);
    return url.toString();
  } catch { return u; }
}

/**
 * Try multiple URL candidates: desktop → desktop+param → mobile → Sales Nav (if provided).
 * Returns { status, usedUrl, finalUrl, authed, error? }
 */
async function navigateLinkedInWithRetries(page, rawUrl, { attempts = 4, salesNavUrl, salesProfileUrn } = {}) {
  const desktop1 = withParams(rawUrl);
  const desktop2 = withParams(rawUrl, { lipi: "urn-li-pi-" + Math.random().toString(36).slice(2) });
  const mobile   = rawUrl.replace("www.linkedin.com/in/", "m.linkedin.com/in/");

  const snFromPayload = salesNavUrl
    ? salesNavUrl
    : (salesProfileUrn ? `https://www.linkedin.com/sales/people/${encodeURIComponent(salesProfileUrn)}` : null);

  const candidates = [desktop1, desktop2, mobile].concat(snFromPayload ? [snFromPayload] : []);

  let lastErr, lastStatus = null, usedUrl = null, finalUrl = null;
  for (let i = 0; i < Math.min(attempts, candidates.length); i++) {
    const target = candidates[i];
    try {
      const resp = await page.goto(target, { waitUntil: "domcontentloaded", timeout: 28000 });
      usedUrl = target;
      lastStatus = resp ? resp.status() : null;
      await page.waitForTimeout(900);
      const authed = !(await isAuthWalledOrGuest(page));
      finalUrl = page.url();
      if (lastStatus && lastStatus >= 200 && lastStatus < 400 && authed) {
        return { status: lastStatus, usedUrl, finalUrl, authed: true };
      }
      await page.waitForTimeout(700 + Math.random() * 700); // small backoff before next candidate
    } catch (e) {
      lastErr = e; await page.waitForTimeout(900 + Math.random() * 900);
    }
  }
  return { status: lastStatus, usedUrl, finalUrl, authed: false, error: lastErr?.message || "auth/999/guest" };
}

/**
 * If currently on a Sales Navigator person page, hop to the public LinkedIn profile (/in/...).
 * Returns { moved: boolean, publicUrl?: string }
 */
async function moveToPublicProfileIfSalesNav(page) {
  const url = page.url();
  if (!/linkedin\.com\/sales\/people\//i.test(url)) return { moved: false };

  // Try to find a public profile link on the page
  const candidates = [
    'a[href*="linkedin.com/in/"]',
    'a[data-anonymize="profile-link"]',
    'a:has-text("View LinkedIn Profile")',
    'a[href^="https://www.linkedin.com/in/"]',
  ];

  for (const sel of candidates) {
    try {
      const loc = page.locator(sel).first();
      if (await loc.isVisible({ timeout: 1000 }).catch(() => false)) {
        const href = await loc.getAttribute("href").catch(() => null);
        if (href && /linkedin\.com\/in\//i.test(href)) {
          const publicUrl = href.startsWith("http") ? href : `https://www.linkedin.com${href}`;
          await page.goto(withParams(publicUrl), { waitUntil: "domcontentloaded", timeout: 25000 });
          await page.waitForTimeout(800);
          return { moved: true, publicUrl };
        }
      }
    } catch {}
  }
  return { moved: false };
}

// =========================
// Connect flow helpers
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
// Message flow helpers
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
        // Wait for composer: dialog OR side panel
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
  const limited = String(text).slice(0, 3000); // message cap safety
  // Common contenteditable targets
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
        // Prefer fill for textarea; for contenteditable, use keyboard type
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
        // Wait for a small confirmation or textarea clearing
        const sent = await Promise.race([
          page.locator('div:has-text("Message sent")').waitFor({ timeout: 3000 }).then(() => true).catch(() => false),
          page.locator('.msg-form__contenteditable[contenteditable="true"]').evaluate(el => (el.innerText || "").trim().length === 0).catch(() => false),
          sleep(1200).then(() => true), // fallback optimistic
        ]);
        if (sent) return true;
      }
    } catch {}
  }
  return false;
}

async function sendMessageFlow(page, messageText) {
  // Ensure we have the Message CTA
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
  if (sent) {
    return { actionTaken: "sent", relationshipStatus: "connected", details: "Message sent" };
  }
  return { actionTaken: "failed_to_send", relationshipStatus: "connected", details: "Failed to send message" };
}

// =========================
// Job handlers
// =========================
async function handleSendConnection(job) {
  const { payload } = job || {};
  if (!payload) throw new Error("Job has no payload");
  const profileUrl = payload.profileUrl;
  const note = payload.note || null;
  const cookieBundle = payload.cookieBundle || {};
  const salesNavUrl = payload.salesNavUrl || null;
  const salesProfileUrn = payload.salesProfileUrn || null;
  if (!profileUrl && !salesNavUrl && !salesProfileUrn) throw new Error("payload.profileUrl or salesNavUrl/salesProfileUrn required");

  if (SOFT_MODE) {
    await throttle.reserve("linkedin.com", "SOFT send_connection"); await microDelay(); throttle.success("linkedin.com");
    return { mode: "soft", profileUrl, noteUsed: note, message: "Soft mode success (no browser launched).", at: new Date().toISOString() };
  }

  await throttle.reserve("linkedin.com", "SEND_CONNECTION");

  let browser, context, page;
  try {
    ({ browser, context, page } = await createBrowserContext(cookieBundle, HEADLESS));
    await context.tracing.start({ screenshots: true, snapshots: false });

    const targetUrl = profileUrl || salesNavUrl || `https://www.linkedin.com/sales/people/${encodeURIComponent(salesProfileUrn)}`;
    const nav = await navigateLinkedInWithRetries(page, targetUrl, { attempts: 4, salesNavUrl, salesProfileUrn });
    await microDelay();

    if (!nav.authed) {
      const result = {
        mode: "real", profileUrl: profileUrl || null, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl || page.url(),
        httpStatus: nav.status, pageTitle: await page.title().catch(() => null),
        relationshipStatus: "not_connected", actionTaken: "unavailable", details: "Not authenticated (guest/authwall). Refresh cookies.",
        at: new Date().toISOString(),
      };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {}); throttle.failure("linkedin.com");
      return result;
    }

    // If we landed on Sales Nav, hop to public profile
    const hop = await moveToPublicProfileIfSalesNav(page);
    await microDelay();

    const connectOutcome = await sendConnectionRequest(page, note);
    await microDelay();

    const result = {
      mode: "real",
      profileUrl: profileUrl || hop.publicUrl || null,
      usedUrl: nav.usedUrl,
      finalUrl: hop.publicUrl || nav.finalUrl || page.url(),
      noteUsed: note,
      httpStatus: nav.status,
      pageTitle: await page.title().catch(() => null),
      relationshipStatus: connectOutcome.relationshipStatus,
      actionTaken: connectOutcome.actionTaken,
      details: connectOutcome.details,
      at: new Date().toISOString(),
    };

    try { await context.tracing.stop({ path: "/tmp/trace.zip" }); } catch {}
    await browser.close().catch(() => {});
    if (connectOutcome.actionTaken === "sent" || connectOutcome.actionTaken === "sent_maybe") throttle.success("linkedin.com");
    else if (connectOutcome.actionTaken === "failed_to_send" || connectOutcome.actionTaken === "unavailable") throttle.failure("linkedin.com");
    else throttle.success("linkedin.com"); // connected/pending already

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
  const profileUrl = payload.profileUrl;
  const messageText = payload.message;
  const cookieBundle = payload.cookieBundle || {};
  const salesNavUrl = payload.salesNavUrl || null;
  const salesProfileUrn = payload.salesProfileUrn || null;
  if (!messageText) throw new Error("payload.message required");
  if (!profileUrl && !salesNavUrl && !salesProfileUrn) throw new Error("payload.profileUrl or salesNavUrl/salesProfileUrn required");

  if (SOFT_MODE) {
    await throttle.reserve("linkedin.com", "SOFT send_message"); await microDelay(); throttle.success("linkedin.com");
    return { mode: "soft", profileUrl, messageUsed: messageText, message: "Soft mode success (no browser launched).", at: new Date().toISOString() };
  }

  await throttle.reserve("linkedin.com", "SEND_MESSAGE");

  let browser, context, page;
  try {
    ({ browser, context, page } = await createBrowserContext(cookieBundle, HEADLESS));
    await context.tracing.start({ screenshots: true, snapshots: false });

    const targetUrl = profileUrl || salesNavUrl || `https://www.linkedin.com/sales/people/${encodeURIComponent(salesProfileUrn)}`;
    const nav = await navigateLinkedInWithRetries(page, targetUrl, { attempts: 4, salesNavUrl, salesProfileUrn });
    await microDelay();

    if (!nav.authed) {
      const result = {
        mode: "real", profileUrl: profileUrl || null, usedUrl: nav.usedUrl, finalUrl: nav.finalUrl || page.url(),
        httpStatus: nav.status, pageTitle: await page.title().catch(() => null),
        relationshipStatus: "unknown", actionTaken: "unavailable", details: "Not authenticated (guest/authwall). Refresh cookies.",
        at: new Date().toISOString(),
      };
      try { await context.tracing.stop({ path: "/tmp/trace-failed.zip" }); } catch {}
      await browser.close().catch(() => {}); throttle.failure("linkedin.com");
      return result;
    }

    // If we landed on Sales Nav, hop to public profile
    const hop = await moveToPublicProfileIfSalesNav(page);
    await microDelay();

    const outcome = await sendMessageFlow(page, messageText);
    await microDelay();

    const result = {
      mode: "real",
      profileUrl: profileUrl || hop.publicUrl || null,
      usedUrl: nav.usedUrl,
      finalUrl: hop.publicUrl || nav.finalUrl || page.url(),
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
    // Supports both connection + message jobs (backend may return either)
    next = await apiPost("/jobs/next", { types: ["SEND_CONNECTION", "SEND_MESSAGE"] });
  } catch (e) {
    logFetchError("jobs/next", e);
    return;
  }

  const job = next?.job;
  if (!job) return;

  try {
    let result = null;

    switch (job.type) {
      case "SEND_CONNECTION":
        result = await handleSendConnection(job); break;
    case "SEND_MESSAGE":
        result = await handleSendMessage(job); break;
      default:
        result = { note: `Unhandled job type: ${job.type}` }; break;
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
  console.log(`[worker] starting. API_BASE=${API_BASE} Headless: ${HEADLESS} Soft mode: ${SOFT_MODE}`);
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
