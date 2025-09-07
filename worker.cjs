// worker.cjs — LinqBridge Worker (FINAL ROBUST & COMBINED VERSION)
//
// What’s included (high-level):
// - Combines the robust connection request flow from V1 and message flow from V2.
// - Strict single-thread flow: FEED → slow human scroll → then open PROFILE tab.
// - Auth & storageState per user (email).
// - Authwall recovery (login nudge + retry).
// - Mobile-first profile hop (env flag, fixed URL builder).
// - Degree-aware relationship detection (1st/2nd/3rd).
// - InMail/Open Profile recognition.
// - Connect selection filtered (button-only, no anchors; no "View in Sales Navigator").
// - Sends plain invites by default (no note) — FORCE_NO_NOTES default true.
// - Message flow only when truly 1st-degree.
// - Per-domain throttle + gentle pacing.
// - Optional HTTPS proxy.
// - Safer fetch import, Mac/Win-friendly keybinds (Mod).
// - Misclick goBack recovery if a click navigates to people search.

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

// ---------- Utils ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();
const within = (min, max) => min + Math.floor(Math.random() * (max - min + 1));

async function microDelay() { await sleep(within(MICRO_DELAY_MIN_MS, MICRO_DELAY_MAX_MS)); }

async function sprout(label = "") {
    const n = within(1, 3);
    for (let i = 0; i < n; i++) await microDelay();
    if (label) console.log(`[sprout] ${label} x${n}`);
}

const sanitizeUserId = (s) => String(s || "default").toLowerCase().replace(/[^a-z0-9]+/g, "_");
const statePathForUser = (userId) => path.join(STATE_DIR, `${sanitizeUserId(userId)}.json`);

function logFetchError(where, err) {
    const code = err?.cause?.code || err?.code || "unknown";
    console.error("[worker]", `${where} fetch failed:`, code, err?.message || err);
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
    const checks = await Promise.all(locs.map((l) => l.first().isVisible({ timeout: 800 }).catch(() => false)));
    return checks.some(Boolean);
}

async function waitFullLoad(page, timeout = 45000) {
    await page.waitForLoadState("domcontentloaded", { timeout }).catch(() => {});
    try {
        await Promise.race([ page.waitForLoadState("networkidle", { timeout: Math.min(timeout, 12000) }), sleep(6000) ]);
    } catch {}
}

async function slowHumanScroll(page, totalMs = 6000) {
    const start = Date.now(); let y = 0;
    while (Date.now() - start < totalMs) {
        const step = within(20, 55); y += step;
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
        const title = (await page.title().catch(() => "")) || "";
        const bodyText = await page.locator("body").innerText().catch(() => "");
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
            if (st.events.length >= MAX_ACTIONS_PER_HOUR) waits.push(st.events[0] + 3600_000 - nowTs);
            const sinceLast = nowTs - (st.lastActionAt || 0);
            if (sinceLast < MIN_GAP_MS) waits.push(MIN_GAP_MS - sinceLast);
            if (waits.length) {
                const waitMs = Math.max(...waits) + within(1500, 3500);
                console.log(`[throttle] Waiting ${Math.ceil(waitMs/1000)}s before ${label} (used: ${st.events.length}/${MAX_ACTIONS_PER_HOUR})`);
                await sleep(waitMs);
                continue;
            }
            st.lastActionAt = nowTs;
            st.events.push(nowTs);
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
    await fsp.mkdir(path.dirname(userStatePath || DEFAULT_STATE_PATH), { recursive: true }).catch(() => {});
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
        (!FORCE_RELOGIN && userStatePath && fs.existsSync(userStatePath)) ? userStatePath
        : (!FORCE_RELOGIN && fs.existsSync(DEFAULT_STATE_PATH) ? DEFAULT_STATE_PATH : undefined);
    const context = await browser.newContext({
        userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        locale: "en-US", timezoneId: "America/Los_Angeles",
        viewport: { width: vw, height: vh }, javaScriptEnabled: true,
        recordVideo: { dir: "/tmp/pw-video" }, storageState: storageStateOpt,
    });
    await context.setExtraHTTPHeaders({
        "accept-language":"en-US,en;q=0.9","upgrade-insecure-requests":"1",
        "sec-ch-ua":'"Chromium";v="124", "Not:A-Brand";v="8"',
        "sec-ch-ua-platform":'"Windows"',"sec-ch-ua-mobile":"?0", referer:"https://www.google.com/",
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
    try { await fsp.mkdir(path.dirname(outPath), { recursive: true }); await context.storageState({ path: outPath }); console.log("[auth] storageState saved to", outPath); }
    catch (e) { console.log("[auth] storageState save failed:", e?.message || e); }
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

// ---------- Relationship helpers (V2) ----------
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
        const hasMessage = await anyVisible(
            page.getByRole("button", { name: /^Message$/i }),
            page.getByRole("link", { name: /^Message$/i })
        );
        const hasConnect = await anyVisible(
            page.getByRole("button", { name: /^Connect$/i }),
            page.locator('button:has-text("Connect")')
        );
        const inmailText = await page.getByText(/InMail/i).first().isVisible({ timeout: 600 }).catch(() => false);
        const openText = await page.getByText(/Open Profile|Open to messages/i).first().isVisible({ timeout: 600 }).catch(() => false);
        return (inmailText || openText) && !hasConnect && hasMessage;
    } catch {}
    return false;
}

// *** COMBINED: USING V2's robust detection logic ***
async function detectRelationshipStatus(page) {
    // 1) Pending request?
    const pending = await anyVisible(
        page.getByRole("button", { name: /Pending|Requested|Withdraw|Pending invitation/i })
    );
    if (pending) return { status: "pending", reason: "Pending/Requested visible" };

    // 2) Degree badge (fast path)
    const degree = await getConnectionDegree(page);

    // 3) Broad message/connect detection (handles aria-label="Message <Name>")
    const messageVisible = await anyVisible(
        page.getByRole("button", { name: /(^|\s)Message(\s|$)/i }),
        page.getByRole("link",  { name: /(^|\s)Message(\s|$)/i }),
        page.locator('button[aria-label^="Message"]'),
        page.locator('a[aria-label^="Message"]'),
        page.locator('button:has(span.artdeco-button__text:has-text("Message"))'),
        page.locator('a:has(span.artdeco-button__text:has-text("Message"))'),
        page.locator('button[aria-label="Message"]'),
        page.locator('a[data-control-name*="message"]'),
        page.locator('button:has-text("Message")'),
    );

    const connectVisible = await anyVisible(
        page.getByRole("button", { name: /^Connect$/i }),
        page.locator('button[aria-label="Connect"]'),
        page.locator('button[data-control-name="connect"]'),
        page.locator('button:has-text("Connect")')
    );

    // 4) Decide status
    if (degree === "1st") return { status: "connected", reason: 'Degree badge "1st"' };

    if (messageVisible) {
        const paid = await looksLikeInMailOrOpenProfile(page);
        if (!paid) return { status: "connected", reason: "Message button visible (not InMail/Open Profile)" };
        return { status: "not_connected", reason: "Message is InMail/Open Profile (not 1st)" };
    }

    if (connectVisible) return { status: "not_connected", reason: "Connect button visible" };

    const moreVisible = await anyVisible(
        page.getByRole("button", { name: /^More$/i }),
        page.getByRole("button", { name: /More actions/i }),
        page.locator('button[aria-label="More actions"]')
    );
    if (moreVisible) return { status: "not_connected", reason: "Connect may be under More" };

    return { status: "not_connected", reason: "Unable to confirm; defaulting to not connected" };
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
                if (!stillAuthwall && !hard2 && isProfileUrl(final2)) { return { authed: true, status: 200, usedUrl: u, finalUrl: final2 }; }
            } else {
                const authed = !(await isAuthWalledOrGuest(page));
                if (authed && status && status >= 200 && status < 400 && !hard && isProfileUrl(finalUrl)) {
                    return { authed: true, status, usedUrl: u, finalUrl };
                }
            }

            if (i === tries.length - 1) {
                return {
                    authed: false, status, usedUrl: u, finalUrl,
                    error: /\/authwall/i.test(finalUrl) ? "authwall" : (isProfileUrl(finalUrl) ? "authwall_or_unknown" : "not_profile"),
                };
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

// ---------- Open Connect (V1) ----------
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

// ---------- Complete Connect (V1) ----------
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

// *** COMBINED: USING V1's robust connection request function ***
async function sendConnectionRequest(page, note) {
    const rs1 = await detectRelationshipStatus(page);
    if (rs1.status === "connected") return { actionTaken: "none", relationshipStatus: "connected", details: "Already 1st-degree" };
    if (rs1.status === "pending")  return { actionTaken: "none", relationshipStatus: "pending", details: "Invitation already pending" };

    let opened = await openConnectDialog(page);
    if (!opened.opened) {
        try { await page.mouse.wheel(0, within(600, 1000)); await sleep(within(600, 1000)); } catch {}
        opened = await openConnectDialog(page);
        if (!opened.opened) return { actionTaken: "unavailable", relationshipStatus: "not_connected", details: "Connect button not found" };
    }

    const completed = await completeConnectDialog(page, null);
    if (completed.sent) return { actionTaken: "sent_without_note", relationshipStatus: "pending", details: "Invitation sent" };

    const rs2 = await detectRelationshipStatus(page);
    if (rs2.status === "pending")  return { actionTaken: "sent_maybe", relationshipStatus: "pending", details: "Pending after dialog" };
    if (rs2.status === "connected") return { actionTaken: "none", relationshipStatus: "connected", details: "Connected" };

    return { actionTaken: "failed_to_send", relationshipStatus: "not_connected", details: "Unable to send invite" };
}

// ---------- Message Flow (V2) ----------
async function openMessageDialog(page) {
    try { await page.evaluate(() => window.scrollTo(0, 0)); } catch {}
    await microDelay();

    // UPDATED: broaden "Message" selectors (supports aria-label="Message <Name>")
    const direct = [
        page.getByRole("button", { name: /(^|\s)Message(\s|$)/i }),
        page.getByRole("link",  { name: /(^|\s)Message(\s|$)/i }),
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
                await handle.fill(limited, { timeout: 3000 });
                return { typed: true };
            }
        } catch {}
    }
    return { typed: false };
}

async function sendMessage(page, messageText) {
    const rs1 = await detectRelationshipStatus(page);
    if (rs1.status !== "connected") return { actionTaken: "none", relationshipStatus: rs1.status, details: "Not a 1st-degree connection" };

    const opened = await openMessageDialog(page);
    if (!opened.opened) return { actionTaken: "unavailable", relationshipStatus: rs1.status, details: "Message dialog not found" };

    const typed = await typeIntoComposer(page, messageText);
    if (!typed.typed) return { actionTaken: "unavailable", relationshipStatus: rs1.status, details: "Message composer not found" };

    const sendButton = await page.getByRole("button", { name: /Send/i }).first().isVisible({ timeout: 2000 }).catch(() => false);
    if (!sendButton) return { actionTaken: "unavailable", relationshipStatus: rs1.status, details: "Send button not found" };

    await sprout('send-message');
    await page.getByRole("button", { name: /Send/i }).first().click({ timeout: 4000 });

    const sent = await Promise.race([
        page.locator('div:has-text("Message sent")').waitFor({ timeout: 5000 }).then(() => true).catch(() => false),
        page.locator('.msg-form__contenteditable').waitFor({ state: "hidden", timeout: 5000 }).then(() => true).catch(() => false)
    ]);

    if (sent) return { actionTaken: "sent", relationshipStatus: rs1.status, details: "Message sent successfully" };
    return { actionTaken: "failed_to_send", relationshipStatus: rs1.status, details: "Message send failed" };
}

// Main execution loop and orchestration
async function main() {
    let browser, context, page;
    let currentUser = null;

    try {
        while (true) {
            console.log("-----------------------------------------");
            if (!currentUser) {
                console.log("[worker] polling for new user...");
                try {
                    currentUser = await apiGet("users/next");
                    if (!currentUser || !currentUser.email) {
                        console.log("[worker] no new users available. waiting...");
                        await sleep(POLL_INTERVAL_MS);
                        continue;
                    }
                    console.log(`[worker] assigned user: ${currentUser.email}`);
                    const userStatePath = statePathForUser(currentUser.email);
                    if (browser) await browser.close();
                    ({ browser, context, page } = await createBrowserContext({
                        headless: HEADLESS,
                        userStatePath: userStatePath,
                    }));
                } catch (e) {
                    logFetchError("users/next", e);
                    await sleep(POLL_INTERVAL_MS);
                    continue;
                }
            }

            const profile = await apiGet("profiles/next");
            if (!profile || !profile.url) {
                console.log("[worker] no profiles to process for user. waiting...");
                await sleep(POLL_INTERVAL_MS);
                continue;
            }
            console.log(`[action] processing profile: ${profile.url}`);

            await throttle.reserve("linkedin.com", "profile visit");
            const navResult = await navigateProfileClean(page, profile.url);

            if (!navResult.authed) {
                console.error(`[auth] failed to navigate to profile. re-auth attempt.`);
                const authResult = await ensureAuthenticated(context, page, statePathForUser(currentUser.email));
                if (authResult.ok) {
                    console.log("[auth] re-authenticated successfully. retrying profile.");
                    await sleep(1500);
                    continue; // Rerun loop with same profile
                } else {
                    console.error("[auth] re-authentication failed. user is likely logged out or blocked.");
                    await apiPost(`profiles/${profile.id}/status`, { status: "auth_failed", reason: authResult.reason });
                    currentUser = null; // Force a new user
                    throttle.failure("linkedin.com");
                    await sleep(POOLDOWN_AFTER_FAIL_MS);
                    continue;
                }
            }

            if (navResult.error) {
                console.error(`[nav] profile navigation failed: ${navResult.error}`);
                await apiPost(`profiles/${profile.id}/status`, { status: navResult.error, reason: navResult.error });
                throttle.failure("linkedin.com");
                await sleep(COOLDOWN_AFTER_FAIL_MS);
                continue;
            }

            await briefProfileScroll(page, 2000);
            await waitFullLoad(page, 15000);
            await sleep(within(2500, 4000));

            const relation = await detectRelationshipStatus(page);
            console.log(`[relation] status: ${relation.status}, reason: ${relation.reason}`);

            let actionResult;
            if (relation.status === "connected") {
                actionResult = await sendMessage(page, profile.message);
                console.log(`[message] result: ${actionResult.actionTaken}`);
            } else {
                actionResult = await sendConnectionRequest(page, profile.note);
                console.log(`[connect] result: ${actionResult.actionTaken}`);
            }

            await apiPost(`profiles/${profile.id}/status`, {
                status: actionResult.actionTaken,
                reason: actionResult.details,
            });

            if (actionResult.actionTaken === "sent" || actionResult.actionTaken === "sent_without_note" || actionResult.actionTaken === "sent_maybe") {
                throttle.success("linkedin.com");
            } else {
                throttle.failure("linkedin.com");
            }

            await sleep(actionResult.actionTaken.includes("sent") ? COOLDOWN_AFTER_SENT_MS : COOLDOWN_AFTER_FAIL_MS);
        }
    } catch (e) {
        console.error("[worker] fatal error:", e?.message || e);
        if (browser) await browser.close();
        process.exit(1);
    }
}

if (require.main === module) {
    main().catch(e => {
        console.error("Main function crashed:", e);
        process.exit(1);
    });
}
