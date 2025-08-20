// worker.js — LinqBridge SEND_CONNECTION worker (Playwright)

import { chromium } from "playwright"; // Railway supports Playwright well
import process from "node:process";

// ====== ENV ======
const SERVER_BASE_URL     = process.env.SERVER_BASE_URL;         // e.g. https://calm-rejoicing-linqbridge.up.railway.app
const WORKER_SHARED_SECRET= process.env.WORKER_SHARED_SECRET;     // must match API
const POLL_INTERVAL_MS    = parseInt(process.env.POLL_INTERVAL_MS || "15000", 10);
const HEADLESS            = (process.env.HEADLESS ?? "true") !== "false"; // "false" to watch locally
const SOFT_MODE           = (process.env.LB_SOFT_MODE ?? "false") === "true"; // when true, only validates session, doesn't click
const NAV_TIMEOUT         = 30000;

// Basic guardrails
if (!SERVER_BASE_URL || !WORKER_SHARED_SECRET) {
  console.error("Missing SERVER_BASE_URL or WORKER_SHARED_SECRET env.");
  process.exit(1);
}

const api = async (path, opts = {}) => {
  const url = path.startsWith("/") ? `${SERVER_BASE_URL}${path}` : `${SERVER_BASE_URL}/${path}`;
  const res = await fetch(url, {
    ...opts,
    headers: {
      "content-type": "application/json",
      "x-worker-secret": WORKER_SHARED_SECRET,
      ...(opts.headers || {})
    }
  });
  if (!res.ok) {
    const txt = await (async()=>{ try {return await res.text();} catch{return ""}})();
    throw new Error(`API ${path} HTTP ${res.status}: ${txt.slice(0,200)}`);
  }
  const ct = res.headers.get("content-type") || "";
  if (!ct.includes("application/json")) return {};
  return await res.json();
};

async function setLinkedInCookies(context, bundle) {
  const cookieDefs = [];

  // li_at is mandatory
  if (bundle.li_at) {
    cookieDefs.push({ name: "li_at", value: bundle.li_at, domain: ".linkedin.com", path: "/", httpOnly: true, secure: true });
  }

  // JSESSIONID **must** be quoted when set via cookie header; Playwright handles raw value.
  if (bundle.jsessionid) {
    cookieDefs.push({ name: "JSESSIONID", value: bundle.jsessionid, domain: ".linkedin.com", path: "/", httpOnly: true, secure: true });
  }

  if (bundle.bcookie) {
    cookieDefs.push({ name: "bcookie", value: bundle.bcookie, domain: ".linkedin.com", path: "/", httpOnly: false, secure: true });
  }

  if (!cookieDefs.length) throw new Error("No cookies provided");

  await context.addCookies(cookieDefs);
}

async function validateSession(page) {
  // Quick ping via web UI: open feed and check for a known element
  await page.goto("https://www.linkedin.com/feed/", { timeout: NAV_TIMEOUT, waitUntil: "domcontentloaded" });

  // If page redirects to login, session is invalid
  if (page.url().includes("/checkpoint") || page.url().includes("/login")) {
    throw new Error("Session invalid (redirected to login/checkpoint)");
  }

  // Look for global nav
  const nav = await page.locator("header.global-nav").first();
  if (await nav.count() === 0) {
    // Not fatal, but suspicious — still allow continuation
    console.warn("[validateSession] Global nav not found; continuing.");
  }
}

// Attempt to send a connection request on a profile page
async function sendConnection(page, profileUrl, note) {
  await page.goto(profileUrl, { timeout: NAV_TIMEOUT, waitUntil: "domcontentloaded" });

  // Sometimes LinkedIn loads lazy content; wait a little
  await page.waitForTimeout(1500);

  // Possible places for the Connect button:
  const connectCandidates = [
    'button:has-text("Connect")',
    'button[aria-label="Connect"]',
    'div.pvs-profile-actions button:has-text("Connect")',
    'div.pvs-profile-actions button[aria-label="Connect"]',
    'button:has-text("More")' // as a fallback; will open menu
  ];

  let clickedConnect = false;

  for (const sel of connectCandidates) {
    const el = page.locator(sel).first();
    if (await el.count()) {
      const label = await el.textContent().catch(()=>sel);
      if (label && label.toLowerCase().includes("more")) {
        // Click "More", then try a connect item in the dropdown
        await el.click({ timeout: 5000 });
        const menuItem = page.locator('div[role="menu"] div:has-text("Connect")').first();
        if (await menuItem.count()) {
          await menuItem.click({ timeout: 5000 });
          clickedConnect = true;
          break;
        }
      } else {
        await el.click({ timeout: 5000 });
        clickedConnect = true;
        break;
      }
    }
  }

  if (!clickedConnect) {
    // Could already be connected or button hidden
    throw new Error("Connect button not found (maybe already connected or requires follow)");
  }

  // If a "Add a note" button appears, add note
  if (note && note.trim()) {
    const addNote = page.locator('button:has-text("Add a note")').first();
    if (await addNote.count()) {
      await addNote.click({ timeout: 5000 });
      const textarea = page.locator('textarea[id],textarea').first();
      await textarea.fill(note.slice(0, 280), { timeout: 5000 }); // LinkedIn note limit ~300, keep safe
    }
  }

  // Click Send
  const sendBtn = page.locator('button:has-text("Send")').first();
  if (await sendBtn.count() === 0) {
    // Sometimes the first modal has 'Next' then Send
    const nextBtn = page.locator('button:has-text("Next")').first();
    if (await nextBtn.count()) {
      await nextBtn.click({ timeout: 5000 });
    }
  }

  const finalSend = page.locator('button:has-text("Send")').first();
  if (await finalSend.count()) {
    await finalSend.click({ timeout: 8000 });
  } else {
    throw new Error("Send button not found after opening connect dialog.");
  }

  // Brief wait for confirmation toast/dialog to disappear
  await page.waitForTimeout(1500);
  return { ok: true };
}

// Main polling loop
async function runOnce() {
  // Pull a job
  const next = await api("/jobs/next", {
    method: "POST",
    body: JSON.stringify({ types: ["SEND_CONNECTION"] })
  });

  const job = next?.job || null;
  if (!job) return false; // nothing to do

  const { id, payload } = job;
  try {
    const { profileUrl, note, cookieBundle } = payload || {};
    if (!profileUrl) throw new Error("Missing profileUrl in payload");
    if (!cookieBundle?.li_at) throw new Error("Missing cookieBundle.li_at in payload");

    // Launch browser
    const browser = await chromium.launch({ headless: HEADLESS });
    const context = await browser.newContext({
      viewport: { width: 1366, height: 768 },
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    });
    const page = await context.newPage();

    // Cookies + session
    await setLinkedInCookies(context, {
      li_at: cookieBundle.li_at,
      jsessionid: cookieBundle.jsessionid || null,
      bcookie: cookieBundle.bcookie || null,
    });
    await validateSession(page);

    if (SOFT_MODE) {
      // Only validate session, don't click
      await browser.close();
      await api(`/jobs/${id}/complete`, { method: "POST", body: JSON.stringify({ result: { soft: true } }) });
      console.log(`[worker] SOFT completed job ${id}`);
      return true;
    }

    // Real connect flow
    const result = await sendConnection(page, profileUrl, note || "");
    await browser.close();

    await api(`/jobs/${id}/complete`, { method: "POST", body: JSON.stringify({ result }) });
    console.log(`[worker] Completed job ${id} OK`);
    return true;
  } catch (err) {
    console.error(`[worker] Job ${id} failed:`, err?.message || err);
    try {
      await api(`/jobs/${id}/fail`, {
        method: "POST",
        body: JSON.stringify({ error: String(err?.message || err), requeue: false })
      });
    } catch {
      // swallow
    }
    return true; // processed something
  }
}

async function main() {
  console.log("[worker] starting. Headless:", HEADLESS, "Soft mode:", SOFT_MODE);
  while (true) {
    try {
      const didWork = await runOnce();
      if (!didWork) {
        await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
      }
    } catch (e) {
      console.error("[worker] loop error:", e?.message || e);
      await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
    }
  }
}

main().catch(e => {
  console.error("[worker] fatal:", e);
  process.exit(1);
});
