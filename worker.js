
const { chromium } = require('playwright');
const axios = require('axios');

const SERVER_BASE_URL = process.env.SERVER_BASE_URL;
const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET;
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '10000', 10);

if (!SERVER_BASE_URL || !WORKER_SHARED_SECRET) {
  console.error('Set SERVER_BASE_URL and WORKER_SHARED_SECRET in Replit Secrets.');
  process.exit(1);
}

// Queue HTTP helpers
async function fetchNextJob() {
  const r = await axios.post(`${SERVER_BASE_URL}/jobs/next`, {}, { 
    headers: { 'x-worker-secret': WORKER_SHARED_SECRET },
    timeout: 10000
  });
  return r.data?.job || null;
}

async function completeJob(id, result) {
  await axios.post(`${SERVER_BASE_URL}/jobs/${id}/complete`, { result }, { 
    headers: { 'x-worker-secret': WORKER_SHARED_SECRET }
  });
}

async function failJob(id, errorMessage, { requeue=false, delayMs=0 }={}) {
  await axios.post(`${SERVER_BASE_URL}/jobs/${id}/fail`, { 
    error: String(errorMessage||'Unknown'), 
    requeue, 
    delayMs 
  }, { 
    headers: { 'x-worker-secret': WORKER_SHARED_SECRET }
  });
}

function jitter(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function normalizeSpace(s) {
  return (s || '').replace(/\s+/g, ' ').trim();
}

function withWatchdog(promise, ms, label='task') {
  return Promise.race([
    promise,
    new Promise((_, rej) => setTimeout(() => rej(new Error(`${label} timed out after ${ms}ms`)), ms))
  ]);
}

async function debugCookieOpen(profileUrl, { li_at, jsessionid, bcookie }) {
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true, args: ['--disable-blink-features=AutomationControlled'] });
  const ctx = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    locale: 'en-US'
  });

  // set cookies on BOTH domains
  const cookies = [
    { name: 'li_at', value: li_at, domain: '.linkedin.com', path: '/', httpOnly: true, secure: true, sameSite: 'None' },
    { name: 'li_at', value: li_at, domain: '.www.linkedin.com', path: '/', httpOnly: true, secure: true, sameSite: 'None' },
  ];
  if (jsessionid) cookies.push(
    { name: 'JSESSIONID', value: jsessionid, domain: '.linkedin.com', path: '/', httpOnly: true, secure: true, sameSite: 'None' },
    { name: 'JSESSIONID', value: jsessionid, domain: '.www.linkedin.com', path: '/', httpOnly: true, secure: true, sameSite: 'None' },
  );
  if (bcookie) cookies.push(
    { name: 'bcookie', value: bcookie, domain: '.linkedin.com', path: '/', httpOnly: false, secure: true, sameSite: 'None' },
    { name: 'bcookie', value: bcookie, domain: '.www.linkedin.com', path: '/', httpOnly: false, secure: true, sameSite: 'None' },
  );
  await ctx.addCookies(cookies);

  const page = await ctx.newPage();
  // go straight to profile (skip /feed warmup)
  await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
  console.log('[debug] landed URL:', page.url());
  console.log('[debug] title:', await page.title().catch(()=>'-'));
  await browser.close();
}



async function clickConnect(page) {
  const PAUSE_MS = 3000;
  const pause = async (ms = PAUSE_MS) => { await page.waitForTimeout(ms); };

  const topCard = () =>
    page.locator('section[data-view-name*="ProfileTopCard"], .pv-top-card').first();

  const waitTopCardMounted = async () => {
    const top = topCard();
    await top.waitFor({ state: 'visible', timeout: 12000 }).catch(()=>{});
    await page.waitForLoadState('domcontentloaded').catch(()=>{});
    await page.waitForFunction(() => {
      const scope = document.querySelector('section[data-view-name*="ProfileTopCard"], .pv-top-card') || document;
      const items = Array.from(scope.querySelectorAll('button,[role="button"],a[role="button"]'));
      return items.some(el => /connect|message|pending|more|follow/i.test((el.innerText||el.getAttribute('aria-label')||'')));
    }, { timeout: 12000 }).catch(()=>{});
  };

  const dumpButtonsTopCard = async (label) => {
    try {
      const rows = await page.evaluate(() => {
        const scope = document.querySelector('section[data-view-name*="ProfileTopCard"], .pv-top-card') || document;
        return Array.from(scope.querySelectorAll('button,[role="button"],a[role="button"]'))
          .filter(el => el.offsetParent !== null)
          .map(el => ({
            txt: (el.innerText || el.getAttribute('aria-label') || '').trim(),
            cls: el.className || '',
            id : el.id || ''
          }))
          .slice(0, 100);
      });
      console.log(`[UI] ${label} top-card buttons: ${JSON.stringify(rows)}`);
    } catch {}
  };

  const clickIfPresent = async (loc) => {
    if (await loc.count()) {
      try {
        const el = loc.first();
        await el.scrollIntoViewIfNeeded();
        await el.click({ delay: 60 });
        await pause(); // wait after each click
        return true;
      } catch {}
    }
    return false;
  };

  // --- start: prep & mount ---
  await page.setViewportSize({ width: 1366, height: 850 }).catch(()=>{});
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'instant' })).catch(()=>{});
  await waitTopCardMounted();
  await pause(); // give SPA time

  // --- recover from "Try again" empty state if shown ---
  const tryAgain = page.locator('button:has-text("Try again")');
  if (await tryAgain.count()) {
    console.log('[UI] Found "Try again" — clicking and re-mounting');
    await clickIfPresent(tryAgain);
    await waitTopCardMounted();
    await pause();
  }

  await dumpButtonsTopCard('after-mount');
  await pause();

  const top = topCard();

  // --- state detection scoped to TOP CARD only ---
  const firstBadge = top.locator('abbr[aria-label*="1st" i], .dist-value:has-text("1st")');
  if (await firstBadge.count()) {
    console.log('[State] 1st-degree badge present — already connected');
    return 'already_connected';
  }

  const pendingTop = top.locator('button:has-text("Pending"), [aria-label*="Pending" i]');
  if (await pendingTop.count()) {
    console.log('[State] Invitation pending (top-card)');
    await pause();
    return 'pending';
  }

  // --- direct Connect variants in TOP CARD ---
  const ariaInvite = top.locator('button[aria-label$=" to connect" i], button[aria-label^="Invite " i][aria-label$=" to connect" i]');
  if (await clickIfPresent(ariaInvite)) { console.log('[Connect] aria "... to connect" (top-card)'); return true; }

  const primaryConnect = top.locator([
    'button.artdeco-button--primary:has-text("Connect")',
    'button:has(span.artdeco-button__text:has-text("Connect"))',
    'a[role="button"]:has-text("Connect")'
  ].join(', '));
  if (await clickIfPresent(primaryConnect)) { console.log('[Connect] primary (top-card)'); return true; }

  const dataCn = top.locator('[data-control-name="connect"], a[data-control-name="connect"]');
  if (await clickIfPresent(dataCn)) { console.log('[Connect] data-control-name=connect (top-card)'); return true; }

  // --- overflow: More → Connect (top-card) ---
  const moreBtn = top.locator([
    'button[aria-label="More actions"]',
    'button[aria-label*="More" i]',
    'button.artdeco-dropdown__trigger[aria-haspopup="menu"]'
  ].join(', '));
  if (await clickIfPresent(moreBtn)) {
    // dropdown animate
    const menuConnect = page.locator([
      '.artdeco-dropdown__content [role="menuitem"]:has-text("Connect")',
      '.artdeco-dropdown__content [role="menuitemcheckbox"]:has-text("Connect")',
      '.artdeco-dropdown__content [role="button"]:has-text("Connect")',
      '.artdeco-dropdown__content a:has-text("Connect")'
    ].join(', '));
    if (await clickIfPresent(menuConnect)) { console.log('[Connect] More → Connect (top-card)'); return true; }
  }

  // --- follow-only clue in top-card ---
  const followTop = top.locator('button:has-text("Follow"), [aria-label*="Follow" i]');
  if (await followTop.count()) {
    console.log('[State] Follow-only profile (top-card shows Follow)');
    await pause();
    return 'follow_only';
  }

  console.log('[State] No Connect in top-card; not connected and no action available (no_connect_ui)');
  await dumpButtonsTopCard('final');
  await pause();
  return 'no_connect_ui';
}

async function launchFirefox() {
  return await chromium.launch({ headless: true });
}

async function addLinkedInCookies(ctx, { li_at, jsessionid, bcookie }) {
  const base = [
    { name: 'li_at', value: li_at, domain: '.linkedin.com', path: '/', httpOnly: true, secure: true, sameSite: 'None' },
    { name: 'li_at', value: li_at, domain: '.www.linkedin.com', path: '/', httpOnly: true, secure: true, sameSite: 'None' },
  ];
  const extras = [];
  if (jsessionid) {
    extras.push(
      { name: 'JSESSIONID', value: jsessionid, domain: '.linkedin.com', path: '/', httpOnly: true, secure: true, sameSite: 'None' },
      { name: 'JSESSIONID', value: jsessionid, domain: '.www.linkedin.com', path: '/', httpOnly: true, secure: true, sameSite: 'None' },
    );
  }
  if (bcookie) {
    extras.push(
      { name: 'bcookie', value: bcookie, domain: '.linkedin.com', path: '/', httpOnly: false, secure: true, sameSite: 'None' },
      { name: 'bcookie', value: bcookie, domain: '.www.linkedin.com', path: '/', httpOnly: false, secure: true, sameSite: 'None' },
    );
  }
  await ctx.addCookies([...base, ...extras]);
}

async function warmSession(browser, { li_at, jsessionid, bcookie }) {
  // attempt #1: full cookie set → /feed/
  let ctx = await browser.newContext({ locale: 'en-US' });
  let page = await ctx.newPage();
  await addLinkedInCookies(ctx, { li_at, jsessionid, bcookie });
  try {
    console.log('[warmup] try /feed with full cookies');
    await page.goto('https://www.linkedin.com/feed/', { waitUntil: 'domcontentloaded', timeout: 45000 });
    await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(()=>{});
    const u = page.url();
    if (/authwall|checkpoint|login/i.test(u)) throw new Error('authwall');
    return { ctx, page };
  } catch (e) {
    console.warn('[warmup] /feed failed:', e.message);
    await ctx.close();
  }

  // attempt #2: li_at only → /feed/
  ctx = await browser.newContext({ locale: 'en-US' });
  page = await ctx.newPage();
  await addLinkedInCookies(ctx, { li_at, jsessionid: null, bcookie: null });
  try {
    console.log('[warmup] try /feed with li_at only');
    await page.goto('https://www.linkedin.com/feed/', { waitUntil: 'domcontentloaded', timeout: 45000 });
    await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(()=>{});
    const u = page.url();
    if (/authwall|checkpoint|login/i.test(u)) throw new Error('authwall');
    return { ctx, page };
  } catch (e) {
    console.warn('[warmup] /feed (li_at only) failed:', e.message);
    await ctx.close();
  }

  // attempt #3: li_at only → /mynetwork/
  ctx = await browser.newContext({ locale: 'en-US' });
  page = await ctx.newPage();
  await addLinkedInCookies(ctx, { li_at, jsessionid: null, bcookie: null });
  try {
    console.log('[warmup] try /mynetwork with li_at only');
    await page.goto('https://www.linkedin.com/mynetwork/', { waitUntil: 'domcontentloaded', timeout: 45000 });
    await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(()=>{});
    const u = page.url();
    if (/authwall|checkpoint|login/i.test(u)) throw new Error('authwall');
    return { ctx, page };
  } catch (e) {
    console.warn('[warmup] /mynetwork failed:', e.message);
    await ctx.close();
  }

  throw new Error('Warmup failed: invalid or expired cookies (redirect loop/authwall).');
}

async function sendConnection({ profileUrl, note, li_at, jsessionid, bcookie }) {
  if (!profileUrl) throw new Error('Missing profileUrl');
  if (!li_at) throw new Error('Missing li_at cookie in payload');

  console.log('[flow] launch firefox');
  const browser = await launchFirefox();

  console.log('[flow] warmup (multi-strategy)');
  const { ctx, page } = await warmSession(browser, { li_at, jsessionid, bcookie });
  await page.waitForTimeout(3000); // small settle pause

  page.setDefaultTimeout(10000);
  page.setDefaultNavigationTimeout(30000);
  await page.setViewportSize({ width: 1366, height: 850 });

  // helpful listeners
  page.on('console', msg => console.log('[page]', msg.text()));
  page.on('requestfailed', req => console.warn('[req-failed]', req.method(), req.url(), req.failure()?.errorText));

  const snap = async (label) => {
    try {
      await page.screenshot({
        path: `/tmp/${Date.now()}_${label}.png`,
        fullPage: true,
      });
    } catch {}
  };

  console.log('[flow] goto profile');
  await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(()=>{});
  await page.waitForTimeout(3000);

  // Simulate human activity to unblock "bounce-tracker" throttles
  await page.mouse.move(200, 200);
  await page.mouse.wheel(0, 800);
  await page.waitForTimeout(3000);      // <— pause
  await page.keyboard.press('End');
  await page.waitForTimeout(3000);      // <— pause
  await page.keyboard.press('Home');
  await page.waitForTimeout(3000);      // <— pause

  console.log('[flow] loaded', await page.title().catch(()=>'(no title)'));
  await page.waitForTimeout(800);

  const currentUrl = page.url();
  if (/\/authwall|\/checkpoint|\/login/i.test(currentUrl)) {
    await snap("profile_auth_failed");
    await ctx.close(); 
    await browser.close();
    throw new Error('Authwall on profile — session not recognized. Try fresh li_at / add JSESSIONID & bcookie.');
  }

  // Already connected / pending?
  const alreadyMsg =
    (await page.getByRole("button", { name: /^message$/i }).count()) > 0;
  const pending =
    (await page.getByRole("button", { name: /pending|invite sent/i }).count()) >
    0;
  if (alreadyMsg || pending) {
    await snap("already_connected_or_pending");
    await ctx.close();
    await browser.close();
    return { alreadyConnected: alreadyMsg, pending };
  }

  // Wake UI a bit
  await page.mouse.wheel(0, 400);
  await page.waitForTimeout(350);

  console.log('[flow] click connect');
  const res = await clickConnect(page);
  if (res === true) {
    // proceed to add note + Send/Done
  } else if (res === 'already_connected') {
    await snap("already_connected");
    await ctx.close();
    await browser.close();
    return { outcome: 'already_connected' };
  } else if (res === 'pending') {
    await snap("pending");
    await ctx.close();
    await browser.close();
    return { outcome: 'pending' };
  } else if (res === 'follow_only') {
    await snap("follow_only");
    await ctx.close();
    await browser.close();
    return { outcome: 'follow_only' };
  } else if (res === 'no_connect_ui') {
    await snap("no_connect_ui");
    await ctx.close();
    await browser.close();
    throw new Error('no_connect_ui');
  } else {
    await snap("no_connect_button");
    await ctx.close();
    await browser.close();
    throw new Error('Connect not found after exhaustive selectors');
  }

  console.log('[flow] handle dialog or autosend');
  // After clicking Connect, a modal may or may not appear.
  // Wait briefly for a dialog; if none, look for auto-sent signs.
  const maybeDialog = await page
    .waitForSelector('div[role="dialog"]', { state: "visible", timeout: 4000 })
    .catch(() => null);

  if (!maybeDialog) {
    // No modal → it might have auto-sent or failed silently.
    // Check for pending/confirmation or a toast.
    const toast = await page.locator(".artdeco-toast-item__message").first();
    const nowPending =
      (await page
        .getByRole("button", { name: /pending|invite sent/i })
        .count()) > 0;
    if ((await toast.count()) || nowPending) {
      await snap("auto_sent_or_pending");
      await ctx.close();
      await browser.close();
      return { autoSent: true, noteSent: false };
    }
    // Otherwise fall through to try modal buttons anyway (UI may be slow)
  }

  await page.waitForTimeout(jitter(400, 900));

  // Optional note flow
  try {
    const addNote = page.getByRole('button', { name: /add a note/i }).first();
    if (await addNote.count()) {
      await addNote.click({ delay: 60 });
      await page.waitForTimeout(3000);  // <— pause
      const textarea = page.locator('textarea, textarea[name], textarea[id]').first();
      if (await textarea.count() && note) {
        await textarea.fill(note);
        await page.waitForTimeout(3000); // <— pause
      }
    }
  } catch {}

  let clickedSend = false;
  const sendBtn = page.getByRole('button', { name: /^send$/i }).first();
  if (await sendBtn.count()) {
    await sendBtn.click({ delay: 60 });
    await page.waitForTimeout(3000);    // <— pause
    clickedSend = true;
  } else {
    const doneBtn = page.getByRole('button', { name: /^done$/i }).first();
    if (await doneBtn.count()) {
      await doneBtn.click({ delay: 60 });
      await page.waitForTimeout(3000);  // <— pause
      clickedSend = true;
    }
  }

  if (!clickedSend) {
    const modalText = await page
      .locator('div[role="dialog"]')
      .first()
      .innerText()
      .catch(() => "");
    if (/limit|weekly|too many invitations|try again later/i.test(modalText)) {
      await snap("invite_limit");
      await ctx.close();
      await browser.close();
      throw new Error("Invite limit hit or temporary restriction");
    }

    // Check if button changed to Pending anyway
    const nowPending =
      (await page
        .getByRole("button", { name: /pending|invite sent/i })
        .count()) > 0;
    if (nowPending) {
      await snap("became_pending");
      await ctx.close();
      await browser.close();
      return { autoSent: true, noteSent: !!note };
    }

    await snap("send_button_not_found");
    await ctx.close();
    await browser.close();
    throw new Error("Send/Done button not found after Connect");
  }

  console.log('[flow] send finished; verifying');
  // post-click settle
  await page.waitForTimeout(jitter(600, 1200));

  // Verify by toast or Pending state
  const toast = await page.locator(".artdeco-toast-item__message").first();
  const nowPending =
    (await page.getByRole("button", { name: /pending|invite sent/i }).count()) >
    0;

  await ctx.close();
  await browser.close();

  return {
    ok: true,
    noteSent: !!note,
    confirmedBy: (await toast.count())
      ? "toast"
      : nowPending
        ? "pending"
        : "unknown",
  };
}

// Processors
const processors = {
  async SEND_CONNECTION(job) {
    const { profileUrl, note, cookieBundle } = job.payload || {};
    const { li_at, jsessionid, bcookie } = cookieBundle || {};
    console.log('[job] SEND_CONNECTION start', { hasCookie: !!li_at, hasJsession: !!jsessionid, hasBcookie: !!bcookie, profileUrl });

    const result = await withWatchdog(
      sendConnection({ profileUrl, note, li_at, jsessionid, bcookie }),
      90_000,
      'sendConnection'
    );

    console.log('[job] SEND_CONNECTION done', result);
    return result;
  }
};

async function runOnce() {
  const job = await fetchNextJob();
  if (!job) { 
    console.log('No jobs available'); 
    return; 
  }

  console.log(`Processing job ${job.id}: ${job.type}`);
  const handler = processors[job.type];
  if (!handler) { 
    await failJob(job.id, `No processor for ${job.type}`); 
    return; 
  }

  try {
    const result = await handler(job);
    console.log(`✅ ${job.type} ${job.id} completed:`, result);
    await completeJob(job.id, result);
  } catch (e) {
    const msg = String(e.message || '');
    console.error('Worker error:', msg);
    const transient = /redirect|authwall|network|timeout|temporary|loadstate/i.test(msg);
    const knownNonTransient = /Already connected|pending|follow-only/i.test(msg);
    await failJob(job.id, msg, { 
      requeue: transient && !knownNonTransient, 
      delayMs: transient ? 20000 : 0 
    });
  }
}

// Boot the poller
(async () => {
  const host = new URL(SERVER_BASE_URL).host;
  console.log('SERVER host:', host, '| secret set:', !!WORKER_SHARED_SECRET);
  console.log(`Worker started. Polling every ${POLL_INTERVAL_MS} ms`);
  
  const tick = () => console.log('tick at', new Date().toISOString());
  const loop = async () => { 
    tick(); 
    await runOnce().catch(e => console.error('runOnce error:', e.message)); 
  };
  
  await loop();
  setInterval(loop, POLL_INTERVAL_MS);
})();
