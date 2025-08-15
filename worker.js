
const { firefox } = require('playwright');
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

async function waitTopCard(page) {
  // Wait for the top-card area so the buttons are mounted
  const topCard = page.locator('section[data-view-name*="ProfileTopCard"], .pv-top-card').first();
  await topCard.waitFor({ state: 'visible', timeout: 10000 }).catch(() => {});
  await page.waitForLoadState('domcontentloaded').catch(() => {});
}

async function listVisibleButtons(page, label) {
  try {
    const texts = await page.$$eval('button, [role="button"]', els =>
      els
        .filter(el => el.offsetParent !== null)
        .map(el => (el.innerText || el.getAttribute('aria-label') || '').trim())
        .filter(Boolean)
        .slice(0, 80)
    );
    console.log(`[UI] ${label} visible buttons:`, JSON.stringify(texts));
  } catch {}
}

/**
 * Try all known Connect entry points, then fall back to More → Connect.
 * Returns true if we clicked Connect.
 */
async function clickConnect(page) {
  await page.setViewportSize({ width: 1366, height: 850 }).catch(()=>{});
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'instant' })).catch(()=>{});
  await waitTopCard(page);
  await listVisibleButtons(page, 'top-of-profile');

  // 0) Early state detection (so we can bail gracefully)
  const pendingBtn  = page.locator('button:has-text("Pending"), [aria-label*="Pending" i]').first();
  const messageBtn  = page.locator('button:has-text("Message"), [aria-label*="Message" i]').first();
  const followBtn   = page.locator('button:has-text("Follow"), [aria-label*="Follow" i]').first();
  if (await messageBtn.count()) { console.log('[State] Already connected (Message present)'); return 'already_connected'; }
  if (await pendingBtn.count()) { console.log('[State] Invitation pending'); return 'pending'; }

  // 1) Aria pattern: Invite <Name> to connect
  const inviteBtn = page.locator('button[aria-label$=" to connect" i], button[aria-label^="Invite " i][aria-label$=" to connect" i]').first();
  if (await inviteBtn.count()) {
    try { await inviteBtn.scrollIntoViewIfNeeded(); await inviteBtn.click({ delay: 60 }); console.log('[Connect] clicked aria-label "... to connect"'); return true; } catch {}
  }

  // 2) Primary button with Connect text (span or direct)
  const primaryConnect = page.locator([
    'section[data-view-name*="ProfileTopCard"] button.artdeco-button--primary:has-text("Connect")',
    'button.artdeco-button--primary:has-text("Connect")',
    'button:has(span.artdeco-button__text:has-text("Connect"))'
  ].join(', ')).first();
  if (await primaryConnect.count()) {
    try { await primaryConnect.scrollIntoViewIfNeeded(); await primaryConnect.click({ delay: 60 }); console.log('[Connect] clicked primary Connect'); return true; } catch {}
  }

  // 3) Buttons with connect control-name (LinkedIn frequently sets this)
  const dataConnect = page.locator('[data-control-name="connect"], a[data-control-name="connect"]').first();
  if (await dataConnect.count()) {
    try { await dataConnect.scrollIntoViewIfNeeded(); await dataConnect.click({ delay: 60 }); console.log('[Connect] clicked data-control-name=connect'); return true; } catch {}
  }

  // 4) Overflow: "More actions" and then menu item
  // The trigger can vary; try several
  const moreTriggers = page.locator([
    'button[aria-label="More actions"]',
    'button[aria-label*="More" i]',
    'button.artdeco-dropdown__trigger[aria-haspopup="menu"]',
    'section[data-view-name*="ProfileTopCard"] button.artdeco-dropdown__trigger'
  ].join(', '));
  if (await moreTriggers.count()) {
    for (let i = 0; i < Math.min(await moreTriggers.count(), 3); i++) {
      const btn = moreTriggers.nth(i);
      try {
        await btn.scrollIntoViewIfNeeded();
        await btn.click({ delay: 60 });
        await page.waitForTimeout(400);
        // Connect can be a menuitem or a nested span
        const menuConnect = page.locator([
          '[role="menuitem"]:has-text("Connect")',
          '[role="menuitemcheckbox"]:has-text("Connect")',
          '.artdeco-dropdown__content [role="button"]:has-text("Connect")',
          '.artdeco-dropdown__content a:has-text("Connect")'
        ].join(', ')).first();
        if (await menuConnect.count()) {
          await menuConnect.click({ delay: 60 });
          console.log('[Connect] clicked More → Connect');
          return true;
        }
      } catch {}
    }
  }

  // 5) Follow-only clue: primary is Follow and no Connect in overflow
  if (await followBtn.count()) {
    console.log('[State] Follow-only profile (Follow visible, no Connect found)');
    return 'follow_only';
  }

  await listVisibleButtons(page, 'no-connect-found');
  return false;
}

async function launchFirefox() {
  return await firefox.launch({
    headless: true,
    args: [
      '--disable-dev-shm-usage',
      '--disable-extensions',
      '--disable-plugins',
      '--disable-background-timer-throttling',
      '--disable-backgrounding-occluded-windows',
      '--disable-renderer-backgrounding'
    ]
  });
}

async function sendConnection({ profileUrl, note, li_at, jsessionid, bcookie }) {
  if (!profileUrl) throw new Error('Missing profileUrl');
  if (!li_at) throw new Error('Missing li_at cookie in payload');

  console.log('[flow] launch firefox');
  const browser = await launchFirefox();
  console.log('[flow] new context');
  const ctx = await browser.newContext({
    locale: 'en-US',
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0',
    extraHTTPHeaders: { 'accept-language': 'en-US,en;q=0.9' }
  });

  // ---- Add cookies (li_at required; others optional) ----
  console.log('[flow] add cookies');
  const cookies = [
    { name: 'li_at', value: li_at, domain: '.linkedin.com', path: '/', httpOnly: true, secure: true, sameSite: 'None' },
  ];
  if (jsessionid) cookies.push({ name: 'JSESSIONID', value: jsessionid, domain: '.linkedin.com', path: '/', httpOnly: true, secure: true, sameSite: 'None' });
  if (bcookie)    cookies.push({ name: 'bcookie',    value: bcookie,    domain: '.linkedin.com', path: '/', httpOnly: false, secure: true, sameSite: 'None' });
  await ctx.addCookies(cookies);

  console.log('[flow] new page');
  const page = await ctx.newPage();
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

  // ---- 1) Warm up on feed to establish session ----
  console.log('[flow] warmup feed');
  await page.goto('https://www.linkedin.com/feed/', { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.waitForTimeout(800);

  // If we hit authwall/login/checkpoint here, cookie isn't valid
  const warmUrl = page.url();
  if (/\/authwall|\/checkpoint|\/login/i.test(warmUrl)) {
    await snap("warmup_auth_failed");
    await ctx.close(); 
    await browser.close();
    throw new Error('Authwall/login after warmup — li_at is invalid or expired. Capture fresh cookies.');
  }

  // ---- 2) Now go to the target profile ----
  console.log('[flow] goto profile');
  await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
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
    // proceed with note dialog + Send/Done etc.
  } else if (res === 'already_connected') {
    await snap("already_connected");
    await ctx.close();
    await browser.close();
    throw new Error('Already connected (Message button present)');
  } else if (res === 'pending') {
    await snap("pending");
    await ctx.close();
    await browser.close();
    throw new Error('Invitation already pending');
  } else if (res === 'follow_only') {
    await snap("follow_only");
    await ctx.close();
    await browser.close();
    throw new Error('Profile is follow-only (no Connect action available)');
  } else {
    await snap("no_connect_button");
    await ctx.close();
    await browser.close();
    throw new Error('Connect button not found after exhaustive selectors');
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

  // If a dialog opens, try "Add a note" then Send
  try {
    const addNote = page.getByRole('button', { name: /add a note/i }).first();
    if (await addNote.count()) {
      await addNote.click({ delay: 50 });
      const textarea = page.locator('textarea, textarea[name], textarea[id]').first();
      if (await textarea.count() && note) {
        await textarea.fill(note);
        await page.waitForTimeout(250);
      }
    }
  } catch {}

  // Possible "Send" buttons (LinkedIn has many variants)
  const sendCandidates = [
    () => page.getByRole("button", { name: /^send$/i }).first(),
    () => page.getByRole("button", { name: /send without a note/i }).first(),
    () => page.locator('button:has-text("Send without a note")').first(),
    () => page.locator('button:has-text("Send")').first(),
    () => page.locator('[data-control-name*="send_invitation"]').first(),
  ];

  // If note, fill textarea before clicking send
  if (note) {
    try {
      const textarea = page
        .locator("textarea, textarea[name], textarea[id]")
        .first();
      if (await textarea.count()) {
        await textarea.fill(note);
        await page.waitForTimeout(jitter(250, 600));
      }
    } catch (e) {
      // note fill optional; continue sending without note
    }
  }

  let sent = false;
  for (const fn of sendCandidates) {
    const btn = fn();
    if (await btn.count()) {
      try {
        await btn.click({ delay: jitter(40, 120) });
        sent = true;
        break;
      } catch {}
    }
  }

  // Some flows use "Done"
  if (!sent) {
    const doneBtn = page.getByRole("button", { name: /^done$/i }).first();
    if (await doneBtn.count()) {
      try {
        await doneBtn.click({ delay: jitter(40, 120) });
        sent = true;
      } catch {}
    }
  }

  // If still not sent, check for error/limit modals
  if (!sent) {
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
    const transient = /timeout|network|temporary|loadstate/i.test(msg);
    const knownNonTransient = /Already connected|pending|follow-only/i.test(msg);
    await failJob(job.id, msg, { 
      requeue: transient && !knownNonTransient, 
      delayMs: transient ? 15000 : 0 
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
