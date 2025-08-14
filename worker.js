
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

/**
 * Find and click the Connect action using the precise selectors you provided.
 * Returns true if we clicked something that should open the invite flow or send directly.
 */
async function clickConnect(page) {
  // Ensure desktop layout & top card in view
  await page.setViewportSize({ width: 1366, height: 850 }).catch(()=>{});
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'instant' })).catch(()=>{});
  await page.waitForTimeout(400);

  // 1) Exact aria-label pattern: "Invite <Name> to connect"
  const inviteBtn = page.locator('button[aria-label$=" to connect"]'); // ends with " to connect"
  if (await inviteBtn.count()) {
    try {
      // Guard against false positives by checking visible text contains "Connect"
      const first = inviteBtn.first();
      const txt = normalizeSpace(await first.innerText().catch(()=>''));      
      if (/connect/i.test(txt)) {
        await first.scrollIntoViewIfNeeded();
        await first.click({ delay: 60 });
        console.log('[Connect] clicked aria-label "... to connect" button');
        return true;
      }
    } catch {}
  }

  // 2) Primary Connect button on the top card
  const primaryConnect = page.locator([
    // Top-card contextual primary button that says Connect
    'section[data-view-name*="ProfileTopCard"] button.artdeco-button--primary:has-text("Connect")',
    // or any primary connect button visible on the page
    'button.artdeco-button--primary:has-text("Connect")'
  ].join(', ')).first();

  if (await primaryConnect.count()) {
    try {
      await primaryConnect.scrollIntoViewIfNeeded();
      await primaryConnect.click({ delay: 60 });
      console.log('[Connect] clicked primary Connect button');
      return true;
    } catch {}
  }

  // 3) "More actions" → menu item "Connect"
  const moreBtn = page.locator([
    'button[aria-label="More actions"]',
    'button[aria-label*="More" i]',
    'button.artdeco-dropdown__trigger[aria-haspopup="menu"]'
  ].join(', ')).first();

  if (await moreBtn.count()) {
    try {
      await moreBtn.scrollIntoViewIfNeeded();
      await moreBtn.click({ delay: 60 });
      await page.waitForTimeout(300);

      // menuitem that contains "Connect"
      const menuConnect = page.locator('[role="menuitem"]:has-text("Connect"), [role="menuitemcheckbox"]:has-text("Connect")').first();
      if (await menuConnect.count()) {
        await menuConnect.click({ delay: 60 });
        console.log('[Connect] clicked More → Connect');
        return true;
      }
    } catch {}
  }

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

async function sendConnection({ profileUrl, note, li_at }) {
  if (!profileUrl) throw new Error("Missing profileUrl");
  if (!li_at) throw new Error("Missing li_at cookie in payload");

  console.log('[flow] launch firefox');
  const browser = await launchFirefox();
  console.log('[flow] new context');
  const ctx = await browser.newContext({
    locale: "en-US",
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
  });

  console.log('[flow] add cookies');
  // Inject LinkedIn session
  await ctx.addCookies([
    {
      name: "li_at",
      value: li_at,
      domain: ".linkedin.com",
      path: "/",
      httpOnly: true,
      secure: true,
    },
  ]);

  console.log('[flow] new page');
  const page = await ctx.newPage();
  page.setDefaultTimeout(10000);               // 10s for element ops
  page.setDefaultNavigationTimeout(30000);     // 30s for navigations

  // Helpful logging for debugging:
  page.on('console', msg => console.log('[page]', msg.text()));
  page.on('requestfailed', req => console.warn('[req-failed]', req.method(), req.url(), req.failure()?.errorText));
  page.on('response', resp => {
    const url = resp.url();
    if (url.includes('linkedin')) console.log('[resp]', resp.status(), url.slice(0,120));
  });

  const snap = async (label) => {
    try {
      await page.screenshot({
        path: `/tmp/${Date.now()}_${label}.png`,
        fullPage: true,
      });
    } catch {}
  };

  console.log('[flow] goto profile');
  // Go to profile
  await page.goto(profileUrl, {
    waitUntil: "domcontentloaded",
    timeout: 45000,
  });
  console.log('[flow] loaded', await page.title().catch(()=>'(no title)'));
  await page.waitForTimeout(jitter(600, 1200));

  // Check auth
  if (page.url().includes("login") || page.url().includes("/checkpoint/")) {
    await snap("auth_redirect");
    throw new Error(
      "Auth failed or cookie invalid; redirected to login/checkpoint",
    );
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
  // CLICK CONNECT using exact selectors and fallbacks
  const clicked = await clickConnect(page);
  if (!clicked) {
    await snap("no_connect_button");
    await ctx.close();
    await browser.close();
    throw new Error('Connect button not found with exact selectors (aria-label "... to connect", primary Connect, or More → Connect)');
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

  // If note was requested, try "Add a note" path first.
  if (note) {
    try {
      const addNoteBtn = page
        .getByRole("button", { name: /add a note/i })
        .first();
      if (await addNoteBtn.count()) {
        await addNoteBtn.click({ delay: jitter(40, 120) });
        await page.waitForTimeout(jitter(300, 700));
      }
    } catch {}
  }

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
    const li_at = cookieBundle?.li_at || job.payload?.li_at;
    console.log('[job] SEND_CONNECTION start', { hasCookie: !!li_at, profileUrl });

    // 90s total guard for the whole run
    const result = await withWatchdog(
      sendConnection({ profileUrl, note, li_at }),
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
    console.error('Worker error:', e.message);
    const transient = /timeout|navigation|rate limit|temporary|network/i.test(e.message||'');
    await failJob(job.id, e.message, { 
      requeue: transient, 
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
