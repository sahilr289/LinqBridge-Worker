const { firefox } = require('playwright');
const axios = require('axios');

const API_BASE = process.env.SERVER_BASE_URL || process.env.API_BASE || 'http://localhost:5000';
const USER_EMAIL = process.env.USER_EMAIL;
const USER_PASSWORD = process.env.USER_PASSWORD;
const DAILY_CAP = parseInt(process.env.DAILY_CAP || '40', 10);
const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET;
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '10000', 10);

// Quick sanity check
try {
  const host = new URL(API_BASE).host;
  console.log('SERVER host:', host, '| secret set:', !!WORKER_SHARED_SECRET);
} catch {
  console.log('SERVER_BASE_URL is invalid or missing');
}

function jitter(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
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

async function runOnce() {
  console.log('tick at', new Date().toISOString());
  
  try {
    // Fetch next job
    const response = await axios.post(`${API_BASE}/jobs/next`, {}, {
      headers: {
        'x-worker-secret': WORKER_SHARED_SECRET,
        'Content-Type': 'application/json'
      },
      timeout: 10000
    });

    const { job } = response.data;
    if (!job) {
      console.log('No jobs available');
      return;
    }

    console.log(`Processing job ${job.id}: ${job.type}`);

    if (job.type === 'SEND_CONNECTION') {
      const result = await sendConnection(job.payload);
      console.log(`✅ SEND_CONNECTION ${job.id} completed:`, result);
    }

    // Mark job as completed
    await axios.post(`${API_BASE}/jobs/${job.id}/complete`, {
      status: 'completed',
      result: 'success'
    }, {
      headers: {
        'x-worker-secret': WORKER_SHARED_SECRET,
        'Content-Type': 'application/json'
      }
    });

  } catch (error) {
    console.error('Worker error:', error.message);
  }
}

async function sendConnection({ profileUrl, note, li_at }) {
  if (!profileUrl) throw new Error("Missing profileUrl");
  if (!li_at) throw new Error("Missing li_at cookie in payload");

  const browser = await launchFirefox();
  const ctx = await browser.newContext({
    locale: "en-US",
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
  });

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

  const page = await ctx.newPage();
  page.setDefaultTimeout(15000);

  const snap = async (label) => {
    try {
      await page.screenshot({
        path: `/tmp/${Date.now()}_${label}.png`,
        fullPage: true,
      });
    } catch {}
  };

  // Go to profile
  await page.goto(profileUrl, {
    waitUntil: "domcontentloaded",
    timeout: 45000,
  });
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

  // Try to find Connect (direct OR inside "More" menu)
  let connectClicked = false;

  // Try direct “Connect”
  const connectCandidates = [
    () => page.getByRole("button", { name: /connect/i }).first(),
    () => page.locator('button:has-text("Connect")').first(),
    () => page.locator('div[role="button"]:has-text("Connect")').first(),
    () => page.locator('button:has(span:has-text("Connect"))').first(),
  ];

  for (const fn of connectCandidates) {
    const btn = fn();
    if (await btn.count()) {
      try {
        await btn.scrollIntoViewIfNeeded();
        await btn.click({ delay: jitter(40, 120) });
        connectClicked = true;
        break;
      } catch {}
    }
  }

  // If not found, open “More” menu → “Connect”
  if (!connectClicked) {
    const moreBtn = page
      .getByRole("button", { name: /^more( actions)?$/i })
      .first();
    if (await moreBtn.count()) {
      try {
        await moreBtn.click({ delay: jitter(40, 120) });
        await page.waitForTimeout(jitter(300, 700));
        const menuConnect = page
          .locator('div[role="menuitem"]:has-text("Connect")')
          .first();
        if (await menuConnect.count()) {
          await menuConnect.click({ delay: jitter(40, 120) });
          connectClicked = true;
        }
      } catch {}
    }
  }

  if (!connectClicked) {
    await snap("no_connect_button");
    await ctx.close();
    await browser.close();
    throw new Error("Connect button not found (direct or via More)");
  }

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

  // If note was requested, try “Add a note” path first.
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

  // Possible “Send” buttons (LinkedIn has many variants)
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

  // Some flows use “Done”
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

// --- boot the poller ---
(async () => {
  console.log(`Worker started. Polling every ${POLL_INTERVAL_MS} ms`);
  // fire once immediately so you see logs even if no interval yet
  try { 
    await runOnce(); 
  } catch (e) { 
    console.error('runOnce immediate error:', e.message); 
  }

  setInterval(async () => {
    try {
      await runOnce();
    } catch (e) {
      console.error('runOnce error:', e.message);
    }
  }, POLL_INTERVAL_MS);
})();
