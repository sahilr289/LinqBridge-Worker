// worker.js — LinqBridge connection sender (Playwright + Firefox)

// Run with: npm run worker
// Env vars required (Replit Secrets):
//   API_BASE       -> e.g. https://<your>-pike.replit.dev:5000   (use the exact URL that works in your extension)
//   USER_EMAIL     -> your BDR login (same one you use for /login)
//   USER_PASSWORD  -> that user's password
//   DAILY_CAP      -> optional (default 40)

const { firefox } = require('playwright');

// Node 22 has global fetch; if your runtime doesn't, uncomment:
// const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));

const API_BASE      = process.env.API_BASE || 'http://localhost:5000';
const USER_EMAIL    = process.env.USER_EMAIL;
const USER_PASSWORD = process.env.USER_PASSWORD;
const DAILY_CAP     = parseInt(process.env.DAILY_CAP || '40', 10);

// Gentle human timing
const SLEEP_BETWEEN_MS = { min: 35_000, max: 90_000 };

function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }
function jitter(min, max){ return Math.floor(Math.random()*(max-min+1))+min; }

async function getJWT() {
  const r = await fetch(`${API_BASE}/login`, {
    method: 'POST',
    headers: { 'Content-Type':'application/json' },
    body: JSON.stringify({ email: USER_EMAIL, password: USER_PASSWORD })
  });
  const j = await r.json().catch(()=> ({}));
  if (!r.ok || !j.success || !j.token) {
    throw new Error('JWT login failed: ' + (j.message || r.status));
  }
  return j.token;
}

async function getLiAt(token) {
  const r = await fetch(`${API_BASE}/api/me/liat`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const j = await r.json().catch(()=> ({}));
  if (!r.ok || !j.success || !j.li_at) {
    throw new Error('No li_at stored for this user. Capture cookies via extension first.');
  }
  return j.li_at;
}

async function getNextLead(token) {
  const r = await fetch(`${API_BASE}/api/automation/runner/tick?limit=1`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const j = await r.json().catch(()=> ({}));
  if (!r.ok || !j.success) throw new Error('runner/tick failed');
  return j.leads?.[0] || null;
}

async function updateStatus(token, leadId, status, action_details = {}) {
  const r = await fetch(`${API_BASE}/api/automation/update-status`, {
    method: 'POST',
    headers: {
      'Content-Type':'application/json',
      'Authorization': `Bearer ${token}`
    },
    body: JSON.stringify({
      lead_id: leadId,
      status,
      message: status,
      action_details
    })
  });
  const j = await r.json().catch(()=> ({}));
  if (!r.ok || !j.success) {
    console.warn('update-status failed:', j.message || r.status);
  }
}

// Utility: click the first *visible* locator among candidates
async function clickFirstVisible(page, locators) {
  for (const locator of locators) {
    try {
      const handle = page.locator(locator).first();
      if (await handle.count()) {
        await handle.waitFor({ state: 'visible', timeout: 2000 }).catch(()=>{});
        const box = await handle.boundingBox().catch(()=>null);
        if (box) {
          await handle.click({ delay: jitter(30, 120) });
          return true;
        }
      }
    } catch (_) {}
  }
  return false;
}

async function sendConnection({ profileUrl, note }, li_at) {
  const browser = await firefox.launch({ headless: true, args: ['--no-sandbox','--disable-setuid-sandbox'] });
  const ctx = await browser.newContext({
    userAgent: 'Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0'
  });

  // Important cookie
  await ctx.addCookies([{
    name: 'li_at',
    value: li_at,
    domain: '.linkedin.com',
    path: '/',
    httpOnly: true,
    secure: true
  }]);

  const page = await ctx.newPage();

  // Navigate to the public profile URL
  await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 45_000 });
  await page.waitForTimeout(jitter(800, 1500));

  // Detect logged-out / invalid cookie (sign-in page)
  if ((await page.url()).includes('/login')) {
    throw new Error('Invalid or expired li_at (redirected to login).');
  }

  // Light scroll to trigger lazy UI
  await page.mouse.wheel(0, jitter(200, 600));
  await page.waitForTimeout(jitter(600, 1200));

  // If already connected/pending, skip safely
  const alreadyConnected = await page.locator('button:has-text("Message"), button:has-text("Pending"), span:has-text("Pending")').first().count();
  if (alreadyConnected) {
    await ctx.close(); await browser.close();
    return { status: 'skipped', reason: 'Already connected or pending' };
  }

  // --- Find the real Connect button ---
  // Primary robust selector: go from the <span> to its ancestor <button>
  const connectCandidates = [
    'xpath=//span[normalize-space(.)="Connect"]/ancestor::button[1]',
    'button[aria-label*="Connect"]',
    'button:has-text("Connect")',
    'div[role="button"]:has-text("Connect")'
  ];

  let clickedConnect = await clickFirstVisible(page, connectCandidates);

  // If Connect is in the overflow "More" menu
  if (!clickedConnect) {
    const openedMore = await clickFirstVisible(page, [
      'button[aria-label*="More"]',
      'button:has-text("More")',
      'div[role="button"]:has-text("More")'
    ]);

    if (openedMore) {
      await page.waitForTimeout(jitter(400, 800));
      clickedConnect = await clickFirstVisible(page, [
        'div[role="menu"] button:has-text("Connect")',
        'div[role="menu"] div[role="button"]:has-text("Connect")',
        // Some UIs render as anchor
        'div[role="menu"] a:has-text("Connect")'
      ]);
    }
  }

  if (!clickedConnect) {
    await ctx.close(); await browser.close();
    throw new Error('Connect button not found');
  }

  // Small human delay
  await page.waitForTimeout(jitter(500, 1200));

  // Some accounts require knowing the person’s email -> detect email field and abort
  const emailGate = await page.locator('input[type="email"], input[name*="email"]').first().count();
  if (emailGate) {
    await ctx.close(); await browser.close();
    return { status: 'skipped', reason: 'Connect requires email' };
  }

  // Add a note (optional)
  if (note) {
    try {
      const openedNote = await clickFirstVisible(page, [
        'button:has-text("Add a note")',
        'div[role="button"]:has-text("Add a note")'
      ]);
      if (openedNote) {
        await page.waitForTimeout(jitter(300, 700));
        const textarea = page.locator('textarea, textarea[name], textarea[id]').first();
        if (await textarea.count()) {
          await textarea.fill(note, { timeout: 5000 });
          await page.waitForTimeout(jitter(300, 600));
        }
      }
    } catch (e) {
      // ok to continue without note
      console.warn('Add note failed:', e.message);
    }
  }

  // Click Send in the dialog
  const sent = await clickFirstVisible(page, [
    'div[role="dialog"] button:has-text("Send")',
    'button[aria-label="Send now"]',
    'button:has-text("Send now")'
  ]);
  if (!sent) {
    // Sometimes dialog is not there — try a generic send
    const fallback = await clickFirstVisible(page, ['button:has-text("Send")']);
    if (!fallback) {
      await ctx.close(); await browser.close();
      throw new Error('Send button not found');
    }
  }

  await page.waitForTimeout(jitter(600, 1200));
  await ctx.close();
  await browser.close();

  return { status: 'connection_sent' };
}

async function main() {
  if (!USER_EMAIL || !USER_PASSWORD) {
    console.error('Set USER_EMAIL and USER_PASSWORD in Replit Secrets.');
    process.exit(1);
  }

  const token = await getJWT();
  const li_at  = await getLiAt(token);

  let sentToday = 0;

  while (sentToday < DAILY_CAP) {
    const lead = await getNextLead(token);
    if (!lead) {
      console.log('No leads ready. Exiting.');
      break;
    }

    const profileUrl = lead.profile_url; // already COALESCE(public_profile_url, profile_url) on server
    const note = null; // wire your message template later

    console.log(`→ Connecting: ${lead.first_name || ''} ${lead.last_name || ''} | ${profileUrl}`);
    try {
      await updateStatus(token, lead.id, 'in_progress');

      const result = await sendConnection({ profileUrl, note }, li_at);

      if (result.status === 'connection_sent') {
        await updateStatus(token, lead.id, 'connection_sent', { connection_note_sent: note || '' });
        sentToday++;
        const pause = jitter(SLEEP_BETWEEN_MS.min, SLEEP_BETWEEN_MS.max);
        console.log(`✅ Sent. Sleeping ${Math.round(pause/1000)}s`);
        await sleep(pause);
      } else if (result.status === 'skipped') {
        console.log(`↷ Skipped: ${result.reason}`);
        await updateStatus(token, lead.id, 'skipped', { reason: result.reason });
        await sleep(jitter(10_000, 20_000));
      } else {
        throw new Error('Unknown result');
      }

    } catch (e) {
      console.error('❌ Connect failed:', e.message);
      await updateStatus(token, lead.id, 'error', { error_message: e.message });
      await sleep(jitter(10_000, 20_000)); // small pause even on error
    }
  }

  console.log(`Done. Sent today: ${sentToday}/${DAILY_CAP}`);
}

main().catch(e => { console.error(e); process.exit(1); });
