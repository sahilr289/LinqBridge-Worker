
// worker.js — Playwright runner for connection sending (Firefox)

const { firefox } = require('playwright');

const API_BASE      = process.env.API_BASE || 'http://localhost:5000';
const USER_EMAIL    = process.env.USER_EMAIL;
const USER_PASSWORD = process.env.USER_PASSWORD;
const DAILY_CAP     = parseInt(process.env.DAILY_CAP || '40', 10);

const SLEEP_BETWEEN_MS = { min: 35_000, max: 90_000 };
function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }
function jitter(min, max){ return Math.floor(Math.random()*(max-min+1))+min; }

async function getJWT() {
  const r = await fetch(`${API_BASE}/login`, {
    method: 'POST',
    headers: { 'Content-Type':'application/json' },
    body: JSON.stringify({ email: USER_EMAIL, password: USER_PASSWORD })
  });
  const j = await r.json();
  if (!r.ok || !j.success || !j.token) throw new Error('JWT login failed: ' + (j.message || r.status));
  return j.token;
}

async function getLiAt(token) {
  const r = await fetch(`${API_BASE}/api/me/liat`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const j = await r.json();
  if (!r.ok || !j.success || !j.li_at) throw new Error('No li_at stored for this user.');
  return j.li_at;
}

async function getNextLead(token) {
  const r = await fetch(`${API_BASE}/api/automation/runner/tick?limit=1`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const j = await r.json();
  if (!r.ok || !j.success) throw new Error('runner/tick failed');
  return j.leads?.[0] || null;
}

async function updateStatus(token, leadId, status, action_details={}) {
  const r = await fetch(`${API_BASE}/api/automation/update-status`, {
    method: 'POST',
    headers: {
      'Content-Type':'application/json',
      'Authorization': `Bearer ${token}`
    },
    body: JSON.stringify({ lead_id: leadId, status, message: status, action_details })
  });
  const j = await r.json();
  if (!r.ok || !j.success) console.warn('update-status failed:', j);
}

async function launchFirefox() {
  // No Chromium flags; Firefox will ignore unsupported ones and crash early.
  return await firefox.launch({
    headless: true,         // keep headless in Replit
  });
}

async function sendConnection({ profileUrl, note }, li_at) {
  const browser = await launchFirefox();

  const ctx = await browser.newContext({
    locale: 'en-US',
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0',
  });

  // Inject LinkedIn session
  await ctx.addCookies([{
    name: 'li_at', value: li_at, domain: '.linkedin.com', path: '/', httpOnly: true, secure: true
  }]);

  const page = await ctx.newPage();

  // Open profile
  await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 45_000 });
  await page.waitForTimeout(jitter(800, 1500));

  // Defensive: some profiles redirect to "Sign in" if cookie is bad
  if (page.url().includes('login') || page.url().includes('/checkpoint/')) {
    throw new Error('Auth failed or cookie invalid; page redirected to login/checkpoint');
  }

  // Scroll a bit to trigger lazy UI
  await page.mouse.wheel(0, 400);
  await page.waitForTimeout(jitter(400, 900));

  // --- Find and click "Connect" ---
  // We try several robust selectors; LinkedIn changes markup frequently.
  const candidates = [
    // ARIA role-based (best)
    () => page.getByRole('button', { name: /connect/i }).first(),
    // text fallback
    () => page.locator('button:has-text("Connect")').first(),
    () => page.locator('div[role="button"]:has-text("Connect")').first(),
    // icon+label span fallback (your example)
    () => page.locator('button:has(span:has-text("Connect"))').first(),
  ];

  let clicked = false;
  for (const fn of candidates) {
    const btn = fn();
    if (await btn.count()) {
      try {
        await btn.scrollIntoViewIfNeeded();
        await btn.click({ delay: jitter(40, 120) });
        clicked = true;
        break;
      } catch (e) {
        // try the next candidate
      }
    }
  }
  if (!clicked) throw new Error('Connect button not found');

  // Optional: add a note
  if (note) {
    try {
      const addNoteBtn = page.getByRole('button', { name: /add a note/i }).first();
      if (await addNoteBtn.count()) {
        await addNoteBtn.click();
        await page.waitForTimeout(jitter(300, 700));
        const textarea = page.locator('textarea, textarea[name], textarea[id]').first();
        await textarea.fill(note);
        await page.waitForTimeout(jitter(300, 600));
      }
    } catch (e) {
      console.warn('Add note failed (continuing):', e.message);
    }
  }

  // Send
  const sendBtn = page.getByRole('button', { name: /^send$/i }).first();
  if (await sendBtn.count()) {
    await sendBtn.click({ delay: jitter(40, 120) });
    await page.waitForTimeout(jitter(600, 1200));
  } else {
    // Some dialogs use "Done"
    const altBtn = page.getByRole('button', { name: /^done$/i }).first();
    if (await altBtn.count()) {
      await altBtn.click({ delay: jitter(40, 120) });
      await page.waitForTimeout(jitter(600, 1200));
    } else {
      throw new Error('Send/Done button not found after Connect');
    }
  }

  await ctx.close();
  await browser.close();
  return true;
}

async function main() {
  if (!USER_EMAIL || !USER_PASSWORD) {
    console.error('Set USER_EMAIL and USER_PASSWORD in Replit Secrets.');
    process.exit(1);
  }

  const token = await getJWT();
  const li_at = await getLiAt(token);

  let sentToday = 0;

  while (sentToday < DAILY_CAP) {
    const lead = await getNextLead(token);
    if (!lead) {
      console.log('No leads ready. Exiting.');
      break;
    }

    const profileUrl = lead.profile_url || lead.public_profile_url;
    console.log(`→ Connecting: ${lead.first_name || ''} ${lead.last_name || ''} | ${profileUrl}`);

    try {
      await updateStatus(token, lead.id, 'in_progress');
      await sendConnection({ profileUrl, note: null }, li_at);
      await updateStatus(token, lead.id, 'connection_sent', { connection_note_sent: '' });
      sentToday++;

      const pause = jitter(SLEEP_BETWEEN_MS.min, SLEEP_BETWEEN_MS.max);
      console.log(`✅ Sent. Sleeping ${Math.round(pause / 1000)}s`);
      await sleep(pause);
    } catch (e) {
      console.error('❌ Connect failed:', e.message);
      await updateStatus(token, lead.id, 'error', { error_message: e.message });
      await sleep(jitter(10_000, 20_000)); // small pause even on error
    }
  }

  console.log(`Done. Sent today: ${sentToday}/${DAILY_CAP}`);
}

main().catch(e => { console.error(e); process.exit(1); });
