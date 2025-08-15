
const express = require('express');
const app = express();
const port = process.env.PORT || 5000;

app.use(express.json());

const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET || 'Rohira@0803';

// In-memory job queue (replace with database in production)
let jobs = [];
let jobIdCounter = 1;

// In-memory cookie storage (replace with database in production)
let userCookies = {};

app.get('/', (req, res) => {
  res.json({ 
    status: 'LinqBridge server running',
    worker: 'Use npm run worker to start LinkedIn automation'
  });
});

// Add a new job to the queue
app.post('/jobs', (req, res) => {
  const { type, payload, priority = 1 } = req.body;
  
  const job = {
    id: jobIdCounter++,
    type,
    payload,
    priority,
    status: 'pending',
    createdAt: new Date().toISOString()
  };
  
  jobs.push(job);
  jobs.sort((a, b) => b.priority - a.priority); // Higher priority first
  
  console.log(`Job ${job.id} added to queue:`, type);
  res.json({ success: true, job });
});

// Get next job for worker
app.post('/jobs/next', (req, res) => {
  const workerSecret = req.headers['x-worker-secret'];
  
  if (workerSecret !== WORKER_SHARED_SECRET) {
    return res.status(401).json({ success: false, message: 'Invalid worker secret' });
  }
  
  const job = jobs.find(j => j.status === 'pending');
  
  if (job) {
    job.status = 'processing';
    job.startedAt = new Date().toISOString();
  }
  
  res.json({ ok: true, job: job || null });
});

// Mark job as completed
app.post('/jobs/:id/complete', (req, res) => {
  const workerSecret = req.headers['x-worker-secret'];
  
  if (workerSecret !== WORKER_SHARED_SECRET) {
    return res.status(401).json({ success: false, message: 'Invalid worker secret' });
  }
  
  const jobId = parseInt(req.params.id);
  const job = jobs.find(j => j.id === jobId);
  
  if (job) {
    job.status = 'completed';
    job.completedAt = new Date().toISOString();
    job.result = req.body.result;
    console.log(`Job ${jobId} completed`);
  }
  
  res.json({ success: true });
});

// Mark job as failed
app.post('/jobs/:id/fail', (req, res) => {
  const workerSecret = req.headers['x-worker-secret'];
  
  if (workerSecret !== WORKER_SHARED_SECRET) {
    return res.status(401).json({ success: false, message: 'Invalid worker secret' });
  }
  
  const jobId = parseInt(req.params.id);
  const job = jobs.find(j => j.id === jobId);
  const { error, requeue = false, delayMs = 0 } = req.body;
  
  if (job) {
    if (requeue) {
      job.status = 'pending';
      job.attempts = (job.attempts || 0) + 1;
      job.lastError = error;
      job.nextRetryAt = new Date(Date.now() + delayMs).toISOString();
      console.log(`Job ${jobId} failed, requeued for retry in ${delayMs}ms`);
    } else {
      job.status = 'failed';
      job.failedAt = new Date().toISOString();
      job.error = error;
      console.log(`Job ${jobId} failed permanently:`, error);
    }
  }
  
  res.json({ success: true });
});

// Get job status
app.get('/jobs/:id', (req, res) => {
  const jobId = parseInt(req.params.id);
  const job = jobs.find(j => j.id === jobId);
  
  if (!job) {
    return res.status(404).json({ success: false, message: 'Job not found' });
  }
  
  res.json({ success: true, job });
});

// Store cookies for a user
app.post('/store-cookies', (req, res) => {
  const { email, cookies, timestamp } = req.body;
  
  if (!email || !cookies || !cookies.li_at) {
    return res.status(400).json({ success: false, message: 'Missing email or li_at cookie' });
  }
  
  userCookies[email] = {
    ...cookies,
    timestamp: timestamp || new Date().toISOString()
  };
  
  console.log(`Cookies stored for ${email}`);
  res.json({ success: true, message: 'Cookies stored successfully' });
});

// Simple login endpoint (for demo - use proper auth in production)
app.post('/login', (req, res) => {
  const { email, password } = req.body;
  
  // Simple validation - replace with proper auth
  if (email === 'sahilrohira55@gmail.com' && password === 'S@hil123') {
    const token = 'demo-jwt-token-' + Date.now(); // Replace with proper JWT
    res.json({ success: true, token });
  } else {
    res.status(401).json({ success: false, message: 'Invalid credentials' });
  }
});

// Enqueue connection job with stored cookies
app.post('/jobs/enqueue-send-connection', (req, res) => {
  const { profileUrl, note } = req.body;
  const auth = req.headers.authorization;
  
  if (!auth || !auth.startsWith('Bearer ')) {
    return res.status(401).json({ success: false, message: 'Missing or invalid token' });
  }
  
  // Extract email from token (simplified - use proper JWT parsing in production)
  const email = 'sahilrohira55@gmail.com'; // Hardcoded for demo
  const cookies = userCookies[email];
  
  if (!cookies) {
    return res.status(400).json({ success: false, message: 'No cookies stored for user' });
  }
  
  const job = {
    id: jobIdCounter++,
    type: 'SEND_CONNECTION',
    payload: {
      profileUrl,
      note,
      cookieBundle: cookies
    },
    priority: 1,
    status: 'pending',
    createdAt: new Date().toISOString()
  };
  
  jobs.push(job);
  jobs.sort((a, b) => b.priority - a.priority);
  
  console.log(`Connection job ${job.id} queued for ${profileUrl}`);
  res.json({ success: true, job });
});

app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on port ${port}`);
  console.log(`Worker secret: ${WORKER_SHARED_SECRET}`);
});
