
const express = require('express');
const app = express();
const port = process.env.PORT || 5000;

app.use(express.json());

const WORKER_SHARED_SECRET = process.env.WORKER_SHARED_SECRET || 'Rohira@0803';

// In-memory job queue (replace with database in production)
let jobs = [];
let jobIdCounter = 1;

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

// Get job status
app.get('/jobs/:id', (req, res) => {
  const jobId = parseInt(req.params.id);
  const job = jobs.find(j => j.id === jobId);
  
  if (!job) {
    return res.status(404).json({ success: false, message: 'Job not found' });
  }
  
  res.json({ success: true, job });
});

app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on port ${port}`);
  console.log(`Worker secret: ${WORKER_SHARED_SECRET}`);
});
