# ---- base image
FROM node:22-slim

WORKDIR /app

# Install system libs required by Playwright/Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libexpat1 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libu2f-udev \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxshmfence1 \
    libxss1 \
    libxtst6 \
    wget \
    xvfb \
 && rm -rf /var/lib/apt/lists/*

# Copy only manifests first (better layer caching)
COPY package*.json ./

# Install production dependencies
RUN npm install --omit=dev

# Install Chromium browser (already present in this base image, but safe to run)
RUN npx playwright install chromium --with-deps

# Now copy the rest of the app, including worker.cjs
COPY . .

# Environment defaults (override in Railway)
ENV API_BASE=https://calm-rejoicing-linqbridge.up.railway.app
ENV WORKER_SHARED_SECRET=S@hil123
ENV HEADLESS=false
ENV SOFT_MODE=false
ENV POLL_INTERVAL_MS=5000

CMD ["npm", "start"]
