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

# Copy manifests first for caching
COPY package*.json ./

# Install prod deps: prefer npm ci if lockfile present, else npm install
RUN if [ -f package-lock.json ]; then \
      npm ci --omit=dev; \
    else \
      npm install --omit=dev; \
    fi

# IMPORTANT: set the browsers path BEFORE installing browsers,
# so install and runtime paths match.
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV NODE_ENV=production

# Install Chromium browser binaries into /ms-playwright
RUN npx playwright install chromium --with-deps

# Copy the rest of the worker code
COPY . .

# Start the worker
CMD ["node", "worker.js"]
