FROM node:20-slim

# Playwright deps + Xvfb
RUN apt-get update && apt-get install -y \
    xvfb libnss3 libatk-bridge2.0-0 libgtk-3-0 libdrm2 libgbm1 libasound2 \
    libxdamage1 libxfixes3 libxcomposite1 libxrandr2 libxkbcommon0 libpango-1.0-0 \
    libpangocairo-1.0-0 libcairo2 fonts-liberation && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests first to leverage Docker layer cache
COPY package*.json ./

# If lockfile exists use ci, otherwise install
RUN if [ -f package-lock.json ]; then npm ci --omit=dev; else npm install --omit=dev; fi

# Install browsers + any missing system deps (safe to run even after apt)
RUN npx playwright install --with-deps chromium

# Copy the rest of your app (including worker.cjs)
COPY . .

ENV NODE_ENV=production
ENV SOFT_MODE=false
ENV HEADLESS=false
# optional: slow down actions a bit so you can see them in headed mode
ENV SLOWMO_MS=50

# Run headed under a virtual display
CMD ["xvfb-run", "-a", "node", "worker.cjs"]
