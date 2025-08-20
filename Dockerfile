FROM node:20-slim

# System deps for Playwright + X stack
RUN apt-get update && apt-get install -y \
    xvfb x11vnc novnc websockify \
    libnss3 libatk-bridge2.0-0 libgtk-3-0 libdrm2 libgbm1 libasound2 \
    libxdamage1 libxfixes3 libxcomposite1 libxrandr2 libxkbcommon0 libpango-1.0-0 \
    libpangocairo-1.0-0 libcairo2 fonts-liberation \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests first for better caching
COPY package*.json ./

# Install node deps (deterministic if lockfile present)
RUN if [ -f package-lock.json ]; then npm ci --omit=dev; else npm install --omit=dev; fi

# Install Playwright browsers + any missing system deps
RUN npx playwright install --with-deps chromium

# Copy app code and entrypoint
COPY . .
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

ENV NODE_ENV=production
ENV SOFT_MODE=false          # set true to dry-run without browser
ENV HEADLESS=false           # headed so you can watch
ENV SLOWMO_MS=50             # slow down actions a bit so they are visible

# On most PaaS, the platform sets $PORT; start.sh binds noVNC on $PORT
CMD ["/app/start.sh"]
