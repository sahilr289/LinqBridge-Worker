FROM node:20-slim

# Playwright deps + X stack (Xvfb + VNC/noVNC for live viewing)
RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb x11vnc novnc websockify \
    libnss3 libatk-bridge2.0-0 libgtk-3-0 libdrm2 libgbm1 libasound2 \
    libxdamage1 libxfixes3 libxcomposite1 libxrandr2 libxkbcommon0 \
    libpango-1.0-0 libpangocairo-1.0-0 libcairo2 \
    libxss1 libxshmfence1 libx11-xcb1 libglib2.0-0 ca-certificates \
    fonts-liberation fonts-noto-color-emoji \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests first to leverage cache
COPY package*.json ./

# Deterministic if lockfile exists; else fallback
RUN if [ -f package-lock.json ]; then npm ci --omit=dev; else npm install --omit=dev; fi

# Install Playwright browsers + any missing system deps
RUN npx playwright install --with-deps chromium

# Copy the rest (worker + start.sh, etc.)
COPY . .
RUN chmod +x /app/start.sh

ENV NODE_ENV=production
# set true to dry-run without launching the browser
ENV SOFT_MODE=false
# headed so you can watch in noVNC
ENV HEADLESS=false
# slow down actions a bit so they are visible
ENV SLOWMO_MS=50
# default VNC password (override in Railway)
ENV NOVNC_PASSWORD=changeme123
# Railway will inject PORT; keeping a default helps local runs
ENV PORT=8080

# Optional but nice for docs/local
EXPOSE 8080

# start.sh will boot Xvfb + x11vnc + noVNC and then run node
CMD ["/app/start.sh"]
