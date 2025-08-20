FROM node:20-slim

# Playwright deps + Xvfb
RUN apt-get update && apt-get install -y \
    xvfb libnss3 libatk-bridge2.0-0 libgtk-3-0 libdrm2 libgbm1 libasound2 \
    libxdamage1 libxfixes3 libxcomposite1 libxrandr2 libxkbcommon0 libpango-1.0-0 \
    libpangocairo-1.0-0 libcairo2 fonts-liberation && rm -rf /var/lib/apt/lists/*

# Install deps
WORKDIR /app
COPY package*.json ./
RUN npm ci

# Install browsers
RUN npx playwright install --with-deps chromium

COPY . .

ENV NODE_ENV=production
ENV HEADLESS=false

CMD ["xvfb-run", "-a", "node", "worker.cjs"]
