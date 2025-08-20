# Use Playwright’s image with all browser deps preinstalled
FROM mcr.microsoft.com/playwright:v1.46.1-jammy

WORKDIR /app

# Install only production deps
COPY package*.json ./
RUN npm ci --omit=dev

# Install the Chromium browser (cached by base image; still safe to run)
RUN npx playwright install chromium

# Copy worker code
COPY . .

# Env hints (optional)
ENV NODE_ENV=production \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Start worker
CMD ["node", "worker.js"]
