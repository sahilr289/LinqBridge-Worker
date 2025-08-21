#!/usr/bin/env bash
set -euo pipefail

# ---- Settings ----
PORT="${PORT:-8080}"                 # Platform will usually set this (Railway/Render/etc.)
DISPLAY=":99"
SCREEN="${SCREEN_SIZE:-1366x768x24}" # You can override via env
NOVNC_PASSWORD="${NOVNC_PASSWORD:-changeme123}"

echo "[entrypoint] Starting virtual display on $DISPLAY ($SCREEN)"
Xvfb "$DISPLAY" -screen 0 "$SCREEN" -ac +extension GLX +render -noreset > /tmp/xvfb.log 2>&1 &

if [ "$NOVNC_PASSWORD" = "changeme123" ]; then
  echo "[entrypoint] WARNING: Using default NOVNC_PASSWORD. Set a strong password via env."
fi

echo "[entrypoint] Starting x11vnc server on :5900"
x11vnc -display "$DISPLAY" -passwd "$NOVNC_PASSWORD" -forever -shared -rfbport 5900 -noxdamage > /tmp/x11vnc.log 2>&1 &

# Debian/Ubuntu package path for noVNC static files:
NOVNC_WEB_ROOT="/usr/share/novnc"
if [ ! -d "$NOVNC_WEB_ROOT" ]; then
  echo "[entrypoint] ERROR: noVNC web root not found at $NOVNC_WEB_ROOT"
  exit 1
fi

echo "[entrypoint] Starting noVNC (websockify) on :$PORT"
websockify --web="$NOVNC_WEB_ROOT" "$PORT" localhost:5900 > /tmp/novnc.log 2>&1 &

echo "[entrypoint] noVNC URL: http://<your-app-host>:$PORT/?password=$NOVNC_PASSWORD"
echo "[entrypoint] ENV: HEADLESS=${HEADLESS:-} SOFT_MODE=${SOFT_MODE:-} SLOWMO_MS=${SLOWMO_MS:-}"

export DISPLAY="$DISPLAY"
exec node worker.cjs
