#!/usr/bin/env bash
set -euo pipefail

# Use the platform's port for the noVNC web UI (Railway/Render/Heroku set $PORT)
PORT="${PORT:-8080}"
DISPLAY=":99"
SCREEN="1280x800x24"

echo "[entrypoint] Starting virtual display on $DISPLAY ($SCREEN)"
Xvfb "$DISPLAY" -screen 0 "$SCREEN" -ac +extension GLX +render -noreset >/tmp/xvfb.log 2>&1 &

# Password for VNC/web viewer (required on public PaaS)
NOVNC_PASSWORD="${NOVNC_PASSWORD:-changeme123}"
if [ "$NOVNC_PASSWORD" = "changeme123" ]; then
  echo "[entrypoint] WARNING: Using default NOVNC_PASSWORD. Set NOVNC_PASSWORD env for security."
fi

echo "[entrypoint] Starting x11vnc (VNC server)"
x11vnc -display "$DISPLAY" -nopw -passwd "$NOVNC_PASSWORD" -forever -shared -rfbport 5900 >/tmp/x11vnc.log 2>&1 &

echo "[entrypoint] Starting noVNC on :$PORT (web viewer)"
# websockify serves the noVNC static client and proxies to localhost:5900
websockify --web=/usr/share/novnc "$PORT" localhost:5900 >/tmp/novnc.log 2>&1 &

echo "[entrypoint] noVNC URL will be: http://<your-app-host>:$PORT/?password=$NOVNC_PASSWORD"
echo "[entrypoint] ENV: HEADLESS=${HEADLESS:-} SOFT_MODE=${SOFT_MODE:-} SLOWMO_MS=${SLOWMO_MS:-}"

# Launch the Node worker on the same X display
export DISPLAY="$DISPLAY"
exec node worker.cjs
