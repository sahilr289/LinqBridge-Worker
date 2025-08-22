#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-8080}"
DISPLAY=":99"
SCREEN="${SCREEN_SIZE:-1366x768x24}"
NOVNC_PASSWORD="${NOVNC_PASSWORD:-changeme123}"

echo "[entrypoint] Starting Xvfb $DISPLAY ($SCREEN)"
Xvfb "$DISPLAY" -screen 0 "$SCREEN" -ac +extension GLX +render -noreset > /tmp/xvfb.log 2>&1 &

# Export DISPLAY early so any child reader sees it
export DISPLAY="$DISPLAY"

# Wait for Xvfb socket to be ready (no extra packages needed)
echo "[entrypoint] Waiting for X socket..."
for i in $(seq 1 50); do
  if [ -S "/tmp/.X11-unix/X${DISPLAY#:}" ]; then
    echo "[entrypoint] Xvfb is ready."
    break
  fi
  sleep 0.1
done

echo "[entrypoint] Starting x11vnc on :5900"
# Extra flags here fix common “can’t type/click” cases
x11vnc -display "$DISPLAY" \
  -passwd "$NOVNC_PASSWORD" \
  -forever -shared -rfbport 5900 \
  -noxdamage -xkb -repeat -nomodtweak -skip_lockkeys \
  > /tmp/x11vnc.log 2>&1 &

echo "[entrypoint] Starting noVNC on :$PORT"
# Prefer the helper script; falls back to raw websockify if missing
if [ -x /usr/share/novnc/utils/novnc_proxy ]; then
  /usr/share/novnc/utils/novnc_proxy --vnc localhost:5900 --listen "0.0.0.0:$PORT" > /tmp/novnc.log 2>&1 &
else
  websockify --web=/usr/share/novnc "0.0.0.0:$PORT" localhost:5900 > /tmp/novnc.log 2>&1 &
fi

# Handy connection hint (works with stock noVNC)
echo "[entrypoint] noVNC URL:  https://<your-railway-domain>/vnc_auto.html?password=$NOVNC_PASSWORD"
echo "[entrypoint] Logs: tail -f /tmp/xvfb.log /tmp/x11vnc.log /tmp/novnc.log"

# Finally: start the worker (headed Playwright). Make sure HEADLESS=false in env.
exec node worker.cjs
