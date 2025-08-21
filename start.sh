#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-8080}"
DISPLAY=":99"
SCREEN="${SCREEN_SIZE:-1366x768x24}"
NOVNC_PASSWORD="${NOVNC_PASSWORD:-changeme123}"

echo "[entrypoint] Starting Xvfb $DISPLAY ($SCREEN)"
Xvfb "$DISPLAY" -screen 0 "$SCREEN" -ac +extension GLX +render -noreset > /tmp/xvfb.log 2>&1 &

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
  /usr/share/novnc/utils/novnc_proxy --vnc localhost:5900 --listen "$PORT" > /tmp/novnc.log 2>&1 &
else
  websockify --web=/usr/share/novnc "$PORT" localhost:5900 > /tmp/novnc.log 2>&1 &
fi

echo "[entrypoint] noVNC URL: https://<your-app>/?password=$NOVNC_PASSWORD"
export DISPLAY="$DISPLAY"
exec node worker.cjs
