#!/bin/zsh
set -euo pipefail

ROOT="/Users/luke/Documents/polymarket/work/wallet_copy_paper"
RUNTIME="$ROOT/runtime"
PYTHON="/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/python"
LABEL="com.luke.polymarket.wallet-copy-paper"
TARGET="/Users/luke/Library/LaunchAgents/$LABEL.plist"

mkdir -p "$RUNTIME" "/Users/luke/Library/LaunchAgents"
rm -f "$RUNTIME/STOP"
"$PYTHON" "$ROOT/wallet_copy_paper.py" --data-dir "$RUNTIME" init >/dev/null
cp "$ROOT/$LABEL.plist" "$TARGET"
PROXY_URL="${HTTPS_PROXY:-${https_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}"
if [[ -n "$PROXY_URL" ]]; then
  /usr/libexec/PlistBuddy -c "Delete :EnvironmentVariables" "$TARGET" >/dev/null 2>&1 || true
  /usr/libexec/PlistBuddy -c "Add :EnvironmentVariables dict" "$TARGET"
  /usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:HTTPS_PROXY string $PROXY_URL" "$TARGET"
  /usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:HTTP_PROXY string $PROXY_URL" "$TARGET"
  /usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:https_proxy string $PROXY_URL" "$TARGET"
  /usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:http_proxy string $PROXY_URL" "$TARGET"
fi
launchctl bootout "gui/$UID/$LABEL" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$UID" "$TARGET"
launchctl kickstart -k "gui/$UID/$LABEL"
open "$RUNTIME/status.html"
