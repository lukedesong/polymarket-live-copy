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
launchctl bootout "gui/$UID/$LABEL" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$UID" "$TARGET"
launchctl kickstart -k "gui/$UID/$LABEL"
open "$RUNTIME/status.html"
