#!/bin/zsh
set -euo pipefail

ROOT="/Users/luke/Documents/polymarket/work/wallet_copy_paper"
PYTHON="/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/python"
LABEL="com.luke.polymarket.wallet-copy-paper"

launchctl print "gui/$UID/$LABEL" 2>/dev/null | sed -n '1,35p' || true
"$PYTHON" "$ROOT/wallet_copy_paper.py" --data-dir "$ROOT/runtime" status
