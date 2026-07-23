#!/bin/zsh
set -euo pipefail

ROOT="/Users/luke/Documents/polymarket/work/wallet_copy_paper"
LABEL="com.luke.polymarket.wallet-copy-paper"

mkdir -p "$ROOT/runtime"
touch "$ROOT/runtime/STOP"
launchctl bootout "gui/$UID/$LABEL" >/dev/null 2>&1 || true
