#!/bin/zsh
set -eu

LABEL="com.luke.polymarket.tian-wen-chain-shadow"
DOMAIN="gui/$(id -u)"
RUNTIME_DIR="/Users/luke/Documents/polymarket/work/wallet_copy_paper/tian_wen_chain_shadow_runtime"
DATABASE="$RUNTIME_DIR/shadow.sqlite3"

if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl print "$DOMAIN/$LABEL"
else
    echo "launchd: not loaded"
fi

if [[ -f "$DATABASE" ]]; then
    echo "sqlite_integrity_check:"
    # Estimate: a status read may wait up to ten seconds for the writer's
    # short transaction; this does not change observer or trading behavior.
    sqlite3 -cmd ".timeout 10000" "$DATABASE" "PRAGMA integrity_check;"
fi

if [[ -f "$RUNTIME_DIR/status.json" ]]; then
    echo "status_json:"
    /bin/cat "$RUNTIME_DIR/status.json"
fi
