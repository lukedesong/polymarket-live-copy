#!/bin/zsh
set -eu

LABEL="com.luke.polymarket.tian-wen-chain-shadow"
SOURCE_PLIST="/Users/luke/Documents/polymarket/work/wallet_copy_paper/com.luke.polymarket.tian-wen-chain-shadow.plist"
TARGET_PLIST="/Users/luke/Library/LaunchAgents/com.luke.polymarket.tian-wen-chain-shadow.plist"
RUNTIME_DIR="/Users/luke/Documents/polymarket/work/wallet_copy_paper/tian_wen_chain_shadow_runtime"
DOMAIN="gui/$(id -u)"
PYTHON="/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/python"
TRACKER="/Users/luke/Documents/polymarket/work/wallet_copy_paper/tian_wen_chain_shadow.py"

mkdir -p "$RUNTIME_DIR" "/Users/luke/Library/LaunchAgents"
cp "$SOURCE_PLIST" "$TARGET_PLIST"

if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl bootout "$DOMAIN/$LABEL"
fi

# Formula-derived shutdown allowance: thirty half-second checks cover fifteen
# seconds, longer than the daemon's estimated ten-second public HTTP timeout.
for attempt in {1..30}; do
    if ! pgrep -f "^${PYTHON} ${TRACKER}$" >/dev/null 2>&1; then
        break
    fi
    if [[ "$attempt" -eq 30 ]]; then
        echo "old chain-shadow process did not exit; service remains stopped" >&2
        exit 1
    fi
    sleep 0.5
done

launchctl bootstrap "$DOMAIN" "$TARGET_PLIST"
