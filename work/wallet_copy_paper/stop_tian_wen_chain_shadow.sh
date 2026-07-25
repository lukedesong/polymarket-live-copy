#!/bin/zsh
set -eu

LABEL="com.luke.polymarket.tian-wen-chain-shadow"
DOMAIN="gui/$(id -u)"

if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl bootout "$DOMAIN/$LABEL"
fi
