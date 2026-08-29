"""Zockdo non-tennis copy notional cap.

Tennis keeps the frozen 0.5 share scale. Other sports still follow, but our
BUY notional is clipped to tennis's own event-size p90 times that scale.
The p90 is from official Data API trades, not a round number.
"""

from __future__ import annotations

from decimal import Decimal

ZERO = Decimal("0")
ZOCKDO_PROFILE_KEY = "zockdo_full_wallet"

# Official /trades for 0xcd741947… fetched 2026-08-29T10:42:03Z.
# 966 tennis events, event-level fill notional p90.
TENNIS_EVENT_P90_SOURCE_NOTIONAL_USD = Decimal("543.30")
TENNIS_EVENT_SLUG_PREFIXES = ("atp-", "wta-", "itf-")


def is_tennis_event_slug(event_slug: str) -> bool:
    slug = str(event_slug or "").strip().lower()
    return slug.startswith(TENNIS_EVENT_SLUG_PREFIXES)


def nontennis_max_copy_notional_usd(scale: Decimal) -> Decimal:
    ratio = Decimal(str(scale))
    if ratio <= ZERO:
        raise ValueError("scale must be positive")
    return TENNIS_EVENT_P90_SOURCE_NOTIONAL_USD * ratio


def max_buy_notional_usd_for_profile(
    *,
    profile_key: str | None,
    event_slug: str,
    scale: Decimal,
) -> Decimal | None:
    """Return a BUY notional cap, or None when the action is uncapped.

    Missing slug is treated as non-tennis so a huge other-sport fill cannot
    pass because metadata was empty. Tennis slugs are prefix-matched so MLS
    Cincinnati is not classified as tennis.
    """

    if str(profile_key or "") != ZOCKDO_PROFILE_KEY:
        return None
    if is_tennis_event_slug(event_slug):
        return None
    return nontennis_max_copy_notional_usd(scale)
