"""Prospective live-copy profile filters and reproducible scale evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping
from urllib.parse import urlencode


ZERO = Decimal("0")
PublicJSONReader = Callable[[str], Any]


class LiveProfileConfigurationError(RuntimeError):
    """Official metadata contradicts the configured live profile."""


class LiveProfileMetadataUnavailable(LiveProfileConfigurationError, ConnectionError):
    """Official metadata is not yet available and the action must be retried."""


@dataclass(frozen=True)
class ScopeDecision:
    follow: bool
    reason: str
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class ScopedScaleSnapshot:
    source_wallet: str
    allocation_usd: Decimal
    observed_at_ms: int
    page_size: int
    raw_position_count: int
    included_position_count: int
    scoped_current_value_usd: Decimal
    fixed_share_scale: Decimal
    raw_positions_hash: str
    snapshot_hash: str
    included_positions: tuple[Mapping[str, str], ...]

    def as_receipt(self) -> dict[str, Any]:
        return {
            "source_wallet": self.source_wallet,
            "allocation_usd": str(self.allocation_usd),
            "observed_at_ms": self.observed_at_ms,
            "page_size": self.page_size,
            "raw_position_count": self.raw_position_count,
            "included_position_count": self.included_position_count,
            "scoped_current_value_usd": str(self.scoped_current_value_usd),
            "fixed_share_scale": str(self.fixed_share_scale),
            "raw_positions_hash": self.raw_positions_hash,
            "snapshot_hash": self.snapshot_hash,
            "included_positions": [dict(row) for row in self.included_positions],
            "scale_formula": "allocation_usd / scoped_current_value_usd",
            "scale_basis": "ATP_WTA_MAINLINE_NONREDEEMABLE_CURRENT_VALUE",
        }


def _text(value: Any) -> str:
    return str(value or "").strip()


def _decimal(value: Any, *, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise LiveProfileConfigurationError(f"INVALID_{field.upper()}") from exc
    if not result.is_finite():
        raise LiveProfileConfigurationError(f"INVALID_{field.upper()}")
    return result


def _canonical_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _token_ids(value: Any) -> tuple[str, ...]:
    raw = value
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise LiveProfileConfigurationError("INVALID_GAMMA_TOKEN_IDS") from exc
    if not isinstance(raw, list) or len(raw) != 2:
        raise LiveProfileConfigurationError("INVALID_GAMMA_TOKEN_IDS")
    tokens = tuple(_text(item) for item in raw)
    if not all(tokens) or tokens[0] == tokens[1]:
        raise LiveProfileConfigurationError("INVALID_GAMMA_TOKEN_IDS")
    return tokens


class OfficialEventMetadataResolver:
    """Resolve one outcome token to its unique official market and event."""

    def __init__(self, public_get_json: PublicJSONReader):
        self.public_get_json = public_get_json

    def _resolve_gamma_market_via_clob_condition(
        self,
        *,
        condition: str,
        mapped_tokens: set[str],
    ) -> Mapping[str, Any]:
        """Recover an exact Gamma event when its condition index is empty.

        The CLOB condition endpoint is used only to discover the immutable
        market slug.  The returned market still has to be present exactly once
        inside the unique Gamma event for that slug, and both official sources
        must agree on the condition and two-token set.
        """

        clob_market = self.public_get_json(
            "https://clob.polymarket.com/markets/" + condition
        )
        if not isinstance(clob_market, dict):
            raise LiveProfileMetadataUnavailable("INVALID_CLOB_CONDITION_MARKET")
        clob_condition = _text(clob_market.get("condition_id")).lower()
        if clob_condition != condition:
            raise LiveProfileConfigurationError("CLOB_CONDITION_MISMATCH")
        raw_clob_tokens = clob_market.get("tokens")
        if not isinstance(raw_clob_tokens, list) or len(raw_clob_tokens) != 2:
            raise LiveProfileConfigurationError("INVALID_CLOB_CONDITION_TOKENS")
        clob_tokens = {
            _text(row.get("token_id"))
            for row in raw_clob_tokens
            if isinstance(row, dict)
        }
        if clob_tokens != mapped_tokens:
            raise LiveProfileConfigurationError(
                "CLOB_CONDITION_TOKEN_SET_MISMATCH"
            )
        market_slug = _text(clob_market.get("market_slug")).lower()
        if not market_slug:
            raise LiveProfileConfigurationError("MISSING_CLOB_MARKET_SLUG")
        minimum_order_size = _decimal(
            clob_market.get("minimum_order_size"),
            field="CLOB_MINIMUM_ORDER_SIZE",
        )
        if minimum_order_size <= ZERO:
            raise LiveProfileConfigurationError(
                "NONPOSITIVE_CLOB_MINIMUM_ORDER_SIZE"
            )

        event_url = "https://gamma-api.polymarket.com/events?" + urlencode(
            {"slug": market_slug}
        )
        events = self.public_get_json(event_url)
        if not isinstance(events, list) or len(events) == 0:
            raise LiveProfileMetadataUnavailable("MISSING_GAMMA_EVENT_BY_CLOB_SLUG")
        if len(events) != 1 or not isinstance(events[0], dict):
            raise LiveProfileConfigurationError("AMBIGUOUS_GAMMA_EVENT_BY_CLOB_SLUG")
        event = events[0]
        event_slug = _text(event.get("slug")).lower()
        if event_slug != market_slug:
            raise LiveProfileConfigurationError("CLOB_GAMMA_EVENT_SLUG_MISMATCH")
        event_markets = event.get("markets")
        if not isinstance(event_markets, list):
            raise LiveProfileMetadataUnavailable("INVALID_GAMMA_EVENT_MARKETS")
        exact_markets = [
            row
            for row in event_markets
            if isinstance(row, dict)
            and _text(row.get("conditionId")).lower() == condition
        ]
        if len(exact_markets) != 1:
            raise LiveProfileConfigurationError(
                "GAMMA_EVENT_CONDITION_MARKET_NOT_UNIQUE"
            )
        recovered = dict(exact_markets[0])
        if _text(recovered.get("slug")).lower() != market_slug:
            raise LiveProfileConfigurationError("CLOB_GAMMA_MARKET_SLUG_MISMATCH")
        recovered["events"] = [
            {
                "slug": event_slug,
                "title": _text(event.get("title")),
            }
        ]
        recovered["_clob_minimum_order_size"] = str(minimum_order_size)
        return recovered

    def resolve(self, token_id: str) -> Mapping[str, Any]:
        token = _text(token_id)
        if not token:
            raise LiveProfileConfigurationError("MISSING_ACTION_TOKEN_ID")
        mapping = self.public_get_json(
            "https://clob.polymarket.com/markets-by-token/" + token
        )
        if not isinstance(mapping, dict):
            raise LiveProfileMetadataUnavailable("INVALID_CLOB_TOKEN_MAPPING")
        condition = _text(mapping.get("condition_id")).lower()
        primary_token_id = _text(mapping.get("primary_token_id"))
        secondary_token_id = _text(mapping.get("secondary_token_id"))
        mapped_tokens = {primary_token_id, secondary_token_id}
        if not condition or token not in mapped_tokens:
            raise LiveProfileConfigurationError("ACTION_TOKEN_NOT_IN_CLOB_MAPPING")

        gamma_url = "https://gamma-api.polymarket.com/markets?" + urlencode(
            {"condition_ids": condition}
        )
        markets = self.public_get_json(gamma_url)
        if not isinstance(markets, list):
            raise LiveProfileMetadataUnavailable("INVALID_GAMMA_MARKET_RESPONSE")
        if len(markets) == 0:
            market = self._resolve_gamma_market_via_clob_condition(
                condition=condition,
                mapped_tokens=mapped_tokens,
            )
            metadata_resolution_path = (
                "CLOB_CONDITION_TO_GAMMA_EVENT_FALLBACK"
            )
        else:
            if len(markets) != 1 or not isinstance(markets[0], dict):
                raise LiveProfileConfigurationError("AMBIGUOUS_GAMMA_MARKET")
            market = markets[0]
            metadata_resolution_path = "GAMMA_CONDITION_EXACT"
        gamma_condition = _text(market.get("conditionId")).lower()
        if gamma_condition != condition:
            raise LiveProfileConfigurationError("GAMMA_CONDITION_MISMATCH")
        gamma_tokens = _token_ids(market.get("clobTokenIds"))
        if set(gamma_tokens) != mapped_tokens:
            raise LiveProfileConfigurationError("CLOB_GAMMA_TOKEN_SET_MISMATCH")

        events = market.get("events")
        if not isinstance(events, list) or len(events) != 1 or not isinstance(events[0], dict):
            raise LiveProfileConfigurationError("AMBIGUOUS_GAMMA_EVENT")
        market_slug = _text(market.get("slug")).lower()
        event_slug = _text(events[0].get("slug")).lower()
        if not market_slug:
            raise LiveProfileConfigurationError("MISSING_GAMMA_MARKET_SLUG")
        if not event_slug:
            raise LiveProfileConfigurationError("MISSING_GAMMA_EVENT_SLUG")
        question = _text(market.get("question") or market.get("title"))
        event_title = _text(events[0].get("title"))
        topic_text = " ".join(
            (question, event_title, market_slug, event_slug)
        ).casefold()
        return {
            "condition_id": condition,
            "primary_token_id": primary_token_id,
            "secondary_token_id": secondary_token_id,
            "market_id": _text(market.get("id")),
            "market_slug": market_slug,
            "event_slug": event_slug,
            "question": question,
            "event_title": event_title,
            "topic_classification": (
                "NETFLIX" if "netflix" in topic_text else "NON_NETFLIX"
            ),
            "active": market.get("active"),
            "closed": market.get("closed"),
            "accepting_orders": market.get("acceptingOrders"),
            "enable_order_book": market.get("enableOrderBook"),
            "game_start_time": market.get("gameStartTime"),
            "clob_token_ids": list(gamma_tokens),
            "metadata_resolution_path": metadata_resolution_path,
            "minimum_order_size": _text(
                market.get("orderMinSize")
                or market.get("_clob_minimum_order_size")
            ),
        }

    def resolve_frozen_market_lifecycle(
        self,
        *,
        token_id: str,
        frozen_metadata: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Refresh lifecycle through the immutable event and market identity."""

        token = _text(token_id)
        condition = _text(frozen_metadata.get("condition_id")).lower()
        event_slug = _text(frozen_metadata.get("event_slug")).lower()
        market_slug = _text(frozen_metadata.get("market_slug")).lower()
        primary_token_id = _text(frozen_metadata.get("primary_token_id"))
        secondary_token_id = _text(frozen_metadata.get("secondary_token_id"))
        frozen_tokens = {primary_token_id, secondary_token_id}
        if not token or not condition or not event_slug or not market_slug:
            raise LiveProfileConfigurationError(
                "INCOMPLETE_FROZEN_RETRY_MARKET_IDENTITY"
            )
        if (
            not primary_token_id
            or not secondary_token_id
            or primary_token_id == secondary_token_id
            or token not in frozen_tokens
        ):
            raise LiveProfileConfigurationError(
                "INVALID_FROZEN_RETRY_TOKEN_PAIR"
            )
        recorded_tokens = frozen_metadata.get("clob_token_ids")
        if (
            recorded_tokens is not None
            and set(_token_ids(recorded_tokens)) != frozen_tokens
        ):
            raise LiveProfileConfigurationError(
                "FROZEN_RETRY_TOKEN_SET_MISMATCH"
            )

        event_url = "https://gamma-api.polymarket.com/events?" + urlencode(
            {"slug": event_slug}
        )
        events = self.public_get_json(event_url)
        if not isinstance(events, list) or len(events) == 0:
            raise LiveProfileMetadataUnavailable(
                "MISSING_CURRENT_GAMMA_EVENT_BY_FROZEN_SLUG"
            )
        if len(events) != 1 or not isinstance(events[0], dict):
            raise LiveProfileConfigurationError(
                "AMBIGUOUS_CURRENT_GAMMA_EVENT_BY_FROZEN_SLUG"
            )
        event = events[0]
        if _text(event.get("slug")).lower() != event_slug:
            raise LiveProfileConfigurationError(
                "CURRENT_GAMMA_EVENT_SLUG_MISMATCH"
            )
        event_markets = event.get("markets")
        if not isinstance(event_markets, list):
            raise LiveProfileMetadataUnavailable(
                "INVALID_CURRENT_GAMMA_EVENT_MARKETS"
            )
        exact_markets = [
            row
            for row in event_markets
            if isinstance(row, dict)
            and _text(row.get("conditionId")).lower() == condition
        ]
        if len(exact_markets) != 1:
            raise LiveProfileConfigurationError(
                "CURRENT_GAMMA_EVENT_CONDITION_MARKET_NOT_UNIQUE"
            )
        market = exact_markets[0]
        if _text(market.get("slug")).lower() != market_slug:
            raise LiveProfileConfigurationError(
                "CURRENT_GAMMA_MARKET_SLUG_MISMATCH"
            )
        if set(_token_ids(market.get("clobTokenIds"))) != frozen_tokens:
            raise LiveProfileConfigurationError(
                "CURRENT_GAMMA_TOKEN_SET_MISMATCH"
            )
        return {
            **dict(frozen_metadata),
            "active": market.get("active"),
            "closed": market.get("closed"),
            "accepting_orders": market.get("acceptingOrders"),
            "enable_order_book": market.get("enableOrderBook"),
            "metadata_resolution_path": (
                "FROZEN_EVENT_SLUG_TO_CURRENT_GAMMA_MARKET"
            ),
        }


class FullWalletEventScope:
    """Follow every source action while freezing its official event identity."""

    def __init__(self, public_get_json: PublicJSONReader):
        self.resolver = OfficialEventMetadataResolver(public_get_json)

    def resolve(self, token_id: str) -> ScopeDecision:
        evidence = self.resolver.resolve(token_id)
        return ScopeDecision(True, "FULL_WALLET_ACTION_ELIGIBLE", evidence)

    def resolve_action(self, action: Any) -> ScopeDecision:
        return self.resolve(getattr(action, "token_id", ""))

    def resolve_retry_lifecycle(
        self,
        action: Any,
        frozen_metadata: Mapping[str, Any],
    ) -> ScopeDecision:
        evidence = self.resolver.resolve_frozen_market_lifecycle(
            token_id=getattr(action, "token_id", ""),
            frozen_metadata=frozen_metadata,
        )
        return ScopeDecision(True, "OFFICIAL_RETRY_MARKET_LIFECYCLE", evidence)


class NetflixEventScope:
    """Observe the full source wallet but execute only official Netflix events."""

    def __init__(self, public_get_json: PublicJSONReader):
        self.resolver = OfficialEventMetadataResolver(public_get_json)

    def resolve(self, token_id: str) -> ScopeDecision:
        evidence = self.resolver.resolve(token_id)
        if evidence.get("topic_classification") == "NETFLIX":
            return ScopeDecision(True, "NETFLIX_ACTION_ELIGIBLE", evidence)
        return ScopeDecision(False, "SCOPE_EXCLUDED_NON_NETFLIX", evidence)

    def resolve_action(self, action: Any) -> ScopeDecision:
        return self.resolve(getattr(action, "token_id", ""))

    def resolve_retry_lifecycle(
        self,
        action: Any,
        frozen_metadata: Mapping[str, Any],
    ) -> ScopeDecision:
        evidence = self.resolver.resolve_frozen_market_lifecycle(
            token_id=getattr(action, "token_id", ""),
            frozen_metadata=frozen_metadata,
        )
        return ScopeDecision(True, "OFFICIAL_RETRY_MARKET_LIFECYCLE", evidence)


def _canonical_position(row: Mapping[str, Any]) -> dict[str, str]:
    return {
        "asset": _text(row.get("asset")),
        "condition_id": _text(row.get("conditionId")).lower(),
        "event_slug": _text(row.get("eventSlug")).lower(),
        "slug": _text(row.get("slug")).lower(),
        "redeemable": "true" if bool(row.get("redeemable")) else "false",
        "current_value_usd": str(
            _decimal(row.get("currentValue", "0"), field="current_value")
        ),
    }


def _included_mainline_position(row: Mapping[str, Any]) -> bool:
    event_slug = _text(row.get("eventSlug")).lower()
    market_slug = _text(row.get("slug")).lower()
    current_value = _decimal(row.get("currentValue", "0"), field="current_value")
    return (
        event_slug.startswith(("atp-", "wta-"))
        and market_slug == event_slug
        and not bool(row.get("redeemable"))
        and current_value > ZERO
    )


def build_scoped_scale_snapshot(
    *,
    source_wallet: str,
    allocation_usd: Decimal,
    public_get_json: PublicJSONReader,
    page_size: int,
    observed_at_ms: int,
) -> ScopedScaleSnapshot:
    """Fully paginate the public sleeve and freeze the scale input receipt."""

    wallet = _text(source_wallet).lower()
    if not wallet.startswith("0x") or len(wallet) != 42:
        raise LiveProfileConfigurationError("INVALID_SOURCE_WALLET")
    try:
        int(wallet[2:], 16)
    except ValueError as exc:
        raise LiveProfileConfigurationError("INVALID_SOURCE_WALLET") from exc
    allocation = _decimal(allocation_usd, field="allocation_usd")
    if allocation <= ZERO:
        raise LiveProfileConfigurationError("NONPOSITIVE_ALLOCATION_USD")
    if not isinstance(page_size, int) or page_size <= 0:
        raise LiveProfileConfigurationError("INVALID_PAGE_SIZE")
    if not isinstance(observed_at_ms, int) or observed_at_ms <= 0:
        raise LiveProfileConfigurationError("INVALID_OBSERVED_AT_MS")

    raw_rows: list[Mapping[str, Any]] = []
    offset = 0
    while True:
        url = "https://data-api.polymarket.com/positions?" + urlencode(
            {"user": wallet, "limit": page_size, "offset": offset}
        )
        page = public_get_json(url)
        if not isinstance(page, list) or any(not isinstance(row, dict) for row in page):
            raise LiveProfileMetadataUnavailable("INVALID_SOURCE_POSITIONS_PAGE")
        raw_rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    canonical_raw = sorted(
        (_canonical_position(row) for row in raw_rows),
        key=lambda row: (row["asset"], row["condition_id"]),
    )
    included = tuple(
        sorted(
            (_canonical_position(row) for row in raw_rows if _included_mainline_position(row)),
            key=lambda row: (row["asset"], row["condition_id"]),
        )
    )
    scoped_value = sum(
        (Decimal(row["current_value_usd"]) for row in included), ZERO
    )
    if scoped_value <= ZERO:
        raise LiveProfileConfigurationError("NONPOSITIVE_SCOPED_SOURCE_VALUE")
    scale = allocation / scoped_value
    raw_hash = _canonical_hash(canonical_raw)
    receipt_payload = {
        "source_wallet": wallet,
        "allocation_usd": str(allocation),
        "observed_at_ms": observed_at_ms,
        "page_size": page_size,
        "raw_position_count": len(raw_rows),
        "included_positions": list(included),
        "scoped_current_value_usd": str(scoped_value),
        "fixed_share_scale": str(scale),
        "raw_positions_hash": raw_hash,
        "scale_formula": "allocation_usd / scoped_current_value_usd",
    }
    return ScopedScaleSnapshot(
        source_wallet=wallet,
        allocation_usd=allocation,
        observed_at_ms=observed_at_ms,
        page_size=page_size,
        raw_position_count=len(raw_rows),
        included_position_count=len(included),
        scoped_current_value_usd=scoped_value,
        fixed_share_scale=scale,
        raw_positions_hash=raw_hash,
        snapshot_hash=_canonical_hash(receipt_payload),
        included_positions=included,
    )
