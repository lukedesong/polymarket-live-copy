"""Isolated CD90 cash-copy core.

This module deliberately does not import or alter the shared paper executor.
It records an exact source-action receipt before attempting a real CLOB order,
uses one immutable share-scale for the live sleeve, and never reposts an
action whose submission outcome is uncertain.
"""

from __future__ import annotations

import asyncio
import argparse
from concurrent.futures import Future, ThreadPoolExecutor
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import stat
import sys
import time
from html import escape
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from decimal import (
    Decimal,
    InvalidOperation,
    ROUND_CEILING,
    ROUND_FLOOR,
    ROUND_HALF_UP,
)
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Iterator, Mapping
from urllib.parse import urlencode

from cd90_live_sizing import (
    ActionPlan,
    ScaleInputError,
    derive_fixed_share_scale,
    maximum_buy_fee_usd,
    plan_action,
)
from zockdo_nontennis_cap import max_buy_notional_usd_for_profile
from live_action_fidelity import canonical_hash
from live_wallet_coordinator import (
    AuthenticatedAccountCashSnapshot,
    SharedWalletCoordinator,
    SharedWalletCoordinatorError,
)
from live_chain_client import (
    ORDER_FILLED_TOPIC,
    PublicPolymarketClient,
    RpcClient,
    decode_order_filled,
)


# The production CD90 daemon executes this file as ``__main__``.  Register the
# already-loaded module under its import name so the lazily loaded recovery
# module cannot create a second copy of the live core and a second exception
# type hierarchy in the same cash process.
if __name__ == "__main__":
    sys.modules.setdefault("cd90_live_copy", sys.modules[__name__])


ZERO = Decimal("0")
TOKEN_SCALE = Decimal("1000000")
TOKEN_RAW_UNIT = Decimal("1") / TOKEN_SCALE
MODULE_DIR = Path(__file__).resolve().parent
DEFAULT_RUNTIME_DIR = MODULE_DIR / "cd90_live_runtime"
# Empirical Hong Kong-server validation on 2026-08-04: this public endpoint
# acknowledged ``eth_subscribe(newHeads)`` and delivered a Polygon head. It
# carries no account credentials. A cash copier uses this event stream rather
# than a guessed seconds-based polling cadence.
DEFAULT_WS_RPC_URL = "wss://polygon-bor-rpc.publicnode.com"
ALLOWED_WS_RPC_URLS = frozenset(
    {DEFAULT_WS_RPC_URL, "wss://polygon.drpc.org"}
)
LIVE_REQUIRED_ENV = (
    "POLYMARKET_PRIVATE_KEY",
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_API_PASSPHRASE",
    "POLYMARKET_SIGNATURE_TYPE",
    "POLYMARKET_FUNDER_ADDRESS",
    "CD90_SOURCE_WALLET",
    "CD90_ALLOCATION_USD",
    "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD",
    "POLYMARKET_SHARED_WALLET_LOCK_PATH",
    "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH",
)
AUTO_REDEMPTION_ENV = "CD90_AUTO_REDEEM_ENABLED"
IMMEDIATE_ORDER_TYPE = "GTC_ACTIVE_CANCEL"
BUY_ACTIVE_CANCEL_WAIT_SECONDS = 60


def _uses_active_cancel_limit(
    *, side: str, prepare_gtd: Any, submit_prepared_gtd: Any
) -> bool:
    """Only BUY may rest; SELL must use immediate fill-and-cancel."""

    return (
        str(side).upper() == "BUY"
        and callable(prepare_gtd)
        and callable(submit_prepared_gtd)
    )


MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM = "UPSCALE_TO_CURRENT_MARKET_MINIMUM"
MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM = "SKIP_BELOW_CURRENT_MARKET_MINIMUM"
MINIMUM_SIZE_POLICIES = frozenset(
    {
        MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM,
        MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    }
)
SOURCE_ROLE_CHAIN_MAKER = "maker"
SOURCE_ROLE_VERIFIED_PUBLIC_WALLET = "verified_public_wallet"
FOLLOWABLE_SOURCE_ROLES = frozenset(
    {
        SOURCE_ROLE_CHAIN_MAKER,
        SOURCE_ROLE_VERIFIED_PUBLIC_WALLET,
    }
)
SOURCE_ACTION_DETECTION_CONTRACT_CHAIN_MAKER_ONLY = (
    "ORDER_FILLED_MAKER_ORDER_ONLY"
)
SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET = (
    "ORDER_FILLED_MAKER_OR_VERIFIED_PUBLIC_WALLET_TRADE"
)
# External Data API page-size constraint used for a bounded forward cursor.
# A saturated page without a persisted boundary is a visible cursor gap, not
# permission to silently omit older source-wallet actions.
PUBLIC_WALLET_TRADE_PAGE_SIZE = 1000
PUBLIC_WALLET_TRADE_MAX_PAGES = 10
# A proportional target remains the audit reference.  Under the explicit
# minimum-upscale authorization, one initial BUY may be raised to the current
# official venue minimum.  Its confirmed surplus is carried forward against
# later same-token BUY fragments; a future source SELL may unwind that surplus
# together with its own proportional exit.  This preserves the cumulative
# fixed-scale exposure without replaying an old signal at a later book.
FIXED_SCALE_MINIMUM_CONSTRAINT_REASONS = frozenset(
    {
        "PROPORTIONAL_QUANTITY_BELOW_MARKET_MINIMUM",
        "PROPORTIONAL_BUY_NOTIONAL_BELOW_MARKETABLE_MINIMUM",
        "PRIOR_MINIMUM_UPSCALE_COVERS_PROPORTIONAL_BUY",
    }
)
LEGACY_LOCAL_CASH_MISMATCH_PREFIX = (
    "SHARED_WALLET_INVARIANT: "
    "PHYSICAL_CASH_BELOW_ACCOUNTING_LOW_BOUND:"
)
LIVE_PROFILE_CD90 = "cd90"
LIVE_PROFILE_BDDC_WEATHER = "bddc_weather"
LIVE_PROFILE_TENNIS_MAINLINE = "tennis_atp_wta_mainline"
LIVE_PROFILE_WALLET_44B0_NETFLIX = "wallet_44b0_netflix"
LIVE_PROFILE_ZOCKDO_FULL_WALLET = "zockdo_full_wallet"
LIVE_PROFILE_WALLET_9506_FULL_WALLET = "wallet_9506_full_wallet"
LIVE_PROFILE_FUU_FULL_WALLET = "fuu_full_wallet"
SIZING_MODE_FIXED_SHARES = "FIXED_SHARES"
SIZING_MODE_SOURCE_NOTIONAL = "SOURCE_NOTIONAL"
LIVE_PROFILE_MINIMUM_SIZE_POLICIES = {
    LIVE_PROFILE_CD90: MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    LIVE_PROFILE_BDDC_WEATHER: MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    LIVE_PROFILE_TENNIS_MAINLINE: MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    LIVE_PROFILE_WALLET_44B0_NETFLIX: MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    LIVE_PROFILE_ZOCKDO_FULL_WALLET: MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    LIVE_PROFILE_WALLET_9506_FULL_WALLET: MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    LIVE_PROFILE_FUU_FULL_WALLET: MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
}
RETRYABLE_EMPTY_BOOK_LEVEL_ERRORS = frozenset(
    {"NO_ASK_BOOK_LEVEL", "NO_BID_BOOK_LEVEL"}
)
REDEMPTION_REQUIRED_ENV = (
    "POLYMARKET_RELAYER_API_KEY",
    "POLYMARKET_RELAYER_API_KEY_ADDRESS",
)
# User-specified server-maintenance cadence: one hour.  Source-action discovery
# remains driven by every new Polygon head; only the independent redemption
# maintenance worker is bounded to this cadence so it cannot exhaust the
# public read path used by order execution.
REDEMPTION_MAINTENANCE_INTERVAL_MS = 60 * 60 * 1000

# User-specified live-copy retry windows.  They are execution constraints, not
# empirically optimized thresholds.
BOUNDED_RETRY_NORMAL_WINDOW_MS = 5_000
BOUNDED_RETRY_DEADLINE_MS = 86_400_000  # 2026-08-15 operator-authorized: drop the 30s deadline to 24h
# WebSocket liveness is an operational transport bound, not an order retry
# window.  Keep it independent so changing order policy cannot hide a dead
# subscription for hours.
WS_NEW_HEAD_LIVENESS_TIMEOUT_SECONDS = 30
# User-selected frozen value from the current official-fill sample: 14 / 17
# copied actions filled, with 10.4841926% fill-notional-weighted pure price
# loss; 3 / 17 unfilled actions remain a separate denominator.  The deployed
# policy deliberately freezes the rounded user-selected value and never
# updates it from future outcomes.
BOUNDED_RETRY_MAX_ADVERSE_SLIPPAGE = Decimal("0.104842")
BOUNDED_RETRY_POLICY_ID = "USER_AUTHORIZED_BOUNDED_LIVE_RETRY_V1"
# Prospective-only replacement for the historical bounded retry contract.
# V2 has no elapsed-time deadline and is intentionally narrower: only an
# official zero-fill proof or an official partial fill can become retryable.
LIQUIDITY_RETRY_POLICY_ID = "LIQUIDITY_ONLY_RETRY_V2"
# User-specified forward BUY rule (2026-08-15): above this unit price, submit
# only when the follower execution price per share is no higher than the
# source wallet's observed price per share.  The user explicitly excludes
# protocol fees from this price-loss comparison.
USER_SPECIFIED_HIGH_PRICE_BUY_CEILING = Decimal("0.90")


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def bounded_retry_phase(action_age_ms: int) -> str:
    age = int(action_age_ms)
    if age < 0:
        raise LiveConfigurationError("NEGATIVE_SOURCE_ACTION_AGE")
    if age <= BOUNDED_RETRY_NORMAL_WINDOW_MS:
        return "NORMAL_FOLLOW"
    if age <= BOUNDED_RETRY_DEADLINE_MS:
        return "ADVERSE_PRICE_PROTECTED"
    return "EXPIRED_RETRY_WINDOW"


def bounded_retry_price_boundary(
    *, side: str, source_average_price: Decimal, tick_size: Decimal
) -> Decimal:
    normalized_side = str(side).upper()
    source_price = Decimal(str(source_average_price))
    tick = Decimal(str(tick_size))
    if normalized_side not in {"BUY", "SELL"}:
        raise LiveConfigurationError("INVALID_BOUNDED_RETRY_SIDE")
    if (
        not source_price.is_finite()
        or source_price <= ZERO
        or source_price > Decimal("1")
        or not tick.is_finite()
        or tick <= ZERO
    ):
        raise LiveConfigurationError("INVALID_BOUNDED_RETRY_PRICE_INPUT")
    if normalized_side == "BUY":
        raw = min(
            source_price * (Decimal("1") + BOUNDED_RETRY_MAX_ADVERSE_SLIPPAGE),
            Decimal("1"),
        )
        return (raw / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
    raw = max(
        source_price * (Decimal("1") - BOUNDED_RETRY_MAX_ADVERSE_SLIPPAGE),
        ZERO,
    )
    return (raw / tick).to_integral_value(rounding=ROUND_CEILING) * tick


_PREPARED_ORDER_RECEIPT_KEYS = (
    "order_id",
    "order_version",
    "order_type",
    "neg_risk",
    "order_fields",
)
_SECRET_FIELD_NAMES = frozenset(
    {
        "signature",
        "privatekey",
        "secret",
        "apikey",
        "apisecret",
        "authorization",
        "accesstoken",
        "bearertoken",
        "password",
        "passphrase",
        "signedorder",
    }
)
_SENSITIVE_TEXT_ASSIGNMENT = re.compile(
    r"(?i)(signature|private[ _-]?key|api[ _-]?key|api[ _-]?secret|"
    r"authorization|access[ _-]?token|bearer[ _-]?token|password|passphrase|"
    r"signed[ _-]?order|secret)[\"']?\s*[:=]\s*"
    r"(?:[\"'][^\"']*[\"']|[^,;\]}\r\n]+)"
)


def _redact_sensitive_text(value: Any) -> str:
    """Redact credential assignments from externally supplied text."""

    return _SENSITIVE_TEXT_ASSIGNMENT.sub(r"\1=<REDACTED>", str(value))


def _contains_secret_material_field(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = "".join(
                character
                for character in str(key).lower()
                if character.isalnum()
            )
            if normalized in _SECRET_FIELD_NAMES:
                return True
            if _contains_secret_material_field(nested):
                return True
        return False
    if isinstance(value, (list, tuple)):
        return any(_contains_secret_material_field(item) for item in value)
    return False


def _sanitize_external_payload(value: Any) -> Any:
    """Return a JSON-safe receipt without credential or signed-order fields."""

    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, nested in value.items():
            normalized = "".join(
                character
                for character in str(key).lower()
                if character.isalnum()
            )
            if normalized in _SECRET_FIELD_NAMES:
                continue
            sanitized[str(key)] = _sanitize_external_payload(nested)
        return sanitized
    if isinstance(value, (list, tuple)):
        return [_sanitize_external_payload(item) for item in value]
    if isinstance(value, str):
        return _redact_sensitive_text(value)
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    return f"<{type(value).__name__}>"


def _receipt_json(value: Any) -> str:
    """Serialize one persistence-boundary receipt after recursive redaction."""

    return json.dumps(_sanitize_external_payload(value), sort_keys=True)


def _has_chain_receipt_evidence(value: Any) -> bool:
    """Require at least one persisted Polygon transaction hash for a fill."""

    return bool(value) and isinstance(value, list) and all(
        isinstance(item, Mapping)
        and re.fullmatch(
            r"0x[0-9a-f]{64}",
            str(item.get("transaction_hash", "")).strip().lower(),
        )
        is not None
        for item in value
    )


def redemption_maintenance_due(
    *, store: "LiveStore", observed_at_ms: int
) -> bool:
    """Return whether the user-specified hourly maintenance check is due."""

    observed = int(observed_at_ms)
    last_raw = store.runtime_value("auto_redemption_last_cycle_at_ms")
    if last_raw is None or not str(last_raw).strip():
        return True
    try:
        last = int(last_raw)
    except ValueError as exc:
        raise LiveConfigurationError(
            "INVALID_AUTO_REDEMPTION_LAST_CYCLE_AT_MS"
        ) from exc
    if last > observed:
        raise LiveConfigurationError("AUTO_REDEMPTION_CLOCK_REGRESSION")
    return observed - last >= REDEMPTION_MAINTENANCE_INTERVAL_MS


class LiveDisabledError(RuntimeError):
    """Live submission is impossible unless the explicit live guard is set."""


class SourceDecodeError(ValueError):
    """The source log cannot be transformed into an unambiguous source action."""


class LiveConfigurationError(RuntimeError):
    """The process cannot safely arm a real-money CD90 sleeve."""


def minimum_size_policy_for_profile(profile_key: str) -> str:
    """Return the immutable below-minimum policy for a known live profile."""

    try:
        return LIVE_PROFILE_MINIMUM_SIZE_POLICIES[str(profile_key)]
    except KeyError as exc:
        raise LiveConfigurationError(f"UNKNOWN_LIVE_PROFILE:{profile_key}") from exc


def source_action_detection_contract_for_profile(profile_key: str) -> str:
    """Return the explicitly supported source-action evidence contract."""

    normalized = str(profile_key)
    if normalized in {
        LIVE_PROFILE_CD90,
        LIVE_PROFILE_WALLET_44B0_NETFLIX,
        LIVE_PROFILE_ZOCKDO_FULL_WALLET,
        LIVE_PROFILE_WALLET_9506_FULL_WALLET,
        LIVE_PROFILE_FUU_FULL_WALLET,
    }:
        return SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET
    if normalized in {
        LIVE_PROFILE_BDDC_WEATHER,
        LIVE_PROFILE_TENNIS_MAINLINE,
    }:
        return SOURCE_ACTION_DETECTION_CONTRACT_CHAIN_MAKER_ONLY
    raise LiveConfigurationError(f"UNKNOWN_LIVE_PROFILE:{profile_key}")


def _is_followable_source_role(value: Any) -> bool:
    return str(value).lower() in FOLLOWABLE_SOURCE_ROLES


class RedemptionNotSubmittedError(RuntimeError):
    """The redemption adapter proved no wallet transaction was dispatched."""


def parse_ws_subscription_ack(acknowledgement: Any) -> str:
    """Return the subscription id or preserve an external provider rejection."""

    if isinstance(acknowledgement, dict) and acknowledgement.get("id") == 1:
        error = acknowledgement.get("error")
        if isinstance(error, dict):
            code = error.get("code", "UNKNOWN")
            message = str(error.get("message", "")).strip() or "unspecified"
            raise ConnectionError(f"WS_SUBSCRIPTION_REJECTED:{code}:{message}")
        subscription_id = acknowledgement.get("result")
        if isinstance(subscription_id, str) and subscription_id:
            return subscription_id
    # A malformed acknowledgement is external provider input.  Reconnect
    # rather than letting the supervisor reset the process and its cursor.
    raise ConnectionError("INVALID_WS_SUBSCRIPTION_ACK")


def _bounded_public_json(url: str) -> Any:
    """Use the shared killable public-read path for every CD90 HTTP read.

    The shared reader has a bounded child-process deadline and classifies
    endpoint outages as external reads. An unbounded Python URL read can leave
    the asynchronous redemption worker permanently running while a TLS socket
    remains open.
    """

    return PublicPolymarketClient().get_json(str(url))


def fetch_official_redemption_activities(wallet_address: str) -> list[dict[str, Any]]:
    """Fetch the wallet's complete official REDEEM activity with stable pages.

    Polymarket documents a maximum page size of 500 and a stable ascending
    timestamp order.  If the documented offset ceiling is reached, fail closed
    instead of silently calling a truncated history complete.
    """

    normalized_wallet = str(wallet_address).strip().lower()
    if (
        not normalized_wallet.startswith("0x")
        or len(normalized_wallet) != 42
    ):
        raise LiveConfigurationError("INVALID_OFFICIAL_ACTIVITY_WALLET")
    page_size = 500
    maximum_offset = 5000
    offset = 0
    rows: list[dict[str, Any]] = []
    while True:
        url = "https://data-api.polymarket.com/activity?" + urlencode(
            {
                "user": normalized_wallet,
                "type": "REDEEM",
                "start": 1,
                "sortBy": "TIMESTAMP",
                "sortDirection": "ASC",
                "limit": page_size,
                "offset": offset,
            }
        )
        payload = _bounded_public_json(url)
        if not isinstance(payload, list) or any(
            not isinstance(item, dict) for item in payload
        ):
            raise LiveConfigurationError("INVALID_OFFICIAL_ACTIVITY_RESPONSE")
        rows.extend(dict(item) for item in payload)
        if len(payload) < page_size:
            return rows
        if offset >= maximum_offset:
            raise LiveConfigurationError("OFFICIAL_ACTIVITY_HISTORY_TRUNCATED")
        offset += page_size


def _exact_official_redemption_activity(
    *,
    official_activities: Iterable[Mapping[str, Any]],
    wallet_address: str,
    condition_id: str,
    transaction_hash: str,
) -> dict[str, Any] | None:
    """Return one exact official REDEEM row, including a legitimate zero payout."""

    normalized_wallet = str(wallet_address).strip().lower()
    normalized_condition = str(condition_id).strip().lower()
    normalized_hash = str(transaction_hash).strip().lower()
    matches: dict[tuple[str, str, str, int], dict[str, Any]] = {}
    for raw in official_activities:
        if not isinstance(raw, Mapping) or str(raw.get("type", "")) != "REDEEM":
            continue
        if str(raw.get("proxyWallet", "")).strip().lower() != normalized_wallet:
            continue
        if str(raw.get("conditionId", "")).strip().lower() != normalized_condition:
            continue
        if str(raw.get("transactionHash", "")).strip().lower() != normalized_hash:
            continue
        try:
            payout = Decimal(str(raw.get("usdcSize", "")))
            timestamp_seconds = int(raw.get("timestamp", -1))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise LiveConfigurationError(
                "INVALID_EXACT_OFFICIAL_REDEMPTION_ACTIVITY"
            ) from exc
        if not payout.is_finite() or payout < ZERO or timestamp_seconds < 0:
            raise LiveConfigurationError(
                "INVALID_EXACT_OFFICIAL_REDEMPTION_ACTIVITY"
            )
        identity = (
            normalized_condition,
            normalized_hash,
            str(payout),
            timestamp_seconds,
        )
        matches[identity] = {
            "condition_id": normalized_condition,
            "transaction_hash": normalized_hash,
            "payout_usd": payout,
            "official_activity_timestamp_ms": timestamp_seconds * 1000,
            "official_activity_type": "REDEEM",
        }
    if not matches:
        return None
    if len(matches) != 1:
        raise LiveConfigurationError("AMBIGUOUS_EXACT_OFFICIAL_REDEMPTION_ACTIVITY")
    match = next(iter(matches.values()))
    evidence = {
        "proxy_wallet": normalized_wallet,
        "condition_id": normalized_condition,
        "transaction_hash": normalized_hash,
        "payout_usd": str(match["payout_usd"]),
        "official_activity_timestamp_ms": int(
            match["official_activity_timestamp_ms"]
        ),
    }
    return {
        **match,
        "official_activity_evidence_hash": hashlib.sha256(
            json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest(),
    }


def reconcile_terminal_redemption_payouts(
    *,
    store: "LiveStore",
    wallet_address: str,
    official_activities: Iterable[Mapping[str, Any]],
    created_at_ms: int,
) -> dict[str, Any]:
    """Correct prior predicted payouts only when the exact official row disagrees."""

    corrected_conditions: list[str] = []
    corrected_cash_delta = ZERO
    for receipt in store.redemption_receipts_with_state("REDEEMED"):
        transaction_hash = str(receipt.get("transaction_hash") or "").strip().lower()
        if not transaction_hash:
            continue
        exact = _exact_official_redemption_activity(
            official_activities=official_activities,
            wallet_address=wallet_address,
            condition_id=str(receipt["condition_id"]),
            transaction_hash=transaction_hash,
        )
        if exact is None:
            continue
        prior = Decimal(str(receipt["expected_payout_usd"]))
        official = Decimal(str(exact["payout_usd"]))
        if prior == official:
            continue
        changed = store.correct_terminal_redemption_payout(
            condition_id=str(receipt["condition_id"]),
            transaction_hash=transaction_hash,
            official_payout_usd=official,
            official_activity_type=str(exact["official_activity_type"]),
            evidence_hash=str(exact["official_activity_evidence_hash"]),
            created_at_ms=int(created_at_ms),
        )
        if changed:
            corrected_conditions.append(str(receipt["condition_id"]).lower())
            corrected_cash_delta += official - prior
    return {
        "state": "CORRECTED" if corrected_conditions else "NO_CORRECTION_REQUIRED",
        "condition_count": len(corrected_conditions),
        "conditions": corrected_conditions,
        "cash_and_realized_delta_usd": str(corrected_cash_delta),
    }


def extract_ws_new_head_number(raw_message: str | bytes, *, subscription_id: str) -> int | None:
    """Return a head number only for the active ``newHeads`` subscription.

    Messages from a previously queued subscription or unrelated websocket
    traffic are intentionally ignored.  A malformed message bearing the
    active subscription id forces a safe reconnect: accepting an ambiguous
    cursor would otherwise permit a later book to price an unknown chain
    event, while terminating the process would unnecessarily interrupt the
    forward-only follower.
    """

    try:
        decoded = json.loads(raw_message)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ConnectionError("INVALID_WS_MESSAGE") from exc
    if not isinstance(decoded, dict) or decoded.get("method") != "eth_subscription":
        return None
    params = decoded.get("params")
    if not isinstance(params, dict) or params.get("subscription") != subscription_id:
        return None
    result = params.get("result")
    if not isinstance(result, dict) or "number" not in result:
        raise ConnectionError("INVALID_WS_NEW_HEAD")
    try:
        value = result["number"]
        head = int(value) if isinstance(value, int) else int(str(value), 16)
    except (TypeError, ValueError) as exc:
        raise ConnectionError("INVALID_WS_NEW_HEAD_NUMBER") from exc
    if head < 0:
        raise ConnectionError("NEGATIVE_WS_NEW_HEAD_NUMBER")
    return head


async def _receive_ws_message_with_liveness(
    websocket: Any, *, timeout_seconds: float
) -> Any:
    """Reconnect when an acknowledged subscription silently stops emitting."""

    try:
        return await asyncio.wait_for(
            websocket.recv(), timeout=float(timeout_seconds)
        )
    except asyncio.TimeoutError as exc:
        raise TimeoutError("WS_NEW_HEAD_STALLED") from exc


async def _process_head_while_coalescing_notifications(
    *,
    websocket: Any,
    subscription_id: str,
    head: int,
    process_head: Callable[[int], Awaitable[bool]],
) -> tuple[bool, int | None, int]:
    """Keep consuming ``newHeads`` while one head is being processed.

    The websocket protocol reader must not become a FIFO replay clock.  If
    execution or an external read takes longer than one Polygon block, all
    heads received during that work are observations from the same connected
    live session.  Keep only their newest height; the next source-log query
    still covers the entire persisted-cursor range, so no source action is
    discarded and no disconnected historical action is repriced.
    """

    processing = asyncio.create_task(process_head(int(head)))
    latest_buffered_head: int | None = None
    buffered_count = 0
    try:
        while True:
            receiving = asyncio.create_task(websocket.recv())
            done, _pending = await asyncio.wait(
                {processing, receiving},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if receiving in done:
                try:
                    raw_message = receiving.result()
                except Exception:
                    # Finish the already-authorized head before reconnecting;
                    # cancelling ``to_thread`` would not stop its side effect.
                    await processing
                    raise
                buffered_head = extract_ws_new_head_number(
                    raw_message,
                    subscription_id=subscription_id,
                )
                if buffered_head is not None:
                    buffered_count += 1
                    latest_buffered_head = (
                        int(buffered_head)
                        if latest_buffered_head is None
                        else max(latest_buffered_head, int(buffered_head))
                    )
            else:
                receiving.cancel()
                await asyncio.gather(receiving, return_exceptions=True)
            if processing in done:
                return (
                    bool(processing.result()),
                    latest_buffered_head,
                    buffered_count,
                )
    finally:
        if not processing.done():
            processing.cancel()
            await asyncio.gather(processing, return_exceptions=True)


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise SourceDecodeError(f"invalid decimal: {value!r}") from exc
    if not result.is_finite():
        raise SourceDecodeError(f"non-finite decimal: {value!r}")
    return result


def _require_text(value: Any, field: str) -> str:
    result = str(value or "").strip().lower()
    if not result:
        raise SourceDecodeError(f"missing {field}")
    return result


@dataclass(frozen=True)
class SourceAction:
    """One source action keyed by the required four-field identity."""

    transaction_hash: str
    token_id: str
    side: str
    order_hash: str
    source_quantity: Decimal
    source_notional: Decimal
    source_timestamp: int
    block_number: int
    block_hash: str
    source_role: str
    discovered_at_ms: int
    # Polygon orders logs within a block by ``logIndex``.  It is deliberately
    # not part of the four-field action identity, but it is required to replay
    # source actions in their exact causal order when one block contains more
    # than one action.
    log_index: int = 0

    @property
    def identity(self) -> tuple[str, str, str, str]:
        return (
            self.transaction_hash.lower(),
            str(self.token_id),
            self.side.upper(),
            self.order_hash.lower(),
        )

    @property
    def action_id(self) -> str:
        return hashlib.sha256(
            json.dumps(self.identity, separators=(",", ":")).encode("utf-8")
        ).hexdigest()


def source_action_proportional_quantity(
    *,
    source: SourceAction,
    scale: Decimal,
    sizing_mode: str,
    executable_price: Decimal,
) -> Decimal:
    """Map one source action to shares under the frozen sizing contract."""

    ratio = Decimal(str(scale))
    price = Decimal(str(executable_price))
    mode = str(sizing_mode or SIZING_MODE_FIXED_SHARES)
    if ratio <= ZERO or price <= ZERO:
        raise LiveConfigurationError("INVALID_ACTION_SIZING_INPUT")
    if mode == SIZING_MODE_FIXED_SHARES:
        return source.source_quantity * ratio
    if mode == SIZING_MODE_SOURCE_NOTIONAL:
        return source.source_notional * ratio / price
    raise LiveConfigurationError(f"UNSUPPORTED_SIZING_MODE:{mode}")


def decode_followable_source_action(decoded: dict[str, Any]) -> SourceAction:
    """Decode the followed wallet's own signed order from ``OrderFilled``.

    In the V2 exchange event, ``maker`` is the maker of *that order*.  The
    indexed ``taker`` is the counterparty address and its appearance in this
    event does not identify another order placed by the followed wallet.  A
    source wallet can therefore appear as ``taker`` in every counterparty log
    generated while its one order is matched.  Promoting those logs to inverse
    source actions double-counts one trade and can fabricate a SELL.  Only the
    event whose maker is the followed wallet is a source action.
    """

    source_role = str(decoded.get("source_role", "")).lower()
    side_code = int(decoded.get("side_code", -1))
    maker_amount = _decimal(decoded.get("maker_amount_raw")) / TOKEN_SCALE
    taker_amount = _decimal(decoded.get("taker_amount_raw")) / TOKEN_SCALE
    if maker_amount <= ZERO or taker_amount <= ZERO:
        raise SourceDecodeError("non-positive protocol fill amount")

    if source_role != "maker" or not bool(decoded.get("source_order")):
        raise SourceDecodeError("COUNTERPARTY_ORDER_LOG_NOT_SOURCE_ACTION")
    side = str(decoded.get("side", "")).upper()
    if side == "BUY":
        quantity, notional = taker_amount, maker_amount
    elif side == "SELL":
        quantity, notional = maker_amount, taker_amount
    else:
        raise SourceDecodeError(f"unsupported maker source side: {side!r}")

    return SourceAction(
        transaction_hash=_require_text(decoded.get("transaction_hash"), "transaction_hash"),
        token_id=str(decoded.get("token_id", "")).strip(),
        side=side,
        order_hash=_require_text(decoded.get("order_hash"), "order_hash"),
        source_quantity=quantity,
        source_notional=notional,
        source_timestamp=int(decoded.get("block_timestamp", 0)),
        block_number=int(decoded.get("block_number", 0)),
        block_hash=_require_text(decoded.get("block_hash"), "block_hash"),
        source_role=source_role,
        discovered_at_ms=int(decoded.get("chain_seen_at_ms", 0)),
        log_index=int(decoded.get("log_index", 0)),
    )


def _public_wallet_trades_url(*, wallet: str, offset: int) -> str:
    return "https://data-api.polymarket.com/trades?" + urlencode(
        {
            "user": str(wallet).lower(),
            "takerOnly": "false",
            "limit": PUBLIC_WALLET_TRADE_PAGE_SIZE,
            "offset": int(offset),
        }
    )


def _public_notional_matches_chain(
    *, public_notional: Decimal, chain_notional: Decimal
) -> bool:
    """Compare a public display product at the chain collateral raw precision.

    Chain collateral amounts are fixed at ``TOKEN_RAW_UNIT`` (one millionth
    USD).  The public wallet API reports size and price independently, so their
    Decimal product can contain display-only sub-raw-unit residue.  The source
    quantity remains exact; a public notional that differs after quantization
    is still an evidence mismatch and must retain the cursor.
    """

    return public_notional.quantize(
        TOKEN_RAW_UNIT, rounding=ROUND_HALF_UP
    ) == chain_notional.quantize(TOKEN_RAW_UNIT, rounding=ROUND_HALF_UP)


def _decode_verified_public_wallet_trade(
    raw: Mapping[str, Any], *, source_wallet: str
) -> dict[str, Any]:
    """Validate one public trade row that directly names the followed wallet.

    The public API supplies the wallet's BUY/SELL direction that a bare
    counterparty exchange log intentionally lacks.  This parser never infers
    a side from the chain log and rejects any malformed or other-wallet row.
    """

    wallet = _require_text(raw.get("proxyWallet"), "public_proxy_wallet")
    if wallet != str(source_wallet).lower():
        raise SourceDecodeError("PUBLIC_TRADE_WALLET_MISMATCH")
    transaction_hash = _require_text(
        raw.get("transactionHash"), "public_transaction_hash"
    )
    if re.fullmatch(r"0x[a-f0-9]{64}", transaction_hash) is None:
        raise SourceDecodeError("INVALID_PUBLIC_TRANSACTION_HASH")
    token_id = _require_text(raw.get("asset"), "public_asset")
    if not token_id.isdecimal():
        raise SourceDecodeError("INVALID_PUBLIC_ASSET")
    side = _require_text(raw.get("side"), "public_side").upper()
    if side not in {"BUY", "SELL"}:
        raise SourceDecodeError("INVALID_PUBLIC_SIDE")
    source_quantity = _decimal(raw.get("size"))
    source_price = _decimal(raw.get("price"))
    if source_quantity <= ZERO or source_price <= ZERO or source_price > Decimal("1"):
        raise SourceDecodeError("INVALID_PUBLIC_TRADE_SIZE_OR_PRICE")
    try:
        source_timestamp = int(raw.get("timestamp"))
    except (TypeError, ValueError) as exc:
        raise SourceDecodeError("INVALID_PUBLIC_TRADE_TIMESTAMP") from exc
    if source_timestamp <= 0:
        raise SourceDecodeError("INVALID_PUBLIC_TRADE_TIMESTAMP")
    row_key = {
        "proxy_wallet": wallet,
        "transaction_hash": transaction_hash,
        "token_id": token_id,
        "side": side,
        "source_quantity": str(source_quantity),
        "source_price": str(source_price),
        "source_timestamp": source_timestamp,
    }
    return {
        "row_id": canonical_hash(row_key),
        "transaction_hash": transaction_hash,
        "token_id": token_id,
        "side": side,
        "source_quantity": source_quantity,
        "source_price": source_price,
        "source_timestamp": source_timestamp,
        "raw": dict(raw),
    }


def _public_wallet_action_order_hash(
    *, transaction_hash: str, token_id: str, side: str
) -> str:
    """Generate an auditable synthetic fourth identity field for one group.

    The public user-trades endpoint does not return an order hash.  It can
    nevertheless identify a wallet action at the documented logical-action
    boundary of transaction hash + token + side.  The explicit prefix prevents
    this evidence key from being confused with an on-chain order hash.
    """

    return "public:" + canonical_hash(
        {
            "source": "POLYMARKET_PUBLIC_WALLET_TRADE",
            "transaction_hash": str(transaction_hash).lower(),
            "token_id": str(token_id),
            "side": str(side).upper(),
        }
    )


class LiveStore:
    """SQLite audit/ledger substrate for the standalone CD90 cash sleeve."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._initialized = False

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        create_flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        create_flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.path, create_flags, 0o600)
        except FileExistsError:
            status = self.path.lstat()
            if not stat.S_ISREG(status.st_mode) or self.path.is_symlink():
                raise RuntimeError(f"unsafe SQLite database identity:{self.path}")
            os.chmod(self.path, 0o600, follow_symlinks=False)
        else:
            os.close(descriptor)
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 10000")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        if self._initialized:
            return
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS config_change_receipts (
                    change_id TEXT PRIMARY KEY,
                    config_key TEXT NOT NULL,
                    previous_value TEXT NOT NULL,
                    new_value TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    changed_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL,
                    UNIQUE(config_key, previous_value, new_value, reason)
                );

                CREATE TABLE IF NOT EXISTS action_receipts (
                    action_id TEXT PRIMARY KEY,
                    transaction_hash TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_hash TEXT NOT NULL,
                    source_quantity TEXT NOT NULL,
                    source_notional TEXT NOT NULL,
                    source_timestamp INTEGER NOT NULL,
                    block_number INTEGER NOT NULL,
                    source_log_index INTEGER NOT NULL DEFAULT 0,
                    block_hash TEXT NOT NULL,
                    source_role TEXT NOT NULL,
                    discovered_at_ms INTEGER NOT NULL,
                    UNIQUE(transaction_hash, token_id, side, order_hash)
                );

                CREATE TABLE IF NOT EXISTS public_source_observations (
                    row_id TEXT PRIMARY KEY,
                    transaction_hash TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    source_quantity TEXT NOT NULL,
                    source_price TEXT NOT NULL,
                    source_timestamp INTEGER NOT NULL,
                    observed_at_ms INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    source_action_id TEXT,
                    details_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_public_source_observations_action
                    ON public_source_observations(source_action_id, observed_at_ms);

                CREATE TABLE IF NOT EXISTS action_execution_constraints (
                    action_id TEXT PRIMARY KEY,
                    reason TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id)
                );

                CREATE TABLE IF NOT EXISTS repair_recovery_manifests(
                    manifest_hash TEXT PRIMARY KEY,
                    profile_key TEXT NOT NULL,
                    gap_receipt_id INTEGER NOT NULL,
                    policy_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    activated_at_ms INTEGER NOT NULL,
                    last_processed_head INTEGER,
                    manifest_json TEXT NOT NULL,
                    UNIQUE(profile_key, gap_receipt_id)
                );

                CREATE TABLE IF NOT EXISTS repair_recovery_actions(
                    manifest_hash TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    action_kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    source_unit_price TEXT NOT NULL,
                    last_evaluated_head INTEGER,
                    last_snapshot_json TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY(manifest_hash, action_id),
                    FOREIGN KEY(manifest_hash)
                        REFERENCES repair_recovery_manifests(manifest_hash)
                );

                CREATE TABLE IF NOT EXISTS repair_recovery_transitions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    manifest_hash TEXT NOT NULL,
                    action_id TEXT,
                    state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_repair_recovery_action_state
                    ON repair_recovery_actions(manifest_hash, state, updated_at_ms);

                CREATE TABLE IF NOT EXISTS action_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id)
                );
                CREATE INDEX IF NOT EXISTS idx_live_action_transitions
                    ON action_transitions(action_id, id DESC);

                CREATE TABLE IF NOT EXISTS action_market_metadata (
                    action_id TEXT PRIMARY KEY,
                    condition_id TEXT NOT NULL,
                    market_slug TEXT NOT NULL,
                    event_slug TEXT NOT NULL,
                    profile_follow INTEGER NOT NULL,
                    profile_reason TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    evidence_hash TEXT NOT NULL,
                    frozen_at_ms INTEGER NOT NULL,
                    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id)
                );
                CREATE INDEX IF NOT EXISTS idx_action_market_metadata_event
                    ON action_market_metadata(event_slug, frozen_at_ms);

                CREATE TABLE IF NOT EXISTS source_topic_alerts (
                    action_id TEXT PRIMARY KEY,
                    topic_classification TEXT NOT NULL,
                    event_slug TEXT NOT NULL,
                    market_slug TEXT NOT NULL,
                    side TEXT NOT NULL,
                    source_timestamp INTEGER NOT NULL,
                    discovered_at_ms INTEGER NOT NULL,
                    processing_state TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    acknowledged_at_ms INTEGER,
                    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id)
                );
                CREATE INDEX IF NOT EXISTS idx_source_topic_alerts_unacknowledged
                    ON source_topic_alerts(acknowledged_at_ms, discovered_at_ms);

                CREATE TABLE IF NOT EXISTS decision_units (
                    event_slug TEXT PRIMARY KEY,
                    metadata_hash TEXT NOT NULL,
                    first_source_timestamp INTEGER NOT NULL,
                    last_source_timestamp INTEGER NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS action_targets (
                    action_id TEXT PRIMARY KEY,
                    proportional_quantity TEXT NOT NULL,
                    target_quantity TEXT NOT NULL,
                    cumulative_filled_quantity TEXT NOT NULL DEFAULT '0',
                    state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id)
                );
                CREATE INDEX IF NOT EXISTS idx_action_targets_state
                    ON action_targets(state, updated_at_ms);

                CREATE TABLE IF NOT EXISTS submission_attempts (
                    attempt_id TEXT PRIMARY KEY,
                    action_id TEXT NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    order_id TEXT,
                    prepared_order_json TEXT NOT NULL DEFAULT '{}',
                    state TEXT NOT NULL,
                    requested_quantity TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id),
                    UNIQUE(action_id, attempt_number),
                    UNIQUE(order_id)
                );
                CREATE INDEX IF NOT EXISTS idx_submission_attempts_action
                    ON submission_attempts(action_id, attempt_number);

                CREATE TABLE IF NOT EXISTS positions (
                    token_id TEXT PRIMARY KEY,
                    quantity TEXT NOT NULL,
                    cost_basis_usd TEXT NOT NULL DEFAULT '0',
                    condition_id TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS account_state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    initial_capital_usd TEXT NOT NULL,
                    cash_usd TEXT NOT NULL,
                    realized_pnl_usd TEXT NOT NULL,
                    fees_usd TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS external_cash_reserve_receipts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observed_at_ms INTEGER NOT NULL,
                    observed_collateral_usd TEXT NOT NULL,
                    ledger_cash_before_usd TEXT NOT NULL,
                    credited_cash_usd TEXT NOT NULL,
                    reason TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS settlement_cash_reclassification_receipts (
                    condition_id TEXT PRIMARY KEY,
                    reclassified_cash_usd TEXT NOT NULL,
                    official_activity_timestamp_ms INTEGER NOT NULL,
                    transaction_hash TEXT NOT NULL UNIQUE,
                    created_at_ms INTEGER NOT NULL,
                    evidence_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS fixed_share_scale_rebase_receipts (
                    change_id TEXT PRIMARY KEY,
                    previous_scale TEXT NOT NULL,
                    new_scale TEXT NOT NULL,
                    requested_multiplier TEXT NOT NULL,
                    effective_after_block INTEGER NOT NULL,
                    resume_from_block INTEGER NOT NULL,
                    requested_at_ms INTEGER NOT NULL,
                    prior_scale_basis TEXT NOT NULL,
                    resulting_scale_basis TEXT NOT NULL,
                    details_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS order_reservations (
                    action_id TEXT PRIMARY KEY,
                    token_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    cash_reserved_usd TEXT NOT NULL,
                    condition_id TEXT NOT NULL DEFAULT '',
                    active INTEGER NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id)
                );

                CREATE TABLE IF NOT EXISTS fill_corrections (
                    action_id TEXT PRIMARY KEY,
                    source_transition_id INTEGER NOT NULL,
                    previous_quantity TEXT NOT NULL,
                    authoritative_quantity TEXT NOT NULL,
                    previous_fee_usd TEXT NOT NULL,
                    authoritative_fee_usd TEXT NOT NULL,
                    corrected_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id),
                    FOREIGN KEY(source_transition_id) REFERENCES action_transitions(id)
                );

                CREATE TABLE IF NOT EXISTS runtime_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runtime_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at_ms INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runtime_gap_receipts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    previous_processed_block INTEGER NOT NULL,
                    resume_head INTEGER NOT NULL,
                    skipped_block_count INTEGER NOT NULL,
                    source_action_count INTEGER NOT NULL,
                    detected_at_ms INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    pricing_status TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    UNIQUE(previous_processed_block, resume_head, reason)
                );

                CREATE TABLE IF NOT EXISTS condition_mappings (
                    condition_id TEXT PRIMARY KEY,
                    primary_token_id TEXT NOT NULL UNIQUE,
                    secondary_token_id TEXT NOT NULL UNIQUE,
                    observed_at_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS redemption_receipts (
                    condition_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    expected_payout_usd TEXT NOT NULL,
                    transaction_id TEXT,
                    transaction_hash TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    FOREIGN KEY(condition_id) REFERENCES condition_mappings(condition_id)
                );

                CREATE TABLE IF NOT EXISTS redemption_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    condition_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY(condition_id) REFERENCES condition_mappings(condition_id)
                );
                CREATE INDEX IF NOT EXISTS idx_redemption_transitions
                    ON redemption_transitions(condition_id, id DESC);

                CREATE TABLE IF NOT EXISTS redemption_payout_corrections (
                    condition_id TEXT PRIMARY KEY,
                    prior_state TEXT NOT NULL,
                    prior_expected_payout_usd TEXT NOT NULL,
                    official_payout_usd TEXT NOT NULL,
                    cash_delta_usd TEXT NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    evidence_hash TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    FOREIGN KEY(condition_id) REFERENCES condition_mappings(condition_id)
                );

                CREATE TABLE IF NOT EXISTS redeemed_cash_credit_quarantines (
                    condition_id TEXT PRIMARY KEY,
                    payout_usd TEXT NOT NULL,
                    transaction_id TEXT NOT NULL,
                    transaction_hash TEXT,
                    created_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY(condition_id) REFERENCES condition_mappings(condition_id)
                );
                CREATE TABLE IF NOT EXISTS redeemed_cash_credit_quarantine_verifications (
                    condition_id TEXT PRIMARY KEY,
                    authenticated_collateral_usd TEXT NOT NULL,
                    verified_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY(condition_id)
                        REFERENCES redeemed_cash_credit_quarantines(condition_id)
                );
                CREATE TABLE IF NOT EXISTS redeemed_cash_credit_quarantine_voids (
                    condition_id TEXT PRIMARY KEY,
                    reason TEXT NOT NULL,
                    official_payout_usd TEXT NOT NULL,
                    evidence_hash TEXT NOT NULL,
                    voided_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY(condition_id)
                        REFERENCES redeemed_cash_credit_quarantines(condition_id)
                );

                CREATE TABLE IF NOT EXISTS redeemed_cash_credit_permanent_blocks (
                    condition_id TEXT PRIMARY KEY,
                    payout_usd TEXT NOT NULL,
                    transaction_id TEXT NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    expected_payout_raw TEXT NOT NULL,
                    observed_payout_raw TEXT NOT NULL,
                    evidence_hash TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY(condition_id) REFERENCES condition_mappings(condition_id)
                );
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(positions)")
            }
            if "cost_basis_usd" not in columns:
                connection.execute(
                    "ALTER TABLE positions ADD COLUMN cost_basis_usd TEXT NOT NULL DEFAULT '0'"
                )
            if "condition_id" not in columns:
                connection.execute(
                    "ALTER TABLE positions ADD COLUMN condition_id TEXT NOT NULL DEFAULT ''"
                )
            action_receipt_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(action_receipts)"
                )
            }
            if (
                action_receipt_columns
                and "source_log_index" not in action_receipt_columns
            ):
                connection.execute(
                    """
                    ALTER TABLE action_receipts
                    ADD COLUMN source_log_index INTEGER NOT NULL DEFAULT 0
                    """
                )
            reservation_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(order_reservations)"
                )
            }
            if reservation_columns and "condition_id" not in reservation_columns:
                connection.execute(
                    """
                    ALTER TABLE order_reservations
                    ADD COLUMN condition_id TEXT NOT NULL DEFAULT ''
                    """
                )
            submission_attempt_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(submission_attempts)"
                )
            }
            if (
                submission_attempt_columns
                and "prepared_order_json" not in submission_attempt_columns
            ):
                connection.execute(
                    """
                    ALTER TABLE submission_attempts
                    ADD COLUMN prepared_order_json TEXT NOT NULL DEFAULT '{}'
                    """
                )
            # Initialization is schema-only.  Legacy position and reservation
            # ownership metadata is historical ledger evidence and must never
            # be filled in implicitly during a service start or deployment.
            # Any such repair needs an explicit, separately audited workflow.
            rebase_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(fixed_share_scale_rebase_receipts)"
                )
            }
            if rebase_columns and "resume_from_block" not in rebase_columns:
                connection.execute(
                    """
                    ALTER TABLE fixed_share_scale_rebase_receipts
                    ADD COLUMN resume_from_block INTEGER NOT NULL DEFAULT 0
                    """
                )
        self._initialized = True

    def config(self, key: str) -> str | None:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value FROM config WHERE key = ?", (str(key),)
            ).fetchone()
        return None if row is None else str(row["value"])

    def runtime_value(self, key: str) -> str | None:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value FROM runtime_state WHERE key = ?", (str(key),)
            ).fetchone()
        return None if row is None else str(row["value"])

    def set_runtime(self, key: str, value: Any) -> None:
        self.initialize()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO runtime_state(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(key), str(value)),
            )

    def append_runtime_error(
        self,
        *,
        occurred_at_ms: int,
        category: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.initialize()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO runtime_errors(
                    occurred_at_ms, category, message, details_json
                ) VALUES(?, ?, ?, ?)
                """,
                (
                    int(occurred_at_ms),
                    str(category),
                    _redact_sensitive_text(message),
                    json.dumps(
                        _sanitize_external_payload(details or {}),
                        sort_keys=True,
                    ),
                ),
            )

    def record_external_reconciliation_incident(
        self,
        *,
        occurred_at_ms: int,
        category: str,
        message: str,
        details: dict[str, Any],
    ) -> None:
        """Keep one immutable incident start while reconciliation keeps retrying."""

        sanitized_details = _sanitize_external_payload(details)
        redacted_message = _redact_sensitive_text(message)
        fingerprint = canonical_hash(
            {"category": str(category), "message": redacted_message, "details": sanitized_details}
        )
        key = f"external_reconciliation_incident:{fingerprint}"
        if self.runtime_value(key) != "active":
            self.append_runtime_error(
                occurred_at_ms=occurred_at_ms,
                category=category,
                message=redacted_message,
                details=sanitized_details,
            )
            self.set_runtime(key, "active")
        self.set_runtime(
            "external_reconciliation_last_observed_json",
            json.dumps(
                {"category": str(category), "message": redacted_message,
                 "details": sanitized_details, "occurred_at_ms": int(occurred_at_ms)},
                sort_keys=True,
            ),
        )

    def _set_config(self, connection: sqlite3.Connection, key: str, value: Any) -> None:
        connection.execute(
            "INSERT INTO config(key, value) VALUES(?, ?)", (str(key), str(value))
        )

    def lock_config_once(self, key: str, value: Any) -> str:
        """Persist a live-bearing setting once and reject later drift."""

        self.initialize()
        expected = str(value)
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value FROM config WHERE key = ?", (str(key),)
            ).fetchone()
            if row is None:
                try:
                    self._set_config(connection, key, expected)
                    return expected
                except sqlite3.IntegrityError:
                    # Primary and hot standby can observe the same absent key
                    # before either commits it.  Adopt only the identical
                    # concurrent winner; a different value remains fatal.
                    row = connection.execute(
                        "SELECT value FROM config WHERE key = ?", (str(key),)
                    ).fetchone()
                    if row is None:
                        raise
            actual = str(row["value"])
            if actual != expected:
                raise LiveConfigurationError(
                    f"IMMUTABLE_CONFIG_MISMATCH:{key}:{actual}:{expected}"
                )
            return actual

    def activate_bounded_retry_policy(
        self,
        *,
        effective_after_block: int,
        activated_at_ms: int,
        change_id: str,
    ) -> dict[str, Any]:
        """Lock the prospective-only retry contract without editing history."""

        boundary = int(effective_after_block)
        activated = int(activated_at_ms)
        normalized_change_id = str(change_id).strip()
        if boundary < 0 or activated <= 0 or not normalized_change_id:
            raise LiveConfigurationError("INVALID_BOUNDED_RETRY_ACTIVATION")
        receipt = {
            "policy_id": BOUNDED_RETRY_POLICY_ID,
            "effective_after_block": boundary,
            "activated_at_ms": activated,
            "change_id": normalized_change_id,
            "normal_window_ms": BOUNDED_RETRY_NORMAL_WINDOW_MS,
            "deadline_ms": BOUNDED_RETRY_DEADLINE_MS,
            "maximum_adverse_slippage": str(
                BOUNDED_RETRY_MAX_ADVERSE_SLIPPAGE
            ),
            "historical_catch_up": False,
        }
        receipt_hash = canonical_hash(receipt)
        self.lock_config_once("bounded_retry_policy_id", BOUNDED_RETRY_POLICY_ID)
        self.lock_config_once(
            "bounded_retry_effective_after_block", str(boundary)
        )
        self.lock_config_once("bounded_retry_activation_receipt_hash", receipt_hash)
        self.lock_config_once(
            "bounded_retry_activation_receipt_json",
            json.dumps(receipt, sort_keys=True, separators=(",", ":")),
        )
        return {**receipt, "receipt_hash": receipt_hash}

    def bounded_retry_policy_for_source(
        self, source: SourceAction
    ) -> dict[str, Any] | None:
        policy_id = self.config("bounded_retry_policy_id")
        boundary_raw = self.config("bounded_retry_effective_after_block")
        if policy_id is None and boundary_raw is None:
            return None
        if policy_id != BOUNDED_RETRY_POLICY_ID or boundary_raw is None:
            raise LiveConfigurationError("INCOMPLETE_BOUNDED_RETRY_POLICY")
        try:
            boundary = int(boundary_raw)
        except ValueError as exc:
            raise LiveConfigurationError("INVALID_BOUNDED_RETRY_WATERLINE") from exc
        if int(source.block_number) <= boundary:
            return None
        source_average_price = source.source_notional / source.source_quantity
        age_ms = max(now_ms() - int(source.source_timestamp) * 1000, 0)
        target = self.action_target(source.action_id)
        with self.connect() as connection:
            first_attempt = connection.execute(
                """
                SELECT created_at_ms FROM submission_attempts
                WHERE action_id = ? ORDER BY attempt_number ASC LIMIT 1
                """,
                (source.action_id,),
            ).fetchone()
            attempt_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM submission_attempts WHERE action_id = ?",
                    (source.action_id,),
                ).fetchone()[0]
            )
        return {
            "policy_id": BOUNDED_RETRY_POLICY_ID,
            "effective_after_block": boundary,
            "source_average_price": str(source_average_price),
            "source_action_age_ms": age_ms,
            "phase": bounded_retry_phase(age_ms),
            "first_submission_at_ms": (
                None if first_attempt is None else int(first_attempt["created_at_ms"])
            ),
            "retry_deadline_at_ms": (
                int(source.source_timestamp) * 1000 + BOUNDED_RETRY_DEADLINE_MS
            ),
            "attempt_count": attempt_count,
            "cumulative_official_filled_quantity": (
                "0"
                if target is None
                else str(target["cumulative_filled_quantity"])
            ),
            "remaining_quantity": (
                None if target is None else str(target["remaining_quantity"])
            ),
            "historical_catch_up": False,
        }

    def bounded_retry_effective_after_block(self) -> int | None:
        policy_id = self.config("bounded_retry_policy_id")
        boundary_raw = self.config("bounded_retry_effective_after_block")
        if policy_id is None and boundary_raw is None:
            return None
        if policy_id != BOUNDED_RETRY_POLICY_ID or boundary_raw is None:
            raise LiveConfigurationError("INCOMPLETE_BOUNDED_RETRY_POLICY")
        try:
            boundary = int(boundary_raw)
        except ValueError as exc:
            raise LiveConfigurationError("INVALID_BOUNDED_RETRY_WATERLINE") from exc
        if boundary < 0:
            raise LiveConfigurationError("INVALID_BOUNDED_RETRY_WATERLINE")
        return boundary

    def ensure_bounded_retry_policy_at_current_cursor(
        self, *, activated_at_ms: int, change_id: str
    ) -> dict[str, Any]:
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = {
                str(row["key"]): str(row["value"])
                for row in connection.execute(
                    """
                    SELECT key,value FROM config WHERE key IN (
                        'bounded_retry_policy_id',
                        'bounded_retry_effective_after_block',
                        'bounded_retry_activation_receipt_hash',
                        'bounded_retry_activation_receipt_json'
                    )
                    """
                ).fetchall()
            }
            if rows:
                if set(rows) != {
                    "bounded_retry_policy_id",
                    "bounded_retry_effective_after_block",
                    "bounded_retry_activation_receipt_hash",
                    "bounded_retry_activation_receipt_json",
                }:
                    raise LiveConfigurationError("INCOMPLETE_BOUNDED_RETRY_POLICY")
                receipt = json.loads(rows["bounded_retry_activation_receipt_json"])
                if (
                    rows["bounded_retry_policy_id"] != BOUNDED_RETRY_POLICY_ID
                    or canonical_hash(receipt)
                    != rows["bounded_retry_activation_receipt_hash"]
                ):
                    raise LiveConfigurationError("INVALID_BOUNDED_RETRY_POLICY")
                return receipt
            cursor_row = connection.execute(
                "SELECT value FROM runtime_state WHERE key='last_processed_block'"
            ).fetchone()
            if cursor_row is None:
                raise LiveConfigurationError(
                    "BOUNDED_RETRY_REQUIRES_FORWARD_WATERMARK"
                )
            try:
                cursor = int(cursor_row["value"])
            except ValueError as exc:
                raise LiveConfigurationError(
                    "BOUNDED_RETRY_REQUIRES_FORWARD_WATERMARK"
                ) from exc
            receipt = {
                "policy_id": BOUNDED_RETRY_POLICY_ID,
                "effective_after_block": cursor,
                "activated_at_ms": int(activated_at_ms),
                "change_id": str(change_id),
                "normal_window_ms": BOUNDED_RETRY_NORMAL_WINDOW_MS,
                "deadline_ms": BOUNDED_RETRY_DEADLINE_MS,
                "maximum_adverse_slippage": str(
                    BOUNDED_RETRY_MAX_ADVERSE_SLIPPAGE
                ),
                "historical_catch_up": False,
            }
            receipt_json = json.dumps(
                receipt, sort_keys=True, separators=(",", ":")
            )
            receipt_hash = canonical_hash(receipt)
            for key, value in (
                ("bounded_retry_policy_id", BOUNDED_RETRY_POLICY_ID),
                ("bounded_retry_effective_after_block", str(cursor)),
                ("bounded_retry_activation_receipt_hash", receipt_hash),
                ("bounded_retry_activation_receipt_json", receipt_json),
            ):
                self._set_config(connection, key, value)
            return {**receipt, "receipt_hash": receipt_hash}

    def activate_liquidity_retry_policy(
        self,
        *,
        effective_after_block: int,
        activated_at_ms: int,
        change_id: str,
    ) -> dict[str, Any]:
        """Lock the V2 prospective waterline without reopening V1 history."""

        boundary = int(effective_after_block)
        activated = int(activated_at_ms)
        normalized_change_id = str(change_id).strip()
        if boundary < 0 or activated <= 0 or not normalized_change_id:
            raise LiveConfigurationError("INVALID_LIQUIDITY_RETRY_ACTIVATION")
        receipt = {
            "policy_id": LIQUIDITY_RETRY_POLICY_ID,
            "effective_after_block": boundary,
            "activated_at_ms": activated,
            "change_id": normalized_change_id,
            "deadline_ms": None,
            "eligible_attempt_states": ["NO_FILL", "PARTIAL_FILLED"],
            "historical_catch_up": False,
            "minimum_upscale": False,
            "unknown_repost": False,
        }
        receipt_hash = canonical_hash(receipt)
        self.lock_config_once(
            "liquidity_retry_policy_id", LIQUIDITY_RETRY_POLICY_ID
        )
        self.lock_config_once(
            "liquidity_retry_effective_after_block", str(boundary)
        )
        self.lock_config_once(
            "liquidity_retry_activation_receipt_hash", receipt_hash
        )
        self.lock_config_once(
            "liquidity_retry_activation_receipt_json",
            json.dumps(receipt, sort_keys=True, separators=(",", ":")),
        )
        return {**receipt, "receipt_hash": receipt_hash}

    def liquidity_retry_effective_after_block(self) -> int | None:
        policy_id = self.config("liquidity_retry_policy_id")
        boundary_raw = self.config("liquidity_retry_effective_after_block")
        if policy_id is None and boundary_raw is None:
            return None
        if policy_id != LIQUIDITY_RETRY_POLICY_ID or boundary_raw is None:
            raise LiveConfigurationError("INCOMPLETE_LIQUIDITY_RETRY_POLICY")
        try:
            boundary = int(boundary_raw)
        except ValueError as exc:
            raise LiveConfigurationError(
                "INVALID_LIQUIDITY_RETRY_WATERLINE"
            ) from exc
        if boundary < 0:
            raise LiveConfigurationError("INVALID_LIQUIDITY_RETRY_WATERLINE")
        return boundary

    def liquidity_retry_policy_for_source(
        self, source: SourceAction
    ) -> dict[str, Any] | None:
        boundary = self.liquidity_retry_effective_after_block()
        if boundary is None or int(source.block_number) <= boundary:
            return None
        receipt_raw = self.config("liquidity_retry_activation_receipt_json")
        receipt_hash = self.config("liquidity_retry_activation_receipt_hash")
        if receipt_raw is None or receipt_hash is None:
            raise LiveConfigurationError("INCOMPLETE_LIQUIDITY_RETRY_POLICY")
        try:
            receipt = json.loads(receipt_raw)
        except json.JSONDecodeError as exc:
            raise LiveConfigurationError("INVALID_LIQUIDITY_RETRY_POLICY") from exc
        if (
            receipt.get("policy_id") != LIQUIDITY_RETRY_POLICY_ID
            or canonical_hash(receipt) != receipt_hash
        ):
            raise LiveConfigurationError("INVALID_LIQUIDITY_RETRY_POLICY")
        return {**receipt, "receipt_hash": receipt_hash}

    def ensure_liquidity_retry_policy_at_current_cursor(
        self, *, activated_at_ms: int, change_id: str
    ) -> dict[str, Any]:
        """Idempotently bind V2 to the current persisted forward cursor."""

        self.initialize()
        keys = {
            "liquidity_retry_policy_id",
            "liquidity_retry_effective_after_block",
            "liquidity_retry_activation_receipt_hash",
            "liquidity_retry_activation_receipt_json",
        }
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = {
                str(row["key"]): str(row["value"])
                for row in connection.execute(
                    """
                    SELECT key,value FROM config WHERE key IN (
                        'liquidity_retry_policy_id',
                        'liquidity_retry_effective_after_block',
                        'liquidity_retry_activation_receipt_hash',
                        'liquidity_retry_activation_receipt_json'
                    )
                    """
                ).fetchall()
            }
            if rows:
                if set(rows) != keys:
                    raise LiveConfigurationError(
                        "INCOMPLETE_LIQUIDITY_RETRY_POLICY"
                    )
                receipt = json.loads(
                    rows["liquidity_retry_activation_receipt_json"]
                )
                if (
                    rows["liquidity_retry_policy_id"]
                    != LIQUIDITY_RETRY_POLICY_ID
                    or canonical_hash(receipt)
                    != rows["liquidity_retry_activation_receipt_hash"]
                ):
                    raise LiveConfigurationError(
                        "INVALID_LIQUIDITY_RETRY_POLICY"
                    )
                return {
                    **receipt,
                    "receipt_hash": rows[
                        "liquidity_retry_activation_receipt_hash"
                    ],
                }
            cursor_row = connection.execute(
                "SELECT value FROM runtime_state WHERE key='last_processed_block'"
            ).fetchone()
            if cursor_row is None:
                raise LiveConfigurationError(
                    "LIQUIDITY_RETRY_REQUIRES_FORWARD_WATERMARK"
                )
            try:
                cursor = int(cursor_row["value"])
            except ValueError as exc:
                raise LiveConfigurationError(
                    "LIQUIDITY_RETRY_REQUIRES_FORWARD_WATERMARK"
                ) from exc
            receipt = {
                "policy_id": LIQUIDITY_RETRY_POLICY_ID,
                "effective_after_block": cursor,
                "activated_at_ms": int(activated_at_ms),
                "change_id": str(change_id),
                "deadline_ms": None,
                "eligible_attempt_states": ["NO_FILL", "PARTIAL_FILLED"],
                "historical_catch_up": False,
                "minimum_upscale": False,
                "unknown_repost": False,
            }
            receipt_hash = canonical_hash(receipt)
            for key, value in (
                ("liquidity_retry_policy_id", LIQUIDITY_RETRY_POLICY_ID),
                ("liquidity_retry_effective_after_block", str(cursor)),
                ("liquidity_retry_activation_receipt_hash", receipt_hash),
                (
                    "liquidity_retry_activation_receipt_json",
                    json.dumps(receipt, sort_keys=True, separators=(",", ":")),
                ),
            ):
                self._set_config(connection, key, value)
            return {**receipt, "receipt_hash": receipt_hash}

    def migrate_shared_wallet_migration_receipt_hash(
        self,
        *,
        expected_receipt_hash: str,
        receipt_history: Iterable[Mapping[str, Any]],
        changed_at_ms: int,
    ) -> str:
        """Advance only along the coordinator's verified append-only lineage."""

        expected = str(expected_receipt_hash or "").strip().lower()
        history = tuple(dict(receipt) for receipt in receipt_history)
        try:
            changed = int(changed_at_ms)
        except (TypeError, ValueError) as exc:
            raise LiveConfigurationError(
                "UNPROVEN_SHARED_WALLET_MIGRATION_RECEIPT_DESCENDANT"
            ) from exc
        hashes: list[str] = []
        generations: list[int] = []
        funders: list[str] = []
        if changed < 0 or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
            raise LiveConfigurationError(
                "UNPROVEN_SHARED_WALLET_MIGRATION_RECEIPT_DESCENDANT"
            )
        for receipt in history:
            claimed = str(receipt.get("migration_receipt_hash") or "").lower()
            unsigned = dict(receipt)
            unsigned.pop("migration_receipt_hash", None)
            try:
                generation = int(receipt["generation"])
            except (KeyError, TypeError, ValueError) as exc:
                raise LiveConfigurationError(
                    "UNPROVEN_SHARED_WALLET_MIGRATION_RECEIPT_DESCENDANT"
                ) from exc
            if (
                re.fullmatch(r"[0-9a-f]{64}", claimed) is None
                or canonical_hash(unsigned) != claimed
                or generation <= 0
            ):
                raise LiveConfigurationError(
                    "UNPROVEN_SHARED_WALLET_MIGRATION_RECEIPT_DESCENDANT"
                )
            hashes.append(claimed)
            generations.append(generation)
            funders.append(str(receipt.get("funder_address") or "").lower())
        if not history or hashes[-1] != expected:
            raise LiveConfigurationError(
                "UNPROVEN_SHARED_WALLET_MIGRATION_RECEIPT_DESCENDANT"
            )
        for index in range(1, len(history)):
            if (
                str(history[index].get("parent_migration_receipt_hash") or "")
                != hashes[index - 1]
                or generations[index] != generations[index - 1] + 1
                or funders[index] != funders[index - 1]
            ):
                raise LiveConfigurationError(
                    "UNPROVEN_SHARED_WALLET_MIGRATION_RECEIPT_DESCENDANT"
                )

        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT value FROM config "
                "WHERE key = 'shared_wallet_migration_receipt_hash'"
            ).fetchone()
            if row is None:
                self._set_config(
                    connection,
                    "shared_wallet_migration_receipt_hash",
                    expected,
                )
                return expected
            actual = str(row["value"]).strip().lower()
            if actual == expected:
                return actual
            try:
                actual_index = hashes.index(actual)
            except ValueError as exc:
                raise LiveConfigurationError(
                    "UNPROVEN_SHARED_WALLET_MIGRATION_RECEIPT_DESCENDANT"
                ) from exc
            if actual_index >= len(hashes) - 1:
                raise LiveConfigurationError(
                    "UNPROVEN_SHARED_WALLET_MIGRATION_RECEIPT_DESCENDANT"
                )
            reason = "VERIFIED_ADDITIVE_COORDINATOR_DESCENDANT"
            details = {
                "from_generation": generations[actual_index],
                "to_generation": generations[-1],
                "lineage_receipt_hashes": hashes[actual_index:],
                "cash_mutated": False,
                "positions_mutated": False,
                "action_receipts_mutated": False,
                "historical_ledger_rewritten": False,
            }
            change_id = canonical_hash(
                {
                    "config_key": "shared_wallet_migration_receipt_hash",
                    "previous_value": actual,
                    "new_value": expected,
                    "reason": reason,
                    "details": details,
                }
            )
            updated = connection.execute(
                "UPDATE config SET value = ? "
                "WHERE key = 'shared_wallet_migration_receipt_hash' "
                "AND value = ?",
                (expected, actual),
            )
            if updated.rowcount != 1:
                raise LiveConfigurationError(
                    "SHARED_WALLET_MIGRATION_RECEIPT_UPDATE_CONFLICT"
                )
            connection.execute(
                """
                INSERT INTO config_change_receipts(
                    change_id, config_key, previous_value, new_value,
                    reason, changed_at_ms, details_json
                ) VALUES(
                    ?, 'shared_wallet_migration_receipt_hash', ?, ?, ?, ?, ?
                )
                """,
                (
                    change_id,
                    actual,
                    expected,
                    reason,
                    changed,
                    json.dumps(details, sort_keys=True),
                ),
            )
        return expected

    def migrate_source_action_detection_contract(
        self, *, expected_contract: str, changed_at_ms: int
    ) -> str:
        """Upgrade CD90 to a verified full-wallet source feed without replay.

        The only permitted migration adds public-wallet rows that can be
        verified against the source wallet and current chain context.  It
        never treats a raw counterparty log as a source order, and it never
        rewrites historical receipts or submits a historical action.
        """

        expected = str(expected_contract)
        supported = {
            SOURCE_ACTION_DETECTION_CONTRACT_CHAIN_MAKER_ONLY,
            SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET,
        }
        if expected not in supported:
            raise LiveConfigurationError(
                f"UNSUPPORTED_SOURCE_ACTION_DETECTION_CONTRACT:{expected}"
            )
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value FROM config WHERE key = 'source_action_detection_contract'"
            ).fetchone()
            if row is None:
                self._set_config(
                    connection, "source_action_detection_contract", expected
                )
                return expected
            actual = str(row["value"])
            if actual == expected:
                return actual
            if (
                actual
                != SOURCE_ACTION_DETECTION_CONTRACT_CHAIN_MAKER_ONLY
                or expected
                != SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET
            ):
                raise LiveConfigurationError(
                    "IMMUTABLE_CONFIG_MISMATCH:source_action_detection_contract:"
                    f"{actual}:{expected}"
                )
            reason = "USER_AUTHORIZED_FULL_WALLET_SOURCE_ACTION_DISCOVERY"
            details = {
                "applies_to": "FORWARD_SOURCE_ACTIONS_ONLY",
                "chain_counterparty_log_promoted_directly": False,
                "historical_ledger_rewritten": False,
                "historical_orders_submitted": False,
            }
            change_id = canonical_hash(
                {
                    "config_key": "source_action_detection_contract",
                    "previous_value": actual,
                    "new_value": expected,
                    "reason": reason,
                }
            )
            changed = connection.execute(
                """
                UPDATE config SET value = ?
                WHERE key = 'source_action_detection_contract' AND value = ?
                """,
                (expected, actual),
            )
            if changed.rowcount != 1:
                raise LiveConfigurationError(
                    "SOURCE_ACTION_DETECTION_CONTRACT_MIGRATION_CONFLICT"
                )
            connection.execute(
                """
                INSERT OR IGNORE INTO config_change_receipts(
                    change_id, config_key, previous_value, new_value,
                    reason, changed_at_ms, details_json
                ) VALUES(?, 'source_action_detection_contract', ?, ?, ?, ?, ?)
                """,
                (
                    change_id,
                    actual,
                    expected,
                    reason,
                    int(changed_at_ms),
                    json.dumps(details, sort_keys=True),
                ),
            )
        return expected

    def migrate_legacy_minimum_size_policy(
        self, *, expected_policy: str, changed_at_ms: int
    ) -> str:
        """Record a forward-only, auditable minimum-size-policy transition.

        The authorized CD90 transition from skip to upsize affects only a new
        BUY action's initial target.  Neither direction can rebase the fixed
        share scale or rewrite a target, fill, position, or historical receipt.
        """

        expected = str(expected_policy)
        if expected not in MINIMUM_SIZE_POLICIES:
            raise LiveConfigurationError(
                f"UNSUPPORTED_MINIMUM_SIZE_POLICY:{expected}"
            )
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value FROM config WHERE key = 'minimum_size_policy'"
            ).fetchone()
            if row is None:
                self._set_config(connection, "minimum_size_policy", expected)
                return expected
            actual = str(row["value"])
            if actual == expected:
                return actual
            transition = {
                (
                    MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM,
                    MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
                ): {
                    "reason": "P0_FIXED_SHARE_SCALE_NO_MINIMUM_UPSCALE",
                    "details": {
                        "applies_to": "FUTURE_SOURCE_ACTIONS_ONLY",
                        "fixed_share_scale_changed": False,
                        "historical_ledger_rewritten": False,
                        "open_positions_changed": False,
                    },
                },
                (
                    MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
                    MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM,
                ): {
                    "reason": "USER_AUTHORIZED_MINIMUM_UPSCALE_FOR_FUTURE_BUYS",
                    "details": {
                        "applies_to": "FUTURE_SOURCE_ACTIONS_ONLY",
                        "fixed_share_scale_changed": False,
                        "historical_ledger_rewritten": False,
                        "open_positions_changed": False,
                        "upscale_scope": "INITIAL_BUY_TARGET_ONLY",
                    },
                },
            }.get((actual, expected))
            if transition is None:
                raise LiveConfigurationError(
                    "IMMUTABLE_CONFIG_MISMATCH:minimum_size_policy:"
                    f"{actual}:{expected}"
                )
            reason = str(transition["reason"])
            change_id = canonical_hash(
                {
                    "config_key": "minimum_size_policy",
                    "previous_value": actual,
                    "new_value": expected,
                    "reason": reason,
                }
            )
            changed = connection.execute(
                """
                UPDATE config SET value = ?
                WHERE key = 'minimum_size_policy' AND value = ?
                """,
                (expected, actual),
            )
            if changed.rowcount != 1:
                raise LiveConfigurationError(
                    "MINIMUM_SIZE_POLICY_MIGRATION_CONFLICT"
                )
            connection.execute(
                """
                INSERT OR IGNORE INTO config_change_receipts(
                    change_id, config_key, previous_value, new_value,
                    reason, changed_at_ms, details_json
                ) VALUES(?, 'minimum_size_policy', ?, ?, ?, ?, ?)
                """,
                (
                    change_id,
                    actual,
                    expected,
                    reason,
                    int(changed_at_ms),
                    json.dumps(dict(transition["details"]), sort_keys=True),
                ),
            )
        return expected

    def migrate_future_profile_scope(
        self,
        *,
        expected_scope: str,
        allowed_previous_scope: str,
        changed_at_ms: int,
        effective_after_block: int | None,
    ) -> str:
        """Change only the scope applied to future source actions.

        Frozen historical metadata and transitions remain immutable.  In
        particular, actions excluded by the former scope are never rebound or
        repriced after this migration.
        """

        expected = str(expected_scope).strip()
        previous = str(allowed_previous_scope).strip()
        if not expected or not previous:
            raise LiveConfigurationError("INVALID_PROFILE_SCOPE_MIGRATION")
        reason = "P0_FULL_SOURCE_WALLET_ACTION_FIDELITY"
        details = {
            "effective_after_block": (
                None
                if effective_after_block is None
                else int(effective_after_block)
            ),
            "historical_excluded_actions_replayed": False,
            "fixed_share_scale_changed": False,
        }
        change_id = canonical_hash(
            {
                "config_key": "profile_scope",
                "previous_value": previous,
                "new_value": expected,
                "reason": reason,
                "details": details,
            }
        )
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT value FROM config WHERE key = 'profile_scope'"
            ).fetchone()
            if row is None:
                self._set_config(connection, "profile_scope", expected)
                return expected
            actual = str(row["value"])
            if actual == expected:
                return actual
            if actual != previous:
                raise LiveConfigurationError(
                    "IMMUTABLE_CONFIG_MISMATCH:profile_scope:"
                    f"{actual}:{expected}"
                )
            changed = connection.execute(
                "UPDATE config SET value = ? "
                "WHERE key = 'profile_scope' AND value = ?",
                (expected, previous),
            )
            if changed.rowcount != 1:
                raise LiveConfigurationError("PROFILE_SCOPE_MIGRATION_CONFLICT")
            connection.execute(
                """
                INSERT OR IGNORE INTO config_change_receipts(
                    change_id, config_key, previous_value, new_value,
                    reason, changed_at_ms, details_json
                ) VALUES(?, 'profile_scope', ?, ?, ?, ?, ?)
                """,
                (
                    change_id,
                    previous,
                    expected,
                    reason,
                    int(changed_at_ms),
                    json.dumps(details, sort_keys=True),
                ),
            )
        return expected

    def initialize_scale_once(
        self,
        *,
        allocation_usd: Decimal,
        source_open_position_value_usd: Decimal,
        observed_at_ms: int,
    ) -> Decimal:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value FROM config WHERE key = 'fixed_share_scale'"
            ).fetchone()
            if row is not None:
                account = connection.execute(
                    "SELECT singleton FROM account_state WHERE singleton = 1"
                ).fetchone()
                if account is None:
                    receipt_count = connection.execute(
                        "SELECT COUNT(*) AS count FROM action_receipts"
                    ).fetchone()
                    if int(receipt_count["count"]) > 0:
                        raise LiveConfigurationError(
                            "MISSING_ACCOUNT_STATE_WITH_ACTION_RECEIPTS"
                        )
                    stored_allocation = connection.execute(
                        "SELECT value FROM config WHERE key = 'allocation_usd'"
                    ).fetchone()
                    if stored_allocation is None:
                        raise LiveConfigurationError(
                            "MISSING_ALLOCATION_FOR_LOCKED_SCALE"
                        )
                    try:
                        initial_cash = Decimal(str(stored_allocation["value"]))
                    except (InvalidOperation, ValueError) as exc:
                        raise LiveConfigurationError(
                            "INVALID_ALLOCATION_FOR_LOCKED_SCALE"
                        ) from exc
                    if initial_cash <= ZERO:
                        raise LiveConfigurationError(
                            "NONPOSITIVE_ALLOCATION_FOR_LOCKED_SCALE"
                        )
                    connection.execute(
                        """
                        INSERT INTO account_state(
                            singleton, initial_capital_usd, cash_usd,
                            realized_pnl_usd, fees_usd
                        ) VALUES(1, ?, ?, '0', '0')
                        """,
                        (str(initial_cash), str(initial_cash)),
                    )
                return Decimal(str(row["value"]))
            scale = derive_fixed_share_scale(
                allocation_usd=allocation_usd,
                source_open_position_value_usd=source_open_position_value_usd,
            )
            self._set_config(connection, "allocation_usd", allocation_usd)
            self._set_config(
                connection,
                "source_open_position_value_usd",
                source_open_position_value_usd,
            )
            self._set_config(connection, "source_sleeve_observed_at_ms", observed_at_ms)
            self._set_config(connection, "fixed_share_scale", scale)
            self._set_config(connection, "scale_basis", "OBSERVABLE_OPEN_POSITION_SLEEVE")
            account = connection.execute(
                "SELECT singleton FROM account_state WHERE singleton = 1"
            ).fetchone()
            if account is None:
                connection.execute(
                    """
                    INSERT INTO account_state(
                        singleton, initial_capital_usd, cash_usd,
                        realized_pnl_usd, fees_usd
                    ) VALUES(1, ?, ?, '0', '0')
                    """,
                    (str(allocation_usd), str(allocation_usd)),
                )
            return scale

    def initialize_explicit_fixed_scale_once(
        self,
        *,
        allocation_usd: Decimal,
        fixed_share_scale: Decimal,
        scale_basis: str,
        observed_at_ms: int,
    ) -> dict[str, Any]:
        """Freeze a user-authorized scale without inventing a source value."""

        try:
            allocation = Decimal(str(allocation_usd))
            scale = Decimal(str(fixed_share_scale))
            observed = int(observed_at_ms)
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise LiveConfigurationError(
                "INVALID_EXPLICIT_FIXED_SCALE_INITIALIZATION"
            ) from exc
        basis = str(scale_basis).strip()
        if (
            not allocation.is_finite()
            or allocation < ZERO
            or not scale.is_finite()
            or scale <= ZERO
            or observed < 0
            or not basis
        ):
            raise LiveConfigurationError(
                "INVALID_EXPLICIT_FIXED_SCALE_INITIALIZATION"
            )
        requested = {
            "allocation_usd": str(allocation),
            "fixed_share_scale": str(scale),
            "observed_at_ms": observed,
            "scale_basis": basis,
        }

        self.initialize()
        with self.connect() as connection:
            stored_rows = {
                str(row["key"]): str(row["value"])
                for row in connection.execute(
                    """
                    SELECT key, value FROM config
                    WHERE key IN (
                        'allocation_usd',
                        'fixed_share_scale',
                        'scale_basis',
                        'source_sleeve_observed_at_ms',
                        'source_open_position_value_usd'
                    )
                    """
                )
            }
            if "fixed_share_scale" in stored_rows:
                stored = {
                    "allocation_usd": stored_rows.get("allocation_usd"),
                    "fixed_share_scale": stored_rows.get("fixed_share_scale"),
                    "observed_at_ms": int(
                        stored_rows.get("source_sleeve_observed_at_ms", "-1")
                    ),
                    "scale_basis": stored_rows.get("scale_basis"),
                }
                if (
                    stored != requested
                    or "source_open_position_value_usd" in stored_rows
                ):
                    raise LiveConfigurationError(
                        "EXPLICIT_FIXED_SCALE_INITIALIZATION_MISMATCH"
                    )
                account = connection.execute(
                    "SELECT * FROM account_state WHERE singleton = 1"
                ).fetchone()
                if account is None or (
                    Decimal(str(account["initial_capital_usd"])) != allocation
                ):
                    raise LiveConfigurationError(
                        "EXPLICIT_FIXED_SCALE_INITIALIZATION_MISMATCH"
                    )
                return requested

            if stored_rows:
                raise LiveConfigurationError(
                    "EXPLICIT_FIXED_SCALE_INITIALIZATION_MISMATCH"
                )
            existing_account = connection.execute(
                "SELECT singleton FROM account_state WHERE singleton = 1"
            ).fetchone()
            action_count = connection.execute(
                "SELECT COUNT(*) AS count FROM action_receipts"
            ).fetchone()
            if existing_account is not None or int(action_count["count"]) != 0:
                raise LiveConfigurationError(
                    "EXPLICIT_FIXED_SCALE_REQUIRES_EMPTY_LEDGER"
                )
            self._set_config(connection, "allocation_usd", allocation)
            self._set_config(connection, "fixed_share_scale", scale)
            self._set_config(connection, "scale_basis", basis)
            self._set_config(
                connection, "source_sleeve_observed_at_ms", observed
            )
            connection.execute(
                """
                INSERT INTO account_state(
                    singleton, initial_capital_usd, cash_usd,
                    realized_pnl_usd, fees_usd
                ) VALUES(1, ?, ?, '0', '0')
                """,
                (str(allocation), str(allocation)),
            )
        return requested

    def fixed_share_scale(self) -> Decimal:
        value = self.config("fixed_share_scale")
        if value is None:
            raise ScaleInputError("fixed share scale has not been initialized")
        return Decimal(value)

    def latest_scale_rebase(self) -> dict[str, Any] | None:
        """Return the last immutable future-action scale-change receipt."""

        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT change_id, previous_scale, new_scale, requested_multiplier,
                       effective_after_block, resume_from_block, requested_at_ms, prior_scale_basis,
                       resulting_scale_basis, details_json
                FROM fixed_share_scale_rebase_receipts
                ORDER BY requested_at_ms DESC, change_id DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return {
            "change_id": str(row["change_id"]),
            "previous_scale": str(row["previous_scale"]),
            "new_scale": str(row["new_scale"]),
            "requested_multiplier": str(row["requested_multiplier"]),
            "effective_after_block": int(row["effective_after_block"]),
            "resume_from_block": int(row["resume_from_block"]),
            "requested_at_ms": int(row["requested_at_ms"]),
            "prior_scale_basis": str(row["prior_scale_basis"]),
            "resulting_scale_basis": str(row["resulting_scale_basis"]),
            "details": json.loads(str(row["details_json"])),
        }

    def arm_planned_operator_resume(
        self,
        *,
        resume_from_block: int,
        change_id: str,
        reason: str,
        armed_at_ms: int,
    ) -> dict[str, Any]:
        """Arm one controlled restart to resume exactly from the live cursor."""

        normalized_change_id = str(change_id).strip()
        normalized_reason = str(reason).strip()
        if not normalized_change_id or not normalized_reason:
            raise LiveConfigurationError("INVALID_OPERATOR_PLANNED_RESUME")
        try:
            resume_block = int(resume_from_block)
            armed_at = int(armed_at_ms)
        except (TypeError, ValueError) as exc:
            raise LiveConfigurationError("INVALID_OPERATOR_PLANNED_RESUME") from exc
        if resume_block < 0 or armed_at < 0:
            raise LiveConfigurationError("INVALID_OPERATOR_PLANNED_RESUME")

        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "SELECT value FROM runtime_state WHERE key = 'last_processed_block'"
            ).fetchone()
            if cursor is None or int(cursor["value"]) != resume_block:
                raise LiveConfigurationError(
                    "OPERATOR_PLANNED_RESUME_CURSOR_MISMATCH"
                )
            active = connection.execute(
                "SELECT COUNT(*) AS count FROM order_reservations WHERE active = 1"
            ).fetchone()
            if active is not None and int(active["count"]) != 0:
                raise LiveConfigurationError(
                    "ACTIVE_ORDER_RESERVATIONS_BLOCK_OPERATOR_PLANNED_RESUME"
                )
            unfinished = connection.execute(
                """
                SELECT t.status
                FROM action_transitions AS t
                WHERE t.id = (
                    SELECT latest.id
                    FROM action_transitions AS latest
                    WHERE latest.action_id = t.action_id
                    ORDER BY latest.id DESC
                    LIMIT 1
                )
                -- These states either are terminal or have an explicit,
                -- reservation-free retry path that must survive a controlled
                -- release.  Unknown/submitted/internal-invariant states remain
                -- blocked even if a corrupt ledger lost its reservation.
                AND t.status NOT IN (
                    'FILLED', 'PARTIAL', 'SKIPPED', 'ERROR', 'ERROR_INTERNAL',
                    'EXTERNAL_UNFILLABLE', 'SUPERSEDED_UNFILLED',
                    'EXPIRED_RETRY_WINDOW',
                    'PENDING_LIQUIDITY', 'PARTIAL_PENDING', 'PENDING_CAPITAL',
                    'PENDING_MINIMUM_UNWIND', 'PENDING_MINIMUM_REMAINDER',
                    'PENDING_EXTERNAL_RETRY', 'PENDING_METADATA',
                    'PENDING_CAUSAL_ORDER'
                )
                -- OBSERVED is safe only before any submission attempt exists.
                -- The candidate runtime has an explicit restart recovery path
                -- for this exact pre-side-effect state.
                AND NOT (
                    t.status = 'OBSERVED'
                    AND NOT EXISTS (
                        SELECT 1 FROM submission_attempts AS attempt
                        WHERE attempt.action_id = t.action_id
                    )
                )
                ORDER BY t.id
                LIMIT 1
                """
            ).fetchone()
            if unfinished is not None:
                raise LiveConfigurationError(
                    "NONTERMINAL_ACTIONS_BLOCK_OPERATOR_PLANNED_RESUME:"
                    + str(unfinished["status"])
                )
            closed_unplanned_observed = (
                self._close_pre_release_unplanned_observed_on_connection(
                    connection,
                    effective_after_block=resume_block,
                    operator_resume_change_id=normalized_change_id,
                    changed_at_ms=armed_at,
                )
            )
            legacy_counterparty_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM action_receipts
                    WHERE block_number <= ?
                      AND LOWER(source_role) NOT IN ('maker', 'verified_public_wallet')
                    """,
                    (resume_block,),
                ).fetchone()["count"]
            )
            pending = connection.execute(
                "SELECT value FROM runtime_state WHERE key = 'operator_planned_resume_from_block'"
            ).fetchone()
            superseded_change_id: str | None = None
            if pending is not None and str(pending["value"]).strip():
                existing_change = connection.execute(
                    "SELECT value FROM runtime_state WHERE key = 'operator_planned_resume_change_id'"
                ).fetchone()
                existing_reason = connection.execute(
                    "SELECT value FROM runtime_state WHERE key = 'operator_planned_resume_reason'"
                ).fetchone()
                if (
                    int(pending["value"]) == resume_block
                    and existing_change is not None
                    and str(existing_change["value"]) == normalized_change_id
                    and existing_reason is not None
                    and str(existing_reason["value"]) == normalized_reason
                ):
                    return {
                        "resume_from_block": resume_block,
                        "change_id": normalized_change_id,
                        "reason": normalized_reason,
                        "armed_at_ms": int(
                            connection.execute(
                                "SELECT value FROM runtime_state WHERE key = 'operator_planned_resume_started_at_ms'"
                            ).fetchone()["value"]
                        ),
                        "idempotent": True,
                    }
                existing_state = connection.execute(
                    "SELECT value FROM runtime_state WHERE key = 'operator_planned_resume_state'"
                ).fetchone()
                existing_started = connection.execute(
                    "SELECT value FROM runtime_state WHERE key = 'operator_planned_resume_started_at_ms'"
                ).fetchone()
                if (
                    int(pending["value"]) != resume_block
                    or existing_change is None
                    or existing_reason is None
                    or existing_state is None
                    or str(existing_state["value"]) != "PENDING"
                    or existing_started is None
                ):
                    raise LiveConfigurationError(
                        "OPERATOR_PLANNED_RESUME_ALREADY_ARMED"
                    )
                prior_armed_at = int(existing_started["value"])
                attempts_after_prior_arm = int(
                    connection.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM submission_attempts
                        WHERE created_at_ms >= ?
                        """,
                        (prior_armed_at,),
                    ).fetchone()["count"]
                )
                if attempts_after_prior_arm:
                    raise LiveConfigurationError(
                        "OPERATOR_PLANNED_RESUME_ALREADY_ARMED_WITH_SIDE_EFFECTS"
                    )
                superseded_change_id = str(existing_change["value"])
                takeover_reason = (
                    "SUPERSEDE_STALLED_NO_SIDE_EFFECT_OPERATOR_RESUME"
                )
                takeover_details = {
                    "resume_from_block": resume_block,
                    "prior_reason": str(existing_reason["value"]),
                    "prior_armed_at_ms": prior_armed_at,
                    "submission_attempts_after_prior_arm": 0,
                    "active_order_reservations": 0,
                    "historical_ledger_rewritten": False,
                    "orders_submitted_by_takeover": False,
                }
                takeover_receipt_id = canonical_hash(
                    {
                        "config_key": "operator_planned_resume",
                        "previous_value": superseded_change_id,
                        "new_value": normalized_change_id,
                        "reason": takeover_reason,
                        "details": takeover_details,
                    }
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO config_change_receipts(
                        change_id, config_key, previous_value, new_value,
                        reason, changed_at_ms, details_json
                    ) VALUES(?, 'operator_planned_resume', ?, ?, ?, ?, ?)
                    """,
                    (
                        takeover_receipt_id,
                        superseded_change_id,
                        normalized_change_id,
                        takeover_reason,
                        armed_at,
                        json.dumps(takeover_details, sort_keys=True),
                    ),
                )
            for key, value in {
                "operator_planned_resume_from_block": resume_block,
                "operator_planned_resume_change_id": normalized_change_id,
                "operator_planned_resume_reason": normalized_reason,
                "operator_planned_resume_started_at_ms": armed_at,
                "operator_planned_resume_state": "PENDING",
                "forward_only_release_boundary_block": resume_block,
                "pre_release_unplanned_observed_closed_count": (
                    closed_unplanned_observed
                ),
                "legacy_counterparty_order_receipt_count": (
                    legacy_counterparty_count
                ),
            }.items():
                connection.execute(
                    """
                    INSERT INTO runtime_state(key, value) VALUES(?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (key, str(value)),
                )
        return {
            "resume_from_block": resume_block,
            "change_id": normalized_change_id,
            "reason": normalized_reason,
            "armed_at_ms": armed_at,
            "idempotent": False,
            **(
                {"superseded_change_id": superseded_change_id}
                if superseded_change_id is not None
                else {}
            ),
        }

    def _close_pre_release_unplanned_observed_on_connection(
        self,
        connection: sqlite3.Connection,
        *,
        effective_after_block: int,
        operator_resume_change_id: str,
        changed_at_ms: int,
    ) -> int:
        """Close only old, unplanned observations without executing them.

        Explicit retry states remain intact across a controlled release so a
        temporarily unfillable basket leg can resume on a later book or block.
        This is an accounting transition only.  Cash, positions, fills,
        submission attempts, and historical receipts are not rewritten.
        Unknown or submitted attempts never reach this method because the
        planned-resume safety gate blocks them first.
        """

        unplanned_observed = ("OBSERVED",)
        placeholders = ",".join("?" for _ in unplanned_observed)
        rows = connection.execute(
            f"""
            SELECT a.action_id, latest.status AS prior_status,
                   COALESCE(t.cumulative_filled_quantity, '0') AS filled
            FROM action_receipts AS a
            JOIN action_transitions AS latest
              ON latest.id = (
                  SELECT id FROM action_transitions
                  WHERE action_id = a.action_id
                  ORDER BY id DESC LIMIT 1
              )
            LEFT JOIN action_targets AS t ON t.action_id = a.action_id
            WHERE LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
              AND a.block_number <= ?
              AND latest.status IN ({placeholders})
              AND NOT EXISTS (
                  SELECT 1 FROM order_reservations AS reservation
                  WHERE reservation.action_id = a.action_id
                    AND reservation.active = 1
              )
              AND NOT EXISTS (
                  SELECT 1 FROM submission_attempts AS attempt
                  WHERE attempt.action_id = a.action_id
                    AND attempt.state IN (
                        'SUBMIT_STARTED', 'SUBMITTED_UNRECONCILED',
                        'UNKNOWN_SUBMISSION'
                    )
              )
            ORDER BY a.block_number, a.source_log_index, a.action_id
            """,
            (int(effective_after_block), *unplanned_observed),
        ).fetchall()
        for row in rows:
            filled = Decimal(str(row["filled"]))
            terminal = "PARTIAL" if filled > ZERO else "EXTERNAL_UNFILLABLE"
            reason = "PRE_RELEASE_ACTION_NOT_REPLAYED_FORWARD_ONLY"
            connection.execute(
                """
                UPDATE action_targets
                SET state = ?, reason = ?, updated_at_ms = ?
                WHERE action_id = ?
                """,
                (
                    terminal,
                    reason,
                    int(changed_at_ms),
                    str(row["action_id"]),
                ),
            )
            connection.execute(
                """
                INSERT INTO action_transitions(
                    action_id, status, reason, created_at_ms, details_json
                ) VALUES(?, ?, ?, ?, ?)
                """,
                (
                    str(row["action_id"]),
                    terminal,
                    reason,
                    int(changed_at_ms),
                    _receipt_json(
                        {
                            "effective_after_block": int(
                                effective_after_block
                            ),
                            "historical_action_executed": False,
                            "operator_resume_change_id": str(
                                operator_resume_change_id
                            ),
                            "prior_status": str(row["prior_status"]),
                        }
                    ),
                ),
            )
        if rows:
            details = {
                "effective_after_block": int(effective_after_block),
                "historical_action_executed": False,
                "closed_action_count": len(rows),
            }
            receipt_id = canonical_hash(
                {
                    "config_key": "forward_only_release_boundary",
                    "previous_value": "",
                    "new_value": str(effective_after_block),
                    "reason": "NO_HISTORICAL_MISSED_ACTION_REPLAY",
                    "details": details,
                }
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO config_change_receipts(
                    change_id, config_key, previous_value, new_value,
                    reason, changed_at_ms, details_json
                ) VALUES(?, 'forward_only_release_boundary', '', ?, ?, ?, ?)
                """,
                (
                    receipt_id,
                    str(effective_after_block),
                    "NO_HISTORICAL_MISSED_ACTION_REPLAY",
                    int(changed_at_ms),
                    json.dumps(details, sort_keys=True),
                ),
            )
        return len(rows)

    def rebase_fixed_share_scale(
        self,
        *,
        multiplier: Decimal,
        change_id: str,
        effective_after_block: int,
        resume_from_block: int,
        requested_at_ms: int,
    ) -> dict[str, Any]:
        """Atomically rebase only future source-action sizing.

        The historical ledger, current inventory, original source-sleeve
        observation and allocated capital remain untouched.  A caller must
        stop the live service first, hold the runtime lock, and use the
        persisted chain cursor as the resume point, and apply the new scale
        only to source blocks strictly after ``effective_after_block``.
        """

        normalized_change_id = str(change_id).strip()
        if not normalized_change_id:
            raise LiveConfigurationError("MISSING_SCALE_REBASE_CHANGE_ID")
        try:
            requested_multiplier = Decimal(str(multiplier))
        except (InvalidOperation, ValueError) as exc:
            raise LiveConfigurationError("INVALID_SCALE_REBASE_MULTIPLIER") from exc
        if not requested_multiplier.is_finite() or requested_multiplier <= ZERO:
            raise LiveConfigurationError("INVALID_SCALE_REBASE_MULTIPLIER")
        try:
            effective_block = int(effective_after_block)
            resume_block = int(resume_from_block)
            requested_at = int(requested_at_ms)
        except (TypeError, ValueError) as exc:
            raise LiveConfigurationError("INVALID_SCALE_REBASE_BOUNDARY") from exc
        if effective_block < 0 or resume_block < 0 or requested_at < 0:
            raise LiveConfigurationError("INVALID_SCALE_REBASE_BOUNDARY")
        if resume_block > effective_block:
            raise LiveConfigurationError("INVALID_SCALE_REBASE_BOUNDARY")

        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT change_id, previous_scale, new_scale, requested_multiplier,
                       effective_after_block, resume_from_block, requested_at_ms
                FROM fixed_share_scale_rebase_receipts
                WHERE change_id = ?
                """,
                (normalized_change_id,),
            ).fetchone()
            if existing is not None:
                if (
                    Decimal(str(existing["requested_multiplier"]))
                    != requested_multiplier
                    or int(existing["effective_after_block"]) != effective_block
                    or int(existing["resume_from_block"]) != resume_block
                ):
                    raise LiveConfigurationError(
                        "SCALE_REBASE_CHANGE_ID_PARAMETER_MISMATCH"
                    )
                current_row = connection.execute(
                    "SELECT value FROM config WHERE key = 'fixed_share_scale'"
                ).fetchone()
                if current_row is None:
                    raise LiveConfigurationError("SCALE_REBASE_CONFIG_DRIFT")
                current_scale = Decimal(str(current_row["value"]))
                if current_scale != Decimal(str(existing["new_scale"])):
                    raise LiveConfigurationError("SCALE_REBASE_CONFIG_DRIFT")
                return {
                    "change_id": normalized_change_id,
                    "previous_scale": Decimal(str(existing["previous_scale"])),
                    "new_scale": Decimal(str(existing["new_scale"])),
                    "multiplier": Decimal(str(existing["requested_multiplier"])),
                    "effective_after_block": int(existing["effective_after_block"]),
                    "resume_from_block": int(existing["resume_from_block"]),
                    "requested_at_ms": int(existing["requested_at_ms"]),
                    "idempotent": True,
                }

            active = connection.execute(
                "SELECT COUNT(*) AS count FROM order_reservations WHERE active = 1"
            ).fetchone()
            if active is not None and int(active["count"]) != 0:
                raise LiveConfigurationError(
                    "ACTIVE_ORDER_RESERVATIONS_BLOCK_SCALE_REBASE"
                )
            unfinished = connection.execute(
                """
                SELECT t.action_id, t.status
                FROM action_transitions AS t
                WHERE t.id = (
                    SELECT latest.id
                    FROM action_transitions AS latest
                    WHERE latest.action_id = t.action_id
                    ORDER BY latest.id DESC
                    LIMIT 1
                )
                AND t.status NOT IN ('FILLED', 'PARTIAL', 'SKIPPED')
                ORDER BY t.id
                LIMIT 1
                """
            ).fetchone()
            if unfinished is not None:
                raise LiveConfigurationError(
                    "NONTERMINAL_ACTIONS_BLOCK_SCALE_REBASE:"
                    + str(unfinished["status"])
                )

            prior_rebase = connection.execute(
                """
                SELECT effective_after_block
                FROM fixed_share_scale_rebase_receipts
                ORDER BY effective_after_block DESC, requested_at_ms DESC
                LIMIT 1
                """
            ).fetchone()
            if (
                prior_rebase is not None
                and effective_block <= int(prior_rebase["effective_after_block"])
            ):
                raise LiveConfigurationError("NONMONOTONIC_SCALE_REBASE_BOUNDARY")

            scale_row = connection.execute(
                "SELECT value FROM config WHERE key = 'fixed_share_scale'"
            ).fetchone()
            if scale_row is None:
                raise ScaleInputError("fixed share scale has not been initialized")
            try:
                previous_scale = Decimal(str(scale_row["value"]))
            except (InvalidOperation, ValueError) as exc:
                raise LiveConfigurationError("INVALID_LOCKED_FIXED_SHARE_SCALE") from exc
            new_scale = previous_scale * requested_multiplier
            if (
                not previous_scale.is_finite()
                or previous_scale <= ZERO
                or not new_scale.is_finite()
                or new_scale <= ZERO
            ):
                raise LiveConfigurationError("INVALID_SCALE_REBASE_RESULT")

            basis_row = connection.execute(
                "SELECT value FROM config WHERE key = 'scale_basis'"
            ).fetchone()
            prior_basis = (
                "UNSPECIFIED" if basis_row is None else str(basis_row["value"])
            )
            resulting_basis = "USER_AUTHORIZED_FIXED_SHARE_SCALE_MULTIPLIER"
            allocation_row = connection.execute(
                "SELECT value FROM config WHERE key = 'allocation_usd'"
            ).fetchone()
            sleeve_row = connection.execute(
                "SELECT value FROM config WHERE key = 'source_open_position_value_usd'"
            ).fetchone()
            details = {
                "allocation_usd_unchanged": (
                    None if allocation_row is None else str(allocation_row["value"])
                ),
                "applies_to": "FUTURE_SOURCE_ACTIONS_AFTER_EFFECTIVE_BLOCK",
                "existing_positions_changed": False,
                "historical_ledger_rewritten": False,
                "source_open_position_value_recomputed": False,
                "source_open_position_value_usd_unchanged": (
                    None if sleeve_row is None else str(sleeve_row["value"])
                ),
            }
            changed = connection.execute(
                "UPDATE config SET value = ? WHERE key = 'fixed_share_scale'",
                (str(new_scale),),
            )
            if changed.rowcount != 1:
                raise LiveConfigurationError("MISSING_LOCKED_FIXED_SHARE_SCALE")
            connection.execute(
                """
                INSERT INTO config(key, value) VALUES('scale_basis', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (resulting_basis,),
            )
            connection.execute(
                """
                INSERT INTO fixed_share_scale_rebase_receipts(
                    change_id, previous_scale, new_scale, requested_multiplier,
                    effective_after_block, resume_from_block, requested_at_ms, prior_scale_basis,
                    resulting_scale_basis, details_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_change_id,
                    str(previous_scale),
                    str(new_scale),
                    str(requested_multiplier),
                    effective_block,
                    resume_block,
                    requested_at,
                    prior_basis,
                    resulting_basis,
                    _receipt_json(details),
                ),
            )
            for key, value in {
                "operator_planned_resume_from_block": resume_block,
                "operator_planned_resume_change_id": normalized_change_id,
                "operator_planned_resume_reason": "USER_AUTHORIZED_SCALE_REBASE",
                "operator_planned_resume_started_at_ms": requested_at,
                "operator_planned_resume_state": "PENDING",
            }.items():
                connection.execute(
                    """
                    INSERT INTO runtime_state(key, value) VALUES(?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (key, str(value)),
                )
            return {
                "change_id": normalized_change_id,
                "previous_scale": previous_scale,
                "new_scale": new_scale,
                "multiplier": requested_multiplier,
                "effective_after_block": effective_block,
                "resume_from_block": resume_block,
                "requested_at_ms": requested_at,
                "idempotent": False,
            }

    def fixed_share_scale_for_source_block(self, source_block: int) -> Decimal:
        """Return the immutable scale version applicable to one source block."""

        try:
            block = int(source_block)
        except (TypeError, ValueError) as exc:
            raise LiveConfigurationError("INVALID_SOURCE_BLOCK_FOR_SCALE") from exc
        if block < 0:
            raise LiveConfigurationError("INVALID_SOURCE_BLOCK_FOR_SCALE")
        self.initialize()
        with self.connect() as connection:
            upcoming = connection.execute(
                """
                SELECT previous_scale
                FROM fixed_share_scale_rebase_receipts
                WHERE effective_after_block >= ?
                ORDER BY effective_after_block ASC, requested_at_ms ASC
                LIMIT 1
                """,
                (block,),
            ).fetchone()
            if upcoming is not None:
                return Decimal(str(upcoming["previous_scale"]))
            current = connection.execute(
                "SELECT value FROM config WHERE key = 'fixed_share_scale'"
            ).fetchone()
        if current is None:
            raise ScaleInputError("fixed share scale has not been initialized")
        return Decimal(str(current["value"]))

    def position_quantity(self, token_id: str) -> Decimal:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT quantity FROM positions WHERE token_id = ?", (str(token_id),)
            ).fetchone()
        return ZERO if row is None else Decimal(str(row["quantity"]))

    def available_position_quantity(self, token_id: str) -> Decimal:
        held = self.position_quantity(token_id)
        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT quantity
                FROM order_reservations
                WHERE token_id = ? AND side = 'SELL' AND active = 1
                """,
                (str(token_id),),
            ).fetchall()
        reserved = sum((Decimal(str(row["quantity"])) for row in rows), ZERO)
        return held - reserved

    def frozen_causal_target_prefix_before(
        self, source: SourceAction
    ) -> dict[str, Any]:
        """Return one complete, terminal causal sizing prefix snapshot.

        Production callers hold the profile ``live.lock`` through
        ``_primary_runtime_lock`` and process a head synchronously.  The
        explicit read transaction below therefore pins one SQLite snapshot
        while the complete same-token prefix is validated and serialized;
        the hot-standby lock test proves a second daemon cannot write beside
        the primary.
        """

        terminal_states = (
            "SKIPPED",
            "FILLED",
            "PARTIAL",
            "ERROR",
            "ERROR_INTERNAL",
            "EXTERNAL_UNFILLABLE",
            "SUPERSEDED_UNFILLED",
            "EXPIRED_RETRY_WINDOW",
        )
        self.initialize()
        with self.connect() as connection:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            rows = connection.execute(
                """
                SELECT a.action_id, a.transaction_hash, a.token_id, a.side,
                       a.order_hash, a.source_timestamp, a.block_number,
                       a.source_log_index, t.proportional_quantity,
                       t.target_quantity, t.cumulative_filled_quantity,
                       COALESCE(t.state, '') AS target_state,
                       COALESCE(latest.status, '') AS latest_status
                FROM action_receipts AS a
                LEFT JOIN action_targets AS t ON t.action_id = a.action_id
                LEFT JOIN action_transitions AS latest
                  ON latest.id = (
                      SELECT transition.id
                      FROM action_transitions AS transition
                      WHERE transition.action_id = a.action_id
                      ORDER BY transition.id DESC LIMIT 1
                  )
                WHERE a.token_id = ?
                  AND LOWER(a.source_role) IN (
                      'maker', 'verified_public_wallet'
                  )
                  AND (
                      a.block_number, a.source_log_index, a.source_timestamp,
                      a.transaction_hash, a.token_id, a.side, a.order_hash,
                      a.action_id
                  ) < (?, ?, ?, ?, ?, ?, ?, ?)
                ORDER BY a.block_number, a.source_log_index,
                         a.source_timestamp, a.transaction_hash,
                         a.token_id, a.side, a.order_hash, a.action_id
                """,
                (
                    str(source.token_id),
                    int(source.block_number),
                    int(source.log_index),
                    int(source.source_timestamp),
                    str(source.transaction_hash).lower(),
                    str(source.token_id),
                    str(source.side).upper(),
                    str(source.order_hash).lower(),
                    str(source.action_id),
                ),
            ).fetchall()

        entries: list[dict[str, Any]] = []
        scaled_open_target = ZERO
        for row in rows:
            action_id = str(row["action_id"])
            if (
                str(row["target_state"]) not in terminal_states
                or str(row["latest_status"]) not in terminal_states
                or row["proportional_quantity"] is None
            ):
                raise LiveConfigurationError(
                    f"CAUSAL_TARGET_PREFIX_NOT_TERMINAL:{action_id}"
                )
            if str(row["target_state"]) != str(row["latest_status"]):
                raise LiveConfigurationError(
                    f"CAUSAL_TARGET_PREFIX_STATE_MISMATCH:{action_id}"
                )
            try:
                proportional_quantity = Decimal(
                    str(row["proportional_quantity"])
                )
                target_quantity = Decimal(str(row["target_quantity"]))
                cumulative_filled_quantity = Decimal(
                    str(row["cumulative_filled_quantity"])
                )
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise LiveConfigurationError(
                    f"INVALID_PRIOR_ACTION_TARGET_QUANTITY:{action_id}"
                ) from exc
            if (
                not proportional_quantity.is_finite()
                or proportional_quantity <= ZERO
                or not target_quantity.is_finite()
                or target_quantity <= ZERO
                or not cumulative_filled_quantity.is_finite()
                or cumulative_filled_quantity < ZERO
            ):
                raise LiveConfigurationError(
                    f"INVALID_PRIOR_ACTION_TARGET_QUANTITY:{action_id}"
                )
            side = str(row["side"]).upper()
            if side == "BUY":
                scaled_open_target += proportional_quantity
            elif side == "SELL":
                scaled_open_target = max(
                    scaled_open_target - proportional_quantity,
                    ZERO,
                )
            else:
                raise LiveConfigurationError(
                    f"INVALID_PRIOR_ACTION_SIDE:{action_id}"
                )
            entries.append(
                {
                    "action_id": action_id,
                    "block_number": int(row["block_number"]),
                    "source_log_index": int(row["source_log_index"]),
                    "source_timestamp": int(row["source_timestamp"]),
                    "transaction_hash": str(row["transaction_hash"]).lower(),
                    "token_id": str(row["token_id"]),
                    "side": side,
                    "order_hash": str(row["order_hash"]).lower(),
                    "proportional_quantity": str(proportional_quantity),
                    "target_quantity": str(target_quantity),
                    "cumulative_filled_quantity": str(
                        cumulative_filled_quantity
                    ),
                    "target_state": str(row["target_state"]),
                    "latest_status": str(row["latest_status"]),
                }
            )
        return {
            "scaled_open_target": scaled_open_target,
            "action_count": len(entries),
            "prefix_hash": canonical_hash({"actions": entries}),
        }

    def prior_unresolved_same_token_submission(
        self, source: SourceAction
    ) -> dict[str, Any] | None:
        """Return an earlier causal same-token submission still in flight."""

        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT a.action_id, a.side, a.block_number,
                       a.source_log_index,
                       (
                           SELECT latest.status
                           FROM action_transitions AS latest
                           WHERE latest.action_id = a.action_id
                           ORDER BY latest.id DESC LIMIT 1
                       ) AS latest_status
                FROM action_receipts AS a
                WHERE a.token_id = ?
                  AND LOWER(a.source_role) IN (
                      'maker', 'verified_public_wallet'
                  )
                  AND (
                      a.block_number, a.source_log_index, a.source_timestamp,
                      a.transaction_hash, a.token_id, a.side, a.order_hash,
                      a.action_id
                  ) < (?, ?, ?, ?, ?, ?, ?, ?)
                  AND (
                      EXISTS (
                          SELECT 1 FROM order_reservations AS reservation
                          WHERE reservation.action_id = a.action_id
                            AND reservation.active = 1
                      )
                      OR EXISTS (
                          SELECT 1 FROM submission_attempts AS attempt
                          WHERE attempt.action_id = a.action_id
                            AND attempt.state IN (
                                'SUBMIT_STARTED',
                                'SUBMITTED_UNRECONCILED',
                                'UNKNOWN_SUBMISSION'
                            )
                      )
                  )
                ORDER BY a.block_number DESC, a.source_log_index DESC,
                         a.source_timestamp DESC, a.transaction_hash DESC,
                         a.token_id DESC, a.side DESC, a.order_hash DESC,
                         a.action_id DESC
                LIMIT 1
                """,
                (
                    str(source.token_id),
                    int(source.block_number),
                    int(source.log_index),
                    int(source.source_timestamp),
                    str(source.transaction_hash).lower(),
                    str(source.token_id),
                    str(source.side).upper(),
                    str(source.order_hash).lower(),
                    str(source.action_id),
                ),
            ).fetchone()
        if row is None:
            return None
        return {
            "action_id": str(row["action_id"]),
            "side": str(row["side"]),
            "block_number": int(row["block_number"]),
            "source_log_index": int(row["source_log_index"]),
            "latest_status": str(row["latest_status"] or ""),
        }

    def prior_nonterminal_same_token_action(
        self, source: SourceAction
    ) -> dict[str, Any] | None:
        """Return an earlier causal action that cannot yet enter sizing.

        Metadata and restart recovery can finish out of order even though the
        immutable source receipts are ordered by block/log index.  A later
        same-token action must not calculate cumulative minimum-size credit
        until every earlier action has a terminal target; otherwise an earlier
        proportional BUY is omitted and the later fragment can manufacture an
        extra minimum-sized real order.
        """

        terminal_states = (
            "SKIPPED",
            "FILLED",
            "PARTIAL",
            "ERROR",
            "ERROR_INTERNAL",
            "EXTERNAL_UNFILLABLE",
            "SUPERSEDED_UNFILLED",
            "EXPIRED_RETRY_WINDOW",
        )
        placeholders = ",".join("?" for _ in terminal_states)
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                f"""
                SELECT a.action_id, a.side, a.block_number,
                       a.source_log_index,
                       COALESCE(latest.status, '') AS latest_status,
                       COALESCE(target.state, '') AS target_state
                FROM action_receipts AS a
                LEFT JOIN action_targets AS target
                  ON target.action_id = a.action_id
                LEFT JOIN action_transitions AS latest
                  ON latest.id = (
                      SELECT transition.id
                      FROM action_transitions AS transition
                      WHERE transition.action_id = a.action_id
                      ORDER BY transition.id DESC LIMIT 1
                  )
                WHERE a.token_id = ?
                  AND LOWER(a.source_role) IN (
                      'maker', 'verified_public_wallet'
                  )
                  AND (
                      a.block_number, a.source_log_index, a.source_timestamp,
                      a.transaction_hash, a.token_id, a.side, a.order_hash,
                      a.action_id
                  ) < (?, ?, ?, ?, ?, ?, ?, ?)
                  AND (
                      latest.id IS NULL
                      OR latest.status NOT IN ({placeholders})
                      OR (
                          target.action_id IS NOT NULL
                          AND target.state NOT IN ({placeholders})
                      )
                  )
                  AND NOT (
                      latest.status = 'PENDING_INTERNAL_INVARIANT'
                      AND latest.reason LIKE ?
                      AND NOT EXISTS(
                          SELECT 1 FROM order_reservations AS reservation
                          WHERE reservation.action_id = a.action_id
                            AND reservation.active = 1
                      )
                      AND NOT EXISTS(
                          SELECT 1 FROM submission_attempts AS attempt
                          WHERE attempt.action_id = a.action_id
                            AND attempt.state IN (
                                'SUBMIT_STARTED',
                                'SUBMITTED_UNRECONCILED',
                                'UNKNOWN_SUBMISSION'
                            )
                      )
                  )
                ORDER BY a.block_number DESC, a.source_log_index DESC,
                         a.source_timestamp DESC, a.transaction_hash DESC,
                         a.token_id DESC, a.side DESC, a.order_hash DESC,
                         a.action_id DESC
                LIMIT 1
                """,
                (
                    str(source.token_id),
                    int(source.block_number),
                    int(source.log_index),
                    int(source.source_timestamp),
                    str(source.transaction_hash).lower(),
                    str(source.token_id),
                    str(source.side).upper(),
                    str(source.order_hash).lower(),
                    str(source.action_id),
                    *terminal_states,
                    *terminal_states,
                    LEGACY_LOCAL_CASH_MISMATCH_PREFIX + "%",
                ),
            ).fetchone()
        if row is None:
            return None
        return {
            "action_id": str(row["action_id"]),
            "side": str(row["side"]),
            "block_number": int(row["block_number"]),
            "source_log_index": int(row["source_log_index"]),
            "latest_status": str(row["latest_status"]),
            "target_state": str(row["target_state"]),
        }

    @staticmethod
    def _validate_condition_mapping(
        *,
        token_id: str,
        condition_id: str,
        primary_token_id: str,
        secondary_token_id: str,
    ) -> tuple[str, str, str, str]:
        token = str(token_id).strip()
        condition = str(condition_id).strip().lower()
        primary = str(primary_token_id).strip()
        secondary = str(secondary_token_id).strip()
        if not token or not primary or not secondary or primary == secondary:
            raise LiveConfigurationError("INVALID_CONDITION_TOKEN_MAPPING")
        if token not in {primary, secondary}:
            raise LiveConfigurationError("TOKEN_NOT_IN_CONDITION_MAPPING")
        if not condition.startswith("0x") or len(condition) != 66:
            raise LiveConfigurationError("INVALID_CONDITION_ID")
        try:
            int(condition[2:], 16)
        except ValueError as exc:
            raise LiveConfigurationError("INVALID_CONDITION_ID") from exc
        return token, condition, primary, secondary

    def bind_condition_for_token(
        self,
        *,
        token_id: str,
        condition_id: str,
        primary_token_id: str,
        secondary_token_id: str,
        observed_at_ms: int,
    ) -> None:
        """Persist one immutable binary-market map for CD90 inventory."""

        _, condition, primary, secondary = self._validate_condition_mapping(
            token_id=token_id,
            condition_id=condition_id,
            primary_token_id=primary_token_id,
            secondary_token_id=secondary_token_id,
        )
        self.initialize()
        with self.connect() as connection:
            existing = connection.execute(
                """
                SELECT condition_id, primary_token_id, secondary_token_id
                FROM condition_mappings
                WHERE condition_id = ?
                   OR primary_token_id IN (?, ?)
                   OR secondary_token_id IN (?, ?)
                """,
                (condition, primary, secondary, primary, secondary),
            ).fetchone()
            if existing is not None:
                existing_tuple = (
                    str(existing["condition_id"]),
                    str(existing["primary_token_id"]),
                    str(existing["secondary_token_id"]),
                )
                expected_tuple = (condition, primary, secondary)
                if existing_tuple != expected_tuple:
                    raise LiveConfigurationError("IMMUTABLE_CONDITION_MAPPING_CONFLICT")
                return
            connection.execute(
                """
                INSERT INTO condition_mappings(
                    condition_id, primary_token_id, secondary_token_id, observed_at_ms
                ) VALUES(?, ?, ?, ?)
                """,
                (condition, primary, secondary, int(observed_at_ms)),
            )

    def correct_condition_mapping_order(
        self,
        *,
        condition_id: str,
        primary_token_id: str,
        secondary_token_id: str,
        created_at_ms: int,
        details: dict[str, Any],
    ) -> bool:
        """Auditably swap an existing binary token pair into official YES/NO order.

        No token may be introduced or removed.  This is intentionally narrower
        than replacing a condition mapping: it only corrects the ordering of
        the exact same two token IDs after official Gamma evidence is available.
        """

        _, condition, primary, secondary = self._validate_condition_mapping(
            token_id=primary_token_id,
            condition_id=condition_id,
            primary_token_id=primary_token_id,
            secondary_token_id=secondary_token_id,
        )
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT primary_token_id, secondary_token_id
                FROM condition_mappings
                WHERE condition_id = ?
                """,
                (condition,),
            ).fetchone()
            if row is None:
                raise LiveConfigurationError("MISSING_CONDITION_MAPPING_FOR_ORDER_CORRECTION")
            old_primary = str(row["primary_token_id"])
            old_secondary = str(row["secondary_token_id"])
            if {old_primary, old_secondary} != {primary, secondary}:
                raise LiveConfigurationError("CONDITION_MAPPING_TOKEN_SET_CHANGE_PROHIBITED")
            if (old_primary, old_secondary) == (primary, secondary):
                return False
            connection.execute(
                """
                UPDATE condition_mappings
                SET primary_token_id = ?, secondary_token_id = ?, observed_at_ms = ?
                WHERE condition_id = ?
                """,
                (primary, secondary, int(created_at_ms), condition),
            )
            self._append_redemption_transition(
                connection,
                condition_id=condition,
                state="MAPPING_ORDER_CORRECTED",
                reason="OFFICIAL_GAMMA_YES_NO_ORDER",
                created_at_ms=created_at_ms,
                details={
                    **details,
                    "old_primary_token_id": old_primary,
                    "old_secondary_token_id": old_secondary,
                    "new_primary_token_id": primary,
                    "new_secondary_token_id": secondary,
                },
            )
        return True

    def unmapped_position_token_ids(self) -> list[str]:
        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT p.token_id
                FROM positions AS p
                LEFT JOIN condition_mappings AS c
                  ON p.token_id = c.primary_token_id
                  OR p.token_id = c.secondary_token_id
                WHERE CAST(p.quantity AS REAL) > 0
                  AND c.condition_id IS NULL
                ORDER BY p.token_id
                """
            ).fetchall()
        return [str(row["token_id"]) for row in rows]

    def condition_mapping_for_token(self, token_id: str) -> dict[str, str] | None:
        """Return the immutable local token pair, if it has been established."""

        token = str(token_id).strip()
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT condition_id, primary_token_id, secondary_token_id
                FROM condition_mappings
                WHERE primary_token_id = ? OR secondary_token_id = ?
                LIMIT 1
                """,
                (token, token),
            ).fetchone()
        if row is None:
            return None
        return {
            "condition_id": str(row["condition_id"]),
            "primary_token_id": str(row["primary_token_id"]),
            "secondary_token_id": str(row["secondary_token_id"]),
        }

    def condition_inventory(self, condition_id: str | None = None) -> list[dict[str, Any]]:
        """Return only condition groups with local CD90 inventory."""

        self.initialize()
        where = ""
        params: tuple[Any, ...] = ()
        if condition_id is not None:
            where = "WHERE c.condition_id = ?"
            params = (str(condition_id).lower(),)
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT
                    c.condition_id,
                    c.primary_token_id,
                    c.secondary_token_id,
                    COALESCE(p1.quantity, '0') AS primary_quantity,
                    COALESCE(p1.cost_basis_usd, '0') AS primary_cost_basis_usd,
                    COALESCE(p2.quantity, '0') AS secondary_quantity,
                    COALESCE(p2.cost_basis_usd, '0') AS secondary_cost_basis_usd
                FROM condition_mappings AS c
                LEFT JOIN positions AS p1 ON p1.token_id = c.primary_token_id
                LEFT JOIN positions AS p2 ON p2.token_id = c.secondary_token_id
                {where}
                ORDER BY c.condition_id
                """,
                params,
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            primary_quantity = Decimal(str(row["primary_quantity"]))
            secondary_quantity = Decimal(str(row["secondary_quantity"]))
            if primary_quantity == ZERO and secondary_quantity == ZERO:
                continue
            result.append(
                {
                    "condition_id": str(row["condition_id"]),
                    "primary_token_id": str(row["primary_token_id"]),
                    "secondary_token_id": str(row["secondary_token_id"]),
                    "primary_quantity": primary_quantity,
                    "primary_cost_basis_usd": Decimal(str(row["primary_cost_basis_usd"])),
                    "secondary_quantity": secondary_quantity,
                    "secondary_cost_basis_usd": Decimal(str(row["secondary_cost_basis_usd"])),
                }
            )
        return result

    @staticmethod
    def _append_redemption_transition(
        connection: sqlite3.Connection,
        *,
        condition_id: str,
        state: str,
        reason: str,
        created_at_ms: int,
        details: dict[str, Any] | None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO redemption_transitions(
                condition_id, state, reason, created_at_ms, details_json
            ) VALUES(?, ?, ?, ?, ?)
            """,
            (
                str(condition_id).lower(),
                str(state),
                _redact_sensitive_text(reason),
                int(created_at_ms),
                _receipt_json(details or {}),
            ),
        )

    @staticmethod
    def _insert_redeemed_cash_credit_quarantine(
        connection: sqlite3.Connection,
        *,
        condition_id: str,
        payout_usd: Decimal,
        transaction_id: str,
        transaction_hash: str | None,
        created_at_ms: int,
        details: Mapping[str, Any],
    ) -> bool:
        """Persist one append-only hold for a confirmed but uncredited payout."""

        condition = str(condition_id).lower()
        payout = Decimal(str(payout_usd))
        if not payout.is_finite() or payout <= ZERO:
            raise LiveConfigurationError("INVALID_REDEEMED_CASH_CREDIT_PAYOUT")
        transaction = str(transaction_id or "").strip()
        if not transaction:
            raise LiveConfigurationError(
                "REDEEMED_CASH_CREDIT_QUARANTINE_MISSING_TRANSACTION_ID"
            )
        if int(created_at_ms) < 0:
            raise LiveConfigurationError(
                "INVALID_REDEEMED_CASH_CREDIT_QUARANTINE_TIME"
            )
        normalized_hash = str(transaction_hash or "").strip() or None
        existing = connection.execute(
            """
            SELECT payout_usd, transaction_id, transaction_hash
            FROM redeemed_cash_credit_quarantines
            WHERE condition_id = ?
            """,
            (condition,),
        ).fetchone()
        if existing is not None:
            if (
                Decimal(str(existing["payout_usd"])) == payout
                and str(existing["transaction_id"]) == transaction
                and (str(existing["transaction_hash"] or "") or None)
                == normalized_hash
            ):
                return False
            raise LiveConfigurationError(
                "REDEEMED_CASH_CREDIT_QUARANTINE_IDENTITY_CONFLICT"
            )
        connection.execute(
            """
            INSERT INTO redeemed_cash_credit_quarantines(
                condition_id, payout_usd, transaction_id, transaction_hash,
                created_at_ms, details_json
            ) VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                condition,
                str(payout),
                transaction,
                normalized_hash,
                int(created_at_ms),
                _receipt_json(dict(details)),
            ),
        )
        return True

    def record_redeemed_cash_credit_quarantine(
        self,
        *,
        condition_id: str,
        payout_usd: Decimal,
        created_at_ms: int,
        details: Mapping[str, Any],
    ) -> bool:
        """Add a forward-only cash hold without rewriting the redemption receipt."""

        condition = str(condition_id).lower()
        payout = Decimal(str(payout_usd))
        if not payout.is_finite() or payout <= ZERO:
            raise LiveConfigurationError("INVALID_REDEEMED_CASH_CREDIT_PAYOUT")
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            receipt = connection.execute(
                """
                SELECT state, expected_payout_usd, transaction_id, transaction_hash
                FROM redemption_receipts
                WHERE condition_id = ?
                """,
                (condition,),
            ).fetchone()
            if receipt is None or str(receipt["state"]) != "REDEEMED":
                raise LiveConfigurationError(
                    "REDEEMED_CASH_CREDIT_QUARANTINE_REQUIRES_REDEEMED_RECEIPT"
                )
            if Decimal(str(receipt["expected_payout_usd"])) != payout:
                raise LiveConfigurationError(
                    "REDEEMED_CASH_CREDIT_QUARANTINE_PAYOUT_MISMATCH"
                )
            inserted = self._insert_redeemed_cash_credit_quarantine(
                connection,
                condition_id=condition,
                payout_usd=payout,
                transaction_id=str(receipt["transaction_id"] or ""),
                transaction_hash=(
                    None
                    if receipt["transaction_hash"] is None
                    else str(receipt["transaction_hash"])
                ),
                created_at_ms=created_at_ms,
                details=details,
            )
            if inserted:
                self._append_redemption_transition(
                    connection,
                    condition_id=condition,
                    state="REDEEMED_CASH_CREDIT_QUARANTINED",
                    reason="CONFIRMED_REDEEM_AWAITING_AUTHENTICATED_CLOB_CREDIT",
                    created_at_ms=created_at_ms,
                    details={"payout_usd": str(payout), **dict(details)},
                )
        return inserted

    def record_redeemed_cash_credit_permanent_block(
        self,
        *,
        condition_id: str,
        payout_usd: Decimal,
        transaction_hash: str,
        expected_payout_raw: int,
        observed_payout_raw: int,
        created_at_ms: int,
        details: Mapping[str, Any],
    ) -> bool:
        """Append an availability exclusion for a chain-proven false cash credit.

        The legacy redemption, cash, position, and PnL records remain intact
        for audit.  Only the shared-wallet coordinator's future available-cash
        calculation excludes the exact amount that the transaction receipt did
        not transfer to the authenticated wallet.
        """

        condition = str(condition_id).lower()
        payout = Decimal(str(payout_usd))
        normalized_hash = str(transaction_hash or "").strip().lower()
        expected_raw = _nonnegative_raw_balance(
            expected_payout_raw,
            label="EXPECTED_REDEMPTION_COLLATERAL_PAYOUT",
        )
        observed_raw = _nonnegative_raw_balance(
            observed_payout_raw,
            label="OBSERVED_REDEMPTION_COLLATERAL_PAYOUT",
        )
        if (
            not payout.is_finite()
            or payout <= ZERO
            or not normalized_hash.startswith("0x")
            or len(normalized_hash) != 66
            or expected_raw == observed_raw
            or Decimal(expected_raw) / TOKEN_SCALE != payout
            or int(created_at_ms) < 0
        ):
            raise LiveConfigurationError(
                "INVALID_REDEEMED_CASH_CREDIT_PERMANENT_BLOCK"
            )
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            receipt = connection.execute(
                """
                SELECT state, expected_payout_usd, transaction_id, transaction_hash
                FROM redemption_receipts
                WHERE condition_id = ?
                """,
                (condition,),
            ).fetchone()
            if receipt is None or str(receipt["state"]) != "REDEEMED":
                raise LiveConfigurationError(
                    "REDEEMED_CASH_CREDIT_PERMANENT_BLOCK_REQUIRES_REDEEMED_RECEIPT"
                )
            if Decimal(str(receipt["expected_payout_usd"])) != payout:
                raise LiveConfigurationError(
                    "REDEEMED_CASH_CREDIT_PERMANENT_BLOCK_PAYOUT_MISMATCH"
                )
            if str(receipt["transaction_hash"] or "").strip().lower() != normalized_hash:
                raise LiveConfigurationError(
                    "REDEEMED_CASH_CREDIT_PERMANENT_BLOCK_TRANSACTION_MISMATCH"
                )
            transaction_id = str(receipt["transaction_id"] or "").strip()
            if not transaction_id:
                raise LiveConfigurationError(
                    "REDEEMED_CASH_CREDIT_PERMANENT_BLOCK_MISSING_TRANSACTION_ID"
                )
            evidence_hash = canonical_hash(
                {
                    "condition_id": condition,
                    "payout_usd": str(payout),
                    "transaction_id": transaction_id,
                    "transaction_hash": normalized_hash,
                    "expected_payout_raw": str(expected_raw),
                    "observed_payout_raw": str(observed_raw),
                }
            )
            existing = connection.execute(
                """
                SELECT payout_usd, transaction_id, transaction_hash,
                       expected_payout_raw, observed_payout_raw, evidence_hash
                FROM redeemed_cash_credit_permanent_blocks
                WHERE condition_id = ?
                """,
                (condition,),
            ).fetchone()
            if existing is not None:
                if (
                    Decimal(str(existing["payout_usd"])) == payout
                    and str(existing["transaction_id"]) == transaction_id
                    and str(existing["transaction_hash"]).lower()
                    == normalized_hash
                    and int(existing["expected_payout_raw"]) == expected_raw
                    and int(existing["observed_payout_raw"]) == observed_raw
                    and str(existing["evidence_hash"]) == evidence_hash
                ):
                    return False
                raise LiveConfigurationError(
                    "REDEEMED_CASH_CREDIT_PERMANENT_BLOCK_IDENTITY_CONFLICT"
                )
            connection.execute(
                """
                INSERT INTO redeemed_cash_credit_permanent_blocks(
                    condition_id, payout_usd, transaction_id, transaction_hash,
                    expected_payout_raw, observed_payout_raw, evidence_hash,
                    created_at_ms, details_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    condition,
                    str(payout),
                    transaction_id,
                    normalized_hash,
                    str(expected_raw),
                    str(observed_raw),
                    evidence_hash,
                    int(created_at_ms),
                    _receipt_json(dict(details)),
                ),
            )
            self._append_redemption_transition(
                connection,
                condition_id=condition,
                state="REDEEMED_CASH_CREDIT_PERMANENTLY_BLOCKED",
                reason="CONFIRMED_REDEMPTION_ONCHAIN_COLLATERAL_PAYOUT_MISMATCH",
                created_at_ms=int(created_at_ms),
                details={
                    "payout_usd": str(payout),
                    "expected_payout_raw": str(expected_raw),
                    "observed_payout_raw": str(observed_raw),
                    "transaction_hash": normalized_hash,
                    **dict(details),
                },
            )
        return True

    def backfill_redeemed_cash_credit_quarantines_after_authenticated_sample(
        self,
    ) -> list[str]:
        """Close the crash window from older releases without changing cash history.

        A legacy release could have written a terminal relayer or exact
        official-activity redemption after its most recent authenticated CLOB
        collateral sample.  Those exact receipts are the only safe candidates
        for a forward-only quarantine; pre-baseline reclassifications are
        explicitly excluded because they did not add new ledger cash.
        """

        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT receipt.condition_id, receipt.state,
                       receipt.expected_payout_usd, receipt.transaction_id,
                       receipt.transaction_hash, receipt.updated_at_ms
                FROM redemption_receipts AS receipt
                LEFT JOIN settlement_cash_reclassification_receipts AS baseline
                  ON baseline.condition_id = receipt.condition_id
                WHERE receipt.state IN (
                    'REDEEMED',
                    'REDEEMED_OFFICIAL_ACTIVITY_VERIFIED'
                )
                  AND baseline.condition_id IS NULL
                ORDER BY receipt.updated_at_ms, receipt.condition_id
                """
            ).fetchall()
            if not rows:
                return []
            observed = connection.execute(
                """
                SELECT value FROM runtime_state
                WHERE key = 'last_authenticated_collateral_at_ms'
                """
            ).fetchone()
            if observed is None or not str(observed["value"]).strip():
                raise LiveConfigurationError(
                    "MISSING_AUTHENTICATED_COLLATERAL_FOR_REDEEMED_CASH_CREDIT_BACKFILL"
                )
            try:
                observed_at_ms = int(str(observed["value"]))
            except ValueError as exc:
                raise LiveConfigurationError(
                    "INVALID_AUTHENTICATED_COLLATERAL_FOR_REDEEMED_CASH_CREDIT_BACKFILL"
                ) from exc
            candidates = [
                row for row in rows if int(row["updated_at_ms"]) > observed_at_ms
            ]
            if not candidates:
                return []
            connection.execute("BEGIN IMMEDIATE")
            inserted: list[str] = []
            for row in candidates:
                condition = str(row["condition_id"]).lower()
                payout = Decimal(str(row["expected_payout_usd"]))
                transaction_hash = (
                    None
                    if row["transaction_hash"] is None
                    else str(row["transaction_hash"])
                )
                transaction_id = str(row["transaction_id"] or "").strip()
                if not transaction_id and str(row["state"]) == (
                    "REDEEMED_OFFICIAL_ACTIVITY_VERIFIED"
                ):
                    transaction_id = "official-activity:" + str(
                        transaction_hash or ""
                    )
                if self._insert_redeemed_cash_credit_quarantine(
                    connection,
                    condition_id=condition,
                    payout_usd=payout,
                    transaction_id=transaction_id,
                    transaction_hash=transaction_hash,
                    created_at_ms=int(row["updated_at_ms"]),
                    details={
                        "reason": "POST_RELEASE_RECONCILIATION_AFTER_AUTHENTICATED_CASH_SAMPLE",
                        "last_authenticated_collateral_at_ms": observed_at_ms,
                    },
                ):
                    self._append_redemption_transition(
                        connection,
                        condition_id=condition,
                        state="REDEEMED_CASH_CREDIT_QUARANTINED",
                        reason="POST_RELEASE_RECONCILIATION_AFTER_AUTHENTICATED_CASH_SAMPLE",
                        created_at_ms=int(row["updated_at_ms"]),
                        details={
                            "payout_usd": str(payout),
                            "last_authenticated_collateral_at_ms": observed_at_ms,
                        },
                    )
                    inserted.append(condition)
        return inserted

    def redemption_receipt(self, condition_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM redemption_receipts WHERE condition_id = ?",
                (str(condition_id).lower(),),
            ).fetchone()
        return None if row is None else dict(row)

    def redemption_receipts_with_state(self, state: str) -> list[dict[str, Any]]:
        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM redemption_receipts WHERE state = ? ORDER BY condition_id",
                (str(state),),
            ).fetchall()
        return [dict(row) for row in rows]

    def redemption_receipts_pending_reconciliation(self) -> list[dict[str, Any]]:
        """Return side-effecting redemption attempts that permit read-only recovery.

        An orphaned ``SUBMIT_STARTED`` is uncertain after a process restart and
        must be converted to ``UNKNOWN_SUBMISSION`` without a repost.  An
        unknown attempt with a known transaction id remains eligible for
        relayer reads; an unknown attempt without an id is handled only by the
        stricter aggregate platform-cash proof path.
        """

        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM redemption_receipts
                WHERE state IN (
                    'SUBMIT_STARTED', 'SUBMITTED_UNRECONCILED', 'PENDING'
                )
                   OR (
                       state = 'UNKNOWN_SUBMISSION'
                       AND transaction_id IS NOT NULL
                       AND TRIM(transaction_id) <> ''
                   )
                ORDER BY condition_id
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def record_redemption_terminal_without_submission(
        self,
        *,
        condition_id: str,
        state: str,
        reason: str,
        expected_payout_usd: Decimal,
        created_at_ms: int,
        details: dict[str, Any],
        transaction_hash: str | None = None,
    ) -> bool:
        """Persist a conservative terminal condition before any wallet call.

        This is used for states where automatic redemption is deliberately
        prohibited (for example a balance that includes inventory not owned by
        this isolated sleeve).  A later automatic pass must never silently
        replace that evidence with a wallet submission.
        """

        self.initialize()
        condition = str(condition_id).lower()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT condition_id FROM redemption_receipts WHERE condition_id = ?",
                (condition,),
            ).fetchone()
            if existing is not None:
                return False
            connection.execute(
                """
                INSERT INTO redemption_receipts(
                    condition_id, state, expected_payout_usd, transaction_id,
                    transaction_hash, created_at_ms, updated_at_ms
                ) VALUES(?, ?, ?, NULL, ?, ?, ?)
                """,
                (
                    condition,
                    str(state),
                    str(expected_payout_usd),
                    (
                        None
                        if transaction_hash is None
                        else str(transaction_hash).strip().lower()
                    ),
                    int(created_at_ms),
                    int(created_at_ms),
                ),
            )
            self._append_redemption_transition(
                connection,
                condition_id=condition,
                state=state,
                reason=reason,
                created_at_ms=created_at_ms,
                details=details,
            )
        return True

    def _set_redemption_state(
        self,
        *,
        condition_id: str,
        state: str,
        reason: str,
        created_at_ms: int,
        details: dict[str, Any] | None = None,
        transaction_id: str | None = None,
        transaction_hash: str | None = None,
    ) -> None:
        self.initialize()
        with self.connect() as connection:
            receipt = connection.execute(
                "SELECT condition_id FROM redemption_receipts WHERE condition_id = ?",
                (str(condition_id).lower(),),
            ).fetchone()
            if receipt is None:
                raise LiveConfigurationError("MISSING_REDEMPTION_RECEIPT")
            connection.execute(
                """
                UPDATE redemption_receipts
                SET state = ?,
                    transaction_id = COALESCE(?, transaction_id),
                    transaction_hash = COALESCE(?, transaction_hash),
                    updated_at_ms = ?
                WHERE condition_id = ?
                """,
                (
                    str(state),
                    transaction_id,
                    transaction_hash,
                    int(created_at_ms),
                    str(condition_id).lower(),
                ),
            )
            self._append_redemption_transition(
                connection,
                condition_id=condition_id,
                state=state,
                reason=reason,
                created_at_ms=created_at_ms,
                details=details,
            )

    def start_redemption_submission(
        self,
        *,
        condition_id: str,
        expected_payout_usd: Decimal,
        created_at_ms: int,
        details: dict[str, Any],
    ) -> bool:
        """Atomically acquire the per-condition submission lock before I/O."""

        if expected_payout_usd <= ZERO:
            raise LiveConfigurationError("NONPOSITIVE_REDEMPTION_PAYOUT")
        self.initialize()
        condition = str(condition_id).lower()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT state, expected_payout_usd, transaction_id, transaction_hash
                FROM redemption_receipts
                WHERE condition_id = ?
                """,
                (condition,),
            ).fetchone()
            if existing is not None:
                if str(existing["state"]) != "NOT_SUBMITTED_RETRYABLE":
                    return False
                if existing["transaction_id"] is not None or existing["transaction_hash"] is not None:
                    raise LiveConfigurationError("RETRYABLE_REDEMPTION_HAS_TRANSACTION_ID")
                if Decimal(str(existing["expected_payout_usd"])) != expected_payout_usd:
                    raise LiveConfigurationError("RETRYABLE_REDEMPTION_PAYOUT_CHANGED")
                connection.execute(
                    """
                    UPDATE redemption_receipts
                    SET state = 'SUBMIT_STARTED', updated_at_ms = ?
                    WHERE condition_id = ?
                    """,
                    (int(created_at_ms), condition),
                )
                self._append_redemption_transition(
                    connection,
                    condition_id=condition,
                    state="SUBMIT_STARTED",
                    reason="RETRY_AFTER_PROVEN_NO_SUBMISSION",
                    created_at_ms=created_at_ms,
                    details=details,
                )
                return True
            connection.execute(
                """
                INSERT INTO redemption_receipts(
                    condition_id, state, expected_payout_usd, transaction_id,
                    transaction_hash, created_at_ms, updated_at_ms
                ) VALUES(?, 'SUBMIT_STARTED', ?, NULL, NULL, ?, ?)
                """,
                (condition, str(expected_payout_usd), int(created_at_ms), int(created_at_ms)),
            )
            self._append_redemption_transition(
                connection,
                condition_id=condition,
                state="SUBMIT_STARTED",
                reason="",
                created_at_ms=created_at_ms,
                details=details,
            )
        return True

    def settle_externally_verified_redemption(
        self,
        *,
        condition_id: str,
        payout_usd: Decimal,
        transaction_hash: str,
        created_at_ms: int,
        details: dict[str, Any],
    ) -> None:
        """Credit a proven external redemption without guessing wallet cash.

        ``UNKNOWN_SUBMISSION`` covers a possibly accepted relayer request.
        ``NOT_SUBMITTED_RETRYABLE`` covers a proven relayer rejection followed
        later by an independently observed platform redemption.  Both paths
        still require the exact official REDEEM transaction and zero balances
        for both outcome tokens before this isolated sleeve can be credited.
        """

        normalized_hash = str(transaction_hash).lower()
        try:
            valid_hash = (
                normalized_hash.startswith("0x")
                and len(normalized_hash) == 66
                and int(normalized_hash[2:], 16) >= 0
            )
        except ValueError:
            valid_hash = False
        if not valid_hash:
            raise LiveConfigurationError("INVALID_EXTERNAL_REDEMPTION_TRANSACTION_HASH")
        if payout_usd <= ZERO:
            raise LiveConfigurationError("NONPOSITIVE_EXTERNAL_REDEMPTION_PAYOUT")
        if details.get("official_activity_type") != "REDEEM":
            raise LiveConfigurationError("MISSING_OFFICIAL_REDEMPTION_ACTIVITY_PROOF")
        if details.get("onchain_outcome_balances_zero") is not True:
            raise LiveConfigurationError("MISSING_ZERO_OUTCOME_BALANCE_PROOF")

        inventory = self.condition_inventory(condition_id)
        if len(inventory) != 1:
            raise LiveConfigurationError("MISSING_CONDITION_INVENTORY_FOR_REDEMPTION")
        row = inventory[0]
        total_cost = row["primary_cost_basis_usd"] + row["secondary_cost_basis_usd"]
        condition = str(condition_id).lower()
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            receipt = connection.execute(
                "SELECT * FROM redemption_receipts WHERE condition_id = ?",
                (condition,),
            ).fetchone()
            prior_state = None if receipt is None else str(receipt["state"])
            if prior_state not in {
                "UNKNOWN_SUBMISSION",
                "NOT_SUBMITTED_RETRYABLE",
            }:
                raise LiveConfigurationError("EXTERNAL_REDEMPTION_STATE_NOT_REPAIRABLE")
            if receipt["transaction_id"] is not None:
                raise LiveConfigurationError("EXTERNAL_REDEMPTION_HAS_RELAYER_TRANSACTION_ID")
            if (
                prior_state == "NOT_SUBMITTED_RETRYABLE"
                and receipt["transaction_hash"] is not None
            ):
                raise LiveConfigurationError(
                    "RETRYABLE_EXTERNAL_REDEMPTION_HAS_TRANSACTION_HASH"
                )
            if Decimal(str(receipt["expected_payout_usd"])) != payout_usd:
                raise LiveConfigurationError("REDEMPTION_PAYOUT_MISMATCH")
            account = connection.execute(
                "SELECT * FROM account_state WHERE singleton = 1"
            ).fetchone()
            if account is None:
                raise ScaleInputError("allocation account has not been initialized")
            cash = Decimal(str(account["cash_usd"])) + payout_usd
            realized = Decimal(str(account["realized_pnl_usd"])) + payout_usd - total_cost
            for token_id in (row["primary_token_id"], row["secondary_token_id"]):
                connection.execute(
                    """
                    INSERT INTO positions(token_id, quantity, cost_basis_usd)
                    VALUES(?, '0', '0')
                    ON CONFLICT(token_id) DO UPDATE SET quantity = '0', cost_basis_usd = '0'
                    """,
                    (token_id,),
                )
            connection.execute(
                """
                UPDATE account_state
                SET cash_usd = ?, realized_pnl_usd = ?
                WHERE singleton = 1
                """,
                (str(cash), str(realized)),
            )
            connection.execute(
                """
                UPDATE redemption_receipts
                SET state = 'REDEEMED_EXTERNAL_VERIFIED', transaction_hash = ?,
                    updated_at_ms = ?
                WHERE condition_id = ?
                """,
                (normalized_hash, int(created_at_ms), condition),
            )
            self._append_redemption_transition(
                connection,
                condition_id=condition,
                state="REDEEMED_EXTERNAL_VERIFIED",
                reason="OFFICIAL_ACTIVITY_AND_ONCHAIN_BALANCE_PROOF",
                created_at_ms=created_at_ms,
                details={
                    **details,
                    "payout_usd": str(payout_usd),
                    "prior_state": prior_state,
                },
            )

    def settle_verified_losing_condition_from_terminal(
        self,
        *,
        condition_id: str,
        created_at_ms: int,
        details: dict[str, Any],
    ) -> None:
        """Repair a prior UNKNOWN receipt after official zero-payout proof."""

        if details.get("onchain_inventory_exact") is not True:
            raise LiveConfigurationError("MISSING_EXACT_ONCHAIN_INVENTORY_PROOF")
        if int(details.get("onchain_winner_balance_raw", -1)) != 0:
            raise LiveConfigurationError("WINNER_ONCHAIN_BALANCE_NOT_ZERO")
        numerators = details.get("payout_numerators")
        if not isinstance(numerators, list) or len(numerators) != 2:
            raise LiveConfigurationError("INVALID_LOSS_PAYOUT_NUMERATORS")
        try:
            values = [int(value) for value in numerators]
        except (TypeError, ValueError) as exc:
            raise LiveConfigurationError("INVALID_LOSS_PAYOUT_NUMERATORS") from exc
        if min(values) != 0 or max(values) <= 0 or sum(value > 0 for value in values) != 1:
            raise LiveConfigurationError("INVALID_LOSS_PAYOUT_NUMERATORS")

        inventory = self.condition_inventory(condition_id)
        if len(inventory) != 1:
            raise LiveConfigurationError("MISSING_CONDITION_INVENTORY_FOR_LOSS")
        row = inventory[0]
        winner_index = 0 if values[0] > 0 else 1
        winner_token_id = (
            str(row["primary_token_id"])
            if winner_index == 0
            else str(row["secondary_token_id"])
        )
        if str(details.get("winner_token_id")) != winner_token_id:
            raise LiveConfigurationError("LOSS_WINNER_TOKEN_PROOF_MISMATCH")
        winner_quantity = (
            row["primary_quantity"] if winner_index == 0 else row["secondary_quantity"]
        )
        if winner_quantity != ZERO:
            raise LiveConfigurationError("CONDITION_IS_NOT_A_LOCAL_LOSS")
        total_cost = row["primary_cost_basis_usd"] + row["secondary_cost_basis_usd"]
        condition = str(condition_id).lower()
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            receipt = connection.execute(
                "SELECT * FROM redemption_receipts WHERE condition_id = ?",
                (condition,),
            ).fetchone()
            if receipt is None or str(receipt["state"]) != "UNKNOWN_SUBMISSION":
                raise LiveConfigurationError("LOSS_REDEMPTION_STATE_NOT_REPAIRABLE")
            if receipt["transaction_id"] is not None or receipt["transaction_hash"] is not None:
                raise LiveConfigurationError("LOSS_REDEMPTION_HAS_TRANSACTION_ID")
            account = connection.execute(
                "SELECT * FROM account_state WHERE singleton = 1"
            ).fetchone()
            if account is None:
                raise ScaleInputError("allocation account has not been initialized")
            for token_id in (row["primary_token_id"], row["secondary_token_id"]):
                connection.execute(
                    """
                    INSERT INTO positions(token_id, quantity, cost_basis_usd)
                    VALUES(?, '0', '0')
                    ON CONFLICT(token_id) DO UPDATE SET quantity = '0', cost_basis_usd = '0'
                    """,
                    (token_id,),
                )
            connection.execute(
                """
                UPDATE account_state
                SET realized_pnl_usd = ?
                WHERE singleton = 1
                """,
                (str(Decimal(str(account["realized_pnl_usd"])) - total_cost),),
            )
            connection.execute(
                """
                UPDATE redemption_receipts
                SET state = 'LOSS_RESOLVED_NO_PAYOUT', expected_payout_usd = '0',
                    updated_at_ms = ?
                WHERE condition_id = ?
                """,
                (int(created_at_ms), condition),
            )
            self._append_redemption_transition(
                connection,
                condition_id=condition,
                state="LOSS_RESOLVED_NO_PAYOUT",
                reason="OFFICIAL_PAYOUT_AND_EXACT_ONCHAIN_INVENTORY_PROOF",
                created_at_ms=created_at_ms,
                details={**details, "written_off_cost_usd": str(total_cost)},
            )

    def mark_redemption_submission(
        self,
        *,
        condition_id: str,
        transaction_id: str,
        transaction_hash: str | None,
        created_at_ms: int,
    ) -> None:
        if not str(transaction_id).strip():
            raise LiveConfigurationError("MISSING_REDEMPTION_TRANSACTION_ID")
        self._set_redemption_state(
            condition_id=condition_id,
            state="SUBMITTED_UNRECONCILED",
            reason="",
            created_at_ms=created_at_ms,
            details={"transaction_id": str(transaction_id)},
            transaction_id=str(transaction_id),
            transaction_hash=None if transaction_hash is None else str(transaction_hash),
        )

    def mark_redemption_terminal(
        self,
        *,
        condition_id: str,
        state: str,
        reason: str,
        created_at_ms: int,
        details: dict[str, Any] | None = None,
        transaction_hash: str | None = None,
    ) -> None:
        self._set_redemption_state(
            condition_id=condition_id,
            state=state,
            reason=reason,
            created_at_ms=created_at_ms,
            details=details,
            transaction_hash=transaction_hash,
        )

    def settle_redeemed_condition(
        self,
        *,
        condition_id: str,
        payout_usd: Decimal,
        created_at_ms: int,
        details: dict[str, Any],
        transaction_hash: str | None,
        quarantine_cash_credit_until_authenticated: bool = False,
    ) -> None:
        """Release one confirmed redemption using its official actual payout.

        Shared-wallet callers write an atomic, append-only cash hold alongside
        the terminal ledger entry.  The realized outcome remains factual while
        the newly credited cash stays unavailable until CLOB collateral proves
        it has arrived in the shared physical wallet.
        """

        payout_usd = Decimal(str(payout_usd))
        if not payout_usd.is_finite() or payout_usd < ZERO:
            raise LiveConfigurationError("INVALID_CONFIRMED_REDEMPTION_PAYOUT")
        inventory = self.condition_inventory(condition_id)
        if len(inventory) != 1:
            raise LiveConfigurationError("MISSING_CONDITION_INVENTORY_FOR_REDEMPTION")
        row = inventory[0]
        total_cost = row["primary_cost_basis_usd"] + row["secondary_cost_basis_usd"]
        self.initialize()
        with self.connect() as connection:
            receipt = connection.execute(
                "SELECT state, expected_payout_usd, transaction_id, transaction_hash FROM redemption_receipts WHERE condition_id = ?",
                (str(condition_id).lower(),),
            ).fetchone()
            if receipt is None or str(receipt["state"]) not in {
                "SUBMITTED_UNRECONCILED",
                "PENDING",
                "UNKNOWN_SUBMISSION",
            }:
                raise LiveConfigurationError("REDEMPTION_STATE_NOT_RECONCILABLE")
            if not str(receipt["transaction_id"] or "").strip():
                raise LiveConfigurationError("PENDING_REDEMPTION_WITHOUT_TRANSACTION_ID")
            predicted_payout = Decimal(str(receipt["expected_payout_usd"]))
            account = connection.execute(
                "SELECT * FROM account_state WHERE singleton = 1"
            ).fetchone()
            if account is None:
                raise ScaleInputError("allocation account has not been initialized")
            cash = Decimal(str(account["cash_usd"])) + payout_usd
            realized = Decimal(str(account["realized_pnl_usd"])) + payout_usd - total_cost
            for token_id in (row["primary_token_id"], row["secondary_token_id"]):
                connection.execute(
                    """
                    INSERT INTO positions(token_id, quantity, cost_basis_usd)
                    VALUES(?, '0', '0')
                    ON CONFLICT(token_id) DO UPDATE SET quantity = '0', cost_basis_usd = '0'
                    """,
                    (token_id,),
                )
            connection.execute(
                """
                UPDATE account_state
                SET cash_usd = ?, realized_pnl_usd = ?
                WHERE singleton = 1
                """,
                (str(cash), str(realized)),
            )
            terminal_state = (
                "REDEEMED" if payout_usd > ZERO else "LOSS_RESOLVED_NO_PAYOUT"
            )
            resolved_transaction_hash = (
                str(transaction_hash).strip()
                if transaction_hash is not None and str(transaction_hash).strip()
                else (
                    str(receipt["transaction_hash"]).strip()
                    if receipt["transaction_hash"] is not None
                    and str(receipt["transaction_hash"]).strip()
                    else None
                )
            )
            connection.execute(
                """
                UPDATE redemption_receipts
                SET state = ?, expected_payout_usd = ?,
                    transaction_hash = COALESCE(?, transaction_hash), updated_at_ms = ?
                WHERE condition_id = ?
                """,
                (
                    terminal_state,
                    str(payout_usd),
                    transaction_hash,
                    int(created_at_ms),
                    str(condition_id).lower(),
                ),
            )
            if quarantine_cash_credit_until_authenticated and payout_usd > ZERO:
                self._insert_redeemed_cash_credit_quarantine(
                    connection,
                    condition_id=condition_id,
                    payout_usd=payout_usd,
                    transaction_id=str(receipt["transaction_id"] or ""),
                    transaction_hash=resolved_transaction_hash,
                    created_at_ms=created_at_ms,
                    details={
                        "reason": "CONFIRMED_REDEEM_AWAITING_AUTHENTICATED_CLOB_CREDIT",
                        "redemption_details": details,
                    },
                )
            self._append_redemption_transition(
                connection,
                condition_id=condition_id,
                state=terminal_state,
                reason="EXACT_OFFICIAL_REDEEM_TRANSACTION_PAYOUT",
                created_at_ms=created_at_ms,
                details={
                    **details,
                    "predicted_payout_usd": str(predicted_payout),
                    "official_payout_usd": str(payout_usd),
                    "written_off_cost_usd": str(total_cost),
                    "cash_credit_quarantined": bool(
                        quarantine_cash_credit_until_authenticated
                        and payout_usd > ZERO
                    ),
                },
            )

    def correct_terminal_redemption_payout(
        self,
        *,
        condition_id: str,
        transaction_hash: str,
        official_payout_usd: Decimal,
        official_activity_type: str,
        evidence_hash: str,
        created_at_ms: int,
    ) -> bool:
        """Append one official correction to a previously over/under-credited payout."""

        condition = str(condition_id).strip().lower()
        normalized_hash = str(transaction_hash).strip().lower()
        payout = Decimal(str(official_payout_usd))
        normalized_evidence_hash = str(evidence_hash).strip().lower()
        if official_activity_type != "REDEEM":
            raise LiveConfigurationError("INVALID_REDEMPTION_CORRECTION_ACTIVITY")
        if not payout.is_finite() or payout < ZERO:
            raise LiveConfigurationError("INVALID_REDEMPTION_CORRECTION_PAYOUT")
        try:
            valid_transaction_hash = (
                normalized_hash.startswith("0x")
                and len(normalized_hash) == 66
                and int(normalized_hash[2:], 16) >= 0
            )
            valid_evidence_hash = (
                len(normalized_evidence_hash) == 64
                and int(normalized_evidence_hash, 16) >= 0
            )
        except ValueError:
            valid_transaction_hash = False
            valid_evidence_hash = False
        if not valid_transaction_hash:
            raise LiveConfigurationError("INVALID_REDEMPTION_CORRECTION_TRANSACTION_HASH")
        if not valid_evidence_hash:
            raise LiveConfigurationError("INVALID_REDEMPTION_CORRECTION_EVIDENCE_HASH")

        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM redemption_payout_corrections WHERE condition_id = ?",
                (condition,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["transaction_hash"]) != normalized_hash
                    or Decimal(str(existing["official_payout_usd"])) != payout
                    or str(existing["evidence_hash"]) != normalized_evidence_hash
                ):
                    raise LiveConfigurationError(
                        "REDEMPTION_PAYOUT_CORRECTION_IDENTITY_MISMATCH"
                    )
                return False
            receipt = connection.execute(
                "SELECT * FROM redemption_receipts WHERE condition_id = ?",
                (condition,),
            ).fetchone()
            if receipt is None or str(receipt["state"]) != "REDEEMED":
                raise LiveConfigurationError(
                    "REDEMPTION_PAYOUT_CORRECTION_STATE_NOT_ELIGIBLE"
                )
            receipt_hash = str(receipt["transaction_hash"] or "").strip().lower()
            if receipt_hash != normalized_hash:
                raise LiveConfigurationError(
                    "REDEMPTION_PAYOUT_CORRECTION_TRANSACTION_MISMATCH"
                )
            prior_payout = Decimal(str(receipt["expected_payout_usd"]))
            if prior_payout == payout:
                return False
            account = connection.execute(
                "SELECT * FROM account_state WHERE singleton = 1"
            ).fetchone()
            if account is None:
                raise ScaleInputError("allocation account has not been initialized")
            delta = payout - prior_payout
            terminal_state = (
                "REDEEMED_OFFICIAL_PAYOUT_CORRECTED"
                if payout > ZERO
                else "LOSS_RESOLVED_OFFICIAL_ACTIVITY_CORRECTED"
            )
            connection.execute(
                """
                UPDATE account_state
                SET cash_usd = ?, realized_pnl_usd = ?
                WHERE singleton = 1
                """,
                (
                    str(Decimal(str(account["cash_usd"])) + delta),
                    str(Decimal(str(account["realized_pnl_usd"])) + delta),
                ),
            )
            connection.execute(
                """
                UPDATE redemption_receipts
                SET state = ?, expected_payout_usd = ?, updated_at_ms = ?
                WHERE condition_id = ?
                """,
                (terminal_state, str(payout), int(created_at_ms), condition),
            )
            connection.execute(
                """
                INSERT INTO redemption_payout_corrections(
                    condition_id, prior_state, prior_expected_payout_usd,
                    official_payout_usd, cash_delta_usd, transaction_hash,
                    evidence_hash, created_at_ms
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    condition,
                    str(receipt["state"]),
                    str(prior_payout),
                    str(payout),
                    str(delta),
                    normalized_hash,
                    normalized_evidence_hash,
                    int(created_at_ms),
                ),
            )
            quarantined = connection.execute(
                """
                SELECT condition_id
                FROM redeemed_cash_credit_quarantines
                WHERE condition_id = ?
                """,
                (condition,),
            ).fetchone()
            if quarantined is not None:
                connection.execute(
                    """
                    INSERT INTO redeemed_cash_credit_quarantine_voids(
                        condition_id, reason, official_payout_usd,
                        evidence_hash, voided_at_ms, details_json
                    ) VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (
                        condition,
                        "OFFICIAL_PAYOUT_CORRECTION_VOIDED_QUARANTINE",
                        str(payout),
                        normalized_evidence_hash,
                        int(created_at_ms),
                        _receipt_json(
                            {
                                "prior_quarantined_payout_usd": str(prior_payout),
                                "official_payout_usd": str(payout),
                                "transaction_hash": normalized_hash,
                            }
                        ),
                    ),
                )
            self._append_redemption_transition(
                connection,
                condition_id=condition,
                state=terminal_state,
                reason="OFFICIAL_REDEEM_PAYOUT_CORRECTION",
                created_at_ms=created_at_ms,
                details={
                    "prior_payout_usd": str(prior_payout),
                    "official_payout_usd": str(payout),
                    "cash_and_realized_delta_usd": str(delta),
                    "transaction_hash": normalized_hash,
                    "official_activity_evidence_hash": normalized_evidence_hash,
                },
            )
        return True

    def settle_platform_credited_winners(
        self,
        *,
        candidates: list[Mapping[str, Any]],
        observed_collateral_usd: Decimal,
        created_at_ms: int,
        verified_wallet_cash_before_usd: Decimal | None = None,
    ) -> dict[str, Decimal | int]:
        """Atomically reconcile resolved winners already credited by the platform.

        Some deposit-wallet settlements credit authenticated CLOB collateral
        without a relayer transaction initiated by this process.  That is not
        an external cash contribution.  We only recognize it when the exact
        authenticated-collateral delta equals the complete set of locally held,
        officially resolved winners whose two direct outcome balances are zero.
        """

        observed = Decimal(str(observed_collateral_usd))
        if not observed.is_finite() or observed < ZERO:
            raise LiveConfigurationError("INVALID_PLATFORM_SETTLEMENT_COLLATERAL")
        if not candidates:
            raise LiveConfigurationError("MISSING_PLATFORM_SETTLEMENT_CANDIDATES")

        normalized: list[dict[str, Any]] = []
        seen_conditions: set[str] = set()
        expected_total = ZERO
        for candidate in candidates:
            condition_id = str(candidate.get("condition_id", "")).lower()
            winner_token_id = str(candidate.get("winner_token_id", ""))
            payout = Decimal(str(candidate.get("payout_usd", "")))
            if (
                not condition_id.startswith("0x")
                or len(condition_id) != 66
                or not winner_token_id
                or not payout.is_finite()
                or payout <= ZERO
            ):
                raise LiveConfigurationError("INVALID_PLATFORM_SETTLEMENT_CANDIDATE")
            if condition_id in seen_conditions:
                raise LiveConfigurationError("DUPLICATE_PLATFORM_SETTLEMENT_CONDITION")
            seen_conditions.add(condition_id)
            normalized.append(
                {
                    **dict(candidate),
                    "condition_id": condition_id,
                    "winner_token_id": winner_token_id,
                    "payout_usd": payout,
                }
            )
            expected_total += payout

        allowed_prior_states = {
            "NOT_SUBMITTED_RETRYABLE",
            "BLOCK_PRE_SUBMISSION_REVALIDATION",
            "BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY",
            "UNKNOWN_SUBMISSION",
        }
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            active = connection.execute(
                "SELECT COUNT(*) AS count FROM order_reservations WHERE active = 1"
            ).fetchone()
            if active is not None and int(active["count"]) != 0:
                raise LiveConfigurationError(
                    "ACTIVE_ORDER_RESERVATIONS_BLOCK_PLATFORM_SETTLEMENT"
                )
            account = connection.execute(
                "SELECT * FROM account_state WHERE singleton = 1"
            ).fetchone()
            if account is None:
                raise ScaleInputError("allocation account has not been initialized")
            cash_before = Decimal(str(account["cash_usd"]))
            wallet_cash_before = (
                cash_before
                if verified_wallet_cash_before_usd is None
                else Decimal(str(verified_wallet_cash_before_usd))
            )
            if (
                not wallet_cash_before.is_finite()
                or wallet_cash_before < ZERO
                or observed - wallet_cash_before != expected_total
            ):
                raise LiveConfigurationError("PLATFORM_SETTLEMENT_CASH_DELTA_MISMATCH")

            realized = Decimal(str(account["realized_pnl_usd"]))
            for candidate in normalized:
                condition_id = str(candidate["condition_id"])
                mapping = connection.execute(
                    """
                    SELECT c.condition_id, c.primary_token_id, c.secondary_token_id,
                           COALESCE(p1.quantity, '0') AS primary_quantity,
                           COALESCE(p1.cost_basis_usd, '0') AS primary_cost_basis_usd,
                           COALESCE(p2.quantity, '0') AS secondary_quantity,
                           COALESCE(p2.cost_basis_usd, '0') AS secondary_cost_basis_usd
                    FROM condition_mappings AS c
                    LEFT JOIN positions AS p1 ON p1.token_id = c.primary_token_id
                    LEFT JOIN positions AS p2 ON p2.token_id = c.secondary_token_id
                    WHERE c.condition_id = ?
                    """,
                    (condition_id,),
                ).fetchone()
                if mapping is None:
                    raise LiveConfigurationError(
                        "MISSING_CONDITION_INVENTORY_FOR_PLATFORM_SETTLEMENT"
                    )
                primary_token_id = str(mapping["primary_token_id"])
                secondary_token_id = str(mapping["secondary_token_id"])
                winner_token_id = str(candidate["winner_token_id"])
                if winner_token_id not in {primary_token_id, secondary_token_id}:
                    raise LiveConfigurationError(
                        "PLATFORM_SETTLEMENT_WINNER_TOKEN_MISMATCH"
                    )
                winner_quantity = Decimal(
                    str(
                        mapping["primary_quantity"]
                        if winner_token_id == primary_token_id
                        else mapping["secondary_quantity"]
                    )
                )
                payout = Decimal(str(candidate["payout_usd"]))
                if winner_quantity != payout:
                    raise LiveConfigurationError(
                        "PLATFORM_SETTLEMENT_PAYOUT_INVENTORY_MISMATCH"
                    )
                receipt = connection.execute(
                    "SELECT * FROM redemption_receipts WHERE condition_id = ?",
                    (condition_id,),
                ).fetchone()
                if receipt is not None:
                    prior_state = str(receipt["state"])
                    if prior_state not in allowed_prior_states:
                        raise LiveConfigurationError(
                            "PLATFORM_SETTLEMENT_PRIOR_STATE_NOT_RECONCILABLE"
                        )
                    if receipt["transaction_id"] is not None or receipt["transaction_hash"] is not None:
                        raise LiveConfigurationError(
                            "PLATFORM_SETTLEMENT_HAS_TRANSACTION_EVIDENCE"
                        )
                    previous_expected = Decimal(str(receipt["expected_payout_usd"]))
                    if previous_expected not in {ZERO, payout}:
                        raise LiveConfigurationError(
                            "PLATFORM_SETTLEMENT_PRIOR_PAYOUT_MISMATCH"
                        )
                total_cost = Decimal(str(mapping["primary_cost_basis_usd"])) + Decimal(
                    str(mapping["secondary_cost_basis_usd"])
                )
                for token_id in (primary_token_id, secondary_token_id):
                    connection.execute(
                        """
                        INSERT INTO positions(token_id, quantity, cost_basis_usd)
                        VALUES(?, '0', '0')
                        ON CONFLICT(token_id) DO UPDATE SET quantity = '0', cost_basis_usd = '0'
                        """,
                        (token_id,),
                    )
                if receipt is None:
                    connection.execute(
                        """
                        INSERT INTO redemption_receipts(
                            condition_id, state, expected_payout_usd, transaction_id,
                            transaction_hash, created_at_ms, updated_at_ms
                        ) VALUES(?, 'REDEEMED_PLATFORM_SETTLEMENT_VERIFIED', ?, NULL, NULL, ?, ?)
                        """,
                        (condition_id, str(payout), int(created_at_ms), int(created_at_ms)),
                    )
                else:
                    connection.execute(
                        """
                        UPDATE redemption_receipts
                        SET state = 'REDEEMED_PLATFORM_SETTLEMENT_VERIFIED',
                            expected_payout_usd = ?, updated_at_ms = ?
                        WHERE condition_id = ?
                        """,
                        (str(payout), int(created_at_ms), condition_id),
                    )
                self._append_redemption_transition(
                    connection,
                    condition_id=condition_id,
                    state="REDEEMED_PLATFORM_SETTLEMENT_VERIFIED",
                    reason="OFFICIAL_RESOLUTION_ZERO_OUTCOME_BALANCES_AND_EXACT_AUTHENTICATED_CASH_DELTA",
                    created_at_ms=created_at_ms,
                    details={
                        **dict(candidate),
                        "payout_usd": str(payout),
                        "ledger_cash_before_usd": str(cash_before),
                        "aggregate_wallet_cash_before_usd": str(wallet_cash_before),
                        "authenticated_collateral_usd": str(observed),
                        "exact_reconciliation_delta_usd": str(expected_total),
                        "written_off_cost_usd": str(total_cost),
                    },
                )
                realized += payout - total_cost

            connection.execute(
                """
                UPDATE account_state
                SET cash_usd = ?, realized_pnl_usd = ?
                WHERE singleton = 1
                """,
                (str(cash_before + expected_total), str(realized)),
            )
        return {
            "condition_count": len(normalized),
            "expected_payout_usd": expected_total,
            "ledger_cash_before_usd": cash_before,
            "aggregate_wallet_cash_before_usd": wallet_cash_before,
            "authenticated_collateral_usd": observed,
        }

    def settle_official_activity_redeemed_winners(
        self,
        *,
        candidates: list[Mapping[str, Any]],
        created_at_ms: int,
        quarantine_cash_credit_until_authenticated: bool = False,
    ) -> dict[str, Decimal | int]:
        """Settle exact per-condition official REDEEM activity atomically.

        A wallet can receive an unrelated deposit in the same maintenance
        interval.  Per-condition official activity therefore has stronger
        attribution than an aggregate cash delta.  A redemption that predates
        the coordinator's frozen cash baseline is reclassified out of the
        historical external-reserve bucket instead of crediting cash twice.
        """

        if not candidates:
            raise LiveConfigurationError("MISSING_OFFICIAL_REDEEM_CANDIDATES")
        normalized: list[dict[str, Any]] = []
        seen_conditions: set[str] = set()
        seen_transactions: set[str] = set()
        for candidate in candidates:
            condition_id = str(candidate.get("condition_id", "")).strip().lower()
            winner_token_id = str(candidate.get("winner_token_id", "")).strip()
            transaction_hash = str(
                candidate.get("transaction_hash", "")
            ).strip().lower()
            try:
                payout = Decimal(str(candidate.get("payout_usd", "")))
                activity_timestamp_ms = int(
                    candidate.get("official_activity_timestamp_ms", -1)
                )
                cash_in_baseline = bool(
                    candidate.get("cash_already_in_frozen_baseline", False)
                )
                valid_transaction_hash = (
                    transaction_hash.startswith("0x")
                    and len(transaction_hash) == 66
                    and int(transaction_hash[2:], 16) >= 0
                )
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise LiveConfigurationError(
                    "INVALID_OFFICIAL_REDEEM_CANDIDATE"
                ) from exc
            if (
                not condition_id.startswith("0x")
                or len(condition_id) != 66
                or not winner_token_id
                or not payout.is_finite()
                or payout <= ZERO
                or activity_timestamp_ms < 0
                or not valid_transaction_hash
                or candidate.get("official_activity_type") != "REDEEM"
                or candidate.get("onchain_outcome_balances_zero") is not True
            ):
                raise LiveConfigurationError("INVALID_OFFICIAL_REDEEM_CANDIDATE")
            if condition_id in seen_conditions:
                raise LiveConfigurationError("DUPLICATE_OFFICIAL_REDEEM_CONDITION")
            if transaction_hash in seen_transactions:
                raise LiveConfigurationError("DUPLICATE_OFFICIAL_REDEEM_TRANSACTION")
            seen_conditions.add(condition_id)
            seen_transactions.add(transaction_hash)
            normalized.append(
                {
                    **dict(candidate),
                    "condition_id": condition_id,
                    "winner_token_id": winner_token_id,
                    "transaction_hash": transaction_hash,
                    "payout_usd": payout,
                    "official_activity_timestamp_ms": activity_timestamp_ms,
                    "cash_already_in_frozen_baseline": cash_in_baseline,
                }
            )

        allowed_prior_states = {
            "NOT_SUBMITTED_RETRYABLE",
            "BLOCK_PRE_SUBMISSION_REVALIDATION",
            "BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY",
            "UNKNOWN_SUBMISSION",
            "SUBMIT_STARTED",
            "SUBMITTED_UNRECONCILED",
            "PENDING",
        }
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            active = connection.execute(
                "SELECT COUNT(*) AS count FROM order_reservations WHERE active = 1"
            ).fetchone()
            if active is not None and int(active["count"]) != 0:
                raise LiveConfigurationError(
                    "ACTIVE_ORDER_RESERVATIONS_BLOCK_OFFICIAL_REDEEM_SETTLEMENT"
                )
            account = connection.execute(
                "SELECT * FROM account_state WHERE singleton = 1"
            ).fetchone()
            if account is None:
                raise ScaleInputError("allocation account has not been initialized")
            cash_before = Decimal(str(account["cash_usd"]))
            realized = Decimal(str(account["realized_pnl_usd"]))
            cash_credit = ZERO
            baseline_reclassification = ZERO

            for candidate in normalized:
                condition_id = str(candidate["condition_id"])
                mapping = connection.execute(
                    """
                    SELECT c.condition_id, c.primary_token_id, c.secondary_token_id,
                           COALESCE(p1.quantity, '0') AS primary_quantity,
                           COALESCE(p1.cost_basis_usd, '0') AS primary_cost_basis_usd,
                           COALESCE(p2.quantity, '0') AS secondary_quantity,
                           COALESCE(p2.cost_basis_usd, '0') AS secondary_cost_basis_usd
                    FROM condition_mappings AS c
                    LEFT JOIN positions AS p1 ON p1.token_id = c.primary_token_id
                    LEFT JOIN positions AS p2 ON p2.token_id = c.secondary_token_id
                    WHERE c.condition_id = ?
                    """,
                    (condition_id,),
                ).fetchone()
                if mapping is None:
                    raise LiveConfigurationError(
                        "MISSING_CONDITION_INVENTORY_FOR_OFFICIAL_REDEEM"
                    )
                primary_token_id = str(mapping["primary_token_id"])
                secondary_token_id = str(mapping["secondary_token_id"])
                winner_token_id = str(candidate["winner_token_id"])
                if winner_token_id not in {primary_token_id, secondary_token_id}:
                    raise LiveConfigurationError(
                        "OFFICIAL_REDEEM_WINNER_TOKEN_MISMATCH"
                    )
                winner_quantity = Decimal(
                    str(
                        mapping["primary_quantity"]
                        if winner_token_id == primary_token_id
                        else mapping["secondary_quantity"]
                    )
                )
                payout = Decimal(str(candidate["payout_usd"]))
                if winner_quantity != payout:
                    raise LiveConfigurationError(
                        "OFFICIAL_REDEEM_PAYOUT_INVENTORY_MISMATCH"
                    )
                receipt = connection.execute(
                    "SELECT * FROM redemption_receipts WHERE condition_id = ?",
                    (condition_id,),
                ).fetchone()
                if receipt is not None:
                    prior_state = str(receipt["state"])
                    if prior_state not in allowed_prior_states:
                        raise LiveConfigurationError(
                            "OFFICIAL_REDEEM_PRIOR_STATE_NOT_RECONCILABLE"
                        )
                    prior_hash = str(receipt["transaction_hash"] or "").lower()
                    if prior_hash and prior_hash != str(candidate["transaction_hash"]):
                        raise LiveConfigurationError(
                            "OFFICIAL_REDEEM_TRANSACTION_HASH_MISMATCH"
                        )
                    previous_expected = Decimal(str(receipt["expected_payout_usd"]))
                    if previous_expected not in {ZERO, payout}:
                        raise LiveConfigurationError(
                            "OFFICIAL_REDEEM_PRIOR_PAYOUT_MISMATCH"
                        )

                total_cost = Decimal(str(mapping["primary_cost_basis_usd"])) + Decimal(
                    str(mapping["secondary_cost_basis_usd"])
                )
                for token_id in (primary_token_id, secondary_token_id):
                    connection.execute(
                        """
                        INSERT INTO positions(token_id, quantity, cost_basis_usd)
                        VALUES(?, '0', '0')
                        ON CONFLICT(token_id) DO UPDATE
                        SET quantity = '0', cost_basis_usd = '0'
                        """,
                        (token_id,),
                    )
                if receipt is None:
                    connection.execute(
                        """
                        INSERT INTO redemption_receipts(
                            condition_id, state, expected_payout_usd, transaction_id,
                            transaction_hash, created_at_ms, updated_at_ms
                        ) VALUES(
                            ?, 'REDEEMED_OFFICIAL_ACTIVITY_VERIFIED', ?, NULL, ?, ?, ?
                        )
                        """,
                        (
                            condition_id,
                            str(payout),
                            str(candidate["transaction_hash"]),
                            int(created_at_ms),
                            int(created_at_ms),
                        ),
                    )
                else:
                    connection.execute(
                        """
                        UPDATE redemption_receipts
                        SET state = 'REDEEMED_OFFICIAL_ACTIVITY_VERIFIED',
                            expected_payout_usd = ?, transaction_hash = ?,
                            updated_at_ms = ?
                        WHERE condition_id = ?
                        """,
                        (
                            str(payout),
                            str(candidate["transaction_hash"]),
                            int(created_at_ms),
                            condition_id,
                        ),
                    )

                if bool(candidate["cash_already_in_frozen_baseline"]):
                    connection.execute(
                        """
                        INSERT INTO settlement_cash_reclassification_receipts(
                            condition_id, reclassified_cash_usd,
                            official_activity_timestamp_ms, transaction_hash,
                            created_at_ms, evidence_json
                        ) VALUES(?, ?, ?, ?, ?, ?)
                        """,
                        (
                            condition_id,
                            str(payout),
                            int(candidate["official_activity_timestamp_ms"]),
                            str(candidate["transaction_hash"]),
                            int(created_at_ms),
                            _receipt_json(candidate),
                        ),
                    )
                    baseline_reclassification += payout
                else:
                    cash_credit += payout
                    if quarantine_cash_credit_until_authenticated:
                        self._insert_redeemed_cash_credit_quarantine(
                            connection,
                            condition_id=condition_id,
                            payout_usd=payout,
                            transaction_id=(
                                "official-activity:"
                                + str(candidate["transaction_hash"])
                            ),
                            transaction_hash=str(candidate["transaction_hash"]),
                            created_at_ms=created_at_ms,
                            details={
                                "reason": "OFFICIAL_REDEEM_ACTIVITY_AWAITING_AUTHENTICATED_CLOB_CREDIT",
                                "official_activity": dict(candidate),
                            },
                        )
                realized += payout - total_cost
                self._append_redemption_transition(
                    connection,
                    condition_id=condition_id,
                    state="REDEEMED_OFFICIAL_ACTIVITY_VERIFIED",
                    reason="EXACT_OFFICIAL_REDEEM_ACTIVITY_AND_ZERO_OUTCOME_BALANCES",
                    created_at_ms=created_at_ms,
                    details={
                        **dict(candidate),
                        "written_off_cost_usd": str(total_cost),
                        "cash_credited_usd": (
                            "0"
                            if bool(candidate["cash_already_in_frozen_baseline"])
                            else str(payout)
                        ),
                        "cash_credit_quarantined": bool(
                            quarantine_cash_credit_until_authenticated
                            and not bool(
                                candidate["cash_already_in_frozen_baseline"]
                            )
                        ),
                    },
                )

            gross_reserve_rows = connection.execute(
                "SELECT credited_cash_usd FROM external_cash_reserve_receipts"
            ).fetchall()
            reclassified_rows = connection.execute(
                """
                SELECT reclassified_cash_usd
                FROM settlement_cash_reclassification_receipts
                """
            ).fetchall()
            gross_reserve = sum(
                (
                    Decimal(str(row["credited_cash_usd"]))
                    for row in gross_reserve_rows
                ),
                ZERO,
            )
            total_reclassified = sum(
                (
                    Decimal(str(row["reclassified_cash_usd"]))
                    for row in reclassified_rows
                ),
                ZERO,
            )
            if total_reclassified > gross_reserve:
                raise LiveConfigurationError(
                    "SETTLEMENT_RECLASSIFICATION_EXCEEDS_EXTERNAL_CASH_RESERVE"
                )
            connection.execute(
                """
                UPDATE account_state
                SET cash_usd = ?, realized_pnl_usd = ?
                WHERE singleton = 1
                """,
                (str(cash_before + cash_credit), str(realized)),
            )
        return {
            "condition_count": len(normalized),
            "cash_credited_usd": cash_credit,
            "baseline_cash_reclassified_usd": baseline_reclassification,
            "ledger_cash_before_usd": cash_before,
            "ledger_cash_after_usd": cash_before + cash_credit,
        }

    def settle_losing_condition(
        self,
        *,
        condition_id: str,
        created_at_ms: int,
        details: dict[str, Any],
    ) -> None:
        """Write a fully verified losing condition down without a wallet call."""

        inventory = self.condition_inventory(condition_id)
        if len(inventory) != 1:
            raise LiveConfigurationError("MISSING_CONDITION_INVENTORY_FOR_LOSS")
        row = inventory[0]
        total_cost = row["primary_cost_basis_usd"] + row["secondary_cost_basis_usd"]
        self.initialize()
        with self.connect() as connection:
            account = connection.execute(
                "SELECT * FROM account_state WHERE singleton = 1"
            ).fetchone()
            if account is None:
                raise ScaleInputError("allocation account has not been initialized")
            for token_id in (row["primary_token_id"], row["secondary_token_id"]):
                connection.execute(
                    """
                    INSERT INTO positions(token_id, quantity, cost_basis_usd)
                    VALUES(?, '0', '0')
                    ON CONFLICT(token_id) DO UPDATE SET quantity = '0', cost_basis_usd = '0'
                    """,
                    (token_id,),
                )
            connection.execute(
                """
                UPDATE account_state
                SET realized_pnl_usd = ?
                WHERE singleton = 1
                """,
                (str(Decimal(str(account["realized_pnl_usd"])) - total_cost),),
            )
            connection.execute(
                """
                INSERT INTO redemption_receipts(
                    condition_id, state, expected_payout_usd, transaction_id,
                    transaction_hash, created_at_ms, updated_at_ms
                ) VALUES(?, 'LOSS_RESOLVED_NO_PAYOUT', '0', NULL, NULL, ?, ?)
                """,
                (str(condition_id).lower(), int(created_at_ms), int(created_at_ms)),
            )
            self._append_redemption_transition(
                connection,
                condition_id=condition_id,
                state="LOSS_RESOLVED_NO_PAYOUT",
                reason="",
                created_at_ms=created_at_ms,
                details=details,
            )

    def apply_shared_condition_settlement(
        self,
        *,
        condition_id: str,
        terminal_state: str,
        allocation: Mapping[str, Any],
        transaction_hash: str | None,
        created_at_ms: int,
    ) -> bool:
        """Apply one coordinator-frozen sleeve allocation exactly once."""

        terminal = str(terminal_state)
        allowed_terminal_states = {
            "REDEEMED_SHARED_WALLET",
            "REDEEMED_SHARED_PLATFORM_SETTLEMENT",
            "LOSS_RESOLVED_SHARED_WALLET",
        }
        if terminal not in allowed_terminal_states:
            raise LiveConfigurationError("INVALID_SHARED_SETTLEMENT_STATE")
        condition = str(condition_id).strip().lower()
        primary_token_id = str(allocation.get("primary_token_id") or "").strip()
        secondary_token_id = str(allocation.get("secondary_token_id") or "").strip()
        inventory_hash = str(allocation.get("inventory_hash") or "").strip()
        if not condition or not primary_token_id or not secondary_token_id or not inventory_hash:
            raise LiveConfigurationError("INCOMPLETE_SHARED_SETTLEMENT_ALLOCATION")
        try:
            primary_quantity = Decimal(str(allocation["primary_quantity"]))
            secondary_quantity = Decimal(str(allocation["secondary_quantity"]))
            primary_cost = Decimal(str(allocation["primary_cost_basis_usd"]))
            secondary_cost = Decimal(str(allocation["secondary_cost_basis_usd"]))
            payout = Decimal(str(allocation["payout_usd"]))
        except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
            raise LiveConfigurationError("INVALID_SHARED_SETTLEMENT_ALLOCATION") from exc
        values = (
            primary_quantity,
            secondary_quantity,
            primary_cost,
            secondary_cost,
            payout,
        )
        if any(not value.is_finite() or value < ZERO for value in values):
            raise LiveConfigurationError("INVALID_SHARED_SETTLEMENT_ALLOCATION")
        if terminal == "LOSS_RESOLVED_SHARED_WALLET" and payout != ZERO:
            raise LiveConfigurationError("SHARED_LOSS_HAS_NONZERO_PAYOUT")
        if terminal != "LOSS_RESOLVED_SHARED_WALLET" and payout <= ZERO:
            raise LiveConfigurationError("SHARED_WIN_HAS_NONPOSITIVE_PAYOUT")
        handoff_states = {
            "NOT_SUBMITTED_RETRYABLE",
            "BLOCK_PRE_SUBMISSION_REVALIDATION",
            "BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY",
        }
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            mapping = connection.execute(
                """
                SELECT primary_token_id, secondary_token_id
                FROM condition_mappings WHERE condition_id = ?
                """,
                (condition,),
            ).fetchone()
            if mapping is None or (
                str(mapping["primary_token_id"]),
                str(mapping["secondary_token_id"]),
            ) != (primary_token_id, secondary_token_id):
                raise LiveConfigurationError("SHARED_SETTLEMENT_MAPPING_MISMATCH")
            receipt = connection.execute(
                "SELECT * FROM redemption_receipts WHERE condition_id = ?",
                (condition,),
            ).fetchone()
            if receipt is not None and str(receipt["state"]) in allowed_terminal_states:
                if str(receipt["state"]) != terminal:
                    raise LiveConfigurationError("SHARED_SETTLEMENT_TERMINAL_CONFLICT")
                latest = connection.execute(
                    """
                    SELECT details_json FROM redemption_transitions
                    WHERE condition_id = ? ORDER BY id DESC LIMIT 1
                    """,
                    (condition,),
                ).fetchone()
                details = {} if latest is None else json.loads(str(latest["details_json"]))
                if str(details.get("inventory_hash") or "") != inventory_hash:
                    raise LiveConfigurationError("SHARED_SETTLEMENT_HASH_CONFLICT")
                for token_id in (primary_token_id, secondary_token_id):
                    position = connection.execute(
                        "SELECT quantity, cost_basis_usd FROM positions WHERE token_id = ?",
                        (token_id,),
                    ).fetchone()
                    if position is not None and (
                        Decimal(str(position["quantity"])) != ZERO
                        or Decimal(str(position["cost_basis_usd"])) != ZERO
                    ):
                        raise LiveConfigurationError(
                            "SHARED_SETTLEMENT_IDEMPOTENCE_POSITION_MISMATCH"
                        )
                return False
            if receipt is not None:
                has_transaction = bool(
                    str(receipt["transaction_id"] or "").strip()
                    or str(receipt["transaction_hash"] or "").strip()
                )
                if has_transaction or str(receipt["state"]) not in handoff_states:
                    raise LiveConfigurationError(
                        "LOCAL_REDEMPTION_STATE_BLOCKS_SHARED_SETTLEMENT"
                    )
            current: dict[str, tuple[Decimal, Decimal]] = {}
            for token_id in (primary_token_id, secondary_token_id):
                row = connection.execute(
                    "SELECT quantity, cost_basis_usd FROM positions WHERE token_id = ?",
                    (token_id,),
                ).fetchone()
                current[token_id] = (
                    ZERO if row is None else Decimal(str(row["quantity"])),
                    ZERO if row is None else Decimal(str(row["cost_basis_usd"])),
                )
            if current[primary_token_id] != (primary_quantity, primary_cost):
                raise LiveConfigurationError(
                    "SHARED_SETTLEMENT_PRIMARY_INVENTORY_CHANGED"
                )
            if current[secondary_token_id] != (secondary_quantity, secondary_cost):
                raise LiveConfigurationError(
                    "SHARED_SETTLEMENT_SECONDARY_INVENTORY_CHANGED"
                )
            active = connection.execute(
                """
                SELECT COUNT(*) AS count FROM order_reservations
                WHERE active = 1
                  AND (condition_id = ? OR token_id IN (?, ?))
                """,
                (condition, primary_token_id, secondary_token_id),
            ).fetchone()
            if active is not None and int(active["count"]) != 0:
                raise LiveConfigurationError(
                    "ACTIVE_ORDER_BLOCKS_SHARED_SETTLEMENT"
                )
            account = connection.execute(
                "SELECT * FROM account_state WHERE singleton = 1"
            ).fetchone()
            if account is None:
                raise ScaleInputError("allocation account has not been initialized")
            total_cost = primary_cost + secondary_cost
            cash = Decimal(str(account["cash_usd"])) + payout
            realized = Decimal(str(account["realized_pnl_usd"])) + payout - total_cost
            for token_id in (primary_token_id, secondary_token_id):
                connection.execute(
                    """
                    INSERT INTO positions(
                        token_id, quantity, cost_basis_usd, condition_id
                    ) VALUES(?, '0', '0', ?)
                    ON CONFLICT(token_id) DO UPDATE SET
                        quantity = '0', cost_basis_usd = '0'
                    """,
                    (token_id, condition),
                )
            connection.execute(
                """
                UPDATE account_state SET cash_usd = ?, realized_pnl_usd = ?
                WHERE singleton = 1
                """,
                (str(cash), str(realized)),
            )
            if receipt is None:
                connection.execute(
                    """
                    INSERT INTO redemption_receipts(
                        condition_id, state, expected_payout_usd, transaction_id,
                        transaction_hash, created_at_ms, updated_at_ms
                    ) VALUES(?, ?, ?, NULL, ?, ?, ?)
                    """,
                    (
                        condition,
                        terminal,
                        str(payout),
                        transaction_hash,
                        int(created_at_ms),
                        int(created_at_ms),
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE redemption_receipts
                    SET state = ?, expected_payout_usd = ?,
                        transaction_hash = COALESCE(?, transaction_hash),
                        updated_at_ms = ?
                    WHERE condition_id = ?
                    """,
                    (
                        terminal,
                        str(payout),
                        transaction_hash,
                        int(created_at_ms),
                        condition,
                    ),
                )
            self._append_redemption_transition(
                connection,
                condition_id=condition,
                state=terminal,
                reason="COORDINATOR_FROZEN_SHARED_WALLET_ALLOCATION",
                created_at_ms=created_at_ms,
                details={
                    **dict(allocation),
                    "inventory_hash": inventory_hash,
                    "written_off_cost_usd": str(total_cost),
                },
            )
        return True

    def account_snapshot(self) -> dict[str, Decimal]:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM account_state WHERE singleton = 1"
            ).fetchone()
            reserve_rows = connection.execute(
                "SELECT credited_cash_usd FROM external_cash_reserve_receipts"
            ).fetchall()
            reclassification_rows = connection.execute(
                """
                SELECT reclassified_cash_usd
                FROM settlement_cash_reclassification_receipts
                """
            ).fetchall()
        if row is None:
            raise ScaleInputError("allocation account has not been initialized")
        gross_external_cash_reserve = sum(
            (Decimal(str(receipt["credited_cash_usd"])) for receipt in reserve_rows),
            ZERO,
        )
        settlement_cash_reclassified = sum(
            (
                Decimal(str(receipt["reclassified_cash_usd"]))
                for receipt in reclassification_rows
            ),
            ZERO,
        )
        external_cash_reserve = (
            gross_external_cash_reserve - settlement_cash_reclassified
        )
        if external_cash_reserve < ZERO:
            raise LiveConfigurationError(
                "SETTLEMENT_RECLASSIFICATION_EXCEEDS_EXTERNAL_CASH_RESERVE"
            )
        initial_capital = Decimal(str(row["initial_capital_usd"]))
        gross_capital = initial_capital + external_cash_reserve
        return {
            "initial_capital_usd": initial_capital,
            "cash_usd": Decimal(str(row["cash_usd"])),
            "realized_pnl_usd": Decimal(str(row["realized_pnl_usd"])),
            "fees_usd": Decimal(str(row["fees_usd"])),
            "gross_external_cash_reserve_usd": gross_external_cash_reserve,
            "settlement_cash_reclassified_usd": settlement_cash_reclassified,
            "external_cash_reserve_usd": external_cash_reserve,
            "total_capital_contributed_usd": gross_capital,
        }

    def active_buy_reservations_usd(self) -> Decimal:
        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT cash_reserved_usd
                FROM order_reservations
                WHERE side = 'BUY' AND active = 1
                """
            ).fetchall()
        reserved = sum(
            (Decimal(str(row["cash_reserved_usd"])) for row in rows),
            ZERO,
        )
        return reserved

    def begin_submission_attempt(
        self,
        *,
        source: SourceAction,
        plan: ActionPlan,
        snapshot: Mapping[str, Any],
        condition_id: str,
        created_at_ms: int,
        transition_details: Mapping[str, Any],
        prepared_order: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Atomically reserve, create the attempt, and freeze submit-start."""

        requested = Decimal(str(plan.requested_quantity))
        cash = Decimal(str(plan.reserved_cash_usd)) if plan.side == "BUY" else ZERO
        if requested <= ZERO or cash < ZERO:
            raise LiveConfigurationError("INVALID_SUBMISSION_ATTEMPT_RESERVATION")
        if prepared_order is None:
            prepared_payload: dict[str, Any] = {}
        else:
            candidate = dict(prepared_order)
            order_fields = candidate.get("order_fields")
            if not isinstance(order_fields, Mapping):
                raise LiveConfigurationError("PREPARED_ORDER_FIELDS_MISSING")
            if _contains_secret_material_field(order_fields):
                raise LiveConfigurationError(
                    "PREPARED_ORDER_FIELDS_CONTAIN_SECRET_MATERIAL"
                )
            prepared_payload = {
                key: candidate[key]
                for key in _PREPARED_ORDER_RECEIPT_KEYS
                if key in candidate
            }
            prepared_payload["order_id"] = str(
                prepared_payload.get("order_id") or ""
            ).strip().lower()
            prepared_payload["order_version"] = int(
                prepared_payload.get("order_version")
            )
            prepared_payload["order_type"] = str(
                prepared_payload.get("order_type") or ""
            ).upper()
            prepared_payload["neg_risk"] = bool(
                prepared_payload.get("neg_risk", False)
            )
            prepared_payload["order_fields"] = dict(order_fields)
        prepared_order_id = str(prepared_payload.get("order_id") or "").strip()
        if prepared_payload and not prepared_order_id:
            raise LiveConfigurationError("PREPARED_ORDER_ID_MISSING")
        try:
            prepared_order_json = json.dumps(prepared_payload, sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise LiveConfigurationError(
                "PREPARED_ORDER_RECEIPT_NOT_JSON_SAFE"
            ) from exc
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            unresolved = connection.execute(
                """
                SELECT attempt_id FROM submission_attempts
                WHERE action_id = ? AND state IN (
                    'SUBMIT_STARTED', 'SUBMITTED_UNRECONCILED',
                    'UNKNOWN_SUBMISSION'
                )
                LIMIT 1
                """,
                (source.action_id,),
            ).fetchone()
            if unresolved is not None:
                raise LiveConfigurationError("ACTION_HAS_UNRESOLVED_ATTEMPT")
            reserved = connection.execute(
                """
                INSERT INTO order_reservations(
                    action_id, token_id, side, quantity, cash_reserved_usd,
                    condition_id, active, created_at_ms
                ) VALUES(?, ?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(action_id) DO UPDATE SET
                    token_id = excluded.token_id,
                    side = excluded.side,
                    quantity = excluded.quantity,
                    cash_reserved_usd = excluded.cash_reserved_usd,
                    condition_id = excluded.condition_id,
                    active = 1,
                    created_at_ms = excluded.created_at_ms
                WHERE order_reservations.active = 0
                """,
                (
                    source.action_id,
                    source.token_id,
                    plan.side,
                    str(requested),
                    str(cash),
                    str(condition_id).strip().lower(),
                    int(created_at_ms),
                ),
            )
            if reserved.rowcount != 1:
                raise LiveConfigurationError("ACTION_HAS_ACTIVE_RESERVATION")
            row = connection.execute(
                """
                SELECT COALESCE(MAX(attempt_number), 0) AS number
                FROM submission_attempts WHERE action_id = ?
                """,
                (source.action_id,),
            ).fetchone()
            number = int(row["number"]) + 1
            attempt_id = hashlib.sha256(
                f"{source.action_id}:{number}".encode("utf-8")
            ).hexdigest()
            connection.execute(
                """
                INSERT INTO submission_attempts(
                    attempt_id, action_id, attempt_number, order_id,
                    prepared_order_json, state, requested_quantity,
                    snapshot_json, response_json,
                    created_at_ms, updated_at_ms
                ) VALUES(?, ?, ?, ?, ?, 'SUBMIT_STARTED', ?, ?, '{}', ?, ?)
                """,
                (
                    attempt_id,
                    source.action_id,
                    number,
                    prepared_order_id or None,
                    prepared_order_json,
                    str(requested),
                    _receipt_json(snapshot),
                    int(created_at_ms),
                    int(created_at_ms),
                ),
            )
            target = connection.execute(
                """
                UPDATE action_targets
                SET state = 'SUBMIT_STARTED', reason = ?, updated_at_ms = ?
                WHERE action_id = ?
                """,
                (str(plan.reason), int(created_at_ms), source.action_id),
            )
            if target.rowcount != 1:
                raise LiveConfigurationError("ACTION_TARGET_NOT_FOUND")
            details = {
                **dict(transition_details),
                "attempt_id": attempt_id,
                "attempt_number": number,
            }
            connection.execute(
                """
                INSERT INTO action_transitions(
                    action_id, status, reason, created_at_ms, details_json
                ) VALUES(?, 'SUBMIT_STARTED', ?, ?, ?)
                """,
                (
                    source.action_id,
                    str(plan.reason),
                    int(created_at_ms),
                    _receipt_json(details),
                ),
            )
        return {
            "attempt_id": attempt_id,
            "attempt_number": number,
            "requested_quantity": requested,
            "order_id": prepared_order_id or None,
            "prepared_order": prepared_payload,
            "state": "SUBMIT_STARTED",
        }

    def reconcile_definitive_submission_rejection(
        self,
        *,
        source: SourceAction,
        created_at_ms: int,
    ) -> bool:
        """Append a terminal skip for a prior CLOB response proving no fill.

        It is deliberately narrower than general unknown-submission recovery:
        only an immutable prior ``UNKNOWN_SUBMISSION`` whose original CLOB
        error string matches the strict known-rejection classifier can release
        its cash reservation.  Network ambiguity remains untouched.
        """

        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            terminal = connection.execute(
                """
                SELECT id, status, reason, details_json
                FROM action_transitions
                WHERE action_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (source.action_id,),
            ).fetchone()
            if terminal is None:
                raise LiveConfigurationError("MISSING_HISTORICAL_SUBMISSION_TRANSITION")
            if str(terminal["status"]) != "UNKNOWN_SUBMISSION":
                return False
            definitive_reason = _definitive_clob_rejection_reason(
                RuntimeError(str(terminal["reason"]))
            )
            if definitive_reason is None:
                raise LiveConfigurationError("HISTORICAL_SUBMISSION_NOT_DEFINITIVE_REJECTION")
            connection.execute(
                "UPDATE order_reservations SET active = 0 WHERE action_id = ?",
                (source.action_id,),
            )
            details = {
                "prior_unknown_transition_id": int(terminal["id"]),
                "original_rejection_message": str(terminal["reason"]),
            }
            connection.execute(
                """
                INSERT INTO action_transitions(
                    action_id, status, reason, created_at_ms, details_json
                ) VALUES(?, 'SKIPPED', ?, ?, ?)
                """,
                (
                    source.action_id,
                    definitive_reason,
                    int(created_at_ms),
                    _receipt_json(details),
                ),
            )
        return True

    @staticmethod
    def _apply_fill_on_connection(
        connection: sqlite3.Connection,
        *,
        source: SourceAction,
        quantity: Decimal,
        price: Decimal | None,
        fee_usd: Decimal,
        notional_usd: Decimal | None,
    ) -> None:
        if quantity <= ZERO or fee_usd < ZERO:
            raise ValueError("invalid reconciled fill")
        if notional_usd is None:
            if price is None or price <= ZERO:
                raise ValueError("invalid reconciled fill price")
            notional = quantity * price
        else:
            notional = Decimal(str(notional_usd))
            if notional <= ZERO:
                raise ValueError("invalid reconciled fill notional")
        account = connection.execute(
            "SELECT * FROM account_state WHERE singleton = 1"
        ).fetchone()
        if account is None:
            raise ScaleInputError("allocation account has not been initialized")
        cash = Decimal(str(account["cash_usd"]))
        realized = Decimal(str(account["realized_pnl_usd"]))
        fees = Decimal(str(account["fees_usd"]))
        position = connection.execute(
            "SELECT * FROM positions WHERE token_id = ?", (source.token_id,)
        ).fetchone()
        reservation = connection.execute(
            "SELECT condition_id FROM order_reservations WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()
        held = ZERO if position is None else Decimal(str(position["quantity"]))
        cost_basis = ZERO if position is None else Decimal(str(position["cost_basis_usd"]))
        existing_condition_id = (
            "" if position is None else str(position["condition_id"] or "").lower()
        )
        fill_condition_id = (
            "" if reservation is None else str(reservation["condition_id"] or "").lower()
        )
        if (
            existing_condition_id
            and fill_condition_id
            and existing_condition_id != fill_condition_id
        ):
            raise RuntimeError("POSITION_CONDITION_OWNERSHIP_MISMATCH")
        position_condition_id = existing_condition_id or fill_condition_id
        if source.side == "BUY":
            total_cost = notional + fee_usd
            cash -= total_cost
            held += quantity
            cost_basis += total_cost
        elif source.side == "SELL":
            if quantity > held:
                raise RuntimeError("POSITION_LEDGER_UNDERFLOW")
            average_cost = ZERO if held == ZERO else cost_basis / held
            sold_cost_basis = average_cost * quantity
            proceeds = notional - fee_usd
            cash += proceeds
            realized += proceeds - sold_cost_basis
            held -= quantity
            cost_basis -= sold_cost_basis
        else:
            raise ValueError(f"unsupported fill side: {source.side}")
        connection.execute(
            """
            INSERT INTO positions(token_id, quantity, cost_basis_usd, condition_id)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(token_id) DO UPDATE SET
                quantity = excluded.quantity,
                cost_basis_usd = excluded.cost_basis_usd,
                condition_id = CASE
                    WHEN positions.condition_id = '' THEN excluded.condition_id
                    ELSE positions.condition_id
                END
            """,
            (source.token_id, str(held), str(cost_basis), position_condition_id),
        )
        connection.execute(
            """
            UPDATE account_state
            SET cash_usd = ?, realized_pnl_usd = ?, fees_usd = ?
            WHERE singleton = 1
            """,
            (str(cash), str(realized), str(fees + fee_usd)),
        )
        connection.execute(
            "UPDATE order_reservations SET active = 0 WHERE action_id = ?",
            (source.action_id,),
        )

    def apply_fill_and_finalize(
        self,
        *,
        source: SourceAction,
        quantity: Decimal,
        price: Decimal | None,
        fee_usd: Decimal,
        notional_usd: Decimal | None,
        terminal_status: str,
        reason: str,
        created_at_ms: int,
        details: dict[str, Any],
        maximum_buy_notional_usd: Decimal | None = None,
        maximum_buy_total_cost_usd: Decimal | None = None,
        buy_cash_order_complete: bool = False,
    ) -> None:
        """Atomically mutate cash/inventory, release, and append terminal proof."""

        terminal = str(terminal_status)
        if terminal not in {
            "FILLED",
            "PARTIAL",
            "PARTIAL_PENDING",
            "EXTERNAL_UNFILLABLE",
        }:
            raise ValueError(
                "fill state must be FILLED, PARTIAL, PARTIAL_PENDING, or "
                "EXTERNAL_UNFILLABLE"
            )
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            target = connection.execute(
                "SELECT * FROM action_targets WHERE action_id = ?",
                (source.action_id,),
            ).fetchone()
            if target is not None:
                target_quantity = Decimal(str(target["target_quantity"]))
                previous_filled = Decimal(
                    str(target["cumulative_filled_quantity"])
                )
                cumulative_filled = previous_filled + Decimal(str(quantity))
                requires_buy_cash_proof = (
                    cumulative_filled > target_quantity
                    or bool(buy_cash_order_complete)
                )
                if requires_buy_cash_proof:
                    if (
                        source.side != "BUY"
                        or maximum_buy_notional_usd is None
                        or maximum_buy_total_cost_usd is None
                    ):
                        raise LiveConfigurationError(
                            "CUMULATIVE_FILL_EXCEEDS_ACTION_TARGET"
                        )
                    if notional_usd is None:
                        raise LiveConfigurationError(
                            "BUY_PRICE_IMPROVEMENT_NOTIONAL_MISSING"
                        )
                    maximum_notional = Decimal(str(maximum_buy_notional_usd))
                    maximum_total_cost = Decimal(
                        str(maximum_buy_total_cost_usd)
                    )
                    actual_notional = Decimal(str(notional_usd))
                    reservation = connection.execute(
                        """
                        SELECT side, cash_reserved_usd, active
                        FROM order_reservations WHERE action_id = ?
                        """,
                        (source.action_id,),
                    ).fetchone()
                    if (
                        maximum_notional <= ZERO
                        or actual_notional > maximum_notional
                        or reservation is None
                        or str(reservation["side"]) != "BUY"
                        or int(reservation["active"]) != 1
                        or actual_notional + Decimal(str(fee_usd))
                        > maximum_total_cost
                    ):
                        raise LiveConfigurationError(
                            "BUY_PRICE_IMPROVEMENT_EXCEEDS_FROZEN_CASH_BOUND"
                        )
                    if (
                        buy_cash_order_complete
                        and maximum_notional - actual_notional > TOKEN_RAW_UNIT
                    ):
                        raise LiveConfigurationError(
                            "BUY_CASH_ORDER_COMPLETION_NOT_PROVEN"
                        )
                    terminal = "FILLED"
                elif terminal == "EXTERNAL_UNFILLABLE":
                    if cumulative_filled >= target_quantity:
                        raise LiveConfigurationError(
                            "EXTERNAL_UNFILLABLE_REQUIRES_UNFILLED_REMAINDER"
                        )
                else:
                    terminal = (
                        "FILLED"
                        if cumulative_filled == target_quantity
                        else "PARTIAL_PENDING"
                    )
                connection.execute(
                    """
                    UPDATE action_targets
                    SET cumulative_filled_quantity = ?, state = ?, reason = ?,
                        updated_at_ms = ?
                    WHERE action_id = ?
                    """,
                    (
                        str(cumulative_filled),
                        terminal,
                        str(reason),
                        int(created_at_ms),
                        source.action_id,
                    ),
                )
            self._apply_fill_on_connection(
                connection,
                source=source,
                quantity=quantity,
                price=price,
                fee_usd=fee_usd,
                notional_usd=notional_usd,
            )
            connection.execute(
                """
                INSERT INTO action_transitions(
                    action_id, status, reason, created_at_ms, details_json
                ) VALUES(?, ?, ?, ?, ?)
                """,
                (
                    source.action_id,
                    terminal,
                    str(reason),
                    int(created_at_ms),
                    _receipt_json(details),
                ),
            )
            attempt_id = str(details.get("attempt_id") or "")
            if attempt_id:
                cursor = connection.execute(
                    """
                    UPDATE submission_attempts
                    SET state = ?, response_json = ?, updated_at_ms = ?
                    WHERE attempt_id = ?
                    """,
                    (
                        "FILLED" if terminal == "FILLED" else "PARTIAL_FILLED",
                        _receipt_json(details),
                        int(created_at_ms),
                        attempt_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise LiveConfigurationError("ATTEMPT_NOT_FOUND_FOR_FILL")

    def release_reservation_and_finalize(
        self,
        *,
        source: SourceAction,
        terminal_status: str,
        reason: str,
        created_at_ms: int,
        details: dict[str, Any],
        attempt_id: str | None = None,
        attempt_state: str | None = None,
        attempt_response: Mapping[str, Any] | None = None,
    ) -> None:
        """Atomically close an attempt, record no-fill, and release cash."""

        terminal = str(terminal_status)
        allowed = {
            "SKIPPED",
            "PENDING_LIQUIDITY",
            "PENDING_CONFIRMED_ZERO_FILL",
            "PENDING_PRICE_PROTECTION",
            "PENDING_CAPITAL",
            "PENDING_MINIMUM_UNWIND",
            "PENDING_MINIMUM_REMAINDER",
            "ERROR_INTERNAL",
            "EXTERNAL_UNFILLABLE",
            "EXPIRED_RETRY_WINDOW",
        }
        if terminal not in allowed:
            raise ValueError("unsupported reservation release state")
        normalized_attempt_id = str(attempt_id or "").strip()
        if normalized_attempt_id and not str(attempt_state or "").strip():
            raise ValueError("attempt state is required with attempt id")
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if normalized_attempt_id:
                cursor = connection.execute(
                    """
                    UPDATE submission_attempts
                    SET state = ?, response_json = ?, updated_at_ms = ?
                    WHERE attempt_id = ?
                    """,
                    (
                        str(attempt_state),
                        json.dumps(
                            _sanitize_external_payload(attempt_response or {}),
                            sort_keys=True,
                        ),
                        int(created_at_ms),
                        normalized_attempt_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise LiveConfigurationError("ATTEMPT_NOT_FOUND")
            connection.execute(
                "UPDATE order_reservations SET active = 0 WHERE action_id = ?",
                (source.action_id,),
            )
            connection.execute(
                """
                UPDATE action_targets
                SET state = ?, reason = ?, updated_at_ms = ?
                WHERE action_id = ?
                """,
                (
                    terminal,
                    _redact_sensitive_text(reason),
                    int(created_at_ms),
                    source.action_id,
                ),
            )
            connection.execute(
                """
                INSERT INTO action_transitions(
                    action_id, status, reason, created_at_ms, details_json
                ) VALUES(?, ?, ?, ?, ?)
                """,
                (
                    source.action_id,
                    terminal,
                    _redact_sensitive_text(reason),
                    int(created_at_ms),
                    json.dumps(
                        _sanitize_external_payload(details),
                        sort_keys=True,
                    ),
                ),
            )

    def latest_cash_mutation_transition_id(self) -> int:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COALESCE(MAX(id), 0) AS transition_id
                FROM action_transitions
                WHERE status IN ('FILLED', 'PARTIAL', 'PARTIAL_PENDING')
                """
            ).fetchone()
        return 0 if row is None else int(row["transition_id"])

    def cash_mutation_fingerprint(self) -> str:
        """Return a stable cursor across every ledger cash/PnL mutation path.

        Action fills, platform settlement, relayer redemption, shared-wallet
        settlement, external reserve credit, and authoritative corrections use
        different receipt tables.  A single action-transition id therefore
        cannot prove that the coordinator snapshot reflects the current
        account.  The account values plus each immutable receipt high-water
        mark form one inexpensive restart-safe cursor.
        """

        self.initialize()
        with self.connect() as connection:
            action = connection.execute(
                """
                SELECT COALESCE(MAX(id), 0) AS max_id
                FROM action_transitions
                WHERE status IN ('FILLED', 'PARTIAL', 'PARTIAL_PENDING')
                """
            ).fetchone()
            redemption = connection.execute(
                """
                SELECT COALESCE(MAX(id), 0) AS max_id
                FROM redemption_transitions
                """
            ).fetchone()
            reserve = connection.execute(
                """
                SELECT COALESCE(MAX(id), 0) AS max_id
                FROM external_cash_reserve_receipts
                """
            ).fetchone()
            settlement_reclassification = connection.execute(
                """
                SELECT COUNT(*) AS row_count,
                       COALESCE(MAX(created_at_ms), 0) AS latest_at_ms
                FROM settlement_cash_reclassification_receipts
                """
            ).fetchone()
            correction = connection.execute(
                """
                SELECT COUNT(*) AS row_count,
                       COALESCE(MAX(corrected_at_ms), 0) AS latest_at_ms
                FROM fill_corrections
                """
            ).fetchone()
            mutation_markers = (
                int(action["max_id"]),
                int(redemption["max_id"]),
                int(reserve["max_id"]),
                int(settlement_reclassification["row_count"]),
                int(correction["row_count"]),
            )
            if not any(mutation_markers):
                return ""
            account = connection.execute(
                """
                SELECT initial_capital_usd, cash_usd, realized_pnl_usd, fees_usd
                FROM account_state WHERE singleton = 1
                """
            ).fetchone()
            if account is None:
                return ""
        payload = {
            "account": {
                "initial_capital_usd": str(account["initial_capital_usd"]),
                "cash_usd": str(account["cash_usd"]),
                "realized_pnl_usd": str(account["realized_pnl_usd"]),
                "fees_usd": str(account["fees_usd"]),
            },
            "action_transition_id": mutation_markers[0],
            "redemption_transition_id": mutation_markers[1],
            "external_cash_reserve_receipt_id": mutation_markers[2],
            "settlement_cash_reclassification_count": mutation_markers[3],
            "settlement_cash_reclassification_latest_at_ms": int(
                settlement_reclassification["latest_at_ms"]
            ),
            "fill_correction_count": mutation_markers[4],
            "fill_correction_latest_at_ms": int(correction["latest_at_ms"]),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()

    def apply_authoritative_buy_fill_correction(
        self,
        *,
        source: SourceAction,
        previous_quantity: Decimal,
        authoritative_quantity: Decimal,
        previous_fee_usd: Decimal,
        authoritative_fee_usd: Decimal,
        expected_quantity: Decimal,
        price: Decimal,
        created_at_ms: int,
        details: dict[str, Any],
        previous_notional_usd: Decimal | None = None,
        authoritative_notional_usd: Decimal | None = None,
        full_match: bool | None = None,
    ) -> bool:
        """Append an immutable correction for an already matched live BUY.

        This repairs or verifies ledger state from an official CLOB/on-chain
        receipt.  It never posts, cancels, or re-prices an order.  The prior
        local fee and cash notional may both be corrected downward as well as
        upward; forbidding a downward fee correction would preserve a known
        accounting error.
        """

        if source.side != "BUY":
            raise LiveConfigurationError("BUY_FILL_CORRECTION_SIDE_MISMATCH")
        if (
            previous_quantity < ZERO
            or authoritative_quantity < previous_quantity
            or previous_fee_usd < ZERO
            or authoritative_fee_usd < ZERO
            or expected_quantity <= ZERO
            or price <= ZERO
        ):
            raise LiveConfigurationError("INVALID_BUY_FILL_CORRECTION_INPUT")
        prior_notional = (
            previous_quantity * price
            if previous_notional_usd is None
            else Decimal(str(previous_notional_usd))
        )
        authoritative_notional = (
            authoritative_quantity * price
            if authoritative_notional_usd is None
            else Decimal(str(authoritative_notional_usd))
        )
        if prior_notional < ZERO or authoritative_notional <= ZERO:
            raise LiveConfigurationError("INVALID_BUY_FILL_CORRECTION_NOTIONAL")
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT action_id FROM fill_corrections WHERE action_id = ?",
                (source.action_id,),
            ).fetchone()
            if existing is not None:
                return False
            terminal = connection.execute(
                """
                SELECT id, status, reason, details_json
                FROM action_transitions
                WHERE action_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (source.action_id,),
            ).fetchone()
            if terminal is None or str(terminal["status"]) not in {"PARTIAL", "FILLED", "ERROR"}:
                raise LiveConfigurationError("CORRECTION_SOURCE_NOT_RECONCILED_FILL")
            if (
                str(terminal["status"]) == "ERROR"
                and str(terminal["reason"]) != "FILL_EXCEEDS_RECORDED_TARGET"
            ):
                raise LiveConfigurationError("CORRECTION_SOURCE_ERROR_NOT_PROVEN_MATCH")
            account = connection.execute(
                "SELECT * FROM account_state WHERE singleton = 1"
            ).fetchone()
            if account is None:
                raise ScaleInputError("allocation account has not been initialized")
            position = connection.execute(
                "SELECT * FROM positions WHERE token_id = ?", (source.token_id,)
            ).fetchone()
            held = ZERO if position is None else Decimal(str(position["quantity"]))
            cost_basis = (
                ZERO if position is None else Decimal(str(position["cost_basis_usd"]))
            )
            delta_quantity = authoritative_quantity - previous_quantity
            delta_fee = authoritative_fee_usd - previous_fee_usd
            delta_notional = authoritative_notional - prior_notional
            delta_cost = delta_notional + delta_fee
            cash = Decimal(str(account["cash_usd"]))
            corrected_cash = cash - delta_cost
            corrected_fees = Decimal(str(account["fees_usd"])) + delta_fee
            corrected_held = held + delta_quantity
            corrected_cost_basis = cost_basis + delta_cost
            correction_details = {
                **details,
                "source_transition_id": int(terminal["id"]),
                "previous_quantity": str(previous_quantity),
                "authoritative_quantity": str(authoritative_quantity),
                "delta_quantity": str(delta_quantity),
                "previous_notional_usd": str(prior_notional),
                "authoritative_notional_usd": str(authoritative_notional),
                "delta_notional_usd": str(delta_notional),
                "previous_fee_usd": str(previous_fee_usd),
                "authoritative_fee_usd": str(authoritative_fee_usd),
                "delta_fee_usd": str(delta_fee),
                "delta_cash_cost_usd": str(delta_cost),
                "expected_quantity": str(expected_quantity),
                "price": str(price),
            }
            connection.execute(
                """
                INSERT INTO fill_corrections(
                    action_id, source_transition_id, previous_quantity,
                    authoritative_quantity, previous_fee_usd,
                    authoritative_fee_usd, corrected_at_ms, details_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source.action_id,
                    int(terminal["id"]),
                    str(previous_quantity),
                    str(authoritative_quantity),
                    str(previous_fee_usd),
                    str(authoritative_fee_usd),
                    int(created_at_ms),
                    _receipt_json(correction_details),
                ),
            )
            connection.execute(
                """
                INSERT INTO positions(token_id, quantity, cost_basis_usd)
                VALUES(?, ?, ?)
                ON CONFLICT(token_id) DO UPDATE SET
                    quantity = excluded.quantity,
                    cost_basis_usd = excluded.cost_basis_usd
                """,
                (source.token_id, str(corrected_held), str(corrected_cost_basis)),
            )
            connection.execute(
                """
                UPDATE account_state
                SET cash_usd = ?, fees_usd = ?
                WHERE singleton = 1
                """,
                (str(corrected_cash), str(corrected_fees)),
            )
            is_full_match = (
                authoritative_quantity == expected_quantity
                if full_match is None
                else bool(full_match)
            )
            terminal_status = "FILLED" if is_full_match else "PARTIAL"
            correction_reason = (
                "ACCOUNTING_CORRECTED_FROM_OFFICIAL_ONCHAIN_FILL"
                if (
                    str(terminal["status"]) == "ERROR"
                    or delta_quantity != ZERO
                    or delta_notional != ZERO
                    or delta_fee != ZERO
                )
                else "ACCOUNTING_VERIFIED_FROM_OFFICIAL_ONCHAIN_FILL"
            )
            connection.execute(
                """
                INSERT INTO action_transitions(
                    action_id, status, reason, created_at_ms, details_json
                ) VALUES(?, ?, ?, ?, ?)
                """,
                (
                    source.action_id,
                    terminal_status,
                    correction_reason,
                    int(created_at_ms),
                    _receipt_json(correction_details),
                ),
            )
            connection.execute(
                "UPDATE order_reservations SET active = 0 WHERE action_id = ?",
                (source.action_id,),
            )
        return True

    @staticmethod
    def _insert_action_receipt_on_connection(
        connection: sqlite3.Connection,
        source: SourceAction,
    ) -> bool:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO action_receipts(
                action_id, transaction_hash, token_id, side, order_hash,
                source_quantity, source_notional, source_timestamp,
                block_number, source_log_index, block_hash, source_role,
                discovered_at_ms
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source.action_id,
                source.transaction_hash.lower(),
                str(source.token_id),
                source.side.upper(),
                source.order_hash.lower(),
                str(source.source_quantity),
                str(source.source_notional),
                int(source.source_timestamp),
                int(source.block_number),
                int(source.log_index),
                source.block_hash.lower(),
                source.source_role,
                int(source.discovered_at_ms),
            ),
        )
        return cursor.rowcount == 1

    def record_action_receipt(self, source: SourceAction) -> bool:
        self.initialize()
        with self.connect() as connection:
            return self._insert_action_receipt_on_connection(connection, source)

    def persist_source_action_batch(self, actions: list[SourceAction]) -> None:
        """Atomically persist every receipt and its initial durable state."""

        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for source in actions:
                self._insert_action_receipt_on_connection(connection, source)
                latest = connection.execute(
                    """
                    SELECT id
                    FROM action_transitions
                    WHERE action_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (source.action_id,),
                ).fetchone()
                if latest is not None:
                    continue
                connection.execute(
                    """
                    INSERT INTO action_transitions(
                        action_id, status, reason, created_at_ms, details_json
                    ) VALUES(?, 'OBSERVED', '', ?, ?)
                    """,
                    (
                        source.action_id,
                        int(source.discovered_at_ms),
                        _receipt_json({}),
                    ),
                )

    def public_source_observation_ids(self, row_ids: Iterable[str]) -> set[str]:
        """Return already persisted public-wallet evidence row identifiers."""

        normalized = [str(row_id) for row_id in row_ids if str(row_id)]
        if not normalized:
            return set()
        self.initialize()
        with self.connect() as connection:
            # Do not rely on a driver-specific maximum number of SQLite bind
            # values.  This path is bounded by the official Data API page and
            # runs before any executable public-wallet action is admitted.
            return {
                row_id
                for row_id in normalized
                if connection.execute(
                    "SELECT 1 FROM public_source_observations WHERE row_id = ?",
                    (row_id,),
                ).fetchone()
                is not None
            }

    def record_public_source_observations(
        self,
        *,
        rows: Iterable[Mapping[str, Any]],
        state: str,
        observed_at_ms: int,
        source_action_id: str | None = None,
    ) -> int:
        """Persist immutable public-wallet source evidence before reuse.

        A Data API row may reconcile to a maker-chain action, define a verified
        taker action, or establish a forward watermark.  In every case the
        original public fields are retained, while a later poll cannot mutate
        their state or turn a delayed row into a new source order.
        """

        self.initialize()
        inserted = 0
        with self.connect() as connection:
            for row in rows:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO public_source_observations(
                        row_id, transaction_hash, token_id, side,
                        source_quantity, source_price, source_timestamp,
                        observed_at_ms, state, source_action_id, details_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(row["row_id"]),
                        str(row["transaction_hash"]).lower(),
                        str(row["token_id"]),
                        str(row["side"]).upper(),
                        str(row["source_quantity"]),
                        str(row["source_price"]),
                        int(row["source_timestamp"]),
                        int(observed_at_ms),
                        str(state),
                        None if source_action_id is None else str(source_action_id),
                        json.dumps(
                            _sanitize_external_payload(
                                dict(row.get("raw", {}))
                            ),
                            sort_keys=True,
                        ),
                    ),
                )
                inserted += int(cursor.rowcount)
        return inserted

    def constrain_action_to_no_action_time_book(
        self,
        *,
        source: SourceAction,
        reason: str,
        created_at_ms: int,
        details: Mapping[str, Any],
    ) -> None:
        """Persist a non-executable action-time-book constraint exactly once."""

        self.initialize()
        if not self.record_action_receipt(source):
            pass
        with self.connect() as connection:
            existing = connection.execute(
                """
                SELECT reason, details_json
                FROM action_execution_constraints
                WHERE action_id = ?
                """,
                (source.action_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["reason"]) != str(reason):
                    raise LiveConfigurationError(
                        "ACTION_EXECUTION_CONSTRAINT_CONFLICT"
                    )
                return
            connection.execute(
                """
                INSERT INTO action_execution_constraints(
                    action_id, reason, created_at_ms, details_json
                ) VALUES(?, ?, ?, ?)
                """,
                (
                    source.action_id,
                    str(reason),
                    int(created_at_ms),
                    json.dumps(
                        _sanitize_external_payload(dict(details)),
                        sort_keys=True,
                    ),
                ),
            )

    def action_execution_constraint(
        self, source: SourceAction
    ) -> dict[str, Any] | None:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT reason, created_at_ms, details_json
                FROM action_execution_constraints
                WHERE action_id = ?
                """,
                (source.action_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "reason": str(row["reason"]),
            "created_at_ms": int(row["created_at_ms"]),
            "details": json.loads(str(row["details_json"])),
        }

    def has_followable_source_action_triplet(
        self, *, transaction_hash: str, token_id: str, side: str
    ) -> bool:
        """Check whether a public row already reconciles to a source action."""

        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM action_receipts
                WHERE transaction_hash = ? AND token_id = ? AND side = ?
                  AND LOWER(source_role) IN (?, ?)
                LIMIT 1
                """,
                (
                    str(transaction_hash).lower(),
                    str(token_id),
                    str(side).upper(),
                    SOURCE_ROLE_CHAIN_MAKER,
                    SOURCE_ROLE_VERIFIED_PUBLIC_WALLET,
                ),
            ).fetchone()
        return row is not None

    def freeze_action_metadata(
        self,
        *,
        source: SourceAction,
        metadata: Mapping[str, Any],
        profile_follow: bool,
        profile_reason: str,
        frozen_at_ms: int,
    ) -> bool:
        """Persist the first official action/event identity without overwrites."""

        self.initialize()
        condition_id = str(metadata.get("condition_id") or "").strip().lower()
        market_slug = str(metadata.get("market_slug") or "").strip().lower()
        event_slug = str(metadata.get("event_slug") or "").strip().lower()
        if not condition_id or not market_slug or not event_slug:
            raise LiveConfigurationError("INCOMPLETE_ACTION_MARKET_METADATA")
        payload = dict(metadata)
        metadata_json = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=str
        )
        evidence_hash = canonical_hash(
            {
                "metadata": payload,
                "profile_follow": bool(profile_follow),
                "profile_reason": str(profile_reason),
            }
        )
        event_hash = canonical_hash({"event_slug": event_slug})
        with self.connect() as connection:
            existing = connection.execute(
                "SELECT * FROM action_market_metadata WHERE action_id = ?",
                (source.action_id,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["condition_id"]) != condition_id
                    or str(existing["market_slug"]) != market_slug
                    or str(existing["event_slug"]) != event_slug
                    or int(existing["profile_follow"]) != int(bool(profile_follow))
                    or str(existing["profile_reason"]) != str(profile_reason)
                    or str(existing["evidence_hash"]) != evidence_hash
                ):
                    raise LiveConfigurationError(
                        "FROZEN_ACTION_MARKET_METADATA_CONFLICT"
                    )
                return False
            connection.execute(
                """
                INSERT INTO action_market_metadata(
                    action_id, condition_id, market_slug, event_slug,
                    profile_follow, profile_reason, metadata_json,
                    evidence_hash, frozen_at_ms
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source.action_id,
                    condition_id,
                    market_slug,
                    event_slug,
                    int(bool(profile_follow)),
                    str(profile_reason),
                    metadata_json,
                    evidence_hash,
                    int(frozen_at_ms),
                ),
            )
            connection.execute(
                """
                INSERT INTO decision_units(
                    event_slug, metadata_hash, first_source_timestamp,
                    last_source_timestamp, created_at_ms, updated_at_ms
                ) VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_slug) DO UPDATE SET
                    first_source_timestamp = MIN(
                        decision_units.first_source_timestamp,
                        excluded.first_source_timestamp
                    ),
                    last_source_timestamp = MAX(
                        decision_units.last_source_timestamp,
                        excluded.last_source_timestamp
                    ),
                    updated_at_ms = MAX(
                        decision_units.updated_at_ms,
                        excluded.updated_at_ms
                    )
                """,
                (
                    event_slug,
                    event_hash,
                    int(source.source_timestamp),
                    int(source.source_timestamp),
                    int(frozen_at_ms),
                    int(frozen_at_ms),
                ),
            )
        return True

    def frozen_action_metadata(self, action_id: str) -> dict[str, Any] | None:
        """Return the immutable profile decision after a crash-window restart."""

        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM action_market_metadata WHERE action_id = ?",
                (str(action_id),),
            ).fetchone()
        if row is None:
            return None
        try:
            metadata = json.loads(str(row["metadata_json"]))
        except json.JSONDecodeError as exc:
            raise LiveConfigurationError(
                "INVALID_FROZEN_ACTION_MARKET_METADATA_JSON"
            ) from exc
        if not isinstance(metadata, dict):
            raise LiveConfigurationError(
                "INVALID_FROZEN_ACTION_MARKET_METADATA_JSON"
            )
        if (
            str(metadata.get("condition_id") or "").strip().lower()
            != str(row["condition_id"])
            or str(metadata.get("market_slug") or "").strip().lower()
            != str(row["market_slug"])
            or str(metadata.get("event_slug") or "").strip().lower()
            != str(row["event_slug"])
        ):
            raise LiveConfigurationError(
                "FROZEN_ACTION_MARKET_METADATA_IDENTITY_MISMATCH"
            )
        return {
            "metadata": metadata,
            "profile_follow": bool(int(row["profile_follow"])),
            "profile_reason": str(row["profile_reason"]),
            "evidence_hash": str(row["evidence_hash"]),
            "frozen_at_ms": int(row["frozen_at_ms"]),
        }

    def record_source_topic_alert(
        self,
        *,
        source: SourceAction,
        metadata: Mapping[str, Any],
        processing_state: str,
        created_at_ms: int,
    ) -> bool:
        """Persist a non-Netflix notice without changing execution eligibility."""

        classification = str(
            metadata.get("topic_classification") or ""
        ).strip().upper()
        if classification == "NETFLIX":
            return False
        if classification != "NON_NETFLIX":
            raise LiveConfigurationError("INVALID_SOURCE_TOPIC_CLASSIFICATION")
        event_slug = str(metadata.get("event_slug") or "").strip().lower()
        market_slug = str(metadata.get("market_slug") or "").strip().lower()
        state = str(processing_state or "").strip()
        if not event_slug or not market_slug or not state:
            raise LiveConfigurationError("INCOMPLETE_SOURCE_TOPIC_ALERT")
        details_json = _receipt_json(dict(metadata))
        requested = {
            "action_id": source.action_id,
            "topic_classification": classification,
            "event_slug": event_slug,
            "market_slug": market_slug,
            "side": source.side.upper(),
            "source_timestamp": int(source.source_timestamp),
            "discovered_at_ms": int(source.discovered_at_ms),
            "processing_state": state,
            "details_json": details_json,
        }
        self.initialize()
        with self.connect() as connection:
            existing = connection.execute(
                "SELECT * FROM source_topic_alerts WHERE action_id = ?",
                (source.action_id,),
            ).fetchone()
            if existing is not None:
                actual = {
                    key: existing[key]
                    for key in requested
                }
                if actual != requested:
                    raise LiveConfigurationError(
                        "SOURCE_TOPIC_ALERT_IDENTITY_CONFLICT"
                    )
                return False
            connection.execute(
                """
                INSERT INTO source_topic_alerts(
                    action_id, topic_classification, event_slug, market_slug,
                    side, source_timestamp, discovered_at_ms,
                    processing_state, details_json, acknowledged_at_ms
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                tuple(requested.values()),
            )
        return True

    def source_topic_alerts(
        self, *, unacknowledged_only: bool = True
    ) -> list[dict[str, Any]]:
        """Return immutable topic notices in source causal order."""

        self.initialize()
        where = "WHERE acknowledged_at_ms IS NULL" if unacknowledged_only else ""
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM source_topic_alerts
                {where}
                ORDER BY source_timestamp, discovered_at_ms, action_id
                """
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            details = json.loads(str(row["details_json"]))
            result.append(
                {
                    "action_id": str(row["action_id"]),
                    "topic_classification": str(row["topic_classification"]),
                    "event_slug": str(row["event_slug"]),
                    "market_slug": str(row["market_slug"]),
                    "side": str(row["side"]),
                    "source_timestamp": int(row["source_timestamp"]),
                    "discovered_at_ms": int(row["discovered_at_ms"]),
                    "processing_state": str(row["processing_state"]),
                    "details": details,
                    "acknowledged_at_ms": (
                        None
                        if row["acknowledged_at_ms"] is None
                        else int(row["acknowledged_at_ms"])
                    ),
                }
            )
        return result

    def ensure_action_target(
        self,
        *,
        source: SourceAction,
        proportional_quantity: Decimal,
        target_quantity: Decimal,
        state: str,
        reason: str,
        updated_at_ms: int,
    ) -> dict[str, Any]:
        """Create one immutable quantity target; progress changes separately."""

        proportional = Decimal(str(proportional_quantity))
        target = Decimal(str(target_quantity))
        if proportional <= ZERO or target <= ZERO:
            raise LiveConfigurationError("INVALID_ACTION_TARGET_QUANTITY")
        self.initialize()
        with self.connect() as connection:
            existing = connection.execute(
                "SELECT * FROM action_targets WHERE action_id = ?",
                (source.action_id,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO action_targets(
                        action_id, proportional_quantity, target_quantity,
                        cumulative_filled_quantity, state, reason,
                        created_at_ms, updated_at_ms
                    ) VALUES(?, ?, ?, '0', ?, ?, ?, ?)
                    """,
                    (
                        source.action_id,
                        str(proportional),
                        str(target),
                        str(state),
                        str(reason),
                        int(updated_at_ms),
                        int(updated_at_ms),
                    ),
                )
            else:
                if (
                    Decimal(str(existing["proportional_quantity"])) != proportional
                    or Decimal(str(existing["target_quantity"])) != target
                ):
                    raise LiveConfigurationError("FROZEN_ACTION_TARGET_CONFLICT")
        result = self.action_target(source.action_id)
        if result is None:
            raise LiveConfigurationError("ACTION_TARGET_NOT_PERSISTED")
        return result

    def action_target(self, action_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM action_targets WHERE action_id = ?",
                (str(action_id),),
            ).fetchone()
        if row is None:
            return None
        target = Decimal(str(row["target_quantity"]))
        filled = Decimal(str(row["cumulative_filled_quantity"]))
        state = str(row["state"])
        remaining = ZERO if state == "FILLED" else max(target - filled, ZERO)
        surplus = max(filled - target, ZERO)
        shortfall = (
            max(target - filled, ZERO) if state == "FILLED" else ZERO
        )
        return {
            "action_id": str(row["action_id"]),
            "proportional_quantity": Decimal(str(row["proportional_quantity"])),
            "target_quantity": target,
            "cumulative_filled_quantity": filled,
            "remaining_quantity": remaining,
            "fill_surplus_quantity": surplus,
            "fill_shortfall_quantity": shortfall,
            "state": state,
            "reason": str(row["reason"]),
            "created_at_ms": int(row["created_at_ms"]),
            "updated_at_ms": int(row["updated_at_ms"]),
        }

    def original_submission_minimum_order_size(
        self, *, action_id: str
    ) -> Decimal | None:
        """Return the action's first persisted submission minimum, if present.

        A later retry is a new discovery-time snapshot, not authority to
        reclassify an older FAK residue. The first submission attempt is the
        immutable source for this terminal residual check.
        """

        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT snapshot_json
                FROM submission_attempts
                WHERE action_id = ?
                ORDER BY attempt_number ASC, created_at_ms ASC
                LIMIT 1
                """,
                (str(action_id),),
            ).fetchone()
        if row is None:
            return None
        try:
            snapshot = json.loads(str(row["snapshot_json"]))
        except json.JSONDecodeError as exc:
            raise LiveConfigurationError(
                "INVALID_RECORDED_SUBMISSION_SNAPSHOT"
            ) from exc
        return _recorded_minimum_order_size_from_snapshot(snapshot)

    def set_action_target_state(
        self,
        *,
        source: SourceAction,
        state: str,
        reason: str,
        updated_at_ms: int,
    ) -> None:
        self.initialize()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE action_targets
                SET state = ?, reason = ?, updated_at_ms = ?
                WHERE action_id = ?
                """,
                (
                    str(state),
                    _redact_sensitive_text(reason),
                    int(updated_at_ms),
                    source.action_id,
                ),
            )
            if cursor.rowcount != 1:
                raise LiveConfigurationError("ACTION_TARGET_NOT_FOUND")

    def set_attempt_order_id(
        self,
        *,
        attempt_id: str,
        order_id: str,
        response: Mapping[str, Any],
        updated_at_ms: int,
    ) -> None:
        self.initialize()
        normalized_order_id = str(order_id).strip()
        if not normalized_order_id:
            raise LiveConfigurationError("MISSING_ATTEMPT_ORDER_ID")
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE submission_attempts
                SET order_id = ?, state = 'SUBMITTED_UNRECONCILED',
                    response_json = ?, updated_at_ms = ?
                WHERE attempt_id = ? AND state = 'SUBMIT_STARTED'
                """,
                (
                    normalized_order_id,
                    json.dumps(_sanitize_external_payload(response), sort_keys=True),
                    int(updated_at_ms),
                    str(attempt_id),
                ),
            )
            if cursor.rowcount != 1:
                raise LiveConfigurationError("ATTEMPT_ORDER_ID_STATE_CONFLICT")

    def update_attempt_state(
        self,
        *,
        attempt_id: str,
        state: str,
        response: Mapping[str, Any],
        updated_at_ms: int,
    ) -> None:
        self.initialize()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE submission_attempts
                SET state = ?, response_json = ?, updated_at_ms = ?
                WHERE attempt_id = ?
                """,
                (
                    str(state),
                    json.dumps(_sanitize_external_payload(response), sort_keys=True),
                    int(updated_at_ms),
                    str(attempt_id),
                ),
            )
            if cursor.rowcount != 1:
                raise LiveConfigurationError("ATTEMPT_NOT_FOUND")

    def mark_official_order_absent_as_unknown(
        self, *, source: SourceAction, attempt_id: str, order_id: str,
        created_at_ms: int,
    ) -> bool:
        """Record one absent-order result without erasing accepted POST proof."""
        self.initialize()
        reason = "OFFICIAL_ORDER_NOT_FOUND"
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            latest = connection.execute(
                "SELECT status, reason FROM action_transitions WHERE action_id = ? ORDER BY id DESC LIMIT 1",
                (source.action_id,),
            ).fetchone()
            if (latest is not None and str(latest["status"]) == "UNKNOWN_SUBMISSION"
                    and str(latest["reason"]) == reason):
                return False
            cursor = connection.execute(
                "UPDATE submission_attempts SET state = 'UNKNOWN_SUBMISSION', updated_at_ms = ? WHERE attempt_id = ?",
                (int(created_at_ms), str(attempt_id)),
            )
            if cursor.rowcount != 1:
                raise LiveConfigurationError("ATTEMPT_NOT_FOUND")
            cursor = connection.execute(
                "UPDATE action_targets SET state = 'UNKNOWN_SUBMISSION', reason = ?, updated_at_ms = ? WHERE action_id = ?",
                (reason, int(created_at_ms), source.action_id),
            )
            if cursor.rowcount != 1:
                raise LiveConfigurationError("ACTION_TARGET_NOT_FOUND")
            connection.execute(
                "INSERT INTO action_transitions(action_id, status, reason, created_at_ms, details_json) VALUES(?, 'UNKNOWN_SUBMISSION', ?, ?, ?)",
                (source.action_id, reason, int(created_at_ms), _receipt_json({"order_id": str(order_id), "attempt_id": str(attempt_id)})),
            )
        return True

    def retain_missing_order_id_as_unknown(
        self,
        *,
        source: SourceAction,
        attempt_id: str,
        created_at_ms: int,
    ) -> None:
        """Atomically retain an unqueryable submission as UNKNOWN.

        No order identifier means there is no safe authenticated read and no
        evidence that permits either reposting or releasing its reservation.
        The attempt, target, and immutable transition are therefore advanced
        together so a second recovery pass sees the same unresolved state.
        """

        normalized_attempt_id = str(attempt_id).strip()
        if not normalized_attempt_id:
            raise LiveConfigurationError(
                "MISSING_ATTEMPT_ID_FOR_ORDER_ID_RECOVERY"
            )
        reason = "MISSING_ORDER_ID_FOR_RECONCILIATION"
        terminal_states = {
            "SKIPPED",
            "FILLED",
            "PARTIAL",
            "ERROR",
            "ERROR_INTERNAL",
            "EXTERNAL_UNFILLABLE",
            "SUPERSEDED_UNFILLED",
        }
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            attempt = connection.execute(
                """
                SELECT attempt_number, order_id, state
                FROM submission_attempts
                WHERE attempt_id = ? AND action_id = ?
                """,
                (normalized_attempt_id, source.action_id),
            ).fetchone()
            if attempt is None:
                raise LiveConfigurationError(
                    "ATTEMPT_NOT_FOUND_FOR_ORDER_ID_RECOVERY"
                )
            if str(attempt["state"]) not in {
                "SUBMIT_STARTED",
                "SUBMITTED_UNRECONCILED",
                "UNKNOWN_SUBMISSION",
            }:
                raise LiveConfigurationError(
                    "ATTEMPT_STATE_CONFLICT_FOR_ORDER_ID_RECOVERY"
                )
            if str(attempt["order_id"] or "").strip():
                raise LiveConfigurationError(
                    "ATTEMPT_ORDER_ID_APPEARED_DURING_RECOVERY"
                )
            latest = connection.execute(
                """
                SELECT status, reason FROM action_transitions
                WHERE action_id = ? ORDER BY id DESC LIMIT 1
                """,
                (source.action_id,),
            ).fetchone()
            if latest is None:
                raise LiveConfigurationError(
                    "MISSING_TRANSITION_FOR_ORDER_ID_RECOVERY"
                )
            if str(latest["status"]) in terminal_states:
                raise LiveConfigurationError(
                    "TERMINAL_ACTION_CONFLICT_FOR_ORDER_ID_RECOVERY"
                )
            target = connection.execute(
                "SELECT state FROM action_targets WHERE action_id = ?",
                (source.action_id,),
            ).fetchone()
            if target is None:
                raise LiveConfigurationError(
                    "ACTION_TARGET_NOT_FOUND_FOR_ORDER_ID_RECOVERY"
                )
            if str(target["state"]) in terminal_states:
                raise LiveConfigurationError(
                    "TERMINAL_TARGET_CONFLICT_FOR_ORDER_ID_RECOVERY"
                )
            already_retained = (
                str(attempt["state"]) == "UNKNOWN_SUBMISSION"
                and str(target["state"]) == "UNKNOWN_SUBMISSION"
                and str(latest["status"]) == "UNKNOWN_SUBMISSION"
                and str(latest["reason"]) == reason
            )
            if already_retained:
                return
            connection.execute(
                """
                UPDATE submission_attempts
                SET state = 'UNKNOWN_SUBMISSION', updated_at_ms = ?
                WHERE attempt_id = ?
                """,
                (int(created_at_ms), normalized_attempt_id),
            )
            updated_target = connection.execute(
                """
                UPDATE action_targets
                SET state = 'UNKNOWN_SUBMISSION', reason = ?, updated_at_ms = ?
                WHERE action_id = ?
                """,
                (reason, int(created_at_ms), source.action_id),
            )
            if updated_target.rowcount != 1:
                raise LiveConfigurationError(
                    "ACTION_TARGET_NOT_FOUND_FOR_ORDER_ID_RECOVERY"
                )
            connection.execute(
                """
                INSERT INTO action_transitions(
                    action_id, status, reason, created_at_ms, details_json
                ) VALUES(?, 'UNKNOWN_SUBMISSION', ?, ?, ?)
                """,
                (
                    source.action_id,
                    reason,
                    int(created_at_ms),
                    _receipt_json(
                        {
                            "attempt_id": normalized_attempt_id,
                            "attempt_number": int(attempt["attempt_number"]),
                            "new_order_submitted_by_recovery": False,
                            "original_submission_state": "UNKNOWN",
                            "reservation_released": False,
                        }
                    ),
                ),
            )

    def submission_attempt_count(self, action_id: str | None = None) -> int:
        self.initialize()
        with self.connect() as connection:
            if action_id is None:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM submission_attempts"
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT COUNT(*) AS count FROM submission_attempts
                    WHERE action_id = ?
                    """,
                    (str(action_id),),
                ).fetchone()
        return int(row["count"])

    def reopen_pre_submission_no_book_errors(
        self, *, created_at_ms: int
    ) -> list[str]:
        """Append a retryable state for safe, historical empty-book errors.

        The prior path classified a confirmed absence of the relevant CLOB side
        as ``ERROR``. Only actions with no active or uncertain submission may
        be reopened. Historical transitions remain immutable; this adds a
        repair receipt without changing orders, fills, positions, or cash.
        """

        created_at = int(created_at_ms)
        if created_at < 0:
            raise LiveConfigurationError("INVALID_REOPEN_NO_BOOK_TIMESTAMP")
        runtime_empty_book_reasons = tuple(
            "BOOK_SNAPSHOT_ERROR: RuntimeError: " + reason
            for reason in sorted(RETRYABLE_EMPTY_BOOK_LEVEL_ERRORS)
        )
        retryable_target_states = (
            "PENDING_LIQUIDITY",
            "PARTIAL_PENDING",
            "PENDING_CAPITAL",
            "PENDING_MINIMUM_UNWIND",
            "PENDING_MINIMUM_REMAINDER",
            "PENDING_EXTERNAL_RETRY",
        )
        reason_placeholders = ",".join("?" for _ in runtime_empty_book_reasons)
        state_placeholders = ",".join("?" for _ in retryable_target_states)
        self.initialize()
        reopened: list[str] = []
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                f"""
                SELECT a.action_id, t.state AS target_state,
                       latest.id AS prior_transition_id,
                       latest.reason AS prior_reason
                FROM action_receipts AS a
                JOIN action_targets AS t ON t.action_id = a.action_id
                JOIN action_transitions AS latest
                  ON latest.id = (
                      SELECT id FROM action_transitions
                      WHERE action_id = a.action_id
                      ORDER BY id DESC LIMIT 1
                  )
                WHERE LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                  AND latest.status = 'ERROR'
                  AND (
                    latest.reason IN ({reason_placeholders})
                    OR (
                      latest.reason LIKE 'BOOK_SNAPSHOT_ERROR: PolyApiException:%'
                      AND latest.reason LIKE '%status_code=404%'
                      AND LOWER(latest.reason) LIKE '%no orderbook exists%'
                    )
                  )
                  AND t.state IN ({state_placeholders})
                  AND NOT EXISTS (
                      SELECT 1 FROM order_reservations AS reservation
                      WHERE reservation.action_id = a.action_id
                        AND reservation.active = 1
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM submission_attempts AS attempt
                      WHERE attempt.action_id = a.action_id
                        AND attempt.state IN (
                            'SUBMIT_STARTED', 'SUBMITTED_UNRECONCILED',
                            'UNKNOWN_SUBMISSION'
                        )
                  )
                ORDER BY a.block_number, a.source_log_index,
                         a.source_timestamp, a.action_id
                """,
                (*runtime_empty_book_reasons, *retryable_target_states),
            ).fetchall()
            for row in rows:
                action_id = str(row["action_id"])
                details = {
                    "action_id": action_id,
                    "prior_transition_id": int(row["prior_transition_id"]),
                    "prior_reason": str(row["prior_reason"]),
                    "prior_target_state": str(row["target_state"]),
                    "historical_ledger_rewritten": False,
                    "orders_submitted_by_repair": False,
                }
                connection.execute(
                    """
                    INSERT INTO runtime_errors(
                        occurred_at_ms, category, message, details_json
                    ) VALUES(?, 'INTERNAL_ACTION_STATE_REPAIR', ?, ?)
                    """,
                    (
                        created_at,
                        "REOPENED_PRE_SUBMISSION_EMPTY_BOOK_ERROR",
                        json.dumps(details, sort_keys=True),
                    ),
                )
                connection.execute(
                    """
                    UPDATE action_targets
                    SET state = 'PENDING_EXTERNAL_RETRY',
                        reason = 'REPAIRED_EMPTY_BOOK_RETRYABLE',
                        updated_at_ms = ?
                    WHERE action_id = ?
                    """,
                    (created_at, action_id),
                )
                connection.execute(
                    """
                    INSERT INTO action_transitions(
                        action_id, status, reason, created_at_ms, details_json
                    ) VALUES(
                        ?, 'PENDING_EXTERNAL_RETRY',
                        'REPAIRED_EMPTY_BOOK_RETRYABLE', ?, ?
                    )
                    """,
                    (action_id, created_at, json.dumps(details, sort_keys=True)),
                )
                reopened.append(action_id)
        return reopened

    def repair_retryable_targets_with_terminal_latest_transition(
        self, *, changed_at_ms: int
    ) -> list[str]:
        """Align mutable retry targets with an immutable causal terminal.

        This repairs only a bookkeeping-state mismatch.  It never changes an
        existing transition, submission attempt, fill, position, cash, fee,
        or PnL row.  If a SELL already has a filled prefix and its immutable
        terminal says local inventory was exhausted, one corrective PARTIAL
        transition is appended so the copied prefix is not erased.  Any
        active reservation or uncertain submission fails closed.
        """

        changed_at = int(changed_at_ms)
        if changed_at < 0:
            raise LiveConfigurationError("INVALID_TARGET_ALIGNMENT_TIMESTAMP")
        retryable_states = (
            "PENDING_LIQUIDITY",
            "PENDING_CONFIRMED_ZERO_FILL",
            "PENDING_PRICE_PROTECTION",
            "PARTIAL_PENDING",
            "PENDING_CAPITAL",
            "PENDING_MINIMUM_UNWIND",
            "PENDING_MINIMUM_REMAINDER",
            "PENDING_EXTERNAL_RETRY",
        )
        terminal_states = ("EXTERNAL_UNFILLABLE", "SUPERSEDED_UNFILLED")
        retry_placeholders = ",".join("?" for _ in retryable_states)
        terminal_placeholders = ",".join("?" for _ in terminal_states)
        self.initialize()
        repaired: list[str] = []
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                f"""
                SELECT a.action_id, a.block_number, a.source_log_index,
                       a.side,
                       t.state AS prior_target_state,
                       t.reason AS prior_target_reason,
                       t.target_quantity AS target_quantity,
                       t.cumulative_filled_quantity AS filled_quantity,
                       latest.id AS terminal_transition_id,
                       latest.status AS terminal_status,
                       latest.reason AS terminal_reason,
                       (
                           SELECT COUNT(*) FROM order_reservations AS reservation
                           WHERE reservation.action_id = a.action_id
                             AND reservation.active = 1
                       ) AS active_reservations,
                       (
                           SELECT COUNT(*) FROM submission_attempts AS attempt
                           WHERE attempt.action_id = a.action_id
                             AND attempt.state IN (
                                 'SUBMIT_STARTED', 'SUBMITTED_UNRECONCILED',
                                 'UNKNOWN_SUBMISSION'
                             )
                       ) AS unsafe_attempts
                FROM action_receipts AS a
                JOIN action_targets AS t ON t.action_id = a.action_id
                JOIN action_transitions AS latest
                  ON latest.id = (
                      SELECT id FROM action_transitions
                      WHERE action_id = a.action_id
                      ORDER BY id DESC LIMIT 1
                  )
                WHERE LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                  AND t.state IN ({retry_placeholders})
                  AND latest.status IN ({terminal_placeholders})
                ORDER BY a.block_number, a.source_log_index,
                         a.source_timestamp, a.action_id
                """,
                (*retryable_states, *terminal_states),
            ).fetchall()
            for row in rows:
                filled_quantity = Decimal(str(row["filled_quantity"]))
                target_quantity = Decimal(str(row["target_quantity"]))
                if (
                    int(row["active_reservations"] or 0) != 0
                    or int(row["unsafe_attempts"] or 0) != 0
                    or filled_quantity < ZERO
                    or filled_quantity > target_quantity
                ):
                    raise LiveConfigurationError(
                        "UNSAFE_RETRYABLE_TARGET_TERMINAL_ALIGNMENT:"
                        f"{row['action_id']}"
                    )
                if filled_quantity > ZERO and not (
                    str(row["side"]).upper() == "SELL"
                    and str(row["terminal_status"]) == "EXTERNAL_UNFILLABLE"
                    and str(row["terminal_reason"])
                    == "NO_LOCAL_INVENTORY_AFTER_LOCAL_POSITION_EXHAUSTED"
                ):
                    raise LiveConfigurationError(
                        "UNSAFE_FILLED_TARGET_TERMINAL_ALIGNMENT:"
                        f"{row['action_id']}"
                    )
            for row in rows:
                action_id = str(row["action_id"])
                prior_state = str(row["prior_target_state"])
                prior_terminal_status = str(row["terminal_status"])
                terminal_reason = str(row["terminal_reason"])
                filled_quantity = Decimal(str(row["filled_quantity"]))
                terminal_status = (
                    "PARTIAL" if filled_quantity > ZERO else prior_terminal_status
                )
                corrective_transition = filled_quantity > ZERO
                details = {
                    "action_id": action_id,
                    "prior_target_reason": str(row["prior_target_reason"]),
                    "terminal_transition_id": int(row["terminal_transition_id"]),
                    "prior_terminal_status": prior_terminal_status,
                    "terminal_reason": terminal_reason,
                    "cumulative_filled_quantity": str(filled_quantity),
                    "historical_transition_changed": False,
                    "corrective_transition_appended": corrective_transition,
                    "orders_submitted_by_repair": False,
                    "cash_positions_or_pnl_changed": False,
                    "mutable_target_state_aligned": True,
                }
                config_key = f"action_target_terminal_alignment:{action_id}"
                reason = "ALIGN_TARGET_WITH_IMMUTABLE_TERMINAL_TRANSITION"
                change_id = canonical_hash(
                    {
                        "config_key": config_key,
                        "previous_value": prior_state,
                        "new_value": terminal_status,
                        "reason": reason,
                        "details": details,
                    }
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO config_change_receipts(
                        change_id, config_key, previous_value, new_value,
                        reason, changed_at_ms, details_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        change_id,
                        config_key,
                        prior_state,
                        terminal_status,
                        reason,
                        changed_at,
                        json.dumps(details, sort_keys=True),
                    ),
                )
                if corrective_transition:
                    connection.execute(
                        """
                        INSERT INTO action_transitions(
                            action_id, status, reason,
                            created_at_ms, details_json
                        ) VALUES(?, 'PARTIAL', ?, ?, ?)
                        """,
                        (
                            action_id,
                            terminal_reason,
                            changed_at,
                            json.dumps(details, sort_keys=True),
                        ),
                    )
                cursor = connection.execute(
                    """
                    UPDATE action_targets
                    SET state = ?, reason = ?, updated_at_ms = ?
                    WHERE action_id = ? AND state = ?
                    """,
                    (
                        terminal_status,
                        terminal_reason,
                        changed_at,
                        action_id,
                        prior_state,
                    ),
                )
                if cursor.rowcount != 1:
                    raise LiveConfigurationError(
                        f"TARGET_ALIGNMENT_CONFLICT:{action_id}"
                    )
                repaired.append(action_id)
        return repaired

    @staticmethod
    def _fidelity_state_groups() -> tuple[set[str], set[str], set[str]]:
        return (
            {
                "OBSERVED",
                "READY",
                "SUBMIT_STARTED",
                "SUBMITTED_UNRECONCILED",
                "UNKNOWN_SUBMISSION",
                "PARTIAL_PENDING",
                "PENDING_LIQUIDITY",
                "PENDING_MINIMUM_UNWIND",
                "PENDING_MINIMUM_REMAINDER",
                "PENDING_CAPITAL",
                "PENDING_EXTERNAL_RETRY",
                "PENDING_CAUSAL_ORDER",
            },
            {
                "EXTERNAL_UNFILLABLE",
                "SUPERSEDED_UNFILLED",
                "EXPIRED_RETRY_WINDOW",
            },
            {
                "ERROR_INTERNAL",
                "ERROR",
                "SKIPPED",
                "PENDING_INTERNAL_INVARIANT",
            },
        )

    @staticmethod
    def _is_fixed_scale_minimum_constraint(
        *, state: str, reason: str
    ) -> bool:
        """Identify a fixed-scale non-trade blocked by a venue minimum."""

        return (
            str(state) == "SKIPPED"
            and str(reason) in FIXED_SCALE_MINIMUM_CONSTRAINT_REASONS
        )

    @staticmethod
    def _is_legacy_confirmed_zero_fill(*, state: str, reason: str) -> bool:
        return str(state) == "SKIPPED" and str(reason) in {
            "OFFICIAL_ONCHAIN_ORDER_HASH_NO_FILL",
            "OFFICIAL_CLOB_ZERO_FILL",
            "OFFICIAL_CLOB_NO_MATCH",
        }

    def _recoverable_legacy_stable_causal_prefix_action_ids(self) -> set[str]:
        """Return proven legacy causal blocks with no unresolved side effect."""

        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT action.*
                FROM action_receipts AS action
                JOIN action_market_metadata AS metadata
                  ON metadata.action_id=action.action_id
                JOIN action_targets AS target
                  ON target.action_id=action.action_id
                JOIN action_transitions AS latest
                  ON latest.id=(
                      SELECT candidate.id
                      FROM action_transitions AS candidate
                      WHERE candidate.action_id=action.action_id
                      ORDER BY candidate.id DESC LIMIT 1
                  )
                WHERE metadata.profile_follow=1
                  AND LOWER(action.source_role) IN (
                      'maker','verified_public_wallet'
                  )
                  AND target.state='ERROR_INTERNAL'
                  AND target.reason='INTERNAL_STALE_CAUSAL_TARGET'
                  AND latest.status='ERROR_INTERNAL'
                  AND latest.reason='INTERNAL_STALE_CAUSAL_TARGET'
                ORDER BY action.block_number, action.source_log_index,
                         action.source_timestamp, action.action_id
                """
            ).fetchall()
        recovered: set[str] = set()
        for row in rows:
            source = self._source_from_receipt(row)
            if (
                _legacy_stable_causal_prefix_recovery_evidence(
                    store=self,
                    source=source,
                )
                is not None
            ):
                recovered.add(source.action_id)
        return recovered

    def decision_unit_summary(self) -> list[dict[str, Any]]:
        pending_states, external_states, internal_states = (
            self._fidelity_state_groups()
        )
        self.initialize()
        recoverable_legacy_actions = (
            self._recoverable_legacy_stable_causal_prefix_action_ids()
        )
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT m.action_id, m.event_slug, t.state, t.reason,
                       (
                           SELECT latest.status
                           FROM action_transitions AS latest
                           WHERE latest.action_id = m.action_id
                           ORDER BY latest.id DESC LIMIT 1
                       ) AS latest_status,
                       (
                           SELECT latest.reason
                           FROM action_transitions AS latest
                           WHERE latest.action_id = m.action_id
                           ORDER BY latest.id DESC LIMIT 1
                       ) AS latest_reason
                FROM action_market_metadata AS m
                JOIN action_receipts AS a ON a.action_id = m.action_id
                LEFT JOIN action_targets AS t ON t.action_id = m.action_id
                WHERE m.profile_follow = 1
                  AND LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                ORDER BY m.event_slug, m.frozen_at_ms, m.action_id
                """
            ).fetchall()
        summaries: dict[str, dict[str, Any]] = {}
        for row in rows:
            event_slug = str(row["event_slug"])
            summary = summaries.setdefault(
                event_slug,
                {
                    "event_slug": event_slug,
                    "eligible_observed": 0,
                    "filled": 0,
                    "partial": 0,
                    "pending": 0,
                    "external_or_causal": 0,
                    "internal_error": 0,
                },
            )
            summary["eligible_observed"] += 1
            state = str(row["state"] or row["latest_status"] or "")
            reason = str(row["reason"] or row["latest_reason"] or "")
            if state == "FILLED":
                summary["filled"] += 1
            elif state == "PARTIAL":
                summary["partial"] += 1
            elif self._is_fixed_scale_minimum_constraint(
                state=state, reason=reason
            ) or state in external_states or str(row["action_id"]) in (
                recoverable_legacy_actions
            ):
                summary["external_or_causal"] += 1
            elif state in internal_states:
                summary["internal_error"] += 1
            else:
                summary["pending"] += 1
        return list(summaries.values())

    def action_fidelity_summary(self) -> dict[str, Any]:
        """Quantify profile-eligible action conservation without an edge gate."""

        pending_states, external_or_causal_states, internal_error_states = (
            self._fidelity_state_groups()
        )
        self.initialize()
        recoverable_legacy_actions = (
            self._recoverable_legacy_stable_causal_prefix_action_ids()
        )
        with self.connect() as connection:
            receipt_counts = connection.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(
                           CASE WHEN LOWER(source_role) = 'maker'
                           THEN 1 ELSE 0 END
                       ) AS source_maker,
                       SUM(
                           CASE WHEN LOWER(source_role) = 'verified_public_wallet'
                           THEN 1 ELSE 0 END
                       ) AS source_verified_public_wallet,
                       SUM(
                           CASE WHEN LOWER(source_role) IN (
                               'maker', 'verified_public_wallet'
                           ) THEN 1 ELSE 0 END
                       ) AS followable_source,
                       SUM(
                           CASE WHEN LOWER(source_role) NOT IN (
                               'maker', 'verified_public_wallet'
                           )
                           THEN 1 ELSE 0 END
                       ) AS legacy_nonmaker
                FROM action_receipts
                """
            ).fetchone()
            total_action_receipts = int(receipt_counts["total"] or 0)
            source_maker_action_receipts = int(
                receipt_counts["source_maker"] or 0
            )
            source_verified_public_wallet_action_receipts = int(
                receipt_counts["source_verified_public_wallet"] or 0
            )
            followable_source_action_receipts = int(
                receipt_counts["followable_source"] or 0
            )
            legacy_nonmaker_receipt_count = int(
                receipt_counts["legacy_nonmaker"] or 0
            )
            metadata_counts = connection.execute(
                """
                SELECT
                    SUM(
                        CASE WHEN LOWER(a.source_role) IN (
                            'maker', 'verified_public_wallet'
                        )
                        THEN 1 ELSE 0 END
                    ) AS frozen,
                    SUM(
                        CASE WHEN m.profile_follow = 1
                                  AND LOWER(a.source_role) IN (
                                      'maker', 'verified_public_wallet'
                                  )
                        THEN 1 ELSE 0 END
                    ) AS eligible,
                    SUM(
                        CASE WHEN m.profile_follow = 0
                                  AND LOWER(a.source_role) IN (
                                      'maker', 'verified_public_wallet'
                                  )
                        THEN 1 ELSE 0 END
                    ) AS excluded,
                    SUM(
                        CASE WHEN LOWER(a.source_role) NOT IN (
                            'maker', 'verified_public_wallet'
                        )
                        THEN 1 ELSE 0 END
                    ) AS legacy_nonmaker_metadata
                FROM action_market_metadata AS m
                JOIN action_receipts AS a ON a.action_id = m.action_id
                """
            ).fetchone()
            frozen_metadata_count = int(metadata_counts["frozen"] or 0)
            eligible = int(metadata_counts["eligible"] or 0)
            profile_excluded = int(metadata_counts["excluded"] or 0)
            legacy_nonmaker_metadata_count = int(
                metadata_counts["legacy_nonmaker_metadata"] or 0
            )
            legacy_nonmaker_target_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM action_targets AS t
                    JOIN action_receipts AS a ON a.action_id = t.action_id
                    WHERE LOWER(a.source_role) NOT IN (
                        'maker', 'verified_public_wallet'
                    )
                    """
                ).fetchone()["count"]
                or 0
            )
            rows = connection.execute(
                """
                SELECT t.action_id, t.state, t.reason, t.updated_at_ms
                FROM action_targets AS t
                JOIN action_market_metadata AS m ON m.action_id = t.action_id
                JOIN action_receipts AS a ON a.action_id = t.action_id
                WHERE m.profile_follow = 1
                  AND LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                """
            ).fetchall()
            missing_rows = connection.execute(
                """
                SELECT latest.status, latest.reason, latest.created_at_ms,
                       EXISTS(
                           SELECT 1
                           FROM repair_recovery_actions AS recovery
                           JOIN repair_recovery_manifests AS manifest
                             ON manifest.manifest_hash = recovery.manifest_hash
                           WHERE recovery.action_id = m.action_id
                             AND manifest.state = 'ACTIVE'
                             AND recovery.state IN (
                                 'AUTHORIZED',
                                 'PENDING_PRICE',
                                 'CURRENT_EFFECT_RECONSTRUCTED',
                                 'PENDING_EXTERNAL_LIQUIDITY',
                                 'PARTIAL_PENDING',
                                 'SUBMIT_STARTED',
                                 'SUBMITTED_UNRECONCILED',
                                 'UNKNOWN_SUBMISSION'
                             )
                       ) AS active_repair_managed
                FROM action_market_metadata AS m
                JOIN action_receipts AS a ON a.action_id = m.action_id
                LEFT JOIN action_targets AS target
                  ON target.action_id = m.action_id
                LEFT JOIN action_transitions AS latest
                  ON latest.id = (
                    SELECT id FROM action_transitions
                    WHERE action_id = m.action_id
                    ORDER BY id DESC LIMIT 1
                  )
                WHERE m.profile_follow = 1
                  AND LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                  AND target.action_id IS NULL
                """
            ).fetchall()
            metadata_pending = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM action_receipts AS a
                    JOIN action_transitions AS t
                      ON t.id = (
                        SELECT id FROM action_transitions
                        WHERE action_id = a.action_id
                        ORDER BY id DESC LIMIT 1
                      )
                    WHERE t.status = 'PENDING_METADATA'
                      AND LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                    """
                ).fetchone()["count"]
            )
            retryable_target_terminal_transition_mismatch = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM action_targets AS target
                    JOIN action_receipts AS action
                      ON action.action_id = target.action_id
                    JOIN action_transitions AS latest
                      ON latest.id = (
                          SELECT id FROM action_transitions
                          WHERE action_id = target.action_id
                          ORDER BY id DESC LIMIT 1
                      )
                    WHERE LOWER(action.source_role) IN ('maker', 'verified_public_wallet')
                      AND target.state IN (
                          'PENDING_LIQUIDITY', 'PARTIAL_PENDING',
                          'PENDING_CAPITAL', 'PENDING_MINIMUM_UNWIND',
                          'PENDING_MINIMUM_REMAINDER',
                          'PENDING_EXTERNAL_RETRY'
                      )
                      AND latest.status IN (
                          'EXTERNAL_UNFILLABLE', 'SUPERSEDED_UNFILLED'
                      )
                    """
                ).fetchone()["count"]
                or 0
            )
        state_counts: dict[str, int] = {}
        fixed_scale_minimum_constraints = 0
        recoverable_legacy_stable_causal_prefix_action_count = 0
        pending_timestamps: list[int] = []
        for row in rows:
            state = str(row["state"])
            state_counts[state] = state_counts.get(state, 0) + 1
            if str(row["action_id"]) in recoverable_legacy_actions:
                recoverable_legacy_stable_causal_prefix_action_count += 1
            if self._is_fixed_scale_minimum_constraint(
                state=state, reason=str(row["reason"] or "")
            ) or self._is_legacy_confirmed_zero_fill(
                state=state, reason=str(row["reason"] or "")
            ):
                fixed_scale_minimum_constraints += 1
            if state in pending_states:
                pending_timestamps.append(int(row["updated_at_ms"]))
        filled = state_counts.get("FILLED", 0)
        partial = state_counts.get("PARTIAL", 0)
        pending = sum(state_counts.get(state, 0) for state in pending_states)
        external_or_causal = fixed_scale_minimum_constraints + sum(
            state_counts.get(state, 0) for state in external_or_causal_states
        ) + recoverable_legacy_stable_causal_prefix_action_count
        internal_error = sum(
            state_counts.get(state, 0) for state in internal_error_states
        ) - fixed_scale_minimum_constraints - (
            recoverable_legacy_stable_causal_prefix_action_count
        )
        active_repair_managed_without_target = 0
        missing_target = 0
        unclassified_missing_target = 0
        for row in missing_rows:
            latest_state = str(row["status"] or "")
            latest_reason = str(row["reason"] or "")
            if bool(row["active_repair_managed"]):
                active_repair_managed_without_target += 1
                pending += 1
                if row["created_at_ms"] is not None:
                    pending_timestamps.append(int(row["created_at_ms"]))
            elif self._is_fixed_scale_minimum_constraint(
                state=latest_state, reason=latest_reason
            ):
                external_or_causal += 1
            elif latest_state in pending_states:
                pending += 1
                if row["created_at_ms"] is not None:
                    pending_timestamps.append(int(row["created_at_ms"]))
            elif latest_state in internal_error_states:
                internal_error += 1
                missing_target += 1
            elif latest_state in external_or_causal_states:
                external_or_causal += 1
            else:
                missing_target += 1
                unclassified_missing_target += 1
        classified_states = (
            {"FILLED", "PARTIAL"}
            | pending_states
            | external_or_causal_states
            | internal_error_states
        )
        unclassified = unclassified_missing_target + sum(
            count
            for state, count in state_counts.items()
            if state not in classified_states
        )
        target_count = len(rows)
        accounted = filled + partial + pending + external_or_causal + internal_error
        written_off_raw = self.config(
            "operator_written_off_external_or_causal_unfilled_count"
        )
        try:
            written_off = (
                max(0, int(str(written_off_raw)))
                if written_off_raw is not None
                else 0
            )
        except (TypeError, ValueError):
            written_off = 0
        return {
            "total_action_receipts": total_action_receipts,
            "source_maker_action_receipts": source_maker_action_receipts,
            "source_verified_public_wallet_action_receipts": (
                source_verified_public_wallet_action_receipts
            ),
            "followable_source_action_receipts": followable_source_action_receipts,
            "legacy_nonmaker_receipt_count": legacy_nonmaker_receipt_count,
            "frozen_metadata_count": frozen_metadata_count,
            "legacy_nonmaker_metadata_count": legacy_nonmaker_metadata_count,
            "legacy_nonmaker_target_count": legacy_nonmaker_target_count,
            "profile_excluded_observed": profile_excluded,
            "legacy_or_unclassified_without_metadata": max(
                0,
                followable_source_action_receipts
                - frozen_metadata_count
                - metadata_pending,
            ),
            "profile_eligible_observed": eligible,
            "frozen_target_count": target_count,
            "filled": filled,
            "partial": partial,
            "pending": pending,
            "external_or_causal_unfilled": external_or_causal,
            "external_or_causal_unfilled_acknowledged_baseline": written_off,
            "recoverable_legacy_stable_causal_prefix_action_count": (
                recoverable_legacy_stable_causal_prefix_action_count
            ),
            "internal_error": internal_error,
            "unclassified_target": unclassified,
            "active_repair_managed_without_target": (
                active_repair_managed_without_target
            ),
            "missing_target": missing_target,
            "metadata_pending": metadata_pending,
            "retryable_target_terminal_transition_mismatch": (
                retryable_target_terminal_transition_mismatch
            ),
            "accounted": accounted,
            "conservation_passed": (
                missing_target == 0
                and eligible == accounted
                and unclassified == 0
                and retryable_target_terminal_transition_mismatch == 0
            ),
            "oldest_pending_updated_at_ms": (
                min(pending_timestamps) if pending_timestamps else None
            ),
        }

    def bounded_retry_summary(self) -> dict[str, Any]:
        self.initialize()
        effective_after_block = self.bounded_retry_effective_after_block()
        with self.connect() as connection:
            latest_counts = {
                str(row["status"]): int(row["count"])
                for row in connection.execute(
                    """
                    SELECT latest.status,COUNT(*) AS count
                    FROM action_transitions AS latest
                    WHERE latest.id=(
                        SELECT candidate.id FROM action_transitions AS candidate
                        WHERE candidate.action_id=latest.action_id
                        ORDER BY candidate.id DESC LIMIT 1
                    )
                    GROUP BY latest.status
                    """
                ).fetchall()
            }
            unknown_repost = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM submission_attempts AS later
                    WHERE later.attempt_number>1 AND EXISTS(
                        SELECT 1 FROM submission_attempts AS prior
                        WHERE prior.action_id=later.action_id
                          AND prior.attempt_number<later.attempt_number
                          AND prior.state='UNKNOWN_SUBMISSION'
                    )
                    """
                ).fetchone()[0]
            )
            conservation_violations = sum(
                1
                for row in connection.execute(
                    """
                    SELECT target.target_quantity,target.cumulative_filled_quantity,
                           receipt.side,target.state,target.reason
                    FROM action_targets AS target
                    JOIN action_receipts AS receipt ON receipt.action_id=target.action_id
                    WHERE ? IS NOT NULL AND receipt.block_number>?
                    """,
                    (effective_after_block, effective_after_block),
                ).fetchall()
                if (
                    (filled := Decimal(str(row["cumulative_filled_quantity"])))
                    < ZERO
                    or (
                        filled > (target := Decimal(str(row["target_quantity"])))
                        and not (
                            str(row["side"]).upper() == "BUY"
                            and str(row["state"]).upper() == "FILLED"
                            and str(row["reason"])
                            in {
                                "OFFICIAL_ONCHAIN_BUY_PRICE_IMPROVEMENT_FILL",
                                "OFFICIAL_ASSOCIATED_TRADE_BUY_PRICE_IMPROVEMENT_FILL",
                            }
                        )
                    )
                    or (
                        filled <= target
                        and target != filled + max(ZERO, target - filled)
                    )
                )
            )
            termination_counts = {
                "market_closed": 0,
                "minimum_order": 0,
                "inventory_unavailable": 0,
                "later_source_opposite": 0,
            }
            for row in connection.execute(
                """
                SELECT latest.status,latest.reason
                FROM action_transitions AS latest
                WHERE latest.id=(
                    SELECT candidate.id FROM action_transitions AS candidate
                    WHERE candidate.action_id=latest.action_id
                    ORDER BY candidate.id DESC LIMIT 1
                )
                """
            ).fetchall():
                reason = str(row["reason"] or "").upper()
                status = str(row["status"] or "").upper()
                if reason == "OFFICIAL_MARKET_CLOSED_BEFORE_RETRY":
                    termination_counts["market_closed"] += 1
                if "MINIMUM" in reason and status in {
                    "SKIPPED",
                    "EXTERNAL_UNFILLABLE",
                }:
                    termination_counts["minimum_order"] += 1
                if reason in {
                    "NO_LOCAL_INVENTORY_AFTER_PRIOR_UNREPLICATED_BUY",
                    "NO_LOCAL_INVENTORY",
                    "LOCAL_INVENTORY_EXHAUSTED",
                }:
                    termination_counts["inventory_unavailable"] += 1
                if reason == "LATER_OPPOSITE_SOURCE_ACTION":
                    termination_counts["later_source_opposite"] += 1
        return {
            "policy_id": self.config("bounded_retry_policy_id"),
            "effective_after_block": self.config(
                "bounded_retry_effective_after_block"
            ),
            "normal_window_ms": BOUNDED_RETRY_NORMAL_WINDOW_MS,
            "deadline_ms": BOUNDED_RETRY_DEADLINE_MS,
            "maximum_adverse_slippage": str(
                BOUNDED_RETRY_MAX_ADVERSE_SLIPPAGE
            ),
            "pending_confirmed_zero_fill": latest_counts.get(
                "PENDING_CONFIRMED_ZERO_FILL", 0
            ),
            "pending_price_protection": latest_counts.get(
                "PENDING_PRICE_PROTECTION", 0
            ),
            "expired_retry_window": latest_counts.get(
                "EXPIRED_RETRY_WINDOW", 0
            ),
            "unknown_repost_violation_count": unknown_repost,
            "target_conservation_violation_count": conservation_violations,
            "termination_counts": termination_counts,
        }

    def liquidity_retry_evidence(
        self, source: SourceAction
    ) -> dict[str, Any] | None:
        """Return frozen proof for one V2 retry, otherwise fail closed."""

        policy = self.liquidity_retry_policy_for_source(source)
        if policy is None:
            return None
        target = self.action_target(source.action_id)
        latest = self.latest_transition(source)
        if target is None or latest is None:
            return None
        if str(target["state"]) not in {
            "PENDING_LIQUIDITY",
            "PENDING_CONFIRMED_ZERO_FILL",
            "PENDING_PRICE_PROTECTION",
            "PARTIAL_PENDING",
        }:
            return None
        if str(latest["terminal_status"]) not in {
            "PENDING_LIQUIDITY",
            "PENDING_CONFIRMED_ZERO_FILL",
            "PENDING_PRICE_PROTECTION",
            "PARTIAL_PENDING",
        }:
            return None
        target_quantity = Decimal(str(target["target_quantity"]))
        cumulative = Decimal(str(target["cumulative_filled_quantity"]))
        remaining = Decimal(str(target["remaining_quantity"]))
        if (
            target_quantity <= ZERO
            or cumulative < ZERO
            or cumulative > target_quantity
            or remaining != target_quantity - cumulative
            or remaining <= ZERO
            or self.action_has_unresolved_submission_side_effect(source.action_id)
        ):
            return None
        first_submit = self.first_transition_with_status(source, "SUBMIT_STARTED")
        details = (
            first_submit.get("details")
            if isinstance(first_submit, Mapping)
            else None
        )
        plan = details.get("plan") if isinstance(details, Mapping) else None
        if not isinstance(plan, Mapping):
            return None
        try:
            frozen_worst_price = Decimal(str(plan.get("worst_price")))
        except (InvalidOperation, TypeError, ValueError):
            return None
        if (
            not frozen_worst_price.is_finite()
            or frozen_worst_price <= ZERO
            or frozen_worst_price > Decimal("1")
        ):
            return None
        self.initialize()
        with self.connect() as connection:
            attempts = connection.execute(
                """
                SELECT attempt_number,state,requested_quantity,response_json
                FROM submission_attempts
                WHERE action_id=? ORDER BY attempt_number ASC
                """,
                (source.action_id,),
            ).fetchall()
            proof_rows = connection.execute(
                """
                SELECT status,reason,details_json
                FROM action_transitions
                WHERE action_id=? AND (
                    reason IN (
                        'FINALIZED_CHAIN_PROVES_FAK_ZERO_FILL_RETRYABLE',
                        'OFFICIAL_CONFIRMED_ZERO_FILL_RETRYABLE',
                        'FAK_PARTIAL_FILL'
                    )
                    OR status='PARTIAL_PENDING'
                )
                ORDER BY id
                """,
                (source.action_id,),
            ).fetchall()
        if not attempts or any(
            str(row["state"]) not in {"NO_FILL", "PARTIAL_FILLED", "FILLED"}
            for row in attempts
        ):
            return None
        has_zero_fill_proof = False
        for row in proof_rows:
            parsed = json.loads(str(row["details_json"]))
            reason = str(row["reason"])
            if (
                reason == "FINALIZED_CHAIN_PROVES_FAK_ZERO_FILL_RETRYABLE"
                and parsed.get("chain_scan", {}).get("finality") is not None
            ):
                has_zero_fill_proof = True
                break
        has_partial_transition_proof = any(
            str(row["status"]) == "PARTIAL_PENDING"
            and _has_chain_receipt_evidence(
                json.loads(str(row["details_json"])).get("receipt_evidence")
            )
            for row in proof_rows
        )
        has_partial_fill_proof = (
            cumulative > ZERO
            and any(
                str(row["state"]) == "PARTIAL_FILLED" for row in attempts
            )
            and has_partial_transition_proof
        )
        if not has_zero_fill_proof and not has_partial_fill_proof:
            return None
        original_minimum = self.original_submission_minimum_order_size(
            action_id=source.action_id
        )
        if original_minimum is None or original_minimum <= ZERO:
            return None
        return {
            **policy,
            "target_quantity": str(target_quantity),
            "cumulative_official_filled_quantity": str(cumulative),
            "remaining_quantity": str(remaining),
            "frozen_worst_price": str(frozen_worst_price),
            "original_minimum_order_size": str(original_minimum),
            "attempt_count": len(attempts),
            "retry_attempt_count": max(len(attempts) - 1, 0),
            "proof_type": (
                "PARTIAL_FILL" if has_partial_fill_proof else "FAK_ZERO_FILL"
            ),
        }

    def liquidity_retry_summary(self) -> dict[str, Any]:
        """Current V2 policy state; V1 is exposed only as historical context."""

        self.initialize()
        boundary = self.liquidity_retry_effective_after_block()
        pending_actions: list[dict[str, Any]] = []
        retry_attempt_count = 0
        completed_after_retry = 0
        unknown_repost = 0
        conservation_violations = 0
        pending_cumulative = ZERO
        pending_remaining = ZERO
        termination_counts: dict[str, int] = {}
        if boundary is not None:
            with self.connect() as connection:
                rows = connection.execute(
                    """
                    SELECT receipt.*,target.state,target.reason,
                           target.target_quantity,target.cumulative_filled_quantity,
                           metadata.event_slug,metadata.market_slug
                    FROM action_receipts AS receipt
                    JOIN action_targets AS target
                      ON target.action_id=receipt.action_id
                    LEFT JOIN action_market_metadata AS metadata
                      ON metadata.action_id=receipt.action_id
                    WHERE receipt.block_number>?
                    ORDER BY receipt.block_number,receipt.source_log_index,
                             receipt.action_id
                    """,
                    (boundary,),
                ).fetchall()
                retry_attempt_count = int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM submission_attempts AS attempt
                        JOIN action_receipts AS receipt
                          ON receipt.action_id=attempt.action_id
                        WHERE receipt.block_number>? AND attempt.attempt_number>1
                        """,
                        (boundary,),
                    ).fetchone()[0]
                )
                completed_after_retry = int(
                    connection.execute(
                        """
                        SELECT COUNT(DISTINCT target.action_id)
                        FROM action_targets AS target
                        JOIN action_receipts AS receipt
                          ON receipt.action_id=target.action_id
                        WHERE receipt.block_number>?
                          AND target.state='FILLED'
                          AND EXISTS(
                              SELECT 1 FROM submission_attempts AS attempt
                              WHERE attempt.action_id=target.action_id
                                AND attempt.attempt_number>1
                          )
                        """,
                        (boundary,),
                    ).fetchone()[0]
                )
                unknown_repost = int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM submission_attempts AS later
                        JOIN action_receipts AS receipt
                          ON receipt.action_id=later.action_id
                        WHERE receipt.block_number>? AND later.attempt_number>1
                          AND EXISTS(
                              SELECT 1 FROM submission_attempts AS prior
                              WHERE prior.action_id=later.action_id
                                AND prior.attempt_number<later.attempt_number
                                AND prior.state='UNKNOWN_SUBMISSION'
                          )
                        """,
                        (boundary,),
                    ).fetchone()[0]
                )
                termination_rows = connection.execute(
                    """
                    SELECT target.reason,COUNT(*) AS count
                    FROM action_targets AS target
                    JOIN action_receipts AS receipt
                      ON receipt.action_id=target.action_id
                    WHERE receipt.block_number>?
                      AND target.state IN (
                          'FILLED','PARTIAL','EXTERNAL_UNFILLABLE',
                          'SUPERSEDED_UNFILLED'
                      )
                      AND EXISTS(
                          SELECT 1 FROM action_transitions AS proof
                          WHERE proof.action_id=target.action_id
                            AND (
                                proof.reason=
                                  'FINALIZED_CHAIN_PROVES_FAK_ZERO_FILL_RETRYABLE'
                                OR proof.reason='FAK_PARTIAL_FILL'
                                OR proof.status='PARTIAL_PENDING'
                            )
                      )
                    GROUP BY target.reason ORDER BY target.reason
                    """,
                    (boundary,),
                ).fetchall()
                termination_counts = {
                    str(row["reason"]): int(row["count"])
                    for row in termination_rows
                }
            for row in rows:
                target_quantity = Decimal(str(row["target_quantity"]))
                cumulative = Decimal(str(row["cumulative_filled_quantity"]))
                if cumulative < ZERO or cumulative > target_quantity:
                    conservation_violations += 1
                if str(row["state"]) not in {
                    "PENDING_LIQUIDITY",
                    "PENDING_CONFIRMED_ZERO_FILL",
                    "PENDING_PRICE_PROTECTION",
                    "PARTIAL_PENDING",
                }:
                    continue
                source = self._source_from_receipt(row)
                evidence = self.liquidity_retry_evidence(source)
                if evidence is None:
                    continue
                pending_cumulative += Decimal(
                    str(evidence["cumulative_official_filled_quantity"])
                )
                pending_remaining += Decimal(str(evidence["remaining_quantity"]))
                pending_actions.append(
                    {
                        "action_id": source.action_id,
                        "event_slug": str(row["event_slug"] or ""),
                        "market_slug": str(row["market_slug"] or ""),
                        "side": source.side,
                        "target_quantity": evidence["target_quantity"],
                        "cumulative_filled_quantity": evidence[
                            "cumulative_official_filled_quantity"
                        ],
                        "remaining_quantity": evidence["remaining_quantity"],
                        "frozen_worst_price": evidence["frozen_worst_price"],
                        "attempt_count": evidence["attempt_count"],
                        "retry_attempt_count": evidence["retry_attempt_count"],
                        "state": str(row["state"]),
                        "reason": str(row["reason"]),
                    }
                )
        return {
            "policy_id": self.config("liquidity_retry_policy_id"),
            "effective_after_block": self.config(
                "liquidity_retry_effective_after_block"
            ),
            "deadline_ms": None,
            "historical_catch_up": False,
            "pending_retry_action_count": len(pending_actions),
            "pending_cumulative_official_filled_quantity": str(
                pending_cumulative
            ),
            "pending_remaining_quantity": str(pending_remaining),
            "retried_attempt_count": retry_attempt_count,
            "completed_after_retry_action_count": completed_after_retry,
            "unknown_repost_violation_count": unknown_repost,
            "target_conservation_violation_count": conservation_violations,
            "pending_actions": pending_actions,
            "termination_counts": termination_counts,
            "history_policy_id": self.config("bounded_retry_policy_id"),
            "history_current_policy": False,
        }

    def append_transition(
        self,
        *,
        source: SourceAction,
        status: str,
        reason: str = "",
        created_at_ms: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.initialize()
        normalized_status = str(status)
        normalized_reason = _redact_sensitive_text(reason)
        normalized_details = json.dumps(
            _sanitize_external_payload(details or {}),
            sort_keys=True,
        )
        when = (
            now_ms()
            if (
                created_at_ms is None
                and normalized_status in {
                    "PENDING_EXTERNAL_RETRY",
                    "PENDING_CAUSAL_ORDER",
                }
            )
            else (source.discovered_at_ms if created_at_ms is None else int(created_at_ms))
        )
        with self.connect() as connection:
            if normalized_status in {
                "PENDING_EXTERNAL_RETRY",
                "PENDING_CAUSAL_ORDER",
            }:
                latest = connection.execute(
                    """
                    SELECT status, reason, details_json
                    FROM action_transitions
                    WHERE action_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (source.action_id,),
                ).fetchone()
                if (
                    latest is not None
                    and str(latest["status"]) == normalized_status
                    and str(latest["reason"]) == normalized_reason
                    and str(latest["details_json"]) == normalized_details
                ):
                    return
            connection.execute(
                """
                INSERT INTO action_transitions(
                    action_id, status, reason, created_at_ms, details_json
                ) VALUES(?, ?, ?, ?, ?)
                """,
                (
                    source.action_id,
                    normalized_status,
                    normalized_reason,
                    when,
                    normalized_details,
                ),
            )

    def record_unpriced_runtime_gap(
        self,
        *,
        previous_processed_block: int,
        resume_head: int,
        actions: list[SourceAction],
        detected_at_ms: int,
        reason: str = "UNPRICED_RESTART_GAP",
        terminal_status: str = "SKIPPED",
        pricing_status: str = "UNPRICED_NO_ACTION_TIME_CLOB",
    ) -> dict[str, int]:
        """Close exact missed actions without repricing them from a later book."""

        previous = int(previous_processed_block)
        head = int(resume_head)
        if head <= previous:
            raise ValueError("runtime gap requires resume_head > previous_processed_block")
        unique_actions = {source.action_id: source for source in actions}
        self.initialize()
        with self.connect() as connection:
            existing = connection.execute(
                """
                SELECT source_action_count
                FROM runtime_gap_receipts
                WHERE previous_processed_block = ? AND resume_head = ? AND reason = ?
                """,
                (previous, head, str(reason)),
            ).fetchone()
            if existing is not None:
                return {
                    "source_action_count": int(existing["source_action_count"]),
                    "new_action_receipt_count": 0,
                }
            new_receipts = 0
            new_action_ids: list[str] = []
            for source in unique_actions.values():
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO action_receipts(
                        action_id, transaction_hash, token_id, side, order_hash,
                        source_quantity, source_notional, source_timestamp,
                        block_number, source_log_index, block_hash, source_role,
                        discovered_at_ms
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source.action_id,
                        source.transaction_hash.lower(),
                        str(source.token_id),
                        source.side.upper(),
                        source.order_hash.lower(),
                        str(source.source_quantity),
                        str(source.source_notional),
                        int(source.source_timestamp),
                        int(source.block_number),
                        int(source.log_index),
                        source.block_hash.lower(),
                        source.source_role,
                        int(source.discovered_at_ms),
                    ),
                )
                if cursor.rowcount != 1:
                    continue
                new_receipts += 1
                new_action_ids.append(source.action_id)
                details = {
                    "previous_processed_block": previous,
                    "resume_head": head,
                    "pricing_status": str(pricing_status),
                }
                for status, transition_reason in (
                    ("OBSERVED", ""),
                    (str(terminal_status), str(reason)),
                ):
                    connection.execute(
                        """
                        INSERT INTO action_transitions(
                            action_id, status, reason, created_at_ms, details_json
                        ) VALUES(?, ?, ?, ?, ?)
                        """,
                        (
                            source.action_id,
                            status,
                            transition_reason,
                            int(detected_at_ms),
                            json.dumps(details, sort_keys=True),
                        ),
                    )
            connection.execute(
                """
                INSERT INTO runtime_gap_receipts(
                    previous_processed_block, resume_head, skipped_block_count,
                    source_action_count, detected_at_ms, reason, pricing_status,
                    details_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    previous,
                    head,
                    head - previous,
                    new_receipts,
                    int(detected_at_ms),
                    str(reason),
                    str(pricing_status),
                    json.dumps({"action_ids": sorted(new_action_ids)}, sort_keys=True),
                ),
            )
        return {
            "source_action_count": new_receipts,
            "new_action_receipt_count": new_receipts,
        }

    def runtime_gap_receipt_count(self) -> int:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM runtime_gap_receipts"
            ).fetchone()
        return int(row["count"])

    def unpriced_gap_action_count(self) -> int:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COALESCE(SUM(source_action_count), 0) AS count
                FROM runtime_gap_receipts
                WHERE pricing_status = 'UNPRICED_NO_ACTION_TIME_CLOB'
                """
            ).fetchone()
        return int(row["count"])

    def lossless_handoff_failure_action_count(self) -> int:
        """Count source actions latched rather than silently skipped at restart."""

        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COALESCE(SUM(source_action_count), 0) AS count
                FROM runtime_gap_receipts
                WHERE pricing_status = 'LOSSLESS_HANDOFF_REQUIRED'
                """
            ).fetchone()
        return int(row["count"])

    def unresolved_lossless_handoff_action_count(self) -> int:
        """Count distinct handoff actions whose latest state is still nonterminal.

        The gap receipt remains immutable evidence that a handoff was once
        required.  Current health, however, must depend on whether each exact
        action is unresolved now; otherwise a successfully recovered action
        degrades health forever.
        """

        terminal_states = {
            "SKIPPED",
            "FILLED",
            "PARTIAL",
            "EXTERNAL_UNFILLABLE",
            "SUPERSEDED_UNFILLED",
            "EXPIRED_RETRY_WINDOW",
        }
        self.initialize()
        with self.connect() as connection:
            gap_rows = connection.execute(
                """
                SELECT source_action_count, details_json
                FROM runtime_gap_receipts
                WHERE pricing_status = 'LOSSLESS_HANDOFF_REQUIRED'
                """
            ).fetchall()
            action_ids: set[str] = set()
            unidentified_action_count = 0
            for gap in gap_rows:
                expected = max(0, int(gap["source_action_count"] or 0))
                try:
                    details = json.loads(str(gap["details_json"] or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    unidentified_action_count += expected
                    continue
                raw_action_ids = (
                    details.get("action_ids", [])
                    if isinstance(details, dict)
                    else []
                )
                parsed_ids = {
                    str(action_id).strip()
                    for action_id in raw_action_ids
                    if str(action_id).strip()
                }
                action_ids.update(parsed_ids)
                unidentified_action_count += max(0, expected - len(parsed_ids))

            unresolved = unidentified_action_count
            for action_id in action_ids:
                row = connection.execute(
                    """
                    SELECT latest.status AS latest_status,
                           target.state AS target_state
                    FROM action_receipts AS action
                    LEFT JOIN action_transitions AS latest
                      ON latest.id = (
                          SELECT id FROM action_transitions
                          WHERE action_id = action.action_id
                          ORDER BY id DESC LIMIT 1
                      )
                    LEFT JOIN action_targets AS target
                      ON target.action_id = action.action_id
                    WHERE action.action_id = ?
                    """,
                    (action_id,),
                ).fetchone()
                if row is None:
                    unresolved += 1
                    continue
                latest_status = str(row["latest_status"] or "")
                target_state = (
                    None
                    if row["target_state"] is None
                    else str(row["target_state"])
                )
                if latest_status not in terminal_states or (
                    target_state is not None and target_state not in terminal_states
                ):
                    unresolved += 1
        return unresolved

    def latest_transition(self, source: SourceAction) -> dict[str, Any] | None:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT status, reason, created_at_ms, details_json
                FROM action_transitions
                WHERE action_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (source.action_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "terminal_status": str(row["status"]),
            "reason": str(row["reason"]),
            "created_at_ms": int(row["created_at_ms"]),
            "details": json.loads(str(row["details_json"])),
        }

    def latest_transition_id(self, source: SourceAction) -> int | None:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT id
                FROM action_transitions
                WHERE action_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (source.action_id,),
            ).fetchone()
        return None if row is None else int(row["id"])

    def latest_transition_with_status(
        self, source: SourceAction, status: str
    ) -> dict[str, Any] | None:
        """Return the latest immutable transition with one exact status."""

        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT status, reason, created_at_ms, details_json
                FROM action_transitions
                WHERE action_id = ? AND status = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (source.action_id, str(status)),
            ).fetchone()
        if row is None:
            return None
        return {
            "terminal_status": str(row["status"]),
            "reason": str(row["reason"]),
            "created_at_ms": int(row["created_at_ms"]),
            "details": json.loads(str(row["details_json"])),
        }

    def first_transition_with_status(
        self, source: SourceAction, status: str
    ) -> dict[str, Any] | None:
        """Return the earliest immutable transition with one exact status."""

        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT status, reason, created_at_ms, details_json
                FROM action_transitions
                WHERE action_id = ? AND status = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (source.action_id, str(status)),
            ).fetchone()
        if row is None:
            return None
        return {
            "terminal_status": str(row["status"]),
            "reason": str(row["reason"]),
            "created_at_ms": int(row["created_at_ms"]),
            "details": json.loads(str(row["details_json"])),
        }

    def action_has_unresolved_submission_side_effect(
        self,
        action_id: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> bool:
        """Return whether one action still has an in-flight submit side effect."""

        def unresolved_row(active_connection: sqlite3.Connection) -> sqlite3.Row:
            return active_connection.execute(
                """
                SELECT
                    EXISTS(
                        SELECT 1 FROM order_reservations
                        WHERE action_id = ? AND active = 1
                    ) AS active_reservation,
                    EXISTS(
                        SELECT 1 FROM submission_attempts
                        WHERE action_id = ?
                          AND state NOT IN (
                              'FILLED',
                              'PARTIAL_FILLED',
                              'NO_FILL',
                              'REJECTED_INTERNAL'
                          )
                    ) AS unresolved_attempt
                """,
                (str(action_id), str(action_id)),
            ).fetchone()
        if connection is None:
            self.initialize()
            with self.connect() as active_connection:
                row = unresolved_row(active_connection)
        else:
            row = unresolved_row(connection)
        return bool(
            row is not None
            and (
                int(row["active_reservation"]) != 0
                or int(row["unresolved_attempt"]) != 0
            )
        )

    def record_external_head_incident(
        self,
        *,
        occurred_at_ms: int,
        head: int,
        message: str,
    ) -> int:
        """Aggregate repeated head-read failures without losing start evidence."""

        active = self.runtime_value("external_head_incident_active") == "true"
        count = int(self.runtime_value("external_head_incident_occurrence_count") or "0")
        count = count + 1 if active else 1
        if not active:
            self.append_runtime_error(
                occurred_at_ms=occurred_at_ms,
                category="EXTERNAL_HEAD_INCIDENT_STARTED",
                message=message,
                details={"head": int(head), "occurrence_count": 1},
            )
            self.set_runtime("external_head_incident_started_at_ms", occurred_at_ms)
        self.set_runtime("external_head_incident_active", "true")
        self.set_runtime("external_head_incident_occurrence_count", count)
        self.set_runtime("external_head_incident_last_at_ms", occurred_at_ms)
        self.set_runtime("external_head_incident_last_head", int(head))
        self.set_runtime("external_head_incident_last_message", message)
        return count

    def recover_external_head_incident(
        self, *, recovered_at_ms: int, processed_head: int
    ) -> int:
        if self.runtime_value("external_head_incident_active") != "true":
            return 0
        count = int(self.runtime_value("external_head_incident_occurrence_count") or "0")
        self.append_runtime_error(
            occurred_at_ms=recovered_at_ms,
            category="EXTERNAL_HEAD_INCIDENT_RECOVERED",
            message="external head processing recovered",
            details={
                "occurrence_count": count,
                "started_at_ms": int(
                    self.runtime_value("external_head_incident_started_at_ms") or "0"
                ),
                "last_failed_head": int(
                    self.runtime_value("external_head_incident_last_head") or "0"
                ),
                "processed_head": int(processed_head),
            },
        )
        self.set_runtime("external_head_incident_active", "false")
        self.set_runtime("external_head_incident_recovered_at_ms", recovered_at_ms)
        return count

    def action_receipt_count(self) -> int:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM action_receipts").fetchone()
        return int(row["count"])

    @staticmethod
    def _source_from_receipt(row: sqlite3.Row) -> SourceAction:
        return SourceAction(
            transaction_hash=str(row["transaction_hash"]),
            token_id=str(row["token_id"]),
            side=str(row["side"]),
            order_hash=str(row["order_hash"]),
            source_quantity=Decimal(str(row["source_quantity"])),
            source_notional=Decimal(str(row["source_notional"])),
            source_timestamp=int(row["source_timestamp"]),
            block_number=int(row["block_number"]),
            block_hash=str(row["block_hash"]),
            source_role=str(row["source_role"]),
            discovered_at_ms=int(row["discovered_at_ms"]),
            log_index=int(row["source_log_index"]),
        )

    def unreconciled_submissions(self) -> list[tuple[SourceAction, dict[str, Any]]]:
        """Return unresolved attempts even when their transition audit lags.

        Submission attempts are the side-effect authority.  A process can die
        after an order is posted but before the target and transition rows are
        advanced in their later transactions.  Selecting by the latest
        transition would make that accepted order invisible to both recovery
        paths.  Terminal ledger rows are deliberately excluded: reconciling
        those again could account for one fill twice.
        """

        terminal_states = (
            "SKIPPED",
            "FILLED",
            "PARTIAL",
            "ERROR",
            "ERROR_INTERNAL",
            "EXTERNAL_UNFILLABLE",
            "SUPERSEDED_UNFILLED",
        )
        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT a.*, t.status AS latest_status,
                       target.action_id AS target_action_id,
                       target.state AS target_state,
                       target.target_quantity AS target_quantity,
                       target.cumulative_filled_quantity
                           AS target_filled_quantity,
                       s.attempt_id AS submission_attempt_id,
                       s.attempt_number AS submission_attempt_number,
                       s.order_id AS submission_order_id,
                       s.state AS submission_attempt_state,
                       s.requested_quantity AS submission_requested_quantity,
                       s.snapshot_json AS submission_snapshot_json,
                       s.prepared_order_json,
                       s.response_json AS submission_response_json,
                       started.details_json
                           AS frozen_submission_details_json,
                       (
                           SELECT COUNT(*)
                           FROM submission_attempts AS unresolved
                           WHERE unresolved.action_id = a.action_id
                             AND unresolved.state IN (
                                 'SUBMIT_STARTED',
                                 'SUBMITTED_UNRECONCILED',
                                 'UNKNOWN_SUBMISSION'
                             )
                       ) AS unresolved_attempt_count,
                       CASE WHEN reservation.active = 1 THEN 1 ELSE 0 END
                           AS active_reservation,
                       reservation.quantity AS reservation_quantity,
                       reservation.cash_reserved_usd
                           AS reservation_cash_usd
                FROM action_receipts AS a
                JOIN action_transitions AS t
                  ON t.id = (
                    SELECT id
                    FROM action_transitions
                    WHERE action_id = a.action_id
                    ORDER BY id DESC
                    LIMIT 1
                  )
                LEFT JOIN action_targets AS target
                  ON target.action_id = a.action_id
                JOIN submission_attempts AS s
                  ON s.attempt_id = (
                    SELECT attempt_id
                    FROM submission_attempts
                    WHERE action_id = a.action_id
                      AND state IN (
                          'SUBMIT_STARTED', 'SUBMITTED_UNRECONCILED',
                          'UNKNOWN_SUBMISSION'
                      )
                    ORDER BY attempt_number DESC, created_at_ms DESC
                    LIMIT 1
                  )
                LEFT JOIN action_transitions AS started
                  ON started.id = (
                      SELECT candidate.id
                      FROM action_transitions AS candidate
                      WHERE candidate.action_id = a.action_id
                        AND candidate.status = 'SUBMIT_STARTED'
                        AND json_extract(
                            candidate.details_json, '$.attempt_id'
                        ) = s.attempt_id
                      ORDER BY candidate.id ASC
                      LIMIT 1
                  )
                LEFT JOIN order_reservations AS reservation
                  ON reservation.action_id = a.action_id
                WHERE LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                ORDER BY a.block_number, a.source_log_index,
                         a.discovered_at_ms, a.action_id
                """
            ).fetchall()
            reservation_only_rows = connection.execute(
                """
                SELECT a.action_id, latest.status AS latest_status,
                       COALESCE(target.state, '') AS target_state,
                       reservation.quantity AS reservation_quantity,
                       reservation.cash_reserved_usd
                           AS reservation_cash_usd
                FROM order_reservations AS reservation
                JOIN action_receipts AS a
                  ON a.action_id = reservation.action_id
                JOIN action_transitions AS latest
                  ON latest.id = (
                      SELECT id FROM action_transitions
                      WHERE action_id = a.action_id
                      ORDER BY id DESC LIMIT 1
                  )
                LEFT JOIN action_targets AS target
                  ON target.action_id = a.action_id
                WHERE reservation.active = 1
                  AND LOWER(a.source_role) IN (
                      'maker', 'verified_public_wallet'
                  )
                  AND NOT EXISTS(
                      SELECT 1 FROM submission_attempts AS attempt
                      WHERE attempt.action_id = a.action_id
                        AND attempt.state IN (
                            'SUBMIT_STARTED', 'SUBMITTED_UNRECONCILED',
                            'UNKNOWN_SUBMISSION'
                        )
                  )
                ORDER BY a.block_number, a.source_log_index,
                         a.discovered_at_ms, a.action_id
                """
            ).fetchall()
        results: list[tuple[SourceAction, dict[str, Any]]] = []
        anomalies: list[dict[str, Any]] = []
        for row in rows:
            latest_status = str(row["latest_status"] or "")
            target_state = str(row["target_state"] or "")
            attempt_id = str(row["submission_attempt_id"])
            attempt_state = str(row["submission_attempt_state"])
            problem = ""
            if latest_status in terminal_states:
                problem = "TERMINAL_LATEST_WITH_UNRESOLVED_ATTEMPT"
            elif row["target_action_id"] is None:
                problem = "UNRESOLVED_ATTEMPT_WITHOUT_ACTION_TARGET"
            elif target_state in terminal_states:
                problem = "TERMINAL_TARGET_WITH_UNRESOLVED_ATTEMPT"
            elif int(row["unresolved_attempt_count"]) != 1:
                problem = "MULTIPLE_UNRESOLVED_ATTEMPTS"
            quantity_evidence: dict[str, Any] = {}
            if not problem:
                frozen_submission_details_raw = row[
                    "frozen_submission_details_json"
                ]
                try:
                    frozen_submission_details = json.loads(
                        str(frozen_submission_details_raw or "")
                    )
                    frozen_plan = frozen_submission_details.get("plan")
                    if not isinstance(frozen_plan, Mapping):
                        raise ValueError("frozen plan is missing")
                    attempt_requested = Decimal(
                        str(row["submission_requested_quantity"])
                    )
                    frozen_requested = Decimal(
                        str(frozen_plan["requested_quantity"])
                    )
                    target_quantity = Decimal(str(row["target_quantity"]))
                    target_filled = Decimal(
                        str(row["target_filled_quantity"])
                    )
                    target_remaining = target_quantity - target_filled
                    quantities = (
                        attempt_requested,
                        frozen_requested,
                        target_quantity,
                        target_filled,
                        target_remaining,
                    )
                    if (
                        not all(value.is_finite() for value in quantities)
                        or attempt_requested <= ZERO
                        or frozen_requested <= ZERO
                        or target_quantity <= ZERO
                        or target_filled < ZERO
                        or target_remaining <= ZERO
                        or attempt_requested != frozen_requested
                        or attempt_requested != target_remaining
                    ):
                        raise ValueError("submission quantities disagree")
                except (
                    InvalidOperation,
                    KeyError,
                    TypeError,
                    ValueError,
                    json.JSONDecodeError,
                ):
                    problem = "SUBMISSION_ATTEMPT_FROZEN_QUANTITY_MISMATCH"
                    quantity_evidence = {
                        "attempt_requested_quantity": str(
                            row["submission_requested_quantity"] or ""
                        ),
                        "frozen_plan_requested_quantity": "",
                        "target_remaining_quantity": "",
                    }
                    try:
                        candidate_frozen = json.loads(
                            str(frozen_submission_details_raw or "")
                        )
                        candidate_plan = candidate_frozen.get("plan", {})
                        if isinstance(candidate_plan, Mapping):
                            quantity_evidence[
                                "frozen_plan_requested_quantity"
                            ] = str(
                                candidate_plan.get("requested_quantity") or ""
                            )
                    except (TypeError, ValueError, json.JSONDecodeError):
                        pass
                    try:
                        quantity_evidence["target_remaining_quantity"] = str(
                            Decimal(str(row["target_quantity"]))
                            - Decimal(str(row["target_filled_quantity"]))
                        )
                    except (InvalidOperation, TypeError, ValueError):
                        pass
            if not problem and int(row["active_reservation"]) != 1:
                problem = "UNRESOLVED_ATTEMPT_WITHOUT_ACTIVE_RESERVATION"
            if problem:
                anomalies.append(
                    {
                        "action_id": str(row["action_id"]),
                        "attempt_id": attempt_id,
                        "attempt_state": attempt_state,
                        "latest_status": latest_status,
                        "problem": problem,
                        "target_state": target_state,
                        **(
                            {
                                "unresolved_attempt_count": int(
                                    row["unresolved_attempt_count"]
                                )
                            }
                            if problem == "MULTIPLE_UNRESOLVED_ATTEMPTS"
                            else {}
                        ),
                        **quantity_evidence,
                    }
                )
            if problem in {
                "TERMINAL_LATEST_WITH_UNRESOLVED_ATTEMPT",
                "TERMINAL_TARGET_WITH_UNRESOLVED_ATTEMPT",
                "UNRESOLVED_ATTEMPT_WITHOUT_ACTION_TARGET",
                "MULTIPLE_UNRESOLVED_ATTEMPTS",
                "SUBMISSION_ATTEMPT_FROZEN_QUANTITY_MISMATCH",
            }:
                continue
            details = json.loads(
                str(row["frozen_submission_details_json"])
            )
            details["attempt_id"] = attempt_id
            details["attempt_number"] = int(row["submission_attempt_number"])
            details["order_id"] = (
                None
                if row["submission_order_id"] is None
                else str(row["submission_order_id"])
            )
            details["attempt_state"] = attempt_state
            details["prepared_order"] = json.loads(
                str(row["prepared_order_json"] or "{}")
            )
            details["snapshot"] = json.loads(
                str(row["submission_snapshot_json"] or "{}")
            )
            plan = details.get("plan")
            if not isinstance(plan, Mapping):
                plan = {}
            details["plan"] = {
                **dict(plan),
                "requested_quantity": str(row["submission_requested_quantity"]),
            }
            attempt_response = json.loads(
                str(row["submission_response_json"] or "{}")
            )
            if attempt_response:
                details["response"] = attempt_response
            results.append((self._source_from_receipt(row), details))

        anomalies.extend(
            {
                "action_id": str(row["action_id"]),
                "attempt_id": "",
                "attempt_state": "",
                "automatic_release": False,
                "latest_status": str(row["latest_status"] or ""),
                "new_order_submitted_by_recovery": False,
                "problem": "ACTIVE_RESERVATION_WITHOUT_UNRESOLVED_ATTEMPT",
                "reservation_cash_usd": str(
                    row["reservation_cash_usd"]
                ),
                "reservation_quantity": str(row["reservation_quantity"]),
                "target_state": str(row["target_state"] or ""),
            }
            for row in reservation_only_rows
        )
        if anomalies:
            anomaly_hash = canonical_hash({"anomalies": anomalies})
            if (
                self.runtime_value("submission_reconciliation_anomaly_hash")
                != anomaly_hash
            ):
                self.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category="INTERNAL_SUBMISSION_RECONCILIATION_STATE",
                    message="SUBMISSION_RECONCILIATION_LEDGER_INVARIANT",
                    details={"anomalies": anomalies},
                )
                self.set_runtime(
                    "submission_reconciliation_anomaly_hash", anomaly_hash
                )
            # A missing reservation or conflicting terminal ledger is not
            # self-healed by a later official read.  Keep the operator-visible
            # repair state latched until an explicit ledger repair clears it.
            self.set_runtime(
                "submission_reconciliation_state", "CODE_REPAIR_REQUIRED"
            )
        elif self.runtime_value("submission_reconciliation_state") is None:
            self.set_runtime("submission_reconciliation_state", "OK")
        return results

    def retryable_actions(self) -> list[SourceAction]:
        """Return unresolved business targets in original source order."""

        target_states = (
            "READY",
            "PENDING_LIQUIDITY",
            "PENDING_CONFIRMED_ZERO_FILL",
            "PENDING_PRICE_PROTECTION",
            "PARTIAL_PENDING",
            "PENDING_MINIMUM_UNWIND",
            "PENDING_MINIMUM_REMAINDER",
            "PENDING_EXTERNAL_RETRY",
        )
        allowed_latest_states = (
            "PLANNED",
            "SCOPE_ELIGIBLE",
            "PENDING_CAUSAL_ORDER",
            "PENDING_EXTERNAL_RETRY",
            "PENDING_LIQUIDITY",
            "PENDING_CONFIRMED_ZERO_FILL",
            "PENDING_PRICE_PROTECTION",
            "PARTIAL_PENDING",
            "PENDING_MINIMUM_UNWIND",
            "PENDING_MINIMUM_REMAINDER",
        )
        known_target_states = (
            "READY",
            "SKIPPED",
            "FILLED",
            "PARTIAL",
            "ERROR",
            "ERROR_INTERNAL",
            "EXTERNAL_UNFILLABLE",
            "SUPERSEDED_UNFILLED",
            "SUBMIT_STARTED",
            "SUBMITTED_UNRECONCILED",
            "UNKNOWN_SUBMISSION",
            "PENDING_LIQUIDITY",
            "PENDING_CONFIRMED_ZERO_FILL",
            "PENDING_PRICE_PROTECTION",
            "PARTIAL_PENDING",
            "PENDING_CAPITAL",
            "PENDING_MINIMUM_UNWIND",
            "PENDING_MINIMUM_REMAINDER",
            "PENDING_EXTERNAL_RETRY",
            "PENDING_CAUSAL_ORDER",
            "PENDING_INTERNAL_INVARIANT",
            "EXPIRED_RETRY_WINDOW",
        )
        target_placeholders = ",".join("?" for _ in target_states)
        latest_placeholders = ",".join("?" for _ in allowed_latest_states)
        known_target_placeholders = ",".join(
            "?" for _ in known_target_states
        )
        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT a.*
                FROM action_receipts AS a
                LEFT JOIN action_targets AS t ON t.action_id = a.action_id
                JOIN action_transitions AS latest
                  ON latest.id = (
                      SELECT id FROM action_transitions
                      WHERE action_id = a.action_id
                      ORDER BY id DESC LIMIT 1
                  )
                WHERE LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                  AND NOT EXISTS(
                      SELECT 1
                      FROM repair_recovery_actions AS recovery
                      JOIN repair_recovery_manifests AS manifest
                        ON manifest.manifest_hash = recovery.manifest_hash
                      WHERE recovery.action_id = a.action_id
                        AND manifest.state = 'ACTIVE'
                  )
                  AND NOT EXISTS(
                      SELECT 1 FROM order_reservations AS reservation
                      WHERE reservation.action_id = a.action_id
                        AND reservation.active = 1
                  )
                  AND NOT EXISTS(
                      SELECT 1 FROM submission_attempts AS attempt
                      WHERE attempt.action_id = a.action_id
                        AND attempt.state IN (
                            'SUBMIT_STARTED', 'SUBMITTED_UNRECONCILED',
                            'UNKNOWN_SUBMISSION'
                        )
                  )
                  AND (
                      (
                          t.state IN ({target_placeholders})
                          AND latest.status IN ({latest_placeholders})
                      )
                      OR (
                          t.action_id IS NULL
                          AND latest.status IN (
                              'PENDING_CAUSAL_ORDER',
                              'PENDING_EXTERNAL_RETRY'
                          )
                      )
                  )
                ORDER BY a.block_number, a.source_log_index,
                         a.source_timestamp,
                         a.transaction_hash, a.token_id, a.side, a.order_hash
                """,
                (*target_states, *allowed_latest_states),
            ).fetchall()
            legacy_stale_rows = connection.execute(
                """
                SELECT a.*
                FROM action_receipts AS a
                JOIN action_targets AS t ON t.action_id = a.action_id
                JOIN action_transitions AS latest
                  ON latest.id = (
                      SELECT id FROM action_transitions
                      WHERE action_id = a.action_id
                      ORDER BY id DESC LIMIT 1
                  )
                WHERE LOWER(a.source_role) IN (
                    'maker', 'verified_public_wallet'
                )
                  AND t.state = 'ERROR_INTERNAL'
                  AND t.reason = 'INTERNAL_STALE_CAUSAL_TARGET'
                  AND latest.status = 'ERROR_INTERNAL'
                  AND latest.reason = 'INTERNAL_STALE_CAUSAL_TARGET'
                  AND NOT EXISTS(
                      SELECT 1 FROM order_reservations AS reservation
                      WHERE reservation.action_id = a.action_id
                        AND reservation.active = 1
                  )
                  AND NOT EXISTS(
                      SELECT 1 FROM submission_attempts AS attempt
                      WHERE attempt.action_id = a.action_id
                        AND attempt.state IN (
                            'SUBMIT_STARTED', 'SUBMITTED_UNRECONCILED',
                            'UNKNOWN_SUBMISSION'
                        )
                  )
                  AND NOT EXISTS(
                      SELECT 1
                      FROM repair_recovery_actions AS recovery
                      JOIN repair_recovery_manifests AS manifest
                        ON manifest.manifest_hash = recovery.manifest_hash
                      WHERE recovery.action_id = a.action_id
                        AND manifest.state = 'ACTIVE'
                  )
                ORDER BY a.block_number, a.source_log_index,
                         a.source_timestamp, a.action_id
                """
            ).fetchall()
            anomaly_rows = connection.execute(
                f"""
                SELECT a.action_id, t.state AS target_state,
                       latest.status AS latest_status
                FROM action_receipts AS a
                JOIN action_targets AS t ON t.action_id = a.action_id
                JOIN action_transitions AS latest
                  ON latest.id = (
                      SELECT id FROM action_transitions
                      WHERE action_id = a.action_id
                      ORDER BY id DESC LIMIT 1
                  )
                WHERE LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                  AND t.state IN ({target_placeholders})
                  AND latest.status NOT IN ({latest_placeholders})
                  AND latest.status NOT IN ('OBSERVED', 'PENDING_METADATA')
                  AND NOT EXISTS(
                      SELECT 1
                      FROM repair_recovery_actions AS recovery
                      JOIN repair_recovery_manifests AS manifest
                        ON manifest.manifest_hash = recovery.manifest_hash
                      WHERE recovery.action_id = a.action_id
                        AND manifest.state = 'ACTIVE'
                  )
                  AND NOT (
                      latest.status IN (
                          'SUBMIT_STARTED', 'SUBMITTED_UNRECONCILED',
                          'UNKNOWN_SUBMISSION'
                      )
                      AND (
                          EXISTS (
                              SELECT 1 FROM order_reservations AS reservation
                              WHERE reservation.action_id = a.action_id
                                AND reservation.active = 1
                          )
                          OR EXISTS (
                              SELECT 1 FROM submission_attempts AS attempt
                              WHERE attempt.action_id = a.action_id
                                AND attempt.state IN (
                                    'SUBMIT_STARTED',
                                    'SUBMITTED_UNRECONCILED',
                                    'UNKNOWN_SUBMISSION'
                                )
                          )
                      )
                  )
                  AND NOT (
                      latest.status = 'PENDING_INTERNAL_INVARIANT'
                      AND latest.reason LIKE ?
                      AND NOT EXISTS(
                          SELECT 1 FROM order_reservations AS reservation
                          WHERE reservation.action_id = a.action_id
                            AND reservation.active = 1
                      )
                      AND NOT EXISTS(
                          SELECT 1 FROM submission_attempts AS attempt
                          WHERE attempt.action_id = a.action_id
                            AND attempt.state IN (
                                'SUBMIT_STARTED',
                                'SUBMITTED_UNRECONCILED',
                                'UNKNOWN_SUBMISSION'
                            )
                      )
                  )
                ORDER BY a.block_number, a.source_log_index,
                         a.source_timestamp, a.action_id
                """,
                (
                    *target_states,
                    *allowed_latest_states,
                    LEGACY_LOCAL_CASH_MISMATCH_PREFIX + "%",
                ),
            ).fetchall()
            unknown_target_rows = connection.execute(
                f"""
                SELECT a.action_id, t.state AS target_state,
                       latest.status AS latest_status
                FROM action_receipts AS a
                JOIN action_targets AS t ON t.action_id = a.action_id
                JOIN action_transitions AS latest
                  ON latest.id = (
                      SELECT id FROM action_transitions
                      WHERE action_id = a.action_id
                      ORDER BY id DESC LIMIT 1
                  )
                WHERE LOWER(a.source_role) IN (
                    'maker', 'verified_public_wallet'
                )
                  AND t.state NOT IN ({known_target_placeholders})
                  AND NOT EXISTS(
                      SELECT 1
                      FROM repair_recovery_actions AS recovery
                      JOIN repair_recovery_manifests AS manifest
                        ON manifest.manifest_hash = recovery.manifest_hash
                      WHERE recovery.action_id = a.action_id
                        AND manifest.state = 'ACTIVE'
                  )
                ORDER BY a.block_number, a.source_log_index,
                         a.source_timestamp, a.action_id
                """,
                known_target_states,
            ).fetchall()
        anomalies = [
            {
                "action_id": str(row["action_id"]),
                "target_state": str(row["target_state"]),
                "latest_status": str(row["latest_status"]),
            }
            for row in anomaly_rows
        ]
        unknown_target_states_found = [
            {
                "action_id": str(row["action_id"]),
                "target_state": str(row["target_state"]),
                "latest_status": str(row["latest_status"]),
            }
            for row in unknown_target_rows
        ]
        if anomalies or unknown_target_states_found:
            anomaly_hash = canonical_hash(
                {
                    "anomalies": anomalies,
                    "unknown_target_states": unknown_target_states_found,
                }
            )
            if (
                self.runtime_value("retryable_target_selector_anomaly_hash")
                != anomaly_hash
            ):
                self.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category="INTERNAL_RETRY_SELECTOR_STATE",
                    message=(
                        "ACTION_TARGET_STATE_NOT_RECOGNIZED"
                        if unknown_target_states_found and not anomalies
                        else "RETRYABLE_TARGET_LATEST_STATE_NOT_EXECUTABLE"
                    ),
                    details={
                        "anomalies": anomalies,
                        "unknown_target_states": unknown_target_states_found,
                        "allowed_latest_states": list(allowed_latest_states),
                        "known_target_states": list(known_target_states),
                    },
                )
                self.set_runtime(
                    "retryable_target_selector_anomaly_hash", anomaly_hash
                )
            self.set_runtime(
                "retryable_target_selector_state", "CODE_REPAIR_REQUIRED"
            )
        else:
            self.set_runtime("retryable_target_selector_state", "OK")
            self.set_runtime("retryable_target_selector_anomaly_hash", "")
        prospective_boundary = self.liquidity_retry_effective_after_block()
        selected = [self._source_from_receipt(row) for row in rows]
        if prospective_boundary is not None:
            selected = [
                source
                for source in selected
                if source.block_number > prospective_boundary
            ]
        selected.extend(
            source
            for row in legacy_stale_rows
            if _legacy_stable_causal_prefix_recovery_evidence(
                store=self,
                source=(source := self._source_from_receipt(row)),
            )
            is not None
            and (
                prospective_boundary is None
                or source.block_number > prospective_boundary
            )
        )
        return selected

    def finalize_post_boundary_partial_dust(
        self,
        *,
        effective_after_block: int,
        finalized_at_ms: int,
    ) -> list[str]:
        """Close only post-boundary FAK residues below their frozen minimum.

        This is a receipt-only repair for a partial fill that cannot be made
        whole without submitting a new order at a later book.  It uses the
        original submission snapshot, never reads a current CLOB book, and
        never changes cash, positions, or the recorded fill.
        """

        try:
            boundary = int(effective_after_block)
            finalized_at = int(finalized_at_ms)
        except (TypeError, ValueError) as exc:
            raise LiveConfigurationError("INVALID_PARTIAL_DUST_REPAIR_BOUNDARY") from exc
        if boundary < 0 or finalized_at < 0:
            raise LiveConfigurationError("INVALID_PARTIAL_DUST_REPAIR_BOUNDARY")
        self.initialize()
        finalized: list[str] = []
        partial_retry_states = (
            "PARTIAL_PENDING",
            "PENDING_LIQUIDITY",
            "PENDING_MINIMUM_UNWIND",
            "PENDING_MINIMUM_REMAINDER",
            "PENDING_EXTERNAL_RETRY",
        )
        placeholders = ",".join("?" for _ in partial_retry_states)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                f"""
                SELECT t.action_id, t.target_quantity, t.cumulative_filled_quantity,
                       a.block_number,
                       (
                           SELECT attempt.snapshot_json
                           FROM submission_attempts AS attempt
                           WHERE attempt.action_id = t.action_id
                           ORDER BY attempt.attempt_number ASC, attempt.created_at_ms ASC
                           LIMIT 1
                       ) AS snapshot_json
                FROM action_targets AS t
                JOIN action_receipts AS a ON a.action_id = t.action_id
                WHERE t.state IN ({placeholders})
                  AND a.block_number > ?
                  AND NOT EXISTS(
                      SELECT 1 FROM order_reservations AS reservation
                      WHERE reservation.action_id = t.action_id
                        AND reservation.active = 1
                )
                ORDER BY a.block_number, a.source_log_index, a.action_id
                """,
                (*partial_retry_states, boundary),
            ).fetchall()
            for row in rows:
                raw_snapshot = row["snapshot_json"]
                if raw_snapshot is None:
                    continue
                try:
                    snapshot = json.loads(str(raw_snapshot))
                except json.JSONDecodeError as exc:
                    raise LiveConfigurationError(
                        "INVALID_RECORDED_PARTIAL_DUST_SNAPSHOT"
                    ) from exc
                target_quantity = Decimal(str(row["target_quantity"]))
                cumulative_filled = Decimal(str(row["cumulative_filled_quantity"]))
                if cumulative_filled <= ZERO:
                    continue
                remaining = target_quantity - cumulative_filled
                if remaining <= ZERO:
                    raise LiveConfigurationError(
                        "PARTIAL_DUST_TARGET_HAS_NO_POSITIVE_REMAINDER"
                    )
                evidence = _partial_remainder_below_recorded_minimum(
                    remaining_quantity=remaining,
                    original_minimum_order_size=(
                        _recorded_minimum_order_size_from_snapshot(snapshot)
                    ),
                )
                if evidence is None:
                    continue
                details = {
                    "repair": "POST_BOUNDARY_PARTIAL_DUST_FINALIZED_FROM_FROZEN_SNAPSHOT",
                    "effective_after_block": boundary,
                    "source_block_number": int(row["block_number"]),
                    "current_book_read": False,
                    "new_order_submitted": False,
                    "cash_or_position_rewritten": False,
                    **evidence,
                }
                connection.execute(
                    """
                    UPDATE action_targets
                    SET state = 'EXTERNAL_UNFILLABLE',
                        reason = 'PARTIAL_REMAINDER_BELOW_ORIGINAL_MARKET_MINIMUM',
                        updated_at_ms = ?
                    WHERE action_id = ?
                    """,
                    (finalized_at, str(row["action_id"])),
                )
                connection.execute(
                    """
                    INSERT INTO action_transitions(
                        action_id, status, reason, created_at_ms, details_json
                    ) VALUES(?, 'EXTERNAL_UNFILLABLE',
                             'PARTIAL_REMAINDER_BELOW_ORIGINAL_MARKET_MINIMUM', ?, ?)
                    """,
                    (
                        str(row["action_id"]),
                        finalized_at,
                        _receipt_json(details),
                    ),
                )
                finalized.append(str(row["action_id"]))
        return finalized

    def prior_buy_evidence(self, source: SourceAction) -> dict[str, Any] | None:
        """Return the latest causal BUY before one SELL for the same token."""

        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT a.action_id, latest.status, latest.reason,
                       COALESCE(t.cumulative_filled_quantity, '0') AS filled
                FROM action_receipts AS a
                JOIN action_transitions AS latest
                  ON latest.id = (
                      SELECT id FROM action_transitions
                      WHERE action_id = a.action_id
                      ORDER BY id DESC LIMIT 1
                  )
                LEFT JOIN action_targets AS t ON t.action_id = a.action_id
                WHERE a.token_id = ? AND a.side = 'BUY'
                  AND LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                  AND (
                      a.block_number, a.source_log_index, a.source_timestamp,
                      a.transaction_hash, a.token_id, a.side, a.order_hash,
                      a.action_id
                  ) < (?, ?, ?, ?, ?, ?, ?, ?)
                ORDER BY a.block_number DESC, a.source_log_index DESC,
                         a.source_timestamp DESC, a.action_id DESC
                LIMIT 1
                """,
                (
                    source.token_id,
                    source.block_number,
                    source.log_index,
                    source.source_timestamp,
                    source.transaction_hash.lower(),
                    source.token_id,
                    source.side.upper(),
                    source.order_hash.lower(),
                    source.action_id,
                ),
            ).fetchone()
        if row is None:
            return None
        return {
            "action_id": str(row["action_id"]),
            "status": str(row["status"]),
            "reason": str(row["reason"]),
            "filled_quantity": Decimal(str(row["filled"])),
        }

    def orphaned_metadata_actions(
        self, *, processed_through_block: int
    ) -> list[SourceAction]:
        """Return observed metadata waits whose chain cursor already passed."""

        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT a.*
                FROM action_receipts AS a
                JOIN action_transitions AS t
                  ON t.id = (
                    SELECT id FROM action_transitions
                    WHERE action_id = a.action_id
                    ORDER BY id DESC LIMIT 1
                  )
                WHERE t.status = 'PENDING_METADATA'
                  AND LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                  AND a.block_number <= ?
                  AND NOT EXISTS(
                      SELECT 1
                      FROM repair_recovery_actions AS recovery
                      JOIN repair_recovery_manifests AS manifest
                        ON manifest.manifest_hash = recovery.manifest_hash
                      WHERE recovery.action_id = a.action_id
                        AND manifest.state = 'ACTIVE'
                  )
                ORDER BY a.block_number, a.source_log_index,
                         a.source_timestamp, a.action_id
                """,
                (int(processed_through_block),),
            ).fetchall()
        return [self._source_from_receipt(row) for row in rows]

    def orphaned_observed_actions(
        self, *, processed_through_block: int
    ) -> list[SourceAction]:
        """Return pre-side-effect observations whose chain cursor already passed.

        OBSERVED is written before market metadata, planning, reservation, or
        submission.  A process interruption at that boundary must therefore
        resume the immutable source action instead of leaving it behind the
        forward-only chain cursor forever.  Any action with a submission
        attempt is excluded fail-closed even if its transition history is
        corrupt.
        """

        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT a.*
                FROM action_receipts AS a
                JOIN action_transitions AS t
                  ON t.id = (
                    SELECT id FROM action_transitions
                    WHERE action_id = a.action_id
                    ORDER BY id DESC LIMIT 1
                  )
                WHERE t.status = 'OBSERVED'
                  AND LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                  AND a.block_number <= ?
                  AND NOT EXISTS (
                    SELECT 1 FROM submission_attempts AS attempt
                    WHERE attempt.action_id = a.action_id
                  )
                  AND NOT EXISTS(
                      SELECT 1
                      FROM repair_recovery_actions AS recovery
                      JOIN repair_recovery_manifests AS manifest
                        ON manifest.manifest_hash = recovery.manifest_hash
                      WHERE recovery.action_id = a.action_id
                        AND manifest.state = 'ACTIVE'
                  )
                ORDER BY a.block_number, a.source_log_index,
                         a.source_timestamp, a.action_id
                """,
                (int(processed_through_block),),
            ).fetchall()
        return [self._source_from_receipt(row) for row in rows]

    def retryable_prior_actions(
        self, *, processed_through_block: int | None
    ) -> list[SourceAction]:
        """Return one de-duplicated universe of pre-head durable work."""

        if processed_through_block is None:
            return []
        boundary = int(processed_through_block)
        prospective_boundary = self.bounded_retry_effective_after_block()
        candidates = [
            *self.orphaned_observed_actions(processed_through_block=boundary),
            *self.orphaned_metadata_actions(processed_through_block=boundary),
            *self.retryable_actions(),
        ]
        by_id: dict[str, SourceAction] = {}
        for source in candidates:
            if (
                source.block_number <= boundary
                and (
                    prospective_boundary is None
                    or source.block_number > prospective_boundary
                )
            ):
                by_id[source.action_id] = source
        return list(by_id.values())

    def supersede_earlier_fully_unfilled_opposites(
        self, *, source: SourceAction
    ) -> list[str]:
        """Stop a V2 remainder after the source later reverses direction.

        Canonical source order is the immutable chain tuple, not SQLite row
        insertion order. Metadata recovery and restart replay may insert an
        older receipt after a newer one. A partial fill keeps its inventory,
        while only its still-unfilled remainder becomes terminal.
        """

        retryable_states = (
            "PENDING_LIQUIDITY",
            "PENDING_CONFIRMED_ZERO_FILL",
            "PENDING_PRICE_PROTECTION",
            "PARTIAL_PENDING",
            "PENDING_CAPITAL",
            "PENDING_MINIMUM_UNWIND",
            "PENDING_MINIMUM_REMAINDER",
            "PENDING_EXTERNAL_RETRY",
        )
        placeholders = ",".join("?" for _ in retryable_states)
        self.initialize()
        policy_boundary = self.liquidity_retry_effective_after_block()
        if policy_boundary is None:
            return []
        superseded: list[str] = []
        with self.connect() as connection:
            current = connection.execute(
                """
                SELECT block_number, source_log_index, source_timestamp,
                       transaction_hash, token_id, side, order_hash, action_id
                FROM action_receipts WHERE action_id = ?
                """,
                (source.action_id,),
            ).fetchone()
            if current is None:
                raise LiveConfigurationError("CURRENT_ACTION_RECEIPT_NOT_FOUND")
            rows = connection.execute(
                f"""
                SELECT a.action_id,t.cumulative_filled_quantity
                FROM action_receipts AS a
                JOIN action_targets AS t ON t.action_id = a.action_id
                WHERE (
                    a.block_number, a.source_log_index, a.source_timestamp,
                    a.transaction_hash, a.token_id, a.side, a.order_hash,
                    a.action_id
                ) < (?, ?, ?, ?, ?, ?, ?, ?)
                  AND LOWER(a.source_role) IN ('maker', 'verified_public_wallet')
                  AND a.token_id = ?
                  AND a.side != ?
                  AND a.block_number > ?
                  AND t.state IN ({placeholders})
                ORDER BY a.block_number, a.source_log_index,
                         a.source_timestamp, a.transaction_hash,
                         a.token_id, a.side, a.order_hash, a.action_id
                """,
                (
                    int(current["block_number"]),
                    int(current["source_log_index"]),
                    int(current["source_timestamp"]),
                    str(current["transaction_hash"]),
                    str(current["token_id"]),
                    str(current["side"]),
                    str(current["order_hash"]),
                    str(current["action_id"]),
                    str(source.token_id),
                    str(source.side).upper(),
                    int(policy_boundary),
                    *retryable_states,
                ),
            ).fetchall()
            for row in rows:
                action_id = str(row["action_id"])
                active = connection.execute(
                    """
                    SELECT 1 FROM order_reservations
                    WHERE action_id = ? AND active = 1
                    """,
                    (action_id,),
                ).fetchone()
                if active is not None:
                    raise LiveConfigurationError(
                        "CANNOT_SUPERSEDE_ACTION_WITH_ACTIVE_RESERVATION"
                    )
                if self.action_has_unresolved_submission_side_effect(
                    action_id, connection=connection
                ):
                    raise LiveConfigurationError(
                        "CANNOT_SUPERSEDE_ACTION_WITH_UNRESOLVED_SUBMISSION"
                    )
                filled = Decimal(str(row["cumulative_filled_quantity"]))
                terminal_state = "PARTIAL" if filled > ZERO else "SUPERSEDED_UNFILLED"
                changed = connection.execute(
                    f"""
                    UPDATE action_targets
                    SET state = ?,
                        reason = 'LATER_OPPOSITE_SOURCE_ACTION',
                        updated_at_ms = ?
                    WHERE action_id = ?
                      AND state IN ({placeholders})
                    """,
                    (
                        terminal_state,
                        int(source.discovered_at_ms),
                        action_id,
                        *retryable_states,
                    ),
                )
                if changed.rowcount != 1:
                    continue
                connection.execute(
                    """
                    INSERT INTO action_transitions(
                        action_id, status, reason, created_at_ms, details_json
                    ) VALUES(?, ?, 'LATER_OPPOSITE_SOURCE_ACTION', ?, ?)
                    """,
                    (
                        action_id,
                        terminal_state,
                        int(source.discovered_at_ms),
                        json.dumps(
                            {
                                "superseding_action_id": source.action_id,
                                "cumulative_filled_quantity": str(filled),
                                "unfilled_remainder_terminated": True,
                            },
                            sort_keys=True,
                        ),
                    ),
                )
                superseded.append(action_id)
        return superseded


def initialize_scale_once(
    *,
    store: LiveStore,
    allocation_usd: Decimal,
    source_open_position_value_usd: Decimal,
    observed_at_ms: int,
) -> Decimal:
    return store.initialize_scale_once(
        allocation_usd=allocation_usd,
        source_open_position_value_usd=source_open_position_value_usd,
        observed_at_ms=observed_at_ms,
    )


def _plan_details(
    plan: ActionPlan,
    snapshot: dict[str, Any],
    *,
    execution_order_type: str = IMMEDIATE_ORDER_TYPE,
) -> dict[str, Any]:
    return {
        "plan": {key: str(value) for key, value in asdict(plan).items()},
        "snapshot": snapshot,
        "execution_order_type": execution_order_type,
    }


def _submission_order_id(response: Any) -> str | None:
    if not isinstance(response, dict):
        return None
    for key in ("orderID", "order_id", "id"):
        value = response.get(key)
        if value:
            return str(value)
    return None


def _definitive_clob_rejection_reason(exc: BaseException | str) -> str | None:
    """Classify only CLOB responses that prove no immediate order can have filled.

    A transport failure after a POST remains ``UNKNOWN_SUBMISSION``.  In
    contrast, these documented CLOB validation/FAK-no-match messages are an
    explicit server-side rejection and must release the local reservation;
    keeping them ``UNKNOWN`` indefinitely both blocks cash and obscures the
    difference between a rejected order and an uncertain network outcome.
    """

    message = str(exc)
    lowered = message.lower()
    # ``py-clob-client`` sometimes raises a local ``PolyException`` after it
    # has already parsed the server's explicit invalid-tick response, without
    # preserving the HTTP status in the exception text.  That message is still
    # a deterministic pre-order validation rejection; do not strand its cash
    # reservation as an uncertain submission.
    if "invalid tick size" in lowered and "polyexception" in lowered:
        return "CLOB_REJECTED_INVALID_TICK_SIZE"
    if "status_code=400" not in lowered:
        return None
    if "invalid amount for a marketable buy order" in lowered:
        return "CLOB_REJECTED_INVALID_BUY_AMOUNT"
    if "invalid tick size" in lowered:
        return "CLOB_REJECTED_INVALID_TICK_SIZE"
    if "no orders found to match with fak order" in lowered:
        return "CLOB_REJECTED_FAK_NO_MATCH"
    # Retain this classifier solely for historical FOK submissions already
    # present in the immutable ledger.  New submissions use FAK below.
    if (
        "couldn't be fully filled" in lowered
        and "fok orders are fully filled or killed" in lowered
    ):
        return "CLOB_REJECTED_FOK_KILLED"
    return None


def _definitive_clob_response_rejection_reason(message: str) -> str | None:
    """Classify an explicit unsuccessful CLOB response without HTTP wrapping."""

    lowered = str(message).lower()
    if "no orders found to match with fak order" in lowered:
        return "CLOB_REJECTED_FAK_NO_MATCH"
    if "invalid amount for a marketable buy order" in lowered:
        return "CLOB_REJECTED_INVALID_BUY_AMOUNT"
    if "invalid tick size" in lowered:
        return "CLOB_REJECTED_INVALID_TICK_SIZE"
    return None


class CLOBExecutionAdapter:
    """Narrow authenticated-CLOB adapter for one serial live copier.

    The adapter snapshots a current public order book and submits an immediate
    FAK marketable order with that best price as its worst-price limit.  For a
    BUY, the CLOB requires a dollar amount, so it is derived from the exact
    scaled share target multiplied by the just-snapshotted Ask.  For a SELL,
    the required amount is the exact share target.
    """

    def __init__(
        self,
        client: Any,
        *,
        minimum_marketable_buy_notional_usd: Decimal,
        receipt_reader: Any | None = None,
    ):
        self.client = client
        self.receipt_reader = receipt_reader
        try:
            minimum_buy_notional = Decimal(
                str(minimum_marketable_buy_notional_usd)
            )
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise LiveConfigurationError(
                "INVALID_MARKETABLE_BUY_MIN_NOTIONAL"
            ) from exc
        if not minimum_buy_notional.is_finite() or minimum_buy_notional <= ZERO:
            raise LiveConfigurationError("NONPOSITIVE_MARKETABLE_BUY_MIN_NOTIONAL")
        self.minimum_marketable_buy_notional_usd = minimum_buy_notional
        self._options_by_token: dict[str, Any] = {}

    @staticmethod
    def _best_level(raw_levels: Any, *, side: str) -> tuple[Decimal, Decimal]:
        if not isinstance(raw_levels, list) or not raw_levels:
            raise RuntimeError(f"NO_{side}_BOOK_LEVEL")
        parsed: list[tuple[Decimal, Decimal]] = []
        for level in raw_levels:
            if not isinstance(level, dict):
                continue
            try:
                price = Decimal(str(level["price"]))
                quantity = Decimal(str(level["size"]))
            except (KeyError, InvalidOperation, ValueError) as exc:
                raise RuntimeError(f"INVALID_{side}_BOOK_LEVEL") from exc
            if price > ZERO and quantity > ZERO:
                parsed.append((price, quantity))
        if not parsed:
            raise RuntimeError(f"NO_VALID_{side}_BOOK_LEVEL")
        if side == "ASK":
            return min(parsed, key=lambda item: item[0])
        return max(parsed, key=lambda item: item[0])

    def condition_mapping_for_token(self, token_id: str) -> dict[str, str]:
        """Read the canonical two-token condition before the first live BUY."""

        token = str(token_id).strip()
        payload = _bounded_public_json(
            "https://clob.polymarket.com/markets-by-token/" + token
        )
        if not isinstance(payload, dict):
            raise LiveConfigurationError("INVALID_TOKEN_CONDITION_MAPPING_RESPONSE")
        mapping = {
            "condition_id": str(payload.get("condition_id") or "").strip().lower(),
            "primary_token_id": str(payload.get("primary_token_id") or "").strip(),
            "secondary_token_id": str(payload.get("secondary_token_id") or "").strip(),
        }
        LiveStore._validate_condition_mapping(
            token_id=token,
            condition_id=mapping["condition_id"],
            primary_token_id=mapping["primary_token_id"],
            secondary_token_id=mapping["secondary_token_id"],
        )
        return mapping

    def snapshot(self, *, token_id: str, side: str) -> dict[str, Any]:
        normalized_side = str(side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported side: {side!r}")
        book = self.client.get_order_book(str(token_id))
        if not isinstance(book, dict):
            raise RuntimeError("INVALID_CLOB_BOOK")
        required = ("min_order_size", "tick_size", "neg_risk")
        missing = [key for key in required if book.get(key) in {None, ""}]
        if missing:
            raise RuntimeError("MISSING_CLOB_CONSTRAINTS:" + ",".join(missing))
        level_side = "ASK" if normalized_side == "BUY" else "BID"
        best_price, visible_size = self._best_level(
            book.get("asks") if normalized_side == "BUY" else book.get("bids"),
            side=level_side,
        )
        # V2 no longer uses the legacy ``/fee-rate`` integer as the signed
        # order fee.  Fees are set by the protocol at match time.  Read the
        # market's V2 fee curve from the same CLOB market metadata that governs
        # this token, otherwise a BUY cash reservation can be wrong by a large
        # factor even though the book itself was valid.
        condition_id = str(book.get("market", "")).strip()
        if not condition_id:
            raise RuntimeError("MISSING_CLOB_MARKET_CONDITION_ID")
        try:
            market_info = self.client.get_clob_market_info(condition_id)
            fee_details = market_info.get("fd") if isinstance(market_info, dict) else None
            fee_rate = Decimal(str(fee_details["r"]))
            fee_exponent = Decimal(str(fee_details["e"]))
            # The same official market metadata already contains the current
            # minimum tick (mts).  A second /tick-size GET repeated that fact
            # and added a complete CLOB round trip to every action.
            authoritative_tick = Decimal(str(market_info["mts"]))
            book_minimum = Decimal(str(book["min_order_size"]))
            market_minimum = Decimal(str(market_info.get("mos", book_minimum)))
        except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
            raise RuntimeError("INVALID_CLOB_V2_FEE_METADATA") from exc
        if (
            fee_rate < ZERO
            or fee_exponent < ZERO
            or fee_exponent != fee_exponent.to_integral_value()
            or authoritative_tick <= ZERO
            or book_minimum <= ZERO
            or market_minimum <= ZERO
        ):
            raise RuntimeError("UNSUPPORTED_CLOB_V2_FEE_METADATA")
        minimum_order_size = max(book_minimum, market_minimum)
        rounding = ROUND_CEILING if normalized_side == "BUY" else ROUND_FLOOR
        tick_units = (best_price / authoritative_tick).to_integral_value(
            rounding=rounding
        )
        worst_price = tick_units * authoritative_tick
        if worst_price <= ZERO:
            raise RuntimeError("INVALID_TICK_ALIGNED_WORST_PRICE")
        fee_bps = fee_rate * Decimal("10000")
        try:
            from py_clob_client_v2.clob_types import PartialCreateOrderOptions

            options = PartialCreateOrderOptions(
                tick_size=str(authoritative_tick),
                neg_risk=bool(book["neg_risk"]),
            )
        except Exception as exc:
            raise RuntimeError(f"INVALID_CLOB_ORDER_OPTIONS: {exc}") from exc
        self._options_by_token[str(token_id)] = options
        return {
            "condition_id": condition_id,
            "minimum_order_size": str(minimum_order_size),
            "minimum_marketable_buy_notional_usd": str(
                self.minimum_marketable_buy_notional_usd
            ),
            "best_price": str(worst_price),
            "visible_best_level_size": str(visible_size),
            "fee_bps": str(fee_bps),
            "fee_exponent": str(fee_exponent),
            "fee_source": "CLOB_V2_MARKET_INFO",
            "tick_size": str(authoritative_tick),
            "tick_size_source": "CLOB_V2_MARKET_INFO",
            "book_tick_size_observed": str(book["tick_size"]),
            "market_minimum_order_size_observed": str(market_minimum),
            "raw_book": book,
        }

    def submit_fak_market(
        self,
        *,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        user_usdc_balance: Decimal | None = None,
    ) -> Any:
        options = self._options_by_token.get(str(token_id))
        if options is None:
            raise RuntimeError("ORDER_OPTIONS_NOT_SNAPSHOTTED")
        try:
            from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType

            normalized_side = str(side).upper()
            amount = size * price if normalized_side == "BUY" else size
            args = MarketOrderArgs(
                token_id=str(token_id),
                amount=float(amount),
                price=float(price),
                side=normalized_side,
                order_type=OrderType.FAK,
                # The V2 SDK uses this authoritative balance snapshot only to
                # keep a market BUY fee-aware when the usable balance is the
                # binding constraint.  It never increases our order amount.
                user_usdc_balance=(
                    0.0
                    if user_usdc_balance is None
                    else float(Decimal(str(user_usdc_balance)))
                ),
            )
            return self.client.create_and_post_market_order(
                args,
                options=options,
                order_type=OrderType.FAK,
            )
        finally:
            # A later action must obtain its own book/options snapshot.
            self._options_by_token.pop(str(token_id), None)

    def _prepared_order_identity(
        self, signed_order: Any, *, neg_risk: bool
    ) -> tuple[int, str, dict[str, Any]]:
        """Derive the exchange order hash from the exact signed payload."""

        try:
            from py_clob_client_v2.config import get_contract_config
            from py_clob_client_v2.order_utils import (
                ExchangeOrderBuilderV1,
                ExchangeOrderBuilderV2,
            )
            from py_clob_client_v2.order_utils.model.order_data_v1 import (
                SignedOrderV1,
            )
            from py_clob_client_v2.order_utils.model.order_data_v2 import (
                SignedOrderV2,
            )
        except ImportError as exc:
            raise RuntimeError("CLOB_ORDER_HASH_DEPENDENCY_UNAVAILABLE") from exc
        signer = self.client.builder.signer
        chain_id = int(signer.get_chain_id())
        contract = get_contract_config(chain_id)
        if isinstance(signed_order, SignedOrderV2):
            version = 2
            exchange = (
                contract.neg_risk_exchange_v2
                if neg_risk
                else contract.exchange_v2
            )
            builder = ExchangeOrderBuilderV2(exchange, chain_id, signer)
        elif isinstance(signed_order, SignedOrderV1):
            version = 1
            exchange = (
                contract.neg_risk_exchange if neg_risk else contract.exchange
            )
            builder = ExchangeOrderBuilderV1(exchange, chain_id, signer)
        else:
            raise RuntimeError("UNSUPPORTED_SIGNED_ORDER_TYPE")
        typed_data = builder.build_order_typed_data(signed_order)
        order_id = str(builder.build_order_hash(typed_data)).lower()
        if not order_id.startswith("0x") or len(order_id) != 66:
            raise RuntimeError("INVALID_PREPARED_ORDER_HASH")
        order_fields = asdict(signed_order)
        order_fields.pop("signature", None)
        return version, order_id, order_fields

    def prepare_fak_market(
        self,
        *,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        user_usdc_balance: Decimal | None = None,
    ) -> dict[str, Any]:
        """Sign and identify an exact FAK order before any network POST."""

        options = self._options_by_token.get(str(token_id))
        if options is None:
            raise RuntimeError("ORDER_OPTIONS_NOT_SNAPSHOTTED")
        try:
            from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType

            normalized_side = str(side).upper()
            amount = size * price if normalized_side == "BUY" else size
            args = MarketOrderArgs(
                token_id=str(token_id),
                amount=float(amount),
                price=float(price),
                side=normalized_side,
                order_type=OrderType.FAK,
                user_usdc_balance=(
                    0.0
                    if user_usdc_balance is None
                    else float(Decimal(str(user_usdc_balance)))
                ),
            )
            signed_order = self.client.create_market_order(args, options)
            version, order_id, order_fields = self._prepared_order_identity(
                signed_order, neg_risk=bool(options.neg_risk)
            )
            return {
                "order_id": order_id,
                "order_version": version,
                "order_type": "FAK",
                "neg_risk": bool(options.neg_risk),
                "order_fields": order_fields,
                "_signed_order": signed_order,
            }
        finally:
            self._options_by_token.pop(str(token_id), None)

    def prepare_fak_exact_shares(
        self,
        *,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        user_usdc_balance: Decimal | None = None,
    ) -> dict[str, Any]:
        """Sign a FAK limit order capped at the exact remaining shares.

        A market BUY is cash-denominated and can receive extra shares through
        price improvement.  A liquidity retry must instead conserve the
        immutable target, so it uses the SDK's share-denominated limit-order
        builder and posts that signed order as FAK.  The authenticated cash
        value has already been checked by the planner and is deliberately not
        passed to the SDK's size-adjustment hook.
        """

        del user_usdc_balance
        options = self._options_by_token.get(str(token_id))
        if options is None:
            raise RuntimeError("ORDER_OPTIONS_NOT_SNAPSHOTTED")
        try:
            from py_clob_client_v2.clob_types import OrderArgs

            normalized_side = str(side).upper()
            args = OrderArgs(
                token_id=str(token_id),
                price=float(price),
                size=float(size),
                side=normalized_side,
            )
            signed_order = self.client.create_order(args, options)
            version, order_id, order_fields = self._prepared_order_identity(
                signed_order, neg_risk=bool(options.neg_risk)
            )
            return {
                "order_id": order_id,
                "order_version": version,
                "order_type": "FAK",
                "quantity_mode": "EXACT_SHARES",
                "neg_risk": bool(options.neg_risk),
                "order_fields": order_fields,
                "_signed_order": signed_order,
            }
        finally:
            self._options_by_token.pop(str(token_id), None)

    def prepare_gtd_limit(
        self,
        *,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        user_usdc_balance: Decimal | None = None,
    ) -> dict[str, Any]:
        """Sign one exact-share GTC order for active cancellation."""

        del user_usdc_balance
        options = self._options_by_token.get(str(token_id))
        if options is None:
            raise RuntimeError("ORDER_OPTIONS_NOT_SNAPSHOTTED")
        try:
            from py_clob_client_v2.clob_types import OrderArgs

            args = OrderArgs(
                token_id=str(token_id),
                price=float(price),
                size=float(size),
                side=str(side).upper(),
                expiration=0,
            )
            signed_order = self.client.create_order(args, options)
            version, order_id, order_fields = self._prepared_order_identity(
                signed_order, neg_risk=bool(options.neg_risk)
            )
            return {
                "order_id": order_id,
                "order_version": version,
                "order_type": "GTC",
                "quantity_mode": "EXACT_SHARES",
                "neg_risk": bool(options.neg_risk),
                "order_fields": order_fields,
                "_signed_order": signed_order,
            }
        finally:
            self._options_by_token.pop(str(token_id), None)

    def submit_prepared_gtd_limit(
        self, prepared_order: Mapping[str, Any]
    ) -> Any:
        """POST one GTC order without blocking later source actions.

        The durable action loop records the accepted order immediately.  A
        later maintenance pass cancels the still-open remainder after the
        configured active window, so the submission lock is never held while
        waiting for liquidity.
        """

        try:
            from py_clob_client_v2.clob_types import OrderType
        except ImportError as exc:
            raise RuntimeError("CLOB_ORDER_TYPE_DEPENDENCY_UNAVAILABLE") from exc
        if str(prepared_order.get("order_type", "")).upper() != "GTC":
            raise RuntimeError("PREPARED_ORDER_IS_NOT_GTC")
        signed_order = prepared_order.get("_signed_order")
        if signed_order is None:
            raise RuntimeError("PREPARED_SIGNED_ORDER_NOT_IN_MEMORY")
        _, computed_order_id, _ = self._prepared_order_identity(
            signed_order,
            neg_risk=bool(prepared_order.get("neg_risk", False)),
        )
        if computed_order_id != str(prepared_order.get("order_id") or "").lower():
            raise RuntimeError("PREPARED_ORDER_HASH_MISMATCH")
        response = self.client.post_order(signed_order, OrderType.GTC)
        if isinstance(response, Mapping) and response.get("success") is False:
            return response
        order_id = (
            str(response.get("orderID") or response.get("orderId") or "").lower()
            if isinstance(response, Mapping)
            else ""
        )
        if not order_id:
            raise RuntimeError("ACTIVE_CANCEL_RESPONSE_ORDER_ID_MISSING")
        return response

    def cancel_active_gtd_order(self, order_id: str) -> dict[str, Any]:
        """Cancel one previously accepted GTC order and prove it is not open."""

        try:
            from py_clob_client_v2.clob_types import OrderPayload
        except ImportError as exc:
            raise RuntimeError("CLOB_ORDER_TYPE_DEPENDENCY_UNAVAILABLE") from exc
        normalized_order_id = str(order_id).strip().lower()
        if not normalized_order_id:
            raise RuntimeError("ACTIVE_CANCEL_ORDER_ID_MISSING")
        cancel_response = self.client.cancel_order(OrderPayload(orderID=order_id))
        still_open = any(
            str(
                order.get("id")
                or order.get("orderID")
                or order.get("orderId")
                or ""
            ).lower()
            == normalized_order_id
            for order in self.client.get_open_orders()
        )
        if still_open:
            raise RuntimeError("ACTIVE_CANCEL_ORDER_STILL_OPEN")
        return {
            "active_cancel_response": cancel_response,
            "active_cancel_verified": True,
            "active_cancel_wait_seconds": BUY_ACTIVE_CANCEL_WAIT_SECONDS,
        }

    def submit_prepared_fak_market(
        self, prepared_order: Mapping[str, Any]
    ) -> Any:
        """POST the persisted signed payload; retries retain the same hash."""

        try:
            from py_clob_client_v2.clob_types import OrderType
        except ImportError as exc:
            raise RuntimeError("CLOB_ORDER_TYPE_DEPENDENCY_UNAVAILABLE") from exc
        if str(prepared_order.get("order_type", "")).upper() != "FAK":
            raise RuntimeError("PREPARED_ORDER_IS_NOT_FAK")
        signed_order = prepared_order.get("_signed_order")
        if signed_order is None:
            raise RuntimeError("PREPARED_SIGNED_ORDER_NOT_IN_MEMORY")
        _, computed_order_id, _ = self._prepared_order_identity(
            signed_order,
            neg_risk=bool(prepared_order.get("neg_risk", False)),
        )
        persisted_order_id = str(prepared_order.get("order_id") or "").lower()
        if computed_order_id != persisted_order_id:
            raise RuntimeError("PREPARED_ORDER_HASH_MISMATCH")
        return self.client.post_order(signed_order, OrderType.FAK)

    def get_order(self, order_id: str) -> dict[str, Any] | None:
        result = self.client.get_order(str(order_id))
        if result is None:
            return None
        if not isinstance(result, dict):
            raise RuntimeError("INVALID_CLOB_ORDER_RESPONSE")
        return result

    def authoritative_submission_execution(
        self,
        *,
        source: SourceAction,
        order_id: str,
        response: Any,
    ) -> dict[str, Any] | None:
        """Return exact matched quantity, cash and fee from Polygon receipts.

        The CLOB response's ``price`` is a FAK worst-price limit, not an
        accounting price.  For V2 matches, the exchange's ``OrderFilled`` log
        holds the raw collateral amount, outcome-token amount, and actual fee.
        This method is deliberately fail-closed: an unavailable or ambiguous
        receipt yields no ledger mutation.
        """

        if not isinstance(response, dict):
            return None
        status = str(response.get("status", "")).strip().lower()
        if response.get("success") is not True or status != "matched":
            return None
        hashes_raw = response.get("transactionsHashes")
        if hashes_raw is None or hashes_raw == "":
            # Async CLOB execution can acknowledge a matched order before its
            # transaction hashes are attached.  Let authenticated order/trade
            # reconciliation obtain them instead of freezing the attempt here.
            return None
        if isinstance(hashes_raw, str):
            candidate_hashes = [hashes_raw]
        elif isinstance(hashes_raw, list):
            candidate_hashes = [str(value) for value in hashes_raw if str(value).strip()]
            if not candidate_hashes:
                return None
        else:
            raise RuntimeError("INVALID_MATCHED_TRANSACTION_HASHES")
        normalized_hashes = [value.strip().lower() for value in candidate_hashes]
        if any(
            re.fullmatch(r"0x[0-9a-f]{64}", value) is None
            for value in normalized_hashes
        ):
            raise RuntimeError("INVALID_MATCHED_TRANSACTION_HASHES")
        hashes = list(dict.fromkeys(normalized_hashes))
        if self.receipt_reader is None:
            raise RuntimeError("ONCHAIN_FILL_RECEIPT_READER_UNAVAILABLE")
        try:
            from eth_abi import decode
            from eth_utils import keccak
        except ImportError as exc:
            raise RuntimeError("ONCHAIN_FILL_RECEIPT_DECODER_UNAVAILABLE") from exc
        event_topic = "0x" + keccak(
            text="OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
        ).hex()
        normalized_order = str(order_id).lower()
        expected_side = 0 if source.side == "BUY" else 1
        total_quantity_raw = 0
        total_notional_raw = 0
        total_fee_raw = 0
        receipt_evidence: list[dict[str, str]] = []
        for transaction_hash in hashes:
            receipt = self.receipt_reader.get_receipt(transaction_hash)
            logs = receipt.get("logs") if isinstance(receipt, dict) else None
            if not isinstance(logs, list):
                raise RuntimeError("INVALID_ONCHAIN_FILL_RECEIPT")
            matches: list[tuple[int, int, int, int]] = []
            for log in logs:
                if not isinstance(log, dict):
                    continue
                topics = log.get("topics")
                if (
                    not isinstance(topics, list)
                    or len(topics) != 4
                    or str(topics[0]).lower() != event_topic
                    or str(topics[1]).lower() != normalized_order
                ):
                    continue
                raw_data = str(log.get("data", ""))
                if not raw_data.startswith("0x"):
                    raise RuntimeError("INVALID_ONCHAIN_FILL_LOG_DATA")
                try:
                    side, token_id, maker_amount, taker_amount, fee, _builder, _metadata = decode(
                        [
                            "uint8",
                            "uint256",
                            "uint256",
                            "uint256",
                            "uint256",
                            "bytes32",
                            "bytes32",
                        ],
                        bytes.fromhex(raw_data[2:]),
                    )
                except Exception as exc:
                    raise RuntimeError("INVALID_ONCHAIN_FILL_LOG_ENCODING") from exc
                if int(side) != expected_side or str(token_id) != str(source.token_id):
                    raise RuntimeError("ONCHAIN_FILL_SOURCE_IDENTITY_MISMATCH")
                matches.append((int(maker_amount), int(taker_amount), int(fee), int(side)))
            if len(matches) != 1:
                raise RuntimeError("ONCHAIN_FILL_LOG_NOT_UNIQUE")
            maker_amount, taker_amount, fee, matched_side = matches[0]
            if matched_side == 0:  # BUY: our maker amount is pUSD; taker amount is outcome shares.
                notional_raw, quantity_raw = maker_amount, taker_amount
            else:  # SELL: our maker amount is outcome shares; taker amount is pUSD.
                quantity_raw, notional_raw = maker_amount, taker_amount
            if quantity_raw <= 0 or notional_raw <= 0 or fee < 0:
                raise RuntimeError("INVALID_ONCHAIN_FILL_AMOUNTS")
            total_quantity_raw += quantity_raw
            total_notional_raw += notional_raw
            total_fee_raw += fee
            receipt_evidence.append(
                {
                    "transaction_hash": str(transaction_hash),
                    "quantity_raw": str(quantity_raw),
                    "notional_raw": str(notional_raw),
                    "fee_raw": str(fee),
                }
            )
        quantity = Decimal(total_quantity_raw) / TOKEN_SCALE
        notional = Decimal(total_notional_raw) / TOKEN_SCALE
        fee = Decimal(total_fee_raw) / TOKEN_SCALE
        return {
            "quantity": quantity,
            "notional_usd": notional,
            "fee_usd": fee,
            "vwap_price": notional / quantity,
            "receipt_evidence": receipt_evidence,
        }

    def authoritative_order_hash_execution(
        self, *, source: SourceAction, order_id: str,
    ) -> dict[str, Any] | None:
        """Read a vanished CLOB order's exact Polygon fill logs by order hash."""
        if self.receipt_reader is None:
            raise RuntimeError("ONCHAIN_FILL_RECEIPT_READER_UNAVAILABLE")
        reader = getattr(self.receipt_reader, "order_fill_logs_range", None)
        finalized_block = getattr(self.receipt_reader, "finalized_block_number", None)
        if not callable(reader) or not callable(finalized_block):
            raise RuntimeError("ONCHAIN_ORDER_HASH_READER_UNAVAILABLE")
        scan_to_block = int(finalized_block())
        if scan_to_block < int(source.block_number):
            return None
        logs = reader(
            from_block=int(source.block_number), to_block=scan_to_block,
            order_id=str(order_id),
        )
        if not isinstance(logs, list):
            raise RuntimeError("INVALID_ONCHAIN_ORDER_HASH_LOGS")
        if not logs:
            return {
                "authoritative_no_fill": True,
                "scan_from_block": int(source.block_number),
                "scan_to_block": scan_to_block,
                "finality": "polygon_finalized_block",
            }
        try:
            from eth_abi import decode
        except ImportError as exc:
            raise RuntimeError("ONCHAIN_FILL_RECEIPT_DECODER_UNAVAILABLE") from exc
        expected_side = 0 if source.side == "BUY" else 1
        total_quantity_raw = total_notional_raw = total_fee_raw = 0
        evidence: list[dict[str, str]] = []
        for log in logs:
            topics = log.get("topics") if isinstance(log, Mapping) else None
            raw_data = str(log.get("data", "")) if isinstance(log, Mapping) else ""
            transaction_hash = str(log.get("transactionHash", "")).lower() if isinstance(log, Mapping) else ""
            if (not isinstance(topics, list) or len(topics) != 4
                    or str(topics[0]).lower() != ORDER_FILLED_TOPIC
                    or str(topics[1]).lower() != str(order_id).lower()
                    or not raw_data.startswith("0x")
                    or re.fullmatch(r"0x[0-9a-f]{64}", transaction_hash) is None):
                raise RuntimeError("ONCHAIN_ORDER_HASH_LOG_IDENTITY_MISMATCH")
            try:
                side, token_id, maker_amount, taker_amount, fee, _builder, _metadata = decode(
                    ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"],
                    bytes.fromhex(raw_data[2:]),
                )
            except Exception as exc:
                raise RuntimeError("INVALID_ONCHAIN_FILL_LOG_ENCODING") from exc
            if int(side) != expected_side or str(token_id) != str(source.token_id):
                raise RuntimeError("ONCHAIN_FILL_SOURCE_IDENTITY_MISMATCH")
            notional_raw, quantity_raw = ((maker_amount, taker_amount) if expected_side == 0 else (taker_amount, maker_amount))
            if quantity_raw <= 0 or notional_raw <= 0 or fee < 0:
                raise RuntimeError("INVALID_ONCHAIN_FILL_AMOUNTS")
            total_quantity_raw += quantity_raw
            total_notional_raw += notional_raw
            total_fee_raw += fee
            evidence.append({"transaction_hash": transaction_hash, "quantity_raw": str(quantity_raw), "notional_raw": str(notional_raw), "fee_raw": str(fee)})
        quantity = Decimal(total_quantity_raw) / TOKEN_SCALE
        notional = Decimal(total_notional_raw) / TOKEN_SCALE
        fee = Decimal(total_fee_raw) / TOKEN_SCALE
        return {"quantity": quantity, "notional_usd": notional, "fee_usd": fee, "vwap_price": notional / quantity, "receipt_evidence": evidence}

    def get_associated_trades(
        self,
        *,
        order_id: str,
        trade_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Read each official associated CLOB trade exactly once.

        An order's limit/worst price is not an accounting price.  These trades
        bind the order to transaction hashes; the matching on-chain
        ``OrderFilled`` logs remain authority for quantity, notional, and fee.
        """

        try:
            from py_clob_client_v2.clob_types import TradeParams
        except ImportError as exc:
            raise RuntimeError("CLOB_TRADE_TYPES_UNAVAILABLE") from exc
        normalized_order = str(order_id)
        normalized_ids = [str(trade_id) for trade_id in trade_ids]
        if not normalized_ids or len(set(normalized_ids)) != len(normalized_ids):
            raise RuntimeError("INVALID_ASSOCIATED_TRADE_IDS")
        receipts: list[dict[str, Any]] = []
        for trade_id in normalized_ids:
            page = self.client.get_trades(
                TradeParams(id=trade_id),
                only_first_page=True,
            )
            matched = [
                item
                for item in page
                if isinstance(item, dict) and str(item.get("id", "")) == trade_id
            ] if isinstance(page, list) else []
            if len(matched) != 1:
                raise RuntimeError("OFFICIAL_ASSOCIATED_TRADE_NOT_UNIQUE")
            trade = matched[0]
            if str(trade.get("taker_order_id", "")) != normalized_order:
                raise RuntimeError("OFFICIAL_ASSOCIATED_TRADE_ORDER_MISMATCH")
            # Persist only the fields needed to reconstruct economic outcome;
            # the authenticated response can include unrelated account fields.
            receipts.append(
                {
                    key: trade.get(key)
                    for key in (
                        "id",
                        "asset_id",
                        "side",
                        "trader_side",
                        "size",
                        "price",
                        "status",
                        "match_time",
                        "fee_rate_bps",
                        "taker_order_id",
                        "transaction_hash",
                    )
                }
            )
        return receipts

    def collateral_balance_usd(self) -> Decimal:
        try:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

            result = self.client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
        except Exception as exc:
            raise RuntimeError(f"COLLATERAL_BALANCE_UNAVAILABLE: {exc}") from exc
        if not isinstance(result, dict) or "balance" not in result:
            raise RuntimeError("INVALID_COLLATERAL_BALANCE_RESPONSE")
        allowances = result.get("allowances")
        if not isinstance(allowances, dict) or not allowances:
            raise RuntimeError("INVALID_COLLATERAL_ALLOWANCES_RESPONSE")
        try:
            allowance_values = [Decimal(str(value)) for value in allowances.values()]
        except (InvalidOperation, ValueError) as exc:
            raise RuntimeError("INVALID_COLLATERAL_ALLOWANCE") from exc
        if any(value <= ZERO for value in allowance_values):
            # A real copier must not discover a missing approval by attempting
            # its first order.  The returned allowance set is authoritative
            # for this authenticated account at this instant.
            raise RuntimeError("COLLATERAL_ALLOWANCE_NOT_GRANTED")
        try:
            # The CLOB balance endpoint returns contract fixed math, matching
            # the six-decimal scale documented for CLOB order quantities.
            return Decimal(str(result["balance"])) / TOKEN_SCALE
        except (InvalidOperation, ValueError) as exc:
            raise RuntimeError("INVALID_COLLATERAL_BALANCE") from exc


class LiveSourceFollower:
    """Forward-only source-chain watermarking for the cash copier.

    On a fresh process start, old blocks are intentionally not repriced with a
    later CLOB book.  The follower resumes only from the current chain head;
    a subsequent steady-state cycle is responsible for new source actions.
    """

    def __init__(
        self,
        *,
        store: LiveStore,
        rpc: Any,
        source_wallet: str,
        clock_ms: Any,
        action_scope: Any | None = None,
        public_get_json: Callable[[str], Any] | None = None,
        wallet_lock_path: Path | None = None,
        coordinator: SharedWalletCoordinator | None = None,
        profile_key: str | None = None,
    ) -> None:
        normalized = str(source_wallet or "").lower()
        if not normalized.startswith("0x") or len(normalized) != 42:
            raise ValueError("source_wallet must be a 20-byte hex address")
        int(normalized[2:], 16)
        self.store = store
        self.rpc = rpc
        self.source_wallet = normalized
        self.clock_ms = clock_ms
        self.action_scope = action_scope
        self.public_get_json = public_get_json
        self.wallet_lock_path = (
            None if wallet_lock_path is None else Path(wallet_lock_path)
        )
        self.coordinator = coordinator
        self.profile_key = None if profile_key is None else str(profile_key)
        if self.coordinator is not None and not self.profile_key:
            raise ValueError("profile_key is required with a wallet coordinator")

    @staticmethod
    def _header_number(header: dict[str, Any]) -> int:
        raw = header.get("number")
        if isinstance(raw, int):
            return raw
        return int(str(raw), 16)

    def establish_forward_watermark(
        self,
        *,
        advance_after_recorded_internal_repair_gap: bool = False,
    ) -> dict[str, int | None]:
        previous_raw = self.store.runtime_value("last_processed_block")
        previous = None if previous_raw is None else int(previous_raw)
        # The public endpoint is newest-first and has no chain-height cursor.
        # Establish its complete non-replay baseline before scanning the chain
        # restart gap.  Otherwise a short stopped interval makes the first
        # public-history page appear to be current source actions.
        public_wallet_baseline_rows: int | None = None
        if self.public_get_json is not None:
            public_wallet_baseline_rows = (
                self._establish_public_wallet_forward_watermark()
            )
        head = int(self.rpc.latest_block_number())
        header = self.rpc.get_block(head)
        if not isinstance(header, dict) or self._header_number(header) != head:
            raise RuntimeError("CURRENT_HEAD_HEADER_MISMATCH")
        if previous is not None and head < previous:
            raise ConnectionError("CURRENT_HEAD_BEHIND_PERSISTED_WATERMARK")
        skipped = 0 if previous is None else max(0, head - previous)
        gap_action_count = 0
        if previous is not None and head > previous:
            detected_at_ms = int(self.clock_ms())
            try:
                missed_actions = self._new_source_actions(
                    from_block=previous + 1,
                    to_block=head,
                    include_verified_public_wallet=False,
                )
            except Exception as exc:
                self.store.append_runtime_error(
                    occurred_at_ms=detected_at_ms,
                    category="EXTERNAL_CHAIN_GAP_SCAN",
                    message=f"{type(exc).__name__}: {exc}",
                    details={"from_block": previous + 1, "to_block": head},
                )
                raise
            recovery_reason = (
                "PRE_REPAIR_INTERNAL_UNPRICED_GAP_NO_ACTION_TIME_CLOB"
            )
            gap_receipt = self.store.record_unpriced_runtime_gap(
                previous_processed_block=previous,
                resume_head=head,
                actions=missed_actions,
                detected_at_ms=detected_at_ms,
                reason=(
                    recovery_reason
                    if advance_after_recorded_internal_repair_gap
                    else "LOSSLESS_HANDOFF_REQUIRED"
                ),
                terminal_status=(
                    "ERROR_INTERNAL"
                    if advance_after_recorded_internal_repair_gap
                    else "PENDING_LOSSLESS_HANDOFF"
                ),
                pricing_status=(
                    "PRE_REPAIR_INTERNAL_UNPRICED_NO_ACTION_TIME_CLOB"
                    if advance_after_recorded_internal_repair_gap
                    else "LOSSLESS_HANDOFF_REQUIRED"
                ),
            )
            gap_action_count = int(gap_receipt["source_action_count"])
            if gap_action_count:
                details = {
                    "previous_processed_block": previous,
                    "resume_head": head,
                    "source_action_count": gap_action_count,
                    "cursor_retained": not advance_after_recorded_internal_repair_gap,
                    "new_order_submitted": False,
                    "current_book_read": False,
                    "cash_or_position_rewritten": False,
                }
                if advance_after_recorded_internal_repair_gap:
                    self.store.append_runtime_error(
                        occurred_at_ms=detected_at_ms,
                        category="INTERNAL_PRE_REPAIR_FORWARD_RECOVERY",
                        message=recovery_reason,
                        details={
                            **details,
                            "forward_watermark_advanced": True,
                        },
                    )
                    self.store.set_runtime(
                        "last_pre_repair_internal_gap_at_ms", detected_at_ms
                    )
                    self.store.set_runtime(
                        "last_pre_repair_internal_gap_action_count",
                        gap_action_count,
                    )
                else:
                    self.store.append_runtime_error(
                        occurred_at_ms=detected_at_ms,
                        category="INTERNAL_LOSSLESS_HANDOFF",
                        message="LOSSLESS_HANDOFF_REQUIRED",
                        details=details,
                    )
                    self.store.set_runtime(
                        "last_lossless_handoff_failure_at_ms", detected_at_ms
                    )
                    self.store.set_runtime(
                        "last_lossless_handoff_failure_action_count", gap_action_count
                    )
                    self.store.set_runtime("status", "LOSSLESS_HANDOFF_REQUIRED")
                    self.store.set_runtime(
                        "last_cycle_outcome", "LOSSLESS_HANDOFF_REQUIRED"
                    )
                    raise LiveConfigurationError("LOSSLESS_HANDOFF_REQUIRED")
        self.store.set_runtime("last_processed_block", head)
        self.store.set_runtime("current_head", head)
        self.store.set_runtime("start_head", head)
        self.store.set_runtime("start_head_hash", str(header.get("hash", "")).lower())
        self.store.set_runtime("restart_skipped_block_count", skipped)
        self.store.set_runtime("restart_unpriced_source_action_count", gap_action_count)
        self.store.set_runtime("source_wallet", self.source_wallet)
        self.store.set_runtime("status", "FORWARD_ONLY_RUNNING")
        self.store.set_runtime("heartbeat_at_ms", self.clock_ms())
        result: dict[str, int | None] = {
            "previous_head": previous,
            "start_head": head,
            "skipped_block_count": skipped,
        }
        if skipped:
            result["unpriced_source_action_count"] = gap_action_count
        if advance_after_recorded_internal_repair_gap:
            result["pre_repair_internal_gap_action_count"] = gap_action_count
        if public_wallet_baseline_rows is not None:
            result["public_wallet_baseline_row_count"] = public_wallet_baseline_rows
        return result

    @staticmethod
    def _log_int(value: Any) -> int:
        if isinstance(value, int):
            return value
        return int(str(value), 16)

    @staticmethod
    def _aggregate(actions: list[SourceAction]) -> list[SourceAction]:
        """Merge multiple fill logs sharing the required four-field action key."""

        grouped: dict[tuple[str, str, str, str], SourceAction] = {}
        for action in actions:
            previous = grouped.get(action.identity)
            if previous is None:
                grouped[action.identity] = action
                continue
            grouped[action.identity] = SourceAction(
                transaction_hash=previous.transaction_hash,
                token_id=previous.token_id,
                side=previous.side,
                order_hash=previous.order_hash,
                source_quantity=previous.source_quantity + action.source_quantity,
                source_notional=previous.source_notional + action.source_notional,
                source_timestamp=max(previous.source_timestamp, action.source_timestamp),
                block_number=max(previous.block_number, action.block_number),
                block_hash=action.block_hash,
                source_role=previous.source_role,
                discovered_at_ms=max(previous.discovered_at_ms, action.discovered_at_ms),
                log_index=min(previous.log_index, action.log_index),
            )
        return sorted(
            grouped.values(),
            key=lambda action: (
                action.block_number,
                action.log_index,
                action.source_timestamp,
                action.transaction_hash,
                action.token_id,
                action.side,
                action.order_hash,
            ),
        )

    def _read_verified_public_wallet_trade_page(
        self, *, offset: int
    ) -> list[dict[str, Any]]:
        if self.public_get_json is None:
            return []
        url = _public_wallet_trades_url(
            wallet=self.source_wallet,
            offset=int(offset),
        )
        payload = self.public_get_json(url)
        if not isinstance(payload, list):
            raise ConnectionError("INVALID_PUBLIC_WALLET_TRADES_PAYLOAD")
        if len(payload) > PUBLIC_WALLET_TRADE_PAGE_SIZE:
            raise ConnectionError("PUBLIC_WALLET_TRADE_PAGE_SIZE_EXCEEDED")
        parsed: list[dict[str, Any]] = []
        for raw in payload:
            if not isinstance(raw, Mapping):
                raise ConnectionError("INVALID_PUBLIC_WALLET_TRADE_ROW")
            parsed.append(
                _decode_verified_public_wallet_trade(
                    raw,
                    source_wallet=self.source_wallet,
                )
            )
        return parsed

    def _new_verified_public_wallet_rows(self) -> list[dict[str, Any]]:
        """Fetch through a persisted public-row boundary without truncation."""

        if self.public_get_json is None:
            return []
        unseen: list[dict[str, Any]] = []
        for page_number in range(PUBLIC_WALLET_TRADE_MAX_PAGES):
            page = self._read_verified_public_wallet_trade_page(
                offset=page_number * PUBLIC_WALLET_TRADE_PAGE_SIZE
            )
            known_ids = self.store.public_source_observation_ids(
                row["row_id"] for row in page
            )
            unseen.extend(
                row for row in page if str(row["row_id"]) not in known_ids
            )
            # The API is newest-first.  A full boundary page is still scanned
            # before stopping, so actions sharing a timestamp are not dropped.
            if len(page) < PUBLIC_WALLET_TRADE_PAGE_SIZE or known_ids:
                return unseen
        raise ConnectionError("PUBLIC_WALLET_ACTION_CURSOR_GAP")

    def _establish_public_wallet_forward_watermark(self) -> int:
        """Persist current public rows as non-replayable forward baseline."""

        # Do not write a partial watermark.  If an external read fails or the
        # bounded cursor cannot find the first short page, a later restart
        # must not treat only the newest persisted page as the whole baseline.
        baseline_rows: list[dict[str, Any]] = []
        for page_number in range(PUBLIC_WALLET_TRADE_MAX_PAGES):
            page = self._read_verified_public_wallet_trade_page(
                offset=page_number * PUBLIC_WALLET_TRADE_PAGE_SIZE
            )
            baseline_rows.extend(page)
            if len(page) < PUBLIC_WALLET_TRADE_PAGE_SIZE:
                break
        else:
            raise ConnectionError("PUBLIC_WALLET_FORWARD_WATERMARK_CURSOR_GAP")
        observed_at_ms = int(self.clock_ms())
        inserted = self.store.record_public_source_observations(
            rows=baseline_rows,
            state="FORWARD_WATERMARK_BASELINE_NO_REPLAY",
            observed_at_ms=observed_at_ms,
        )
        self.store.set_runtime("public_wallet_forward_watermark_at_ms", observed_at_ms)
        self.store.set_runtime(
            "public_wallet_forward_watermark_row_count", len(baseline_rows)
        )
        self.store.set_runtime("public_wallet_forward_watermark_new_row_count", inserted)
        return len(baseline_rows)

    def _record_verified_public_source_action(self, action: SourceAction) -> None:
        """Durably observe an executable public-wallet action before its row.

        The source receipt is written first so a crash cannot mark a public row
        as consumed while losing the action that must be followed.  Repeated
        discovery is safe because both artifacts are immutable/idempotent.
        """

        if self.store.record_action_receipt(action):
            self.store.append_transition(source=action, status="OBSERVED")

    def _chain_source_actions_and_context(
        self,
        *,
        from_block: int,
        to_block: int,
        include_verified_public_wallet: bool = True,
    ) -> tuple[list[SourceAction], list[dict[str, Any]]]:
        """Read source maker actions plus non-promoted taker chain context."""

        raw_logs: dict[tuple[str, int], dict[str, Any]] = {}
        roles = (
            ("maker", "taker")
            if self.public_get_json is not None and include_verified_public_wallet
            else ("maker",)
        )
        for role in roles:
            for row in self.rpc.source_logs_range(
                from_block,
                to_block,
                self.source_wallet,
                role,
            ):
                transaction_hash = str(row.get("transactionHash", "")).lower()
                log_index = self._log_int(row.get("logIndex", "0x0"))
                if transaction_hash:
                    raw_logs[(transaction_hash, log_index)] = row
        headers: dict[int, dict[str, Any]] = {}
        maker_actions: list[SourceAction] = []
        taker_context: list[dict[str, Any]] = []
        for _, row in sorted(
            raw_logs.items(),
            key=lambda item: (
                self._log_int(item[1].get("blockNumber", "0x0")),
                item[0][1],
            ),
        ):
            block_number = self._log_int(row.get("blockNumber", "0x0"))
            header = headers.get(block_number)
            if header is None:
                header = self.rpc.get_block(block_number)
                headers[block_number] = header
            decoded = decode_order_filled(row, self.source_wallet)
            if decoded is None:
                continue
            decoded.update(
                {
                    "block_number": block_number,
                    "block_hash": str(header.get("hash", "")).lower(),
                    "block_timestamp": self._log_int(header.get("timestamp", "0x0")),
                    "chain_seen_at_ms": int(self.clock_ms()),
                    "log_index": self._log_int(row.get("logIndex", "0x0")),
                }
            )
            if str(decoded.get("source_role", "")).lower() == SOURCE_ROLE_CHAIN_MAKER:
                maker_actions.append(decode_followable_source_action(decoded))
            elif str(decoded.get("source_role", "")).lower() == "taker":
                taker_context.append(decoded)
        return self._aggregate(maker_actions), taker_context

    def _new_source_actions(
        self,
        *,
        from_block: int,
        to_block: int,
        include_verified_public_wallet: bool = True,
    ) -> list[SourceAction]:
        if from_block > to_block:
            return []
        maker_actions, taker_context = self._chain_source_actions_and_context(
            from_block=from_block,
            to_block=to_block,
            include_verified_public_wallet=include_verified_public_wallet,
        )
        if self.public_get_json is None or not include_verified_public_wallet:
            return maker_actions

        public_rows = self._new_verified_public_wallet_rows()
        maker_by_triplet: dict[tuple[str, str, str], list[SourceAction]] = {}
        for action in maker_actions:
            maker_by_triplet.setdefault(
                (action.transaction_hash.lower(), action.token_id, action.side), []
            ).append(action)
        taker_by_transaction_token: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for context in taker_context:
            taker_by_transaction_token.setdefault(
                (
                    str(context["transaction_hash"]).lower(),
                    str(context["token_id"]),
                ),
                [],
            ).append(context)
        public_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in public_rows:
            public_groups.setdefault(
                (
                    str(row["transaction_hash"]).lower(),
                    str(row["token_id"]),
                    str(row["side"]).upper(),
                ),
                [],
            ).append(row)

        public_actions: list[SourceAction] = []
        observed_at_ms = int(self.clock_ms())
        for (transaction_hash, token_id, side), rows in sorted(public_groups.items()):
            source_quantity = sum(
                (Decimal(str(row["source_quantity"])) for row in rows), ZERO
            )
            source_notional = sum(
                (
                    Decimal(str(row["source_quantity"]))
                    * Decimal(str(row["source_price"]))
                    for row in rows
                ),
                ZERO,
            )
            maker_matches = maker_by_triplet.get(
                (transaction_hash, token_id, side), []
            )
            contexts = taker_by_transaction_token.get(
                (transaction_hash, token_id), []
            )
            if maker_matches:
                maker_quantity = sum(
                    (item.source_quantity for item in maker_matches), ZERO
                )
                maker_notional = sum(
                    (item.source_notional for item in maker_matches), ZERO
                )
                if (
                    maker_quantity != source_quantity
                    or not _public_notional_matches_chain(
                        public_notional=source_notional,
                        chain_notional=maker_notional,
                    )
                ):
                    raise LiveConfigurationError(
                        "PUBLIC_CHAIN_MAKER_ACTION_RECONCILIATION_MISMATCH"
                    )
                # A V2 transaction can include counterparties' paired logs
                # which name the followed wallet as taker.  When the public
                # row exactly reconciles to the source maker action, those
                # contexts are not independent source actions and must not
                # either stall the cursor or cause a second copy.
                self.store.record_public_source_observations(
                    rows=rows,
                    state=(
                        "RECONCILED_CHAIN_MAKER_WITH_COUNTERPARTY_CONTEXT"
                        if contexts
                        else "RECONCILED_CHAIN_MAKER_ACTION"
                    ),
                    observed_at_ms=observed_at_ms,
                    source_action_id=(
                        maker_matches[0].action_id if len(maker_matches) == 1 else None
                    ),
                )
                continue
            if not contexts:
                if self.store.has_followable_source_action_triplet(
                    transaction_hash=transaction_hash,
                    token_id=token_id,
                    side=side,
                ):
                    self.store.record_public_source_observations(
                        rows=rows,
                        state="RECONCILED_PRIOR_FOLLOWABLE_ACTION",
                        observed_at_ms=observed_at_ms,
                    )
                    continue
                receipt = self.rpc.get_receipt(transaction_hash)
                block_number = self._log_int(receipt.get("blockNumber", "0x0"))
                if block_number >= from_block:
                    raise ConnectionError("PUBLIC_WALLET_CHAIN_CONTEXT_MISSING")
                header = self.rpc.get_block(block_number)
                action = SourceAction(
                    transaction_hash=transaction_hash,
                    token_id=token_id,
                    side=side,
                    order_hash=_public_wallet_action_order_hash(
                        transaction_hash=transaction_hash,
                        token_id=token_id,
                        side=side,
                    ),
                    source_quantity=source_quantity,
                    source_notional=source_notional,
                    source_timestamp=self._log_int(header.get("timestamp", "0x0")),
                    block_number=block_number,
                    block_hash=str(header.get("hash", "")).lower(),
                    source_role=SOURCE_ROLE_VERIFIED_PUBLIC_WALLET,
                    discovered_at_ms=observed_at_ms,
                )
                self._record_verified_public_source_action(action)
                self.store.constrain_action_to_no_action_time_book(
                    source=action,
                    reason=(
                        "PUBLIC_WALLET_ACTION_DISCOVERED_AFTER_CHAIN_CURSOR_"
                        "NO_ACTION_TIME_BOOK"
                    ),
                    created_at_ms=observed_at_ms,
                    details={"public_source_row_ids": [row["row_id"] for row in rows]},
                )
                self.store.record_public_source_observations(
                    rows=rows,
                    state="LATE_NO_ACTION_TIME_BOOK",
                    observed_at_ms=observed_at_ms,
                    source_action_id=action.action_id,
                )
                public_actions.append(action)
                continue
            first_context = min(
                contexts,
                key=lambda item: int(item.get("log_index", 0)),
            )
            action = SourceAction(
                transaction_hash=transaction_hash,
                token_id=token_id,
                side=side,
                order_hash=_public_wallet_action_order_hash(
                    transaction_hash=transaction_hash,
                    token_id=token_id,
                    side=side,
                ),
                source_quantity=source_quantity,
                source_notional=source_notional,
                source_timestamp=int(first_context["block_timestamp"]),
                block_number=int(first_context["block_number"]),
                block_hash=str(first_context["block_hash"]).lower(),
                source_role=SOURCE_ROLE_VERIFIED_PUBLIC_WALLET,
                discovered_at_ms=observed_at_ms,
                log_index=int(first_context["log_index"]),
            )
            self._record_verified_public_source_action(action)
            self.store.record_public_source_observations(
                rows=rows,
                state="VERIFIED_PUBLIC_WALLET_ACTION",
                observed_at_ms=observed_at_ms,
                source_action_id=action.action_id,
            )
            public_actions.append(action)
        return self._aggregate([*maker_actions, *public_actions])

    def _record_late_metadata_unfillable(
        self,
        *,
        action: SourceAction,
        metadata: Mapping[str, Any],
        reason: str,
    ) -> dict[str, Any]:
        """Conserve one eligible action whose action-time book is gone."""

        proportional_quantity = (
            action.source_quantity
            * self.store.fixed_share_scale_for_source_block(action.block_number)
        )
        # There is no action-time executable book on this path.  A later
        # metadata response can establish that a market minimum exists, but it
        # cannot authorize replacing the frozen fixed-share target with that
        # minimum.  Preserve the exact proportional action as unfillable
        # evidence; it must never become a larger synthetic order.
        target_quantity = proportional_quantity
        self.store.ensure_action_target(
            source=action,
            proportional_quantity=proportional_quantity,
            target_quantity=target_quantity,
            state="EXTERNAL_UNFILLABLE",
            reason=reason,
            updated_at_ms=int(self.clock_ms()),
        )
        self.store.append_transition(
            source=action,
            status="EXTERNAL_UNFILLABLE",
            reason=reason,
            details={"scope": dict(metadata)},
        )
        return {
            "terminal_status": "EXTERNAL_UNFILLABLE",
            "reason": reason,
        }

    def _process_source_action(
        self,
        *,
        action: SourceAction,
        execution: Any,
        live_enabled: bool,
    ) -> dict[str, Any]:
        if not _is_followable_source_role(action.source_role):
            raise LiveConfigurationError("COUNTERPARTY_ORDER_LOG_NOT_SOURCE_ACTION")
        existing = self.store.latest_transition(action)
        if existing is not None and existing["terminal_status"] in {
            "SKIPPED",
            "SUBMITTED_UNRECONCILED",
            "UNKNOWN_SUBMISSION",
            "FILLED",
            "PARTIAL",
            "ERROR",
            "ERROR_INTERNAL",
            "EXTERNAL_UNFILLABLE",
            "SUPERSEDED_UNFILLED",
        }:
            return existing
        scope_metadata: Mapping[str, Any] = {}
        if self.action_scope is not None:
            inserted = self.store.record_action_receipt(action)
            if inserted:
                self.store.append_transition(
                    source=action,
                    status="OBSERVED",
                )
            frozen_scope = self.store.latest_transition_with_status(
                action, "SCOPE_ELIGIBLE"
            )
            if frozen_scope is None:
                frozen_metadata = self.store.frozen_action_metadata(action.action_id)
                if frozen_metadata is None:
                    try:
                        action_resolver = getattr(
                            self.action_scope, "resolve_action", None
                        )
                        if callable(action_resolver):
                            decision = action_resolver(action)
                        else:
                            decision = self.action_scope.resolve(action.token_id)
                    except Exception as exc:
                        if _is_retryable_external_error(exc):
                            latest = self.store.latest_transition(action)
                            if (
                                latest is None
                                or latest["terminal_status"] != "PENDING_METADATA"
                            ):
                                self.store.append_transition(
                                    source=action,
                                    status="PENDING_METADATA",
                                    reason=f"{type(exc).__name__}: {exc}",
                                )
                            raise
                        self.store.append_transition(
                            source=action,
                            status="ERROR_INTERNAL",
                            reason=(
                                f"ACTION_SCOPE_ERROR: {type(exc).__name__}: {exc}"
                            ),
                        )
                        raise
                    metadata = dict(decision.metadata)
                    profile_follow = bool(decision.follow)
                    profile_reason = str(decision.reason)
                    self.store.freeze_action_metadata(
                        source=action,
                        metadata=metadata,
                        profile_follow=profile_follow,
                        profile_reason=profile_reason,
                        frozen_at_ms=int(self.clock_ms()),
                    )
                else:
                    metadata = dict(frozen_metadata["metadata"])
                    profile_follow = bool(frozen_metadata["profile_follow"])
                    profile_reason = str(frozen_metadata["profile_reason"])
                scope_metadata = metadata
                if self.profile_key == LIVE_PROFILE_WALLET_44B0_NETFLIX:
                    try:
                        self.store.record_source_topic_alert(
                            source=action,
                            metadata=metadata,
                            processing_state="SCOPE_ELIGIBLE",
                            created_at_ms=int(self.clock_ms()),
                        )
                    except Exception as exc:
                        # Topic notification is observability only. It must
                        # never delay an eligible Netflix BUY/SELL. A
                        # non-Netflix action remains outside the authorized
                        # execution scope even if alert persistence fails.
                        self.store.append_runtime_error(
                            occurred_at_ms=int(self.clock_ms()),
                            category="INTERNAL_TOPIC_ALERT_PERSISTENCE",
                            message=f"{type(exc).__name__}: {exc}",
                            details={"action_id": action.action_id},
                        )
                if not profile_follow:
                    self.store.append_transition(
                        source=action,
                        status="SKIPPED",
                        reason=profile_reason,
                        details={"scope": metadata},
                    )
                    return {
                        "terminal_status": "SKIPPED",
                        "reason": profile_reason,
                    }
                late_unfillable_reason: str | None = None
                if (
                    metadata.get("execution_recovery_state")
                    == "EXTERNAL_UNFILLABLE_METADATA_LATE"
                ):
                    late_unfillable_reason = (
                        "METADATA_RECOVERED_AFTER_MARKET_CLOSED_"
                        "NO_ACTION_TIME_BOOK"
                    )
                if frozen_metadata is not None:
                    official_start_raw = metadata.get(
                        "official_game_start_timestamp"
                    )
                    if official_start_raw is not None:
                        try:
                            official_start = int(official_start_raw)
                        except (TypeError, ValueError) as exc:
                            raise LiveConfigurationError(
                                "INVALID_FROZEN_PROFILE_DEADLINE"
                            ) from exc
                        recovered_at = int(self.clock_ms()) // 1000
                        if (
                            action.source_timestamp < official_start
                            and recovered_at >= official_start
                        ):
                            late_unfillable_reason = (
                                "FROZEN_METADATA_RECOVERED_AFTER_PROFILE_"
                                "DEADLINE_NO_ACTION_TIME_BOOK"
                            )
                if late_unfillable_reason is not None:
                    self.store.append_transition(
                        source=action,
                        status="SCOPE_ELIGIBLE",
                        reason=profile_reason,
                        details={"scope": metadata},
                    )
                    return self._record_late_metadata_unfillable(
                        action=action,
                        metadata=metadata,
                        reason=late_unfillable_reason,
                    )
                if self.coordinator is not None:
                    primary_token_id = str(
                        metadata.get("primary_token_id") or ""
                    ).strip()
                    secondary_token_id = str(
                        metadata.get("secondary_token_id") or ""
                    ).strip()
                    if primary_token_id and secondary_token_id:
                        self.store.bind_condition_for_token(
                            token_id=action.token_id,
                            condition_id=str(metadata.get("condition_id") or ""),
                            primary_token_id=primary_token_id,
                            secondary_token_id=secondary_token_id,
                            observed_at_ms=int(self.clock_ms()),
                        )
                self.store.append_transition(
                    source=action,
                    status="SCOPE_ELIGIBLE",
                    reason=profile_reason,
                    details={"scope": metadata},
                )
        execution_constraint = self.store.action_execution_constraint(action)
        if execution_constraint is not None:
            return self._record_late_metadata_unfillable(
                action=action,
                metadata=scope_metadata,
                reason=str(execution_constraint["reason"]),
            )
        return execute_source_action(
            store=self.store,
            source=action,
            execution=execution,
            live_enabled=live_enabled,
            wallet_lock_path=self.wallet_lock_path,
            coordinator=self.coordinator,
            profile_key=self.profile_key,
        )

    def _persist_source_action_batch(self, actions: list[SourceAction]) -> None:
        self.store.persist_source_action_batch(actions)

    def _process_observed_action_safely(self, **kwargs: Any) -> dict[str, Any]:
        action = kwargs["action"]
        transition_id_before = self.store.latest_transition_id(action)
        try:
            return self._process_source_action(**kwargs)
        except sqlite3.DatabaseError:
            raise
        except Exception as exc:
            latest = self.store.latest_transition(action)
            transition_id_after = self.store.latest_transition_id(action)
            transition_added = (
                transition_id_after is not None
                and (
                    transition_id_before is None
                    or transition_id_after > transition_id_before
                )
            )
            status = None if latest is None else latest["terminal_status"]
            if isinstance(exc, SharedWalletCoordinatorError):
                if status == "PENDING_INTERNAL_INVARIANT" and transition_added:
                    return latest
                raise
            if status in {"PENDING_EXTERNAL_RETRY", "PENDING_METADATA"}:
                if _is_retryable_external_error(exc):
                    return latest
                raise
            if status == "PENDING_INTERNAL_INVARIANT":
                if isinstance(exc, LiveConfigurationError) and transition_added:
                    return latest
                raise
            if status in {"ERROR", "ERROR_INTERNAL"} and transition_added:
                return latest
            raise

    def run_cycle_to_head(
        self,
        *,
        head: int,
        execution: Any,
        live_enabled: bool,
    ) -> dict[str, Any]:
        last_raw = self.store.runtime_value("last_processed_block")
        if last_raw is None:
            raise RuntimeError("FORWARD_WATERMARK_NOT_ESTABLISHED")
        last = int(last_raw)
        if head < last:
            # The websocket can still contain an old head buffered before the
            # post-subscription watermark was persisted.  It must never be
            # replayed against a later CLOB book.
            self.store.set_runtime("last_stale_ws_head", head)
            self.store.set_runtime("heartbeat_at_ms", self.clock_ms())
            return {
                "source_action_count": 0,
                "source_action_ids": [],
                "last_processed_block": last,
                "current_head": last,
            }
        self.store.set_runtime("current_head", head)
        if head == last:
            self.store.set_runtime("heartbeat_at_ms", self.clock_ms())
            self._complete_planned_operator_resume(processed_to_block=head)
            return {
                "source_action_count": 0,
                "source_action_ids": [],
                "last_processed_block": last,
                "current_head": head,
            }
        actions = self._new_source_actions(from_block=last + 1, to_block=head)
        self._persist_source_action_batch(actions)
        results: list[dict[str, Any]] = []
        for action in actions:
            results.append(
                self._process_observed_action_safely(
                    action=action,
                    execution=execution,
                    live_enabled=live_enabled,
                )
            )
        self.store.set_runtime("last_processed_block", head)
        self.store.set_runtime("current_head", head)
        self.store.set_runtime("last_source_action_count", len(actions))
        self.store.set_runtime("heartbeat_at_ms", self.clock_ms())
        self._complete_planned_operator_resume(processed_to_block=head)
        return {
            "source_action_count": len(actions),
            "source_action_ids": [action.action_id for action in actions],
            "action_results": results,
            "last_processed_block": head,
            "current_head": head,
        }

    def _complete_planned_operator_resume(self, *, processed_to_block: int) -> None:
        """Mark a controlled restart resume complete only after a good cycle."""

        raw = self.store.runtime_value("operator_planned_resume_from_block")
        if raw is None or not str(raw).strip():
            return
        try:
            resume_from = int(raw)
        except ValueError as exc:
            raise LiveConfigurationError("INVALID_OPERATOR_PLANNED_RESUME_CURSOR") from exc
        if int(processed_to_block) < resume_from:
            raise LiveConfigurationError("OPERATOR_PLANNED_RESUME_CURSOR_REGRESSED")
        completed_at_ms = self.clock_ms()
        values = {
            "operator_planned_resume_from_block": "",
            "operator_planned_resume_state": "COMPLETED",
            "operator_planned_resume_completed_at_ms": completed_at_ms,
            "operator_planned_resume_processed_to_block": int(processed_to_block),
            "operator_pre_repair_forward_recovery_armed": "false",
            "status": "FORWARD_ONLY_RUNNING",
        }
        with self.store.connect() as connection:
            connection.executemany(
                """
                INSERT INTO runtime_state(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                [(key, str(value)) for key, value in values.items()],
            )

    def run_cycle(
        self,
        *,
        execution: Any,
        live_enabled: bool,
    ) -> dict[str, Any]:
        """Compatibility path for deterministic tests and one-shot reads."""

        return self.run_cycle_to_head(
            head=int(self.rpc.latest_block_number()),
            execution=execution,
            live_enabled=live_enabled,
        )


@contextmanager
def _shared_wallet_submission_lock(path: Path) -> Iterator[None]:
    """Serialize the final cash check and submission across live sleeves."""

    lock_path = Path(path)
    if not lock_path.is_absolute():
        raise LiveConfigurationError("SHARED_WALLET_LOCK_PATH_NOT_ABSOLUTE")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _persist_authenticated_collateral_observation(
    *,
    store: LiveStore,
    observed_collateral_usd: Decimal,
    observed_at_ms: int,
    coordinator: SharedWalletCoordinator | None,
    profile_key: str | None,
) -> AuthenticatedAccountCashSnapshot | None:
    """Persist a physical-cash sample only after all-wallet validation."""

    observed = Decimal(str(observed_collateral_usd))
    if not observed.is_finite() or observed < ZERO:
        raise LiveConfigurationError("INVALID_AUTHENTICATED_COLLATERAL")
    snapshot: AuthenticatedAccountCashSnapshot | None = None
    if coordinator is not None:
        if not str(profile_key or "").strip():
            raise LiveConfigurationError(
                "PROFILE_KEY_REQUIRED_WITH_SHARED_WALLET_COORDINATOR"
            )
        snapshot = coordinator.observe_authenticated_account_cash(
            authenticated_collateral_usd=observed,
            observed_at_ms=int(observed_at_ms),
        )
    store.set_runtime("last_authenticated_collateral_usd", str(observed))
    store.set_runtime("last_authenticated_collateral_at_ms", str(int(observed_at_ms)))
    store.set_runtime(
        "last_authenticated_collateral_transition_id",
        str(store.latest_cash_mutation_transition_id()),
    )
    store.set_runtime(
        "last_authenticated_collateral_mutation_fingerprint",
        store.cash_mutation_fingerprint(),
    )
    if snapshot is not None:
        for key, value in snapshot.as_dict().items():
            store.set_runtime("authenticated_account_cash_" + key, value)
    return snapshot


def _refresh_authenticated_collateral_after_cash_mutation(
    *,
    store: LiveStore,
    execution: Any,
    coordinator: SharedWalletCoordinator | None,
    profile_key: str | None,
    force: bool = False,
) -> AuthenticatedAccountCashSnapshot | None:
    """Refresh after every ledger cash/PnL mutation, retrying failed samples."""

    latest_fingerprint = store.cash_mutation_fingerprint()
    sampled_fingerprint = str(
        store.runtime_value(
            "last_authenticated_collateral_mutation_fingerprint"
        )
        or ""
    )
    if not force:
        if not latest_fingerprint or sampled_fingerprint == latest_fingerprint:
            return None
    observed = Decimal(str(execution.collateral_balance_usd()))
    return _persist_authenticated_collateral_observation(
        store=store,
        observed_collateral_usd=observed,
        observed_at_ms=now_ms(),
        coordinator=coordinator,
        profile_key=profile_key,
    )


def _reconcile_submissions_and_refresh_cash(
    *,
    store: LiveStore,
    execution: Any,
    wallet_lock_path: Path | None,
    coordinator: SharedWalletCoordinator | None,
    profile_key: str | None,
    attempt_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Serialize terminal reconciliation and the resulting wallet sample."""

    def reconcile_locked() -> list[dict[str, Any]]:
        results = reconcile_submitted_actions(
            store=store,
            execution=execution,
            attempt_ids=attempt_ids,
        )
        _refresh_authenticated_collateral_after_cash_mutation(
            store=store,
            execution=execution,
            coordinator=coordinator,
            profile_key=profile_key,
        )
        return results

    if wallet_lock_path is None:
        return reconcile_locked()
    with _shared_wallet_submission_lock(wallet_lock_path):
        return reconcile_locked()


def cancel_due_active_gtd_orders(
    *,
    store: LiveStore,
    execution: Any,
    due_at_ms: int,
) -> list[dict[str, Any]]:
    """Cancel only GTC remainders whose already-recorded active window elapsed.

    This is intentionally separate from source-action submission.  The GTC
    order and its cancellation deadline are durable before this maintenance
    pass runs, so a later block or restart can finish the cancellation without
    holding the source-action submission path open.
    """

    cancel = getattr(execution, "cancel_active_gtd_order", None)
    results: list[dict[str, Any]] = []
    for source, details in store.unreconciled_submissions():
        if str(details.get("execution_order_type") or "").upper() != (
            "GTC_ACTIVE_CANCEL"
        ):
            continue
        order_id = str(details.get("order_id") or "").strip()
        response = details.get("response")
        if not order_id or not isinstance(response, Mapping):
            continue
        if response.get("active_cancel_verified") is True:
            continue
        raw_due_at_ms = response.get("active_cancel_due_at_ms")
        try:
            active_cancel_due_at_ms = int(str(raw_due_at_ms))
        except (TypeError, ValueError):
            raise LiveConfigurationError("ACTIVE_CANCEL_DUE_AT_MS_MISSING")
        if active_cancel_due_at_ms > int(due_at_ms):
            continue
        if not callable(cancel):
            raise LiveConfigurationError("ACTIVE_CANCEL_EXECUTION_UNAVAILABLE")
        cancellation = cancel(order_id)
        updated_response = {
            **dict(response),
            **(
                dict(cancellation)
                if isinstance(cancellation, Mapping)
                else {"active_cancel_response": cancellation}
            ),
            "active_cancel_verified": True,
        }
        store.update_attempt_state(
            attempt_id=str(details.get("attempt_id") or ""),
            state="SUBMITTED_UNRECONCILED",
            response=updated_response,
            updated_at_ms=int(due_at_ms),
        )
        store.append_transition(
            source=source,
            status="ACTIVE_CANCEL_COMPLETED",
            reason="GTC_ACTIVE_CANCEL_WINDOW_ELAPSED",
            created_at_ms=int(due_at_ms),
            details={
                "attempt_id": str(details.get("attempt_id") or ""),
                "order_id": order_id,
                "active_cancel_due_at_ms": active_cancel_due_at_ms,
            },
        )
        results.append({"order_id": order_id, "terminal_status": "CANCELED"})
    return results


def execute_source_action(
    *,
    store: LiveStore,
    source: SourceAction,
    execution: Any,
    live_enabled: bool,
    allocated_cash: Decimal | None = None,
    wallet_lock_path: Path | None = None,
    coordinator: SharedWalletCoordinator | None = None,
    profile_key: str | None = None,
) -> dict[str, Any]:
    """Execute under the authenticated-wallet lock when one is configured."""

    # Compatibility-only input retained for existing callers.  Live sizing is
    # frozen in the store/coordinator; this value must never alter an order.
    del allocated_cash

    if wallet_lock_path is None:
        return _execute_source_action_locked(
            store=store,
            source=source,
            execution=execution,
            live_enabled=live_enabled,
            coordinator=coordinator,
            profile_key=profile_key,
        )
    with _shared_wallet_submission_lock(wallet_lock_path):
        return _execute_source_action_locked(
            store=store,
            source=source,
            execution=execution,
            live_enabled=live_enabled,
            coordinator=coordinator,
            profile_key=profile_key,
        )


def _snapshot_with_optional_condition_mapping(
    *,
    execution: Any,
    token_id: str,
    side: str,
    prefetch_mapping: bool,
) -> tuple[dict[str, Any], dict[str, str] | None, Exception | None]:
    resolver = getattr(execution, "condition_mapping_for_token", None)
    if not prefetch_mapping or not callable(resolver):
        return (
            execution.snapshot(token_id=token_id, side=side),
            None,
            None,
        )
    # The canonical token map uses the bounded public urllib reader, while
    # the book snapshot uses the SDK HTTP client.  They are independent GETs
    # on separate transports, so their wait can overlap without sharing the
    # SDK's HTTP/2 connection (which is not safe for concurrent requests).
    pool = ThreadPoolExecutor(max_workers=1)
    mapping_future = pool.submit(resolver, token_id)
    try:
        snapshot = execution.snapshot(token_id=token_id, side=side)
    except Exception:
        pool.shutdown(wait=True)
        raise
    try:
        mapping = dict(mapping_future.result())
        mapping_error = None
    except Exception as exc:
        mapping = None
        mapping_error = exc
    finally:
        pool.shutdown(wait=True)
    return snapshot, mapping, mapping_error


def _legacy_stable_causal_prefix_plan_evidence(
    *, store: LiveStore, source: SourceAction
) -> dict[str, Any] | None:
    if store.action_has_unresolved_submission_side_effect(source.action_id):
        return None
    first_planned = store.first_transition_with_status(source, "PLANNED")
    planned_details = (
        first_planned.get("details")
        if isinstance(first_planned, Mapping)
        else None
    )
    cumulative_sizing = (
        planned_details.get("cumulative_sizing")
        if isinstance(planned_details, Mapping)
        else None
    )
    if not isinstance(cumulative_sizing, Mapping) or str(
        cumulative_sizing.get("policy") or ""
    ) != MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM:
        return None
    try:
        frozen_scaled = Decimal(
            str(cumulative_sizing.get("prior_scaled_open_target"))
        )
    except (InvalidOperation, TypeError, ValueError):
        return None
    if (
        not frozen_scaled.is_finite()
        or frozen_scaled < ZERO
        or cumulative_sizing.get("prior_causal_prefix_count") is not None
        or str(cumulative_sizing.get("prior_causal_prefix_hash") or "")
    ):
        return None
    try:
        current = store.frozen_causal_target_prefix_before(source)
        current_scaled = Decimal(str(current["scaled_open_target"]))
        current_count = int(current["action_count"])
        current_hash = str(current["prefix_hash"]).lower()
    except (InvalidOperation, KeyError, TypeError, ValueError, LiveConfigurationError):
        return None
    if (
        not current_scaled.is_finite()
        or current_scaled != frozen_scaled
        or current_count < 0
        or re.fullmatch(r"[0-9a-f]{64}", current_hash) is None
    ):
        return None
    terminal_states = (
        "SKIPPED",
        "FILLED",
        "PARTIAL",
        "EXTERNAL_UNFILLABLE",
        "SUPERSEDED_UNFILLED",
    )
    with store.connect() as connection:
        planned_boundary = connection.execute(
            """
            SELECT id,created_at_ms FROM action_transitions
            WHERE action_id=? AND status='PLANNED'
            ORDER BY id ASC LIMIT 1
            """,
            (source.action_id,),
        ).fetchone()
        if planned_boundary is None:
            return None
        prior_rows = connection.execute(
            """
            SELECT target.updated_at_ms AS target_updated_at_ms,
                   target.state AS target_state,
                   latest.id AS latest_transition_id,
                   latest.created_at_ms AS latest_created_at_ms,
                   latest.status AS latest_status
            FROM action_receipts AS action
            LEFT JOIN action_targets AS target
              ON target.action_id=action.action_id
            LEFT JOIN action_transitions AS latest
              ON latest.id=(
                  SELECT candidate.id FROM action_transitions AS candidate
                  WHERE candidate.action_id=action.action_id
                  ORDER BY candidate.id DESC LIMIT 1
              )
            WHERE action.token_id=?
              AND LOWER(action.source_role) IN (
                  'maker','verified_public_wallet'
              )
              AND (
                  action.block_number, action.source_log_index,
                  action.source_timestamp, action.transaction_hash,
                  action.token_id, action.side, action.order_hash,
                  action.action_id
              ) < (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(source.token_id),
                int(source.block_number),
                int(source.log_index),
                int(source.source_timestamp),
                str(source.transaction_hash).lower(),
                str(source.token_id),
                str(source.side).upper(),
                str(source.order_hash).lower(),
                str(source.action_id),
            ),
        ).fetchall()
    if len(prior_rows) != current_count or any(
        row["target_updated_at_ms"] is None
        or row["latest_transition_id"] is None
        or int(row["target_updated_at_ms"])
        > int(planned_boundary["created_at_ms"])
        or int(row["latest_transition_id"]) >= int(planned_boundary["id"])
        or int(row["latest_created_at_ms"])
        > int(planned_boundary["created_at_ms"])
        or str(row["target_state"]) not in terminal_states
        or str(row["latest_status"]) != str(row["target_state"])
        for row in prior_rows
    ):
        return None
    return {
        "frozen_prior_scaled_open_target": str(frozen_scaled),
        "current_prior_scaled_open_target": str(current_scaled),
        "current_prior_causal_prefix_count": current_count,
        "current_prior_causal_prefix_hash": current_hash,
    }


def _legacy_stable_causal_prefix_recovery_evidence(
    *, store: LiveStore, source: SourceAction
) -> dict[str, Any] | None:
    target = store.action_target(source.action_id)
    latest = store.latest_transition(source)
    if (
        target is None
        or str(target["state"]) != "ERROR_INTERNAL"
        or str(target["reason"]) != "INTERNAL_STALE_CAUSAL_TARGET"
        or latest is None
        or latest["terminal_status"] != "ERROR_INTERNAL"
        or latest["reason"] != "INTERNAL_STALE_CAUSAL_TARGET"
    ):
        return None
    plan_evidence = _legacy_stable_causal_prefix_plan_evidence(
        store=store, source=source
    )
    if plan_evidence is None:
        return None
    details = latest.get("details")
    if not isinstance(details, Mapping):
        return None
    try:
        frozen_scaled = Decimal(
            str(details.get("frozen_prior_scaled_open_target"))
        )
        current_scaled = Decimal(
            str(details.get("current_prior_scaled_open_target"))
        )
    except (InvalidOperation, TypeError, ValueError):
        return None
    if (
        details.get("causal_target_provenance_failure")
        != "FROZEN_CAUSAL_PREFIX_COUNT_INVALID"
        or details.get("frozen_prior_causal_prefix_count") is not None
        or str(details.get("frozen_prior_causal_prefix_hash") or "")
        or frozen_scaled
        != Decimal(plan_evidence["frozen_prior_scaled_open_target"])
        or current_scaled
        != Decimal(plan_evidence["current_prior_scaled_open_target"])
        or details.get("current_prior_causal_prefix_count")
        != plan_evidence["current_prior_causal_prefix_count"]
        or str(details.get("current_prior_causal_prefix_hash") or "").lower()
        != plan_evidence["current_prior_causal_prefix_hash"]
        or details.get("new_order_submitted") is not False
    ):
        return None
    return {**plan_evidence, "previous_failure": dict(details)}


def _fail_closed_stale_causal_target(
    *,
    store: LiveStore,
    source: SourceAction,
    existing_target: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Block a frozen UPSCALE target whose causal sizing basis changed."""

    if store.action_has_unresolved_submission_side_effect(source.action_id):
        raise LiveConfigurationError(
            "UNRESOLVED_SUBMISSION_BLOCKS_STALE_CAUSAL_TARGET_CHECK"
        )
    legacy_plan_evidence = _legacy_stable_causal_prefix_plan_evidence(
        store=store, source=source
    )
    if legacy_plan_evidence is not None:
        if str(existing_target["state"]) != "ERROR_INTERNAL":
            return None
        recovery_evidence = _legacy_stable_causal_prefix_recovery_evidence(
            store=store, source=source
        )
        if recovery_evidence is not None:
            recovered_at_ms = now_ms()
            recovered_state = (
                "PARTIAL_PENDING"
                if Decimal(str(existing_target["cumulative_filled_quantity"]))
                > ZERO
                else "PENDING_LIQUIDITY"
            )
            reason = (
                "LEGACY_EMPTY_CAUSAL_PREFIX_RECOVERED"
                if recovery_evidence["current_prior_causal_prefix_count"] == 0
                else "LEGACY_STABLE_CAUSAL_PREFIX_RECOVERED"
            )
            store.set_action_target_state(
                source=source,
                state=recovered_state,
                reason=reason,
                updated_at_ms=recovered_at_ms,
            )
            store.append_transition(
                source=source,
                status=recovered_state,
                reason=reason,
                created_at_ms=recovered_at_ms,
                details={**recovery_evidence, "new_order_submitted": False},
            )
            return None
    first_planned = store.first_transition_with_status(source, "PLANNED")
    planned_details = (
        first_planned.get("details")
        if isinstance(first_planned, Mapping)
        else None
    )
    cumulative_sizing = (
        planned_details.get("cumulative_sizing")
        if isinstance(planned_details, Mapping)
        else None
    )
    policy = (
        str(cumulative_sizing.get("policy") or "")
        if isinstance(cumulative_sizing, Mapping)
        else ""
    )
    if policy == MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM:
        return None

    frozen_scaled_raw = (
        cumulative_sizing.get("prior_scaled_open_target")
        if isinstance(cumulative_sizing, Mapping)
        else None
    )
    frozen_count_raw = (
        cumulative_sizing.get("prior_causal_prefix_count")
        if isinstance(cumulative_sizing, Mapping)
        else None
    )
    frozen_hash_raw = (
        cumulative_sizing.get("prior_causal_prefix_hash")
        if isinstance(cumulative_sizing, Mapping)
        else None
    )
    current_prefix_failure: str | None = None
    current_scaled: Decimal | None = None
    current_count: int | None = None
    current_hash: str | None = None
    try:
        current_prefix = store.frozen_causal_target_prefix_before(source)
    except LiveConfigurationError as exc:
        current_prefix_failure = str(exc)
    else:
        current_scaled = Decimal(str(current_prefix["scaled_open_target"]))
        current_count = int(current_prefix["action_count"])
        current_hash = str(current_prefix["prefix_hash"])
    frozen_scaled: Decimal | None = None
    frozen_count: int | None = None
    frozen_hash: str | None = None
    provenance_failure: str | None = None
    if policy != MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM:
        provenance_failure = "FROZEN_CUMULATIVE_SIZING_POLICY_MISSING_OR_UNKNOWN"
    else:
        try:
            frozen_scaled = Decimal(str(frozen_scaled_raw))
        except (InvalidOperation, TypeError, ValueError):
            provenance_failure = "FROZEN_PRIOR_SCALED_OPEN_TARGET_INVALID"
        else:
            if not frozen_scaled.is_finite() or frozen_scaled < ZERO:
                provenance_failure = "FROZEN_PRIOR_SCALED_OPEN_TARGET_INVALID"
    if provenance_failure is None:
        try:
            if isinstance(frozen_count_raw, bool):
                raise ValueError
            frozen_count_decimal = Decimal(str(frozen_count_raw))
            if (
                not frozen_count_decimal.is_finite()
                or frozen_count_decimal < ZERO
                or frozen_count_decimal != frozen_count_decimal.to_integral_value()
            ):
                raise ValueError
            frozen_count = int(frozen_count_decimal)
        except (InvalidOperation, TypeError, ValueError):
            provenance_failure = "FROZEN_CAUSAL_PREFIX_COUNT_INVALID"
    if provenance_failure is None:
        frozen_hash = str(frozen_hash_raw or "").lower()
        if re.fullmatch(r"[0-9a-f]{64}", frozen_hash) is None:
            provenance_failure = "FROZEN_CAUSAL_PREFIX_HASH_INVALID"
    if provenance_failure is None and current_prefix_failure is not None:
        provenance_failure = "CURRENT_CAUSAL_PREFIX_UNPROVABLE"
    if (
        provenance_failure is None
        and frozen_scaled == current_scaled
        and frozen_count == current_count
        and frozen_hash == current_hash
    ):
        return None

    reason = "INTERNAL_STALE_CAUSAL_TARGET"
    details = {
        "causal_target_provenance_failure": provenance_failure,
        "frozen_cumulative_sizing_policy": policy or None,
        "frozen_prior_scaled_open_target": (
            None if frozen_scaled_raw is None else str(frozen_scaled_raw)
        ),
        "current_prior_scaled_open_target": (
            None if current_scaled is None else str(current_scaled)
        ),
        "frozen_prior_causal_prefix_count": frozen_count_raw,
        "current_prior_causal_prefix_count": current_count,
        "frozen_prior_causal_prefix_hash": frozen_hash_raw,
        "current_prior_causal_prefix_hash": current_hash,
        "current_causal_prefix_failure": current_prefix_failure,
        "action_target_id": str(existing_target["action_id"]),
        "action_target_proportional_quantity": str(
            existing_target["proportional_quantity"]
        ),
        "action_target_quantity": str(existing_target["target_quantity"]),
        "action_target_cumulative_filled_quantity": str(
            existing_target["cumulative_filled_quantity"]
        ),
        "action_target_state_before": str(existing_target["state"]),
        "action_target_quantity_mutated": False,
        "new_order_submitted": False,
    }
    store.release_reservation_and_finalize(
        source=source,
        terminal_status="ERROR_INTERNAL",
        reason=reason,
        created_at_ms=now_ms(),
        details=details,
        attempt_id=None,
    )
    return {"terminal_status": "ERROR_INTERNAL", "reason": reason}


def _execute_source_action_locked(
    *,
    store: LiveStore,
    source: SourceAction,
    execution: Any,
    live_enabled: bool,
    coordinator: SharedWalletCoordinator | None = None,
    profile_key: str | None = None,
) -> dict[str, Any]:
    """Plan and submit one action exactly once.

    ``execution`` is deliberately narrow: its snapshot must contain the
    current market minimum, top-of-book price/depth, fee bound, and raw book;
    it submits a FAK marketable order whose BUY amount is derived from the
    exact scaled share target.  The real network adapter is
    kept outside this pure recovery boundary.
    """

    if not _is_followable_source_role(source.source_role):
        raise LiveConfigurationError("COUNTERPARTY_ORDER_LOG_NOT_SOURCE_ACTION")
    sizing_mode = store.config("sizing_mode") or SIZING_MODE_FIXED_SHARES
    if sizing_mode not in {
        SIZING_MODE_FIXED_SHARES,
        SIZING_MODE_SOURCE_NOTIONAL,
    }:
        raise LiveConfigurationError(f"UNSUPPORTED_SIZING_MODE:{sizing_mode}")
    source_vwap = source.source_notional / source.source_quantity
    existing_target = store.action_target(source.action_id)
    liquidity_policy = store.liquidity_retry_policy_for_source(source)
    liquidity_retry = (
        None
        if existing_target is None
        else store.liquidity_retry_evidence(source)
    )
    if (
        existing_target is not None
        and str(existing_target["state"]) == "PENDING_CAPITAL"
    ):
        return {
            "terminal_status": "PENDING_CAPITAL",
            "reason": str(existing_target["reason"]),
        }
    execution_constraint = store.action_execution_constraint(source)
    if execution_constraint is not None:
        existing = store.latest_transition(source)
        if existing is not None and existing["terminal_status"] in {
            "EXTERNAL_UNFILLABLE",
            "PARTIAL",
        }:
            return existing
        proportional = source_action_proportional_quantity(
            source=source,
            scale=store.fixed_share_scale_for_source_block(source.block_number),
            sizing_mode=sizing_mode,
            executable_price=source_vwap,
        )
        target = store.ensure_action_target(
            source=source,
            proportional_quantity=proportional,
            target_quantity=proportional,
            state="EXTERNAL_UNFILLABLE",
            reason=str(execution_constraint["reason"]),
            updated_at_ms=now_ms(),
        )
        filled = Decimal(str(target["cumulative_filled_quantity"]))
        terminal_status = "PARTIAL" if filled > ZERO else "EXTERNAL_UNFILLABLE"
        store.set_action_target_state(
            source=source,
            state=terminal_status,
            reason=str(execution_constraint["reason"]),
            updated_at_ms=now_ms(),
        )
        store.append_transition(
            source=source,
            status=terminal_status,
            reason=str(execution_constraint["reason"]),
            details={"execution_constraint": execution_constraint},
        )
        return {
            "terminal_status": terminal_status,
            "reason": str(execution_constraint["reason"]),
        }
    if not live_enabled:
        raise LiveDisabledError("POLYMARKET_LIVE_TRADING is not explicitly enabled")

    existing = store.latest_transition(source)
    if existing is not None and existing["terminal_status"] == "SUBMIT_STARTED":
        reason = "RECOVERY_UNKNOWN_SUBMISSION_AFTER_SUBMIT_STARTED"
        store.append_transition(
            source=source,
            status="UNKNOWN_SUBMISSION",
            reason=reason,
        )
        return {"terminal_status": "UNKNOWN_SUBMISSION", "reason": reason}
    if existing is not None and existing["terminal_status"] in {
        "SKIPPED",
        "SUBMITTED_UNRECONCILED",
        "UNKNOWN_SUBMISSION",
        "FILLED",
        "PARTIAL",
        "ERROR",
        "ERROR_INTERNAL",
        "EXTERNAL_UNFILLABLE",
        "SUPERSEDED_UNFILLED",
        "EXPIRED_RETRY_WINDOW",
    }:
        return existing

    inserted = store.record_action_receipt(source)
    if inserted:
        store.append_transition(source=source, status="OBSERVED")
    prior_unresolved = store.prior_unresolved_same_token_submission(source)
    if prior_unresolved is not None:
        reason = "PRIOR_SAME_TOKEN_SUBMISSION_UNRESOLVED"
        details = {
            "prior_submission": prior_unresolved,
            "new_order_submitted": False,
            "historical_action_replayed": False,
        }
        store.append_transition(
            source=source,
            status="PENDING_CAUSAL_ORDER",
            reason=reason,
            details=details,
        )
        return {
            "terminal_status": "PENDING_CAUSAL_ORDER",
            "reason": reason,
        }
    store.supersede_earlier_fully_unfilled_opposites(source=source)
    prior_nonterminal = store.prior_nonterminal_same_token_action(source)
    if prior_nonterminal is not None:
        reason = "PRIOR_SAME_TOKEN_ACTION_NOT_TERMINAL"
        details = {
            "prior_action": prior_nonterminal,
            "new_order_submitted": False,
            "historical_action_replayed": False,
        }
        store.append_transition(
            source=source,
            status="PENDING_CAUSAL_ORDER",
            reason=reason,
            details=details,
        )
        return {
            "terminal_status": "PENDING_CAUSAL_ORDER",
            "reason": reason,
        }
    if (
        source.side == "SELL"
        and store.available_position_quantity(source.token_id) == ZERO
    ):
        prior_buy = store.prior_buy_evidence(source)
        if prior_buy is None:
            reason = "NO_LOCAL_INVENTORY_PRE_WATERMARK_OR_PRIOR_MISS"
            details: dict[str, Any] = {"historical_action_executed": False}
        elif Decimal(str(prior_buy["filled_quantity"])) > ZERO or str(
            prior_buy["status"]
        ) in {"FILLED", "PARTIAL"}:
            reason = "NO_LOCAL_INVENTORY_AFTER_LOCAL_POSITION_EXHAUSTED"
            details = {
                "historical_action_executed": False,
                "prior_buy_action_id": str(prior_buy["action_id"]),
                "prior_buy_status": str(prior_buy["status"]),
                "prior_buy_reason": str(prior_buy["reason"]),
            }
        else:
            reason = "NO_LOCAL_INVENTORY_AFTER_PRIOR_UNREPLICATED_BUY"
            details = {
                "historical_action_executed": False,
                "prior_buy_action_id": str(prior_buy["action_id"]),
                "prior_buy_status": str(prior_buy["status"]),
                "prior_buy_reason": str(prior_buy["reason"]),
            }
        proportional = source_action_proportional_quantity(
            source=source,
            scale=store.fixed_share_scale_for_source_block(source.block_number),
            sizing_mode=sizing_mode,
            executable_price=source_vwap,
        )
        terminal_at_ms = now_ms()
        target = store.ensure_action_target(
            source=source,
            proportional_quantity=proportional,
            target_quantity=proportional,
            state="EXTERNAL_UNFILLABLE",
            reason=reason,
            updated_at_ms=terminal_at_ms,
        )
        cumulative_filled_quantity = Decimal(
            str(target["cumulative_filled_quantity"])
        )
        terminal_status = (
            "PARTIAL"
            if cumulative_filled_quantity > ZERO
            else "EXTERNAL_UNFILLABLE"
        )
        if cumulative_filled_quantity > ZERO:
            details["cumulative_filled_quantity"] = str(
                cumulative_filled_quantity
            )
        store.set_action_target_state(
            source=source,
            state=terminal_status,
            reason=reason,
            updated_at_ms=terminal_at_ms,
        )
        store.append_transition(
            source=source,
            status=terminal_status,
            reason=reason,
            details=details,
        )
        return {"terminal_status": terminal_status, "reason": reason}

    if existing_target is not None:
        stale_target_result = _fail_closed_stale_causal_target(
            store=store,
            source=source,
            existing_target=existing_target,
        )
        if stale_target_result is not None:
            return stale_target_result

    execution_scale = store.fixed_share_scale_for_source_block(
        source.block_number
    )
    original_proportional_quantity = source_action_proportional_quantity(
        source=source,
        scale=execution_scale,
        sizing_mode=sizing_mode,
        executable_price=source_vwap,
    )
    minimum_size_policy = (
        store.config("minimum_size_policy")
        or MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    if minimum_size_policy not in MINIMUM_SIZE_POLICIES:
        raise LiveConfigurationError(
            f"UNSUPPORTED_MINIMUM_SIZE_POLICY:{minimum_size_policy}"
        )
    cumulative_sizing: dict[str, Any] = {
        "policy": minimum_size_policy,
        "original_proportional_quantity": str(original_proportional_quantity),
        "historical_action_replayed": False,
    }
    planning_source_quantity = source.source_quantity
    planning_scale = execution_scale
    if (
        existing_target is None
        and minimum_size_policy == MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM
    ):
        causal_prefix = store.frozen_causal_target_prefix_before(source)
        prior_scaled_open_target = Decimal(
            str(causal_prefix["scaled_open_target"])
        )
        available_position = store.available_position_quantity(source.token_id)
        confirmed_minimum_surplus = max(
            available_position - prior_scaled_open_target,
            ZERO,
        )
        if source.side == "BUY":
            required_quantity = max(
                original_proportional_quantity - confirmed_minimum_surplus,
                ZERO,
            )
        else:
            required_quantity = min(
                available_position,
                original_proportional_quantity + confirmed_minimum_surplus,
            )
        cumulative_sizing.update(
            {
                "prior_scaled_open_target": str(prior_scaled_open_target),
                "prior_causal_prefix_count": int(
                    causal_prefix["action_count"]
                ),
                "prior_causal_prefix_hash": str(causal_prefix["prefix_hash"]),
                "available_position_before_action": str(available_position),
                "confirmed_minimum_surplus_before_action": str(
                    confirmed_minimum_surplus
                ),
                "new_order_base_quantity": str(required_quantity),
            }
        )
        if source.side == "BUY" and required_quantity == ZERO:
            reason = "PRIOR_MINIMUM_UPSCALE_COVERS_PROPORTIONAL_BUY"
            terminal_at_ms = now_ms()
            target = store.ensure_action_target(
                source=source,
                proportional_quantity=original_proportional_quantity,
                target_quantity=original_proportional_quantity,
                state="SKIPPED",
                reason=reason,
                updated_at_ms=terminal_at_ms,
            )
            details = {
                "cumulative_sizing": cumulative_sizing,
                "action_target": {
                    key: str(value) if isinstance(value, Decimal) else value
                    for key, value in target.items()
                },
                "new_order_submitted": False,
            }
            store.append_transition(
                source=source,
                status="PLANNED",
                reason=reason,
                created_at_ms=terminal_at_ms,
                details=details,
            )
            store.append_transition(
                source=source,
                status="SKIPPED",
                reason=reason,
                created_at_ms=terminal_at_ms,
                details=details,
            )
            return {"terminal_status": "SKIPPED", "reason": reason}
        planning_source_quantity = required_quantity
        planning_scale = Decimal("1")
    existing_condition_mapping: dict[str, str] | None = None
    existing_condition_mapping_error: Exception | None = None
    should_prefetch_mapping = False
    if source.side == "BUY" and coordinator is not None:
        try:
            existing_condition_mapping = store.condition_mapping_for_token(
                source.token_id
            )
        except Exception as exc:
            existing_condition_mapping_error = exc
        should_prefetch_mapping = (
            existing_condition_mapping_error is None
            and existing_condition_mapping is None
            and callable(
                getattr(execution, "condition_mapping_for_token", None)
            )
        )
    try:
        snapshot, prefetched_mapping, prefetched_mapping_error = (
            _snapshot_with_optional_condition_mapping(
                execution=execution,
                token_id=source.token_id,
                side=source.side,
                prefetch_mapping=should_prefetch_mapping,
            )
        )
    except Exception as exc:
        reason = f"BOOK_SNAPSHOT_ERROR: {type(exc).__name__}: {exc}"
        if liquidity_retry is not None:
            retained_state = str(existing_target["state"])
            retained_reason = "CURRENT_BOOK_UNAVAILABLE_FOR_LIQUIDITY_RETRY"
            store.set_action_target_state(
                source=source,
                state=retained_state,
                reason=retained_reason,
                updated_at_ms=now_ms(),
            )
            store.append_transition(
                source=source,
                status=retained_state,
                reason=retained_reason,
                details={
                    "external_error": reason,
                    "liquidity_retry": liquidity_retry,
                    "new_order_submitted": False,
                },
            )
            return {
                "terminal_status": retained_state,
                "reason": retained_reason,
            }
        if _is_retryable_external_error(exc):
            store.append_transition(
                source=source,
                status="PENDING_EXTERNAL_RETRY",
                reason=reason,
            )
            raise
        store.append_transition(
            source=source,
            status="ERROR",
            reason=reason,
        )
        return {"terminal_status": "ERROR", "reason": reason}
    if existing_target is None and sizing_mode == SIZING_MODE_SOURCE_NOTIONAL:
        original_proportional_quantity = source_action_proportional_quantity(
            source=source,
            scale=execution_scale,
            sizing_mode=sizing_mode,
            executable_price=Decimal(str(snapshot["best_price"])),
        )
        planning_source_quantity = original_proportional_quantity
        planning_scale = Decimal("1")
        cumulative_sizing.update(
            {
                "sizing_mode": sizing_mode,
                "source_notional": str(source.source_notional),
                "action_time_executable_price": str(snapshot["best_price"]),
                "original_proportional_quantity": str(
                    original_proportional_quantity
                ),
            }
        )
    # A strategy ledger is attribution only: it can never be used as the
    # spendable balance for a new BUY.  Start fail-closed until the execution
    # adapter has supplied an authenticated collateral observation below.
    effective_cash = ZERO
    cash_authority = "AUTHENTICATED_COLLATERAL_REQUIRED"
    if coordinator is not None:
        if not profile_key:
            raise LiveConfigurationError(
                "PROFILE_KEY_REQUIRED_WITH_SHARED_WALLET_COORDINATOR"
            )
        condition_id = str(
            snapshot.get("condition_id")
            or (snapshot.get("raw_book") or {}).get("market")
            or ""
        ).strip().lower()
        if not condition_id:
            reason = "MISSING_CONDITION_ID_FOR_SHARED_WALLET_OWNERSHIP"
            store.append_transition(
                source=source,
                status="PENDING_INTERNAL_INVARIANT",
                reason=reason,
            )
            raise SharedWalletCoordinatorError(reason)
        snapshot = {**snapshot, "condition_id": condition_id}
        if source.side == "BUY":
            try:
                if existing_condition_mapping_error is not None:
                    raise existing_condition_mapping_error
                mapping = existing_condition_mapping
                if mapping is None:
                    if prefetched_mapping_error is not None:
                        raise prefetched_mapping_error
                    mapping = prefetched_mapping
                    if mapping is None:
                        resolver = getattr(
                            execution, "condition_mapping_for_token", None
                        )
                        if not callable(resolver):
                            raise LiveConfigurationError(
                                "SHARED_CONDITION_MAPPING_RESOLVER_UNAVAILABLE"
                            )
                        mapping = dict(resolver(source.token_id))
                    store.bind_condition_for_token(
                        token_id=source.token_id,
                        condition_id=str(mapping.get("condition_id") or ""),
                        primary_token_id=str(
                            mapping.get("primary_token_id") or ""
                        ),
                        secondary_token_id=str(
                            mapping.get("secondary_token_id") or ""
                        ),
                        observed_at_ms=now_ms(),
                    )
                if str(mapping["condition_id"]).strip().lower() != condition_id:
                    raise LiveConfigurationError(
                        "BOOK_AND_CANONICAL_CONDITION_MISMATCH"
                    )
            except Exception as exc:
                reason = (
                    "SHARED_CONDITION_MAPPING_ERROR: "
                    + f"{type(exc).__name__}: {exc}"
                )
                if _is_retryable_external_error(exc):
                    store.append_transition(
                        source=source,
                        status="PENDING_METADATA",
                        reason=reason,
                    )
                    raise
                store.append_transition(
                    source=source,
                    status="ERROR_INTERNAL",
                    reason=reason,
                )
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category="INTERNAL_SHARED_WALLET_METADATA",
                    message=reason,
                    details={"action_id": source.action_id},
                )
                raise SharedWalletCoordinatorError(reason) from exc
            collision = coordinator.buy_collision(
                profile_key=str(profile_key),
                token_id=source.token_id,
                condition_id=condition_id,
            )
            if collision["state"] not in {"CLEAR", "CLEAR_SHARED_CONDITION"}:
                reason = str(collision["state"])
                store.append_transition(
                    source=source,
                    status="PENDING_INTERNAL_INVARIANT",
                    reason=reason,
                    details={"wallet_coordinator": collision, "snapshot": snapshot},
                )
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category="INTERNAL_SHARED_WALLET_INVARIANT",
                    message=reason,
                    details={
                        "action_id": source.action_id,
                        "wallet_coordinator": collision,
                    },
                )
                raise SharedWalletCoordinatorError(reason)
    if source.side == "BUY":
        collateral_reader = getattr(execution, "collateral_balance_usd", None)
        if not callable(collateral_reader):
            reason = "MISSING_AUTHENTICATED_COLLATERAL_READER"
            store.append_transition(
                source=source,
                status="PENDING_INTERNAL_INVARIANT",
                reason=reason,
            )
            store.append_runtime_error(
                occurred_at_ms=now_ms(),
                category="INTERNAL_AUTHENTICATED_COLLATERAL",
                message=reason,
                details={"action_id": source.action_id},
            )
            raise LiveConfigurationError(reason)
        try:
            collateral = Decimal(str(collateral_reader()))
        except Exception as exc:
            reason = f"COLLATERAL_CHECK_ERROR: {type(exc).__name__}: {exc}"
            if _is_retryable_external_error(exc):
                store.append_transition(
                    source=source,
                    status="PENDING_EXTERNAL_RETRY",
                    reason=reason,
                )
                raise
            store.append_transition(
                source=source,
                status="ERROR",
                reason=reason,
            )
            return {"terminal_status": "ERROR", "reason": reason}
        if coordinator is not None:
            try:
                wallet_cash = _persist_authenticated_collateral_observation(
                    store=store,
                    observed_collateral_usd=collateral,
                    observed_at_ms=now_ms(),
                    coordinator=coordinator,
                    profile_key=str(profile_key),
                )
            except SharedWalletCoordinatorError as exc:
                reason = f"SHARED_WALLET_INVARIANT: {exc}"
                store.append_transition(
                    source=source,
                    status="PENDING_INTERNAL_INVARIANT",
                    reason=reason,
                )
                raise
            if wallet_cash is None:
                raise LiveConfigurationError("MISSING_SHARED_WALLET_CASH_SNAPSHOT")
            effective_cash = wallet_cash.available_for_new_buy_usd
            cash_authority = (
                "AUTHENTICATED_ACCOUNT_COLLATERAL_"
                "MINUS_ACTIVE_BUY_RESERVATIONS"
            )
            coordinator_details = wallet_cash.as_dict()
            snapshot = {
                **snapshot,
                "authenticated_account_cash": coordinator_details,
                "cash_authority": cash_authority,
                "strategy_budget_used_as_cash": False,
            }
        else:
            effective_cash = max(
                collateral - store.active_buy_reservations_usd(), ZERO
            )
            cash_authority = (
                "AUTHENTICATED_ACCOUNT_COLLATERAL_"
                "MINUS_ACTIVE_BUY_RESERVATIONS"
            )
        snapshot = {**snapshot, "collateral_balance_usd": str(collateral)}
        effective_cash = min(effective_cash, collateral)
    if source.side == "SELL":
        # A sell uses existing inventory, not a strategy-attribution cash
        # field.  Its exit must never depend on a BUY cash calculation.
        effective_cash = ZERO
        cash_authority = "NOT_APPLICABLE_FOR_SELL"
    snapshot = {
        **snapshot,
        "effective_cash_for_this_action_usd": str(effective_cash),
        "cash_authority": cash_authority,
        "strategy_budget_used_as_cash": False,
    }
    snapshot = {**snapshot, "minimum_size_policy": minimum_size_policy}
    allow_minimum_upscale = (
        minimum_size_policy == MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM
    )
    if existing_target is not None:
        remaining = Decimal(str(existing_target["remaining_quantity"]))
        if remaining == ZERO:
            store.set_action_target_state(
                source=source,
                state="FILLED",
                reason="ACTION_TARGET_ALREADY_COMPLETE",
                updated_at_ms=now_ms(),
            )
            return {
                "terminal_status": "FILLED",
                "reason": "ACTION_TARGET_ALREADY_COMPLETE",
            }
        planning_source_quantity = remaining
        planning_scale = Decimal("1")
        allow_minimum_upscale = False
    max_buy_notional_usd = None
    if source.side == "BUY":
        event_slug = ""
        frozen_meta = store.frozen_action_metadata(source.action_id)
        if frozen_meta is not None:
            event_slug = str(
                (frozen_meta.get("metadata") or {}).get("event_slug") or ""
            )
        max_buy_notional_usd = max_buy_notional_usd_for_profile(
            profile_key=profile_key,
            event_slug=event_slug,
            scale=execution_scale,
        )
        if max_buy_notional_usd is not None:
            cumulative_sizing["non_tennis_max_copy_notional_usd"] = str(
                max_buy_notional_usd
            )
            cumulative_sizing["non_tennis_event_slug"] = event_slug
    plan = plan_action(
        side=source.side,
        source_quantity=planning_source_quantity,
        scale=planning_scale,
        held_quantity=store.available_position_quantity(source.token_id),
        minimum_order_size=Decimal(str(snapshot["minimum_order_size"])),
        minimum_marketable_buy_notional_usd=Decimal(
            str(snapshot["minimum_marketable_buy_notional_usd"])
        ),
        best_price=Decimal(str(snapshot["best_price"])),
        minimum_fill_price=Decimal(str(snapshot["tick_size"])),
        visible_best_level_size=Decimal(str(snapshot["visible_best_level_size"])),
        taker_fee_bps=Decimal(str(snapshot["fee_bps"])),
        available_cash=effective_cash,
        fee_exponent=Decimal(str(snapshot.get("fee_exponent", "1"))),
        allow_minimum_upscale=allow_minimum_upscale,
        max_buy_notional_usd=max_buy_notional_usd,
    )

    if (
        source.side == "BUY"
        and plan.terminal_status == "READY"
        and plan.worst_price > USER_SPECIFIED_HIGH_PRICE_BUY_CEILING
        and plan.worst_price > source_vwap
    ):
        plan = ActionPlan(
            terminal_status="EXTERNAL_UNFILLABLE",
            reason="BUY_PRICE_ABOVE_0_90_WITH_EXECUTION_LOSS",
            side=plan.side,
            proportional_quantity=plan.proportional_quantity,
            requested_quantity=plan.requested_quantity,
            order_amount_usd=plan.order_amount_usd,
            worst_price=plan.worst_price,
            reserved_cash_usd=plan.reserved_cash_usd,
        )

    bounded_retry = (
        None
        if liquidity_policy is not None
        else store.bounded_retry_policy_for_source(source)
    )
    bounded_retry_details: dict[str, Any] | None = None
    if bounded_retry is not None:
        price_boundary = bounded_retry_price_boundary(
            side=source.side,
            source_average_price=source_vwap,
            tick_size=Decimal(str(snapshot["tick_size"])),
        )
        best_price = Decimal(str(snapshot["best_price"]))
        bounded_retry_details = {
            **bounded_retry,
            "price_boundary": str(price_boundary),
            "price_boundary_kind": (
                "MAXIMUM_BUY_PRICE"
                if source.side == "BUY"
                else "MINIMUM_SELL_PRICE"
            ),
            "observed_best_price": str(best_price),
            "maximum_adverse_slippage": str(
                BOUNDED_RETRY_MAX_ADVERSE_SLIPPAGE
            ),
        }
        price_blocked = (
            bounded_retry["phase"] == "ADVERSE_PRICE_PROTECTED"
            and (
                (source.side == "BUY" and best_price > price_boundary)
                or (source.side == "SELL" and best_price < price_boundary)
            )
        )
        if price_blocked:
            plan = ActionPlan(
                terminal_status="PENDING_PRICE_PROTECTION",
                reason="CURRENT_BOOK_OUTSIDE_BOUNDED_RETRY_PRICE",
                side=plan.side,
                proportional_quantity=plan.proportional_quantity,
                requested_quantity=plan.requested_quantity,
                order_amount_usd=plan.order_amount_usd,
                worst_price=plan.worst_price,
                reserved_cash_usd=plan.reserved_cash_usd,
            )

    liquidity_retry_details: dict[str, Any] | None = None
    if liquidity_retry is not None:
        frozen_price = Decimal(str(liquidity_retry["frozen_worst_price"]))
        price_blocked = (
            source.side == "BUY" and plan.worst_price > frozen_price
        ) or (
            source.side == "SELL" and plan.worst_price < frozen_price
        )
        liquidity_retry_details = {
            **liquidity_retry,
            "observed_current_worst_price": str(plan.worst_price),
            "price_boundary_kind": (
                "MAXIMUM_BUY_PRICE"
                if source.side == "BUY"
                else "MINIMUM_SELL_PRICE"
            ),
        }
        if plan.terminal_status == "READY" and price_blocked:
            plan = ActionPlan(
                terminal_status="PENDING_PRICE_PROTECTION",
                reason="CURRENT_BOOK_OUTSIDE_FIRST_ATTEMPT_PRICE",
                side=plan.side,
                proportional_quantity=plan.proportional_quantity,
                requested_quantity=plan.requested_quantity,
                order_amount_usd=plan.order_amount_usd,
                worst_price=plan.worst_price,
                reserved_cash_usd=plan.reserved_cash_usd,
            )

    # When the authenticated account could not fund this newly discovered BUY,
    # retaining it
    # as PENDING_CAPITAL would later invite a stale-price replay.  Preserve the
    # discovery-time snapshot and close only this forward action instead.
    # Historical PENDING_CAPITAL receipts return above before any book read.
    if (
        source.side == "BUY"
        and plan.terminal_status == "PENDING_CAPITAL"
        and (coordinator is not None or liquidity_retry is not None)
    ):
        plan = ActionPlan(
            terminal_status="EXTERNAL_UNFILLABLE",
            reason=(
                "INSUFFICIENT_AUTHENTICATED_ACCOUNT_CASH_AT_RETRY"
                if liquidity_retry is not None
                else "INSUFFICIENT_AUTHENTICATED_ACCOUNT_CASH_AT_DISCOVERY"
            ),
            side=plan.side,
            proportional_quantity=plan.proportional_quantity,
            requested_quantity=plan.requested_quantity,
            order_amount_usd=plan.order_amount_usd,
            worst_price=plan.worst_price,
            reserved_cash_usd=plan.reserved_cash_usd,
        )

    if existing_target is not None and plan.terminal_status == "SKIPPED":
        plan = ActionPlan(
            terminal_status=(
                "EXTERNAL_UNFILLABLE"
                if liquidity_policy is not None
                else "EXTERNAL_UNFILLABLE"
                if bounded_retry is not None
                else (
                    "PENDING_MINIMUM_REMAINDER"
                    if source.side == "BUY"
                    else "PENDING_MINIMUM_UNWIND"
                )
            ),
            reason=(
                "RETRY_REMAINDER_BELOW_OFFICIAL_MINIMUM"
                if liquidity_policy is not None or bounded_retry is not None
                else (
                    "REMAINING_QUANTITY_BELOW_CURRENT_MARKET_MINIMUM"
                    if source.side == "BUY"
                    else "REMAINING_SELL_QUANTITY_BELOW_CURRENT_MARKET_MINIMUM"
                )
            ),
            side=plan.side,
            proportional_quantity=plan.proportional_quantity,
            requested_quantity=plan.requested_quantity,
            order_amount_usd=plan.order_amount_usd,
            worst_price=plan.worst_price,
            reserved_cash_usd=plan.reserved_cash_usd,
        )

    if existing_target is None:
        target_quantity = (
            plan.requested_quantity
            if plan.requested_quantity > ZERO
            else plan.proportional_quantity
        )
        existing_target = store.ensure_action_target(
            source=source,
            proportional_quantity=original_proportional_quantity,
            target_quantity=target_quantity,
            state=plan.terminal_status,
            reason=plan.reason,
            updated_at_ms=now_ms(),
        )

    prepare_gtd = getattr(execution, "prepare_gtd_limit", None)
    submit_prepared_gtd = getattr(execution, "submit_prepared_gtd_limit", None)
    use_active_cancel_limit = _uses_active_cancel_limit(
        side=source.side,
        prepare_gtd=prepare_gtd,
        submit_prepared_gtd=submit_prepared_gtd,
    )
    plan_details = {
        **_plan_details(
            plan,
            snapshot,
            execution_order_type=(
                "GTC_ACTIVE_CANCEL" if use_active_cancel_limit else "FAK"
            ),
        ),
        "cumulative_sizing": cumulative_sizing,
        "bounded_retry": bounded_retry_details,
        "liquidity_retry": liquidity_retry_details,
        "action_target": {
            key: str(value) if isinstance(value, Decimal) else value
            for key, value in existing_target.items()
        },
    }
    if liquidity_retry is not None:
        plan_details["retry_order_quantity_mode"] = "EXACT_SHARES"

    store.append_transition(
        source=source,
        status="PLANNED",
        reason=plan.reason,
        details=plan_details,
    )
    if plan.terminal_status != "READY":
        store.set_action_target_state(
            source=source,
            state=plan.terminal_status,
            reason=plan.reason,
            updated_at_ms=now_ms(),
        )
        store.append_transition(
            source=source,
            status=plan.terminal_status,
            reason=plan.reason,
            details=plan_details,
        )
        return {"terminal_status": plan.terminal_status, "reason": plan.reason}

    prepared_order: dict[str, Any] | None = None
    prepare_fak = (
        prepare_gtd
        if use_active_cancel_limit
        else getattr(
            execution,
            (
                "prepare_fak_exact_shares"
                if liquidity_retry is not None
                else "prepare_fak_market"
            ),
            None,
        )
    )
    submit_prepared_fak = (
        submit_prepared_gtd
        if use_active_cancel_limit
        else getattr(execution, "submit_prepared_fak_market", None)
    )
    direct_submit_fak = getattr(
        execution,
        (
            "submit_fak_exact_shares"
            if liquidity_retry is not None
            else "submit_fak_market"
        ),
        None,
    )
    if (
        liquidity_retry is not None
        and not use_active_cancel_limit
        and not (
            callable(prepare_fak) and callable(submit_prepared_fak)
        )
        and not callable(direct_submit_fak)
    ):
        reason = "LIQUIDITY_RETRY_EXACT_SHARE_ORDER_UNAVAILABLE"
        store.set_action_target_state(
            source=source,
            state="ERROR_INTERNAL",
            reason=reason,
            updated_at_ms=now_ms(),
        )
        store.append_transition(
            source=source,
            status="ERROR_INTERNAL",
            reason=reason,
            details={**plan_details, "new_order_submitted": False},
        )
        return {"terminal_status": "ERROR_INTERNAL", "reason": reason}
    if callable(prepare_fak) and callable(submit_prepared_fak):
        try:
            prepared_order = dict(
                prepare_fak(
                    token_id=source.token_id,
                    side=source.side,
                    price=plan.worst_price,
                    size=plan.requested_quantity,
                    user_usdc_balance=(
                        Decimal(str(snapshot["collateral_balance_usd"]))
                        if source.side == "BUY"
                        and "collateral_balance_usd" in snapshot
                        else None
                    ),
                )
            )
            prepared_order_id = str(
                prepared_order.get("order_id") or ""
            ).strip().lower()
            if not prepared_order_id:
                raise LiveConfigurationError("PREPARED_ORDER_ID_MISSING")
            prepared_order["order_id"] = prepared_order_id
        except Exception as exc:
            reason = f"ORDER_PREPARATION_ERROR: {type(exc).__name__}: {exc}"
            status = (
                "PENDING_EXTERNAL_RETRY"
                if _is_retryable_external_error(exc)
                else "ERROR_INTERNAL"
            )
            store.set_action_target_state(
                source=source,
                state=status,
                reason=reason,
                updated_at_ms=now_ms(),
            )
            store.append_transition(
                source=source,
                status=status,
                reason=reason,
                details=plan_details,
            )
            if status == "PENDING_EXTERNAL_RETRY":
                raise
            return {"terminal_status": status, "reason": reason}

    started_at_ms = now_ms()
    try:
        attempt = store.begin_submission_attempt(
            source=source,
            plan=plan,
            snapshot=snapshot,
            condition_id=str(snapshot.get("condition_id") or ""),
            created_at_ms=started_at_ms,
            transition_details=plan_details,
            prepared_order=prepared_order,
        )
    except LiveConfigurationError as exc:
        if str(exc) not in {
            "ACTION_HAS_ACTIVE_RESERVATION",
            "ACTION_HAS_UNRESOLVED_ATTEMPT",
        }:
            raise
        reason = "RECOVERY_UNKNOWN_SUBMISSION_WITH_EXISTING_RESERVATION"
        store.set_action_target_state(
            source=source,
            state="UNKNOWN_SUBMISSION",
            reason=reason,
            updated_at_ms=started_at_ms,
        )
        store.append_transition(
            source=source,
            status="UNKNOWN_SUBMISSION",
            reason=reason,
            details=plan_details,
        )
        return {"terminal_status": "UNKNOWN_SUBMISSION", "reason": reason}
    submission_details = {
        **plan_details,
        "attempt_id": attempt["attempt_id"],
        "attempt_number": attempt["attempt_number"],
        "order_id": attempt.get("order_id"),
    }
    try:
        if prepared_order is not None:
            response = submit_prepared_fak(prepared_order)
        else:
            response = direct_submit_fak(
                token_id=source.token_id,
                side=source.side,
                price=plan.worst_price,
                size=plan.requested_quantity,
                user_usdc_balance=(
                    Decimal(str(snapshot["collateral_balance_usd"]))
                    if source.side == "BUY"
                    and "collateral_balance_usd" in snapshot
                    else None
                ),
            )
    except Exception as exc:
        definitive_rejection = _definitive_clob_rejection_reason(exc)
        if definitive_rejection is not None:
            no_match = definitive_rejection == "CLOB_REJECTED_FAK_NO_MATCH"
            liquidity_policy = store.liquidity_retry_policy_for_source(source)
            bounded_retry = (
                None
                if liquidity_policy is not None
                else store.bounded_retry_policy_for_source(source)
            )
            terminal_status = (
                "PENDING_LIQUIDITY"
                if no_match and bounded_retry is not None
                else "EXTERNAL_UNFILLABLE"
                if no_match
                else "ERROR_INTERNAL"
            )
            terminal_reason = (
                "OFFICIAL_FAK_ZERO_FILL_WITHOUT_FINALIZED_CHAIN_PROOF"
                if no_match and liquidity_policy is not None
                else "FAK_ZERO_FILL_NOT_REOPENED"
                if no_match and bounded_retry is None
                else "FAK_ZERO_FILL_RETRYABLE"
                if no_match and bounded_retry is not None
                else definitive_rejection
            )
            finalized_at_ms = now_ms()
            store.release_reservation_and_finalize(
                source=source,
                terminal_status=terminal_status,
                reason=terminal_reason,
                created_at_ms=finalized_at_ms,
                details={
                    **submission_details,
                    "official_rejection_code": definitive_rejection,
                    "exception_type": type(exc).__name__,
                    "bounded_retry": bounded_retry,
                    "liquidity_retry_policy": liquidity_policy,
                    "termination_reason": None,
                },
                attempt_id=str(attempt["attempt_id"]),
                attempt_state="NO_FILL" if no_match else "REJECTED_INTERNAL",
                attempt_response={
                    "official_rejection_code": definitive_rejection,
                    "exception_type": type(exc).__name__,
                },
            )
            return {
                "terminal_status": terminal_status,
                "reason": terminal_reason,
            }
        transport_reason = f"SUBMISSION_TRANSPORT_UNKNOWN:{type(exc).__name__}"
        store.update_attempt_state(
            attempt_id=str(attempt["attempt_id"]),
            state="UNKNOWN_SUBMISSION",
            response={"exception_type": type(exc).__name__},
            updated_at_ms=now_ms(),
        )
        store.set_action_target_state(
            source=source,
            state="UNKNOWN_SUBMISSION",
            reason=transport_reason,
            updated_at_ms=now_ms(),
        )
        store.append_transition(
            source=source,
            status="UNKNOWN_SUBMISSION",
            reason=transport_reason,
            details=submission_details,
        )
        return {
            "terminal_status": "UNKNOWN_SUBMISSION",
            "reason": transport_reason,
        }

    if use_active_cancel_limit and isinstance(response, Mapping):
        response = {
            **dict(response),
            "active_cancel_due_at_ms": (
                int(started_at_ms) + BUY_ACTIVE_CANCEL_WAIT_SECONDS * 1000
            ),
        }

    response_order_id = _submission_order_id(response)
    predicted_order_id = (
        None
        if prepared_order is None
        else str(prepared_order.get("order_id") or "").lower()
    )
    prepared_response_reason = None
    if prepared_order is not None and not response_order_id:
        prepared_response_reason = (
            "MISSING_RESPONSE_ORDER_ID_FOR_PREPARED_SUBMISSION"
        )
    elif (
        predicted_order_id
        and response_order_id
        and response_order_id.lower() != predicted_order_id
    ):
        prepared_response_reason = "SUBMISSION_ORDER_ID_MISMATCH"
    if prepared_response_reason is not None:
        store.update_attempt_state(
            attempt_id=str(attempt["attempt_id"]),
            state="UNKNOWN_SUBMISSION",
            response=response if isinstance(response, dict) else {"response": response},
            updated_at_ms=now_ms(),
        )
        store.set_action_target_state(
            source=source,
            state="UNKNOWN_SUBMISSION",
            reason=prepared_response_reason,
            updated_at_ms=now_ms(),
        )
        store.append_transition(
            source=source,
            status="UNKNOWN_SUBMISSION",
            reason=prepared_response_reason,
            details={**submission_details, "response": response},
        )
        return {
            "terminal_status": "UNKNOWN_SUBMISSION",
            "reason": prepared_response_reason,
        }

    if isinstance(response, dict) and response.get("success") is False:
        rejection_message = str(response.get("errorMsg") or "UNKNOWN")
        reason = _definitive_clob_response_rejection_reason(rejection_message)
        if reason is None:
            reason = "CLOB_REJECTED: " + rejection_message
        no_match = reason == "CLOB_REJECTED_FAK_NO_MATCH"
        liquidity_policy = store.liquidity_retry_policy_for_source(source)
        bounded_retry = (
            None
            if liquidity_policy is not None
            else store.bounded_retry_policy_for_source(source)
        )
        terminal_status = (
            "PENDING_LIQUIDITY"
            if no_match and bounded_retry is not None
            else "EXTERNAL_UNFILLABLE"
            if no_match
            else "ERROR_INTERNAL"
        )
        terminal_reason = (
            "OFFICIAL_FAK_ZERO_FILL_WITHOUT_FINALIZED_CHAIN_PROOF"
            if no_match and liquidity_policy is not None
            else "FAK_ZERO_FILL_NOT_REOPENED"
            if no_match and bounded_retry is None
            else "FAK_ZERO_FILL_RETRYABLE"
            if no_match and bounded_retry is not None
            else reason
        )
        finalized_at_ms = now_ms()
        store.release_reservation_and_finalize(
            source=source,
            terminal_status=terminal_status,
            reason=terminal_reason,
            created_at_ms=finalized_at_ms,
            details={
                **submission_details,
                "response": response,
                "bounded_retry": bounded_retry,
                "liquidity_retry_policy": liquidity_policy,
                "termination_reason": None,
            },
            attempt_id=str(attempt["attempt_id"]),
            attempt_state="NO_FILL" if no_match else "REJECTED_INTERNAL",
            attempt_response=response,
        )
        return {"terminal_status": terminal_status, "reason": terminal_reason}

    order_id = response_order_id or predicted_order_id
    if not order_id:
        store.update_attempt_state(
            attempt_id=str(attempt["attempt_id"]),
            state="UNKNOWN_SUBMISSION",
            response=response if isinstance(response, dict) else {"response": response},
            updated_at_ms=now_ms(),
        )
        store.set_action_target_state(
            source=source,
            state="UNKNOWN_SUBMISSION",
            reason="MISSING_ORDER_ID_IN_SUBMISSION_RESPONSE",
            updated_at_ms=now_ms(),
        )
        store.append_transition(
            source=source,
            status="UNKNOWN_SUBMISSION",
            reason="MISSING_ORDER_ID_IN_SUBMISSION_RESPONSE",
            details={**submission_details, "response": response},
        )
        return {
            "terminal_status": "UNKNOWN_SUBMISSION",
            "reason": "MISSING_ORDER_ID_IN_SUBMISSION_RESPONSE",
        }
    store.set_attempt_order_id(
        attempt_id=str(attempt["attempt_id"]),
        order_id=order_id,
        response=response if isinstance(response, dict) else {"response": response},
        updated_at_ms=now_ms(),
    )
    store.set_action_target_state(
        source=source,
        state="SUBMITTED_UNRECONCILED",
        reason=plan.reason,
        updated_at_ms=now_ms(),
    )
    store.set_runtime("real_order_submitted", "true")
    store.append_transition(
        source=source,
        status="SUBMITTED_UNRECONCILED",
        reason=plan.reason,
        details={**submission_details, "order_id": order_id, "response": response},
    )
    return {"terminal_status": "SUBMITTED_UNRECONCILED", "reason": plan.reason}


def retry_pending_actions(
    *,
    store: LiveStore,
    execution: Any,
    wallet_lock_path: Path | None = None,
    coordinator: SharedWalletCoordinator | None = None,
    profile_key: str | None = None,
    sources: list[SourceAction] | None = None,
    market_lifecycle_resolver: Callable[[SourceAction], Any] | None = None,
    process_action: Callable[[SourceAction], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Retry one proven V2 liquidity remainder per new processable head."""

    results: list[dict[str, Any]] = []
    pending_sources = store.retryable_actions() if sources is None else list(sources)
    pending_sources = [
        source
        for source in pending_sources
        if store.liquidity_retry_evidence(source) is not None
    ]
    if not pending_sources:
        return results

    # GTD replaces the old liquidity-retry policy. Close every proven old
    # remainder without submitting another order, then let future actions use
    # one fresh GTD order only.
    for source in pending_sources:
        target = store.action_target(source.action_id)
        if target is None:
            raise LiveConfigurationError("RETRYABLE_TARGET_MISSING")
        filled = Decimal(str(target["cumulative_filled_quantity"]))
        terminal = "PARTIAL" if filled > ZERO else "EXTERNAL_UNFILLABLE"
        reason = "GTD_POLICY_REPLACED_LIQUIDITY_RETRY"
        closed_at_ms = now_ms()
        store.set_action_target_state(
            source=source,
            state=terminal,
            reason=reason,
            updated_at_ms=closed_at_ms,
        )
        store.append_transition(
            source=source,
            status=terminal,
            reason=reason,
            created_at_ms=closed_at_ms,
            details={"new_order_submitted": False},
        )
        results.append({"terminal_status": terminal, "reason": reason})
    return results

    def causal_key(source: SourceAction) -> tuple[int, int, int, str, str, str, str]:
        return (
            int(source.block_number),
            int(source.log_index),
            int(source.source_timestamp),
            str(source.transaction_hash).lower(),
            str(source.token_id),
            str(source.side).upper(),
            str(source.order_hash).lower(),
        )

    pending_sources.sort(key=causal_key)
    last_raw = store.runtime_value("pending_retry_after_causal_key")
    last_key: tuple[int, int, int, str, str, str, str] | None = None
    if last_raw:
        try:
            decoded = json.loads(last_raw)
            if not isinstance(decoded, list) or len(decoded) != 7:
                raise ValueError("invalid causal retry cursor shape")
            last_key = (
                int(decoded[0]),
                int(decoded[1]),
                int(decoded[2]),
                str(decoded[3]),
                str(decoded[4]),
                str(decoded[5]),
                str(decoded[6]),
            )
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise LiveConfigurationError("INVALID_PENDING_RETRY_CURSOR") from exc
    source = next(
        (
            candidate
            for candidate in pending_sources
            if last_key is None or causal_key(candidate) > last_key
        ),
        pending_sources[0],
    )
    latest = store.latest_transition(source)
    if latest is None:
        raise LiveConfigurationError("RETRYABLE_TARGET_MISSING_TRANSITION")
    target = store.action_target(source.action_id)
    if target is None:
        raise LiveConfigurationError("RETRYABLE_TARGET_MISSING")
    retry_evidence = store.liquidity_retry_evidence(source)
    if retry_evidence is None:
        return results

    lifecycle_details: dict[str, Any] | None = None
    if not callable(market_lifecycle_resolver):
        retained_state = str(target["state"])
        reason = "OFFICIAL_MARKET_STATE_RESOLVER_UNAVAILABLE_FOR_LIQUIDITY_RETRY"
        store.set_action_target_state(
            source=source,
            state=retained_state,
            reason=reason,
            updated_at_ms=now_ms(),
        )
        store.append_transition(
            source=source,
            status=retained_state,
            reason=reason,
            details={
                "liquidity_retry": retry_evidence,
                "new_order_submitted": False,
            },
        )
        result = {"terminal_status": retained_state, "reason": reason}
        results.append(result)
        store.set_runtime(
            "pending_retry_after_causal_key",
            json.dumps(list(causal_key(source)), separators=(",", ":")),
        )
        return results
    if callable(market_lifecycle_resolver):
        try:
            lifecycle = market_lifecycle_resolver(source)
        except Exception as exc:
            retained_state = str(target["state"])
            reason = "OFFICIAL_MARKET_STATE_UNAVAILABLE_FOR_LIQUIDITY_RETRY"
            store.set_action_target_state(
                source=source,
                state=retained_state,
                reason=reason,
                updated_at_ms=now_ms(),
            )
            store.append_transition(
                source=source,
                status=retained_state,
                reason=reason,
                details={
                    "external_error": f"{type(exc).__name__}: {exc}",
                    "liquidity_retry": retry_evidence,
                    "new_order_submitted": False,
                },
            )
            result = {"terminal_status": retained_state, "reason": reason}
            results.append(result)
            store.set_runtime(
                "pending_retry_after_causal_key",
                json.dumps(list(causal_key(source)), separators=(",", ":")),
            )
            return results
        if isinstance(lifecycle, Mapping):
            lifecycle_details = dict(lifecycle)
        else:
            raw_details = getattr(lifecycle, "metadata", None)
            lifecycle_details = (
                dict(raw_details) if isinstance(raw_details, Mapping) else {}
            )
        market_closed = bool(lifecycle_details.get("closed")) or (
            lifecycle_details.get("accepting_orders") is False
        )
        if market_closed:
            cumulative = Decimal(
                str(target["cumulative_filled_quantity"])
            )
            terminal_status = "PARTIAL" if cumulative > ZERO else "EXTERNAL_UNFILLABLE"
            reason = "OFFICIAL_MARKET_CLOSED_BEFORE_RETRY"
            store.set_action_target_state(
                source=source,
                state=terminal_status,
                reason=reason,
                updated_at_ms=now_ms(),
            )
            store.append_transition(
                source=source,
                status=terminal_status,
                reason=reason,
                details={
                    "official_market_lifecycle": lifecycle_details,
                    "liquidity_retry": retry_evidence,
                    "new_order_submitted": False,
                },
            )
            result = {"terminal_status": terminal_status, "reason": reason}
            results.append(result)
            store.set_runtime(
                "pending_retry_after_causal_key",
                json.dumps(list(causal_key(source)), separators=(",", ":")),
            )
            return results

    if process_action is None:
        raise LiveConfigurationError("LIQUIDITY_RETRY_PROCESSOR_REQUIRED")
    result = process_action(source)
    store.set_runtime(
        "pending_retry_after_causal_key",
        json.dumps(list(causal_key(source)), separators=(",", ":")),
    )
    results.append(result)
    return results


def _matched_shares(
    order: dict[str, Any], *, expected_quantity: Decimal
) -> tuple[Decimal, str]:
    """Return CLOB matched shares with an explicit representation receipt.

    The documented order endpoint describes fixed-math strings, while the
    current Python SDK's authenticated order response has also returned
    human-share strings (for example ``original_size == size_matched == '5'``)
    for a five-share order.  Never infer from the presence of a decimal point:
    choose between the two representations only by comparing the accompanying
    ``original_size`` to the immutable pre-submission target.
    """

    raw = str(order.get("size_matched", "")).strip()
    original_raw = str(order.get("original_size", "")).strip()
    if not raw:
        raise RuntimeError("MISSING_SIZE_MATCHED")
    if not original_raw:
        raise RuntimeError("MISSING_ORIGINAL_SIZE")
    try:
        expected = Decimal(str(expected_quantity))
        matched = Decimal(raw)
        original = Decimal(original_raw)
    except InvalidOperation as exc:
        raise RuntimeError("INVALID_ORDER_SIZE_ENCODING") from exc
    if expected <= ZERO or matched < ZERO or original <= ZERO:
        raise RuntimeError("INVALID_ORDER_SIZE_VALUES")

    candidates = (
        (matched, original, "HUMAN_SHARES"),
        (matched / TOKEN_SCALE, original / TOKEN_SCALE, "FIXED_MATH_6DP"),
    )
    compatible = [
        candidate
        for candidate in candidates
        if candidate[1] <= expected
    ]
    if not compatible:
        raise RuntimeError("ORDER_ORIGINAL_SIZE_EXCEEDS_RECORDED_TARGET")
    quantity, _original_quantity, encoding = min(
        compatible,
        key=lambda candidate: abs(expected - candidate[1]),
    )
    return quantity, encoding


def _official_associated_trade_execution(
    *,
    execution: Any,
    source: SourceAction,
    order_id: str,
    order: Mapping[str, Any],
    matched_quantity: Decimal,
) -> dict[str, Any]:
    """Validate authenticated trades and return their on-chain receipt keys."""

    order_status = str(order.get("status", "")).strip().upper()
    if order_status not in {
        "MATCHED",
        "ORDER_STATUS_MATCHED",
        "CANCELED",
        "ORDER_STATUS_CANCELED",
        "ORDER_STATUS_CANCELED_MARKET_RESOLVED",
    }:
        raise RuntimeError("ORDER_STATUS_DOES_NOT_PROVE_FINAL_MATCH")
    raw_trade_ids = order.get("associate_trades")
    if not isinstance(raw_trade_ids, list):
        raise RuntimeError("MISSING_ASSOCIATED_TRADE_IDS")
    trade_ids = [str(value).strip() for value in raw_trade_ids]
    if (
        not trade_ids
        or any(not value for value in trade_ids)
        or len(set(trade_ids)) != len(trade_ids)
    ):
        raise RuntimeError("INVALID_ASSOCIATED_TRADE_IDS")
    reader = getattr(execution, "get_associated_trades", None)
    if not callable(reader):
        raise RuntimeError("OFFICIAL_ASSOCIATED_TRADE_READER_UNAVAILABLE")
    trades = reader(order_id=order_id, trade_ids=trade_ids)
    if not isinstance(trades, list) or len(trades) != len(trade_ids):
        raise RuntimeError("OFFICIAL_ASSOCIATED_TRADES_INCOMPLETE")

    parsed: list[tuple[Decimal, dict[str, Any]]] = []
    returned_ids: list[str] = []
    transaction_hashes: list[str] = []
    for trade in trades:
        if not isinstance(trade, Mapping):
            raise RuntimeError("INVALID_OFFICIAL_ASSOCIATED_TRADE")
        trade_id = str(trade.get("id", "")).strip()
        returned_ids.append(trade_id)
        if str(trade.get("taker_order_id", "")).strip().lower() != order_id.lower():
            raise RuntimeError("OFFICIAL_ASSOCIATED_TRADE_ORDER_MISMATCH")
        if str(trade.get("asset_id", "")).strip() != str(source.token_id):
            raise RuntimeError("OFFICIAL_ASSOCIATED_TRADE_ASSET_MISMATCH")
        if str(trade.get("side", "")).strip().upper() != source.side:
            raise RuntimeError("OFFICIAL_ASSOCIATED_TRADE_SIDE_MISMATCH")
        if str(trade.get("trader_side", "")).strip().upper() != "TAKER":
            raise RuntimeError("OFFICIAL_ASSOCIATED_TRADE_ROLE_MISMATCH")
        trade_status = str(trade.get("status", "")).strip().upper()
        if trade_status not in {
            "MATCHED",
            "MINED",
            "CONFIRMED",
            "TRADE_STATUS_MATCHED",
            "TRADE_STATUS_MINED",
            "TRADE_STATUS_CONFIRMED",
        }:
            raise RuntimeError("OFFICIAL_ASSOCIATED_TRADE_NOT_CONFIRMED")
        transaction_hash = str(trade.get("transaction_hash", "")).strip().lower()
        if re.fullmatch(r"0x[a-f0-9]{64}", transaction_hash) is None:
            raise RuntimeError("INVALID_OFFICIAL_ASSOCIATED_TRADE_TRANSACTION")
        transaction_hashes.append(transaction_hash)
        try:
            raw_size = Decimal(str(trade["size"]))
            price = Decimal(str(trade["price"]))
            fee_bps = Decimal(str(trade["fee_rate_bps"]))
        except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
            raise RuntimeError("INVALID_OFFICIAL_ASSOCIATED_TRADE_NUMBERS") from exc
        if (
            not raw_size.is_finite()
            or raw_size <= ZERO
            or not price.is_finite()
            or price <= ZERO
            or price > Decimal("1")
            or not fee_bps.is_finite()
            or fee_bps < ZERO
            or fee_bps != fee_bps.to_integral_value()
        ):
            raise RuntimeError("INVALID_OFFICIAL_ASSOCIATED_TRADE_NUMBERS")
        parsed.append((raw_size, dict(trade)))
    if returned_ids != trade_ids:
        raise RuntimeError("OFFICIAL_ASSOCIATED_TRADE_IDS_MISMATCH")

    raw_total = sum((item[0] for item in parsed), ZERO)
    encodings = [
        ("HUMAN_SHARES", Decimal("1")),
        ("FIXED_MATH_6DP", TOKEN_SCALE),
    ]
    compatible = [
        (encoding, scale)
        for encoding, scale in encodings
        if raw_total / scale == matched_quantity
    ]
    if len(compatible) != 1:
        raise RuntimeError("OFFICIAL_ASSOCIATED_TRADE_QUANTITY_MISMATCH")
    quantity_encoding, _quantity_scale = compatible[0]
    return {
        "quantity": matched_quantity,
        "quantity_encoding": quantity_encoding,
        "trade_evidence": [item[1] for item in parsed],
        "transaction_hashes": list(dict.fromkeys(transaction_hashes)),
    }


def _recorded_minimum_order_size_from_snapshot(snapshot: Any) -> Decimal | None:
    if not isinstance(snapshot, Mapping):
        raise LiveConfigurationError("INVALID_RECORDED_SUBMISSION_SNAPSHOT")
    raw_minimum = snapshot.get("minimum_order_size")
    if raw_minimum in {None, ""}:
        return None
    try:
        original_minimum = Decimal(str(raw_minimum))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise LiveConfigurationError(
            "INVALID_RECORDED_MINIMUM_ORDER_SIZE"
        ) from exc
    if original_minimum <= ZERO:
        raise LiveConfigurationError("INVALID_RECORDED_MINIMUM_ORDER_SIZE")
    return original_minimum


def _partial_remainder_below_recorded_minimum(
    *,
    remaining_quantity: Decimal,
    original_minimum_order_size: Decimal | None,
) -> dict[str, str] | None:
    """Return immutable evidence when a FAK residue cannot reach its old min."""

    if remaining_quantity <= ZERO or original_minimum_order_size is None:
        return None
    original_minimum = Decimal(str(original_minimum_order_size))
    if original_minimum <= ZERO:
        raise LiveConfigurationError("INVALID_RECORDED_MINIMUM_ORDER_SIZE")
    if remaining_quantity >= original_minimum:
        return None
    return {
        "original_minimum_order_size": str(original_minimum),
        "remaining_quantity": str(remaining_quantity),
    }


def reconcile_submitted_actions(
    *,
    store: LiveStore,
    execution: Any,
    attempt_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Resolve accepted FAK orders from the authenticated official order view.

    A failed reconciliation remains unreconciled and reserved; it is never
    resubmitted.  This preserves a crash-safe cash and inventory envelope.
    """

    results: list[dict[str, Any]] = []
    selected_attempt_ids = (
        None if attempt_ids is None else {str(value) for value in attempt_ids}
    )
    for source, details in store.unreconciled_submissions():
        if (
            selected_attempt_ids is not None
            and str(details.get("attempt_id") or "") not in selected_attempt_ids
        ):
            continue
        recorded_order_id = details.get("order_id")
        order_id = (
            "" if recorded_order_id is None else str(recorded_order_id).strip()
        )
        if not order_id:
            reason = "MISSING_ORDER_ID_FOR_RECONCILIATION"
            store.retain_missing_order_id_as_unknown(
                source=source,
                attempt_id=str(details.get("attempt_id") or ""),
                created_at_ms=now_ms(),
            )
            results.append({"terminal_status": "UNKNOWN_SUBMISSION", "reason": reason})
            continue
        try:
            expected = Decimal(
                str(details.get("plan", {}).get("requested_quantity", "0"))
            )
        except (InvalidOperation, TypeError, ValueError) as exc:
            reason = f"INVALID_RECORDED_TARGET: {type(exc).__name__}: {exc}"
            store.append_transition(
                source=source,
                status="ERROR",
                reason=reason,
                created_at_ms=now_ms(),
                details={"order_id": order_id, "details": details},
            )
            results.append({"terminal_status": "ERROR", "reason": reason})
            continue
        if expected <= ZERO:
            reason = "INVALID_RECORDED_TARGET_NONPOSITIVE"
            store.append_transition(
                source=source,
                status="ERROR",
                reason=reason,
                created_at_ms=now_ms(),
                details={"order_id": order_id, "details": details},
            )
            results.append({"terminal_status": "ERROR", "reason": reason})
            continue
        execution_order_type = str(
            details.get("execution_order_type") or "UNKNOWN_LEGACY"
        ).upper()
        recorded_response = details.get("response")
        active_cancel_verified = (
            execution_order_type == "GTC_ACTIVE_CANCEL"
            and isinstance(recorded_response, Mapping)
            and recorded_response.get("active_cancel_verified") is True
        )
        prefetched_order: dict[str, Any] | None = None
        if execution_order_type in {"GTD", "GTC_ACTIVE_CANCEL"}:
            try:
                observed_order = execution.get_order(order_id)
            except Exception as exc:
                store.record_external_reconciliation_incident(
                    occurred_at_ms=now_ms(),
                    category="EXTERNAL_ORDER_RECONCILIATION",
                    message=f"{type(exc).__name__}: {exc}",
                    details={"order_id": order_id, "action_id": source.action_id},
                )
                results.append(
                    {"terminal_status": "PENDING", "reason": "ORDER_RECONCILIATION_UNAVAILABLE"}
                )
                continue
            if isinstance(observed_order, Mapping):
                prefetched_order = dict(observed_order)
                observed_status = str(observed_order.get("status", "")).upper()
                if observed_status not in {
                    "MATCHED",
                    "ORDER_STATUS_MATCHED",
                    "ORDER_STATUS_INVALID",
                    "ORDER_STATUS_CANCELED",
                    "ORDER_STATUS_CANCELED_MARKET_RESOLVED",
                    "INVALID",
                    "CANCELED",
                } and not active_cancel_verified:
                    results.append(
                        {"terminal_status": "PENDING", "reason": "GTD_ORDER_STILL_OPEN"}
                    )
                    continue
        response = recorded_response
        authoritative_reader = getattr(execution, "authoritative_submission_execution", None)
        authoritative: dict[str, Any] | None = None
        if callable(authoritative_reader):
            try:
                authoritative = authoritative_reader(
                    source=source,
                    order_id=order_id,
                    response=response,
                )
            except Exception as exc:
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category=(
                        "EXTERNAL_ONCHAIN_FILL_RECONCILIATION"
                        if _is_retryable_external_error(exc)
                        else "INTERNAL_ONCHAIN_FILL_RECONCILIATION"
                    ),
                    message=f"{type(exc).__name__}: {exc}",
                    details={"order_id": order_id, "action_id": source.action_id},
                )
                # The CLOB transaction hash can be temporarily unavailable
                # from Polygon RPCs.  Preserve the external error, then use
                # the finalized order-hash scan below before leaving PENDING.
        order_hash_reader = getattr(execution, "authoritative_order_hash_execution", None)
        if authoritative is None and callable(order_hash_reader):
            try:
                authoritative = order_hash_reader(source=source, order_id=order_id)
            except Exception as exc:
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category=("EXTERNAL_ONCHAIN_ORDER_HASH_RECONCILIATION" if _is_retryable_external_error(exc) else "INTERNAL_ONCHAIN_ORDER_HASH_RECONCILIATION"),
                    message=f"{type(exc).__name__}: {exc}",
                    details={"order_id": order_id, "action_id": source.action_id},
                )
        if authoritative is not None:
                if authoritative.get("authoritative_no_fill") is True:
                    attempt_id = str(details.get("attempt_id") or "")
                    if not attempt_id:
                        raise LiveConfigurationError("MISSING_ATTEMPT_ID_FOR_RECONCILIATION")
                    liquidity_policy = store.liquidity_retry_policy_for_source(
                        source
                    )
                    terminal_status = (
                        "EXTERNAL_UNFILLABLE"
                        if execution_order_type in {"GTD", "GTC_ACTIVE_CANCEL"}
                        else
                        "PENDING_CONFIRMED_ZERO_FILL"
                        if liquidity_policy is not None
                        else "EXTERNAL_UNFILLABLE"
                    )
                    reason = (
                        "GTC_ACTIVE_CANCEL_ZERO_FILL"
                        if execution_order_type == "GTC_ACTIVE_CANCEL"
                        else "GTD_EXPIRED_ZERO_FILL"
                        if execution_order_type == "GTD"
                        else
                        "FINALIZED_CHAIN_PROVES_FAK_ZERO_FILL_RETRYABLE"
                        if liquidity_policy is not None
                        else "FINALIZED_CHAIN_PROVES_FAK_ZERO_FILL"
                    )
                    store.release_reservation_and_finalize(
                        source=source,
                        terminal_status=terminal_status,
                        reason=reason,
                        created_at_ms=now_ms(),
                        details={
                            "order_id": order_id,
                            "attempt_id": attempt_id,
                            "attempt_number": details.get("attempt_number"),
                            "chain_scan": {
                                "from_block": authoritative.get("scan_from_block"),
                                "to_block": authoritative.get("scan_to_block"),
                                "order_filled_log_count": 0,
                                "finality": authoritative.get("finality"),
                            },
                            "historical_repost": False,
                            "liquidity_retry_policy": liquidity_policy,
                        },
                        attempt_id=attempt_id,
                        attempt_state="NO_FILL",
                        attempt_response={
                            "order_id": order_id,
                            "result": "finalized_chain_zero_fill",
                        },
                    )
                    results.append(
                        {"terminal_status": terminal_status, "reason": reason}
                    )
                    continue
                try:
                    quantity = Decimal(str(authoritative["quantity"]))
                    notional = Decimal(str(authoritative["notional_usd"]))
                    fee = Decimal(str(authoritative["fee_usd"]))
                    vwap = Decimal(str(authoritative["vwap_price"]))
                except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
                    raise LiveConfigurationError("INVALID_AUTHORITATIVE_FILL_RESULT") from exc
                if quantity <= ZERO or notional <= ZERO or fee < ZERO or vwap <= ZERO:
                    raise LiveConfigurationError("INVALID_AUTHORITATIVE_FILL_RESULT")
                target = store.action_target(source.action_id)
                remaining = (
                    expected
                    if target is None
                    else Decimal(str(target["remaining_quantity"]))
                )
                buy_price_improvement_surplus = (
                    quantity > expected or quantity > remaining
                )
                maximum_buy_notional: Decimal | None = None
                maximum_buy_total_cost: Decimal | None = None
                buy_cash_order_complete = False
                if source.side == "BUY" and quantity != remaining:
                    raw_maximum_buy_notional = details.get("plan", {}).get(
                        "order_amount_usd"
                    )
                    if raw_maximum_buy_notional not in {None, ""}:
                        try:
                            maximum_buy_notional = Decimal(
                                str(raw_maximum_buy_notional)
                            )
                        except (InvalidOperation, TypeError, ValueError) as exc:
                            raise LiveConfigurationError(
                                "INVALID_RECORDED_BUY_ORDER_NOTIONAL"
                            ) from exc
                        if (
                            maximum_buy_notional <= ZERO
                            or notional > maximum_buy_notional
                        ):
                            raise LiveConfigurationError(
                                "BUY_PRICE_IMPROVEMENT_EXCEEDS_FROZEN_ORDER_NOTIONAL"
                            )
                        snapshot_details = details.get("snapshot", {})
                        try:
                            maximum_buy_total_cost = (
                                maximum_buy_notional
                                + maximum_buy_fee_usd(
                                    order_amount_usd=maximum_buy_notional,
                                    taker_fee_bps=Decimal(
                                        str(snapshot_details.get("fee_bps", "0"))
                                    ),
                                    fee_exponent=Decimal(
                                        str(snapshot_details.get("fee_exponent", "1"))
                                    ),
                                    minimum_fill_price=(
                                        None
                                        if snapshot_details.get("tick_size") in {None, ""}
                                        else Decimal(
                                            str(snapshot_details.get("tick_size"))
                                        )
                                    ),
                                    maximum_fill_price=Decimal(
                                        str(details.get("plan", {}).get("worst_price"))
                                    ),
                                )
                            )
                        except (InvalidOperation, TypeError, ValueError) as exc:
                            raise LiveConfigurationError(
                                "INVALID_RECORDED_BUY_FEE_BOUND"
                            ) from exc
                        buy_cash_order_complete = (
                            quantity < remaining
                            and maximum_buy_notional - notional <= TOKEN_RAW_UNIT
                        )
                if buy_price_improvement_surplus:
                    if source.side != "BUY" or maximum_buy_notional is None:
                        raise LiveConfigurationError(
                            "FILL_EXCEEDS_ACTION_TARGET_REMAINDER"
                        )
                if (
                    target is not None
                    and quantity < remaining
                    and not buy_cash_order_complete
                    and store.liquidity_retry_policy_for_source(source) is not None
                    and not _has_chain_receipt_evidence(
                        authoritative.get("receipt_evidence")
                    )
                ):
                    reason = "ONCHAIN_FILL_RECEIPT_EVIDENCE_INVALID"
                    store.append_runtime_error(
                        occurred_at_ms=now_ms(),
                        category="INTERNAL_ONCHAIN_FILL_RECONCILIATION",
                        message=reason,
                        details={
                            "order_id": order_id,
                            "action_id": source.action_id,
                        },
                    )
                    results.append(
                        {"terminal_status": "PENDING", "reason": reason}
                    )
                    continue
                terminal = (
                    "FILLED"
                    if quantity >= remaining or buy_cash_order_complete
                    else "PARTIAL"
                    if execution_order_type in {"GTD", "GTC_ACTIVE_CANCEL"}
                    else "PARTIAL" if target is None else "PARTIAL_PENDING"
                )
                reason = (
                    "OFFICIAL_ONCHAIN_BUY_PRICE_IMPROVEMENT_FILL"
                    if buy_price_improvement_surplus
                    else "OFFICIAL_ONCHAIN_BUY_CASH_ORDER_COMPLETE"
                    if buy_cash_order_complete
                    else "OFFICIAL_ONCHAIN_FILL_RECEIPT"
                    if terminal == "FILLED"
                    else "GTC_ACTIVE_CANCEL_PARTIAL_FILL"
                    if execution_order_type == "GTC_ACTIVE_CANCEL"
                    else "GTD_EXPIRED_PARTIAL_FILL"
                    if execution_order_type == "GTD"
                    else "FAK_PARTIAL_FILL"
                )
                partial_remainder_evidence = None
                if target is not None and terminal == "PARTIAL_PENDING":
                    partial_remainder_evidence = (
                        _partial_remainder_below_recorded_minimum(
                            remaining_quantity=remaining - quantity,
                            original_minimum_order_size=(
                                store.original_submission_minimum_order_size(
                                    action_id=source.action_id
                                )
                            ),
                        )
                    )
                    if partial_remainder_evidence is not None:
                        terminal = "EXTERNAL_UNFILLABLE"
                        reason = (
                            "PARTIAL_REMAINDER_BELOW_ORIGINAL_MARKET_MINIMUM"
                        )
                # The authenticated receipt is the first durable authority that
                # proves the prepared order reached the venue.  Persist that
                # fact before local accounting so a later local failure cannot
                # restore the unsafe false-negative runtime flag.
                store.set_runtime("real_order_submitted", "true")
                store.apply_fill_and_finalize(
                    source=source,
                    quantity=quantity,
                    price=vwap,
                    notional_usd=notional,
                    fee_usd=fee,
                    terminal_status=terminal,
                    reason=reason,
                    created_at_ms=now_ms(),
                    details={
                        "order_id": order_id,
                        "execution_order_type": str(
                            details.get("execution_order_type") or "UNKNOWN_LEGACY"
                        ),
                        "matched_quantity": str(quantity),
                        "matched_notional_usd": str(notional),
                        "matched_price": str(vwap),
                        "fee_usd": str(fee),
                        "receipt_evidence": authoritative.get("receipt_evidence", []),
                        "attempt_id": str(details.get("attempt_id") or ""),
                        "attempt_number": details.get("attempt_number"),
                        "planned_requested_quantity": str(expected),
                        "planned_buy_notional_usd": (
                            None
                            if maximum_buy_notional is None
                            else str(maximum_buy_notional)
                        ),
                        "maximum_buy_total_cost_usd": (
                            None
                            if maximum_buy_total_cost is None
                            else str(maximum_buy_total_cost)
                        ),
                        "buy_price_improvement_share_surplus": str(
                            max(quantity - remaining, ZERO)
                        ),
                        "buy_cash_order_share_shortfall": str(
                            max(remaining - quantity, ZERO)
                            if buy_cash_order_complete
                            else ZERO
                        ),
                        "partial_remainder_evidence": partial_remainder_evidence,
                    },
                    maximum_buy_notional_usd=(
                        maximum_buy_notional
                        if buy_price_improvement_surplus
                        or buy_cash_order_complete
                        else None
                    ),
                    maximum_buy_total_cost_usd=(
                        maximum_buy_total_cost
                        if buy_price_improvement_surplus
                        or buy_cash_order_complete
                        else None
                    ),
                    buy_cash_order_complete=buy_cash_order_complete,
                )
                results.append({"terminal_status": terminal, "reason": reason})
                continue
        try:
            order = (
                prefetched_order
                if prefetched_order is not None
                else execution.get_order(order_id)
            )
        except Exception as exc:
            store.record_external_reconciliation_incident(
                occurred_at_ms=now_ms(),
                category="EXTERNAL_ORDER_RECONCILIATION",
                message=f"{type(exc).__name__}: {exc}",
                details={"order_id": order_id, "action_id": source.action_id},
            )
            results.append({"terminal_status": "PENDING", "reason": "ORDER_RECONCILIATION_UNAVAILABLE"})
            continue
        if order is None:
            # A delayed accepted FAK can disappear from the CLOB order view
            # without a terminal fill proof.  Its absence is not evidence that
            # no side effect occurred, so retain the reservation and only
            # permit future read-only reconciliation.
            reason = "OFFICIAL_ORDER_NOT_FOUND"
            attempt_id = str(details.get("attempt_id") or "")
            if not attempt_id:
                raise LiveConfigurationError("MISSING_ATTEMPT_ID_FOR_RECONCILIATION")
            store.mark_official_order_absent_as_unknown(
                source=source, attempt_id=attempt_id, order_id=order_id,
                created_at_ms=now_ms(),
            )
            results.append({"terminal_status": "UNKNOWN_SUBMISSION", "reason": reason})
            continue
        status = str(order.get("status", "")).upper()
        try:
            quantity, quantity_encoding = _matched_shares(
                order,
                expected_quantity=expected,
            )
        except Exception as exc:
            reason = f"INVALID_ORDER_RECONCILIATION: {type(exc).__name__}: {exc}"
            store.append_runtime_error(
                occurred_at_ms=now_ms(),
                category="INTERNAL_ORDER_RECONCILIATION",
                message=reason,
                details={"order_id": order_id, "action_id": source.action_id},
            )
            results.append({"terminal_status": "PENDING", "reason": reason})
            continue
        # A prepared order id is not evidence of submission.  Only a valid
        # authenticated official-order payload may repair this runtime flag.
        store.set_runtime("real_order_submitted", "true")
        if quantity > ZERO:
            try:
                official = _official_associated_trade_execution(
                    execution=execution,
                    source=source,
                    order_id=order_id,
                    order=order,
                    matched_quantity=quantity,
                )
                receipt_reader = getattr(
                    execution, "authoritative_submission_execution", None
                )
                if not callable(receipt_reader):
                    raise RuntimeError("ONCHAIN_FILL_RECEIPT_READER_UNAVAILABLE")
                authoritative = receipt_reader(
                    source=source,
                    order_id=order_id,
                    response={
                        "success": True,
                        "status": "matched",
                        "transactionsHashes": official["transaction_hashes"],
                    },
                )
                if not isinstance(authoritative, Mapping):
                    raise RuntimeError("ONCHAIN_FILL_RECEIPT_UNAVAILABLE")
                receipt_quantity = Decimal(str(authoritative["quantity"]))
                notional = Decimal(str(authoritative["notional_usd"]))
                fee = Decimal(str(authoritative["fee_usd"]))
                vwap = Decimal(str(authoritative["vwap_price"]))
                receipt_evidence = authoritative.get("receipt_evidence")
                if (
                    not receipt_quantity.is_finite()
                    or receipt_quantity != quantity
                    or not notional.is_finite()
                    or notional <= ZERO
                    or not fee.is_finite()
                    or fee < ZERO
                    or not vwap.is_finite()
                    or vwap <= ZERO
                    or vwap > Decimal("1")
                    or vwap != notional / receipt_quantity
                    or not isinstance(receipt_evidence, list)
                    or not receipt_evidence
                    or {
                        str(item.get("transaction_hash", "")).strip().lower()
                        for item in receipt_evidence
                        if isinstance(item, Mapping)
                    }
                    != set(official["transaction_hashes"])
                ):
                    raise RuntimeError("INVALID_ONCHAIN_FILL_RECEIPT_RESULT")
            except Exception as exc:
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category=(
                        "EXTERNAL_OFFICIAL_ASSOCIATED_TRADE_RECONCILIATION"
                        if _is_retryable_external_error(exc)
                        else "INTERNAL_OFFICIAL_ASSOCIATED_TRADE_RECONCILIATION"
                    ),
                    message=f"{type(exc).__name__}: {exc}",
                    details={"order_id": order_id, "action_id": source.action_id},
                )
                results.append(
                    {
                        "terminal_status": "PENDING",
                        "reason": "OFFICIAL_ASSOCIATED_TRADE_RECONCILIATION_UNAVAILABLE",
                    }
                )
                continue
            target = store.action_target(source.action_id)
            remaining = (
                expected
                if target is None
                else Decimal(str(target["remaining_quantity"]))
            )
            buy_price_improvement_surplus = (
                quantity > expected or quantity > remaining
            )
            maximum_buy_notional: Decimal | None = None
            maximum_buy_total_cost: Decimal | None = None
            buy_cash_order_complete = False
            if source.side == "BUY" and quantity != remaining:
                try:
                    snapshot_details = details.get("snapshot")
                    if (
                        not isinstance(snapshot_details, Mapping)
                        or "fee_exponent" not in snapshot_details
                    ):
                        raise RuntimeError("MISSING_RECORDED_FEE_EXPONENT")
                    fee_exponent = Decimal(
                        str(snapshot_details["fee_exponent"])
                    )
                    raw_maximum_buy_notional = details.get("plan", {}).get(
                        "order_amount_usd"
                    )
                    if raw_maximum_buy_notional in {None, ""}:
                        raise RuntimeError("MISSING_RECORDED_BUY_ORDER_NOTIONAL")
                    maximum_buy_notional = Decimal(
                        str(raw_maximum_buy_notional)
                    )
                    if (
                        not maximum_buy_notional.is_finite()
                        or maximum_buy_notional <= ZERO
                        or notional > maximum_buy_notional
                    ):
                        raise RuntimeError(
                            "BUY_PRICE_IMPROVEMENT_EXCEEDS_FROZEN_ORDER_NOTIONAL"
                        )
                    maximum_buy_total_cost = (
                        maximum_buy_notional
                        + maximum_buy_fee_usd(
                            order_amount_usd=maximum_buy_notional,
                            taker_fee_bps=Decimal(
                                str(snapshot_details.get("fee_bps", "0"))
                            ),
                            fee_exponent=fee_exponent,
                            minimum_fill_price=(
                                None
                                if snapshot_details.get("tick_size") in {None, ""}
                                else Decimal(str(snapshot_details.get("tick_size")))
                            ),
                            maximum_fill_price=Decimal(
                                str(details.get("plan", {}).get("worst_price"))
                            ),
                        )
                    )
                    if notional + fee > maximum_buy_total_cost:
                        raise RuntimeError(
                            "BUY_PRICE_IMPROVEMENT_EXCEEDS_FROZEN_TOTAL_COST"
                        )
                    buy_cash_order_complete = (
                        quantity < remaining
                        and maximum_buy_notional - notional <= TOKEN_RAW_UNIT
                    )
                except Exception as exc:
                    store.append_runtime_error(
                        occurred_at_ms=now_ms(),
                        category="INTERNAL_OFFICIAL_ASSOCIATED_TRADE_RECONCILIATION",
                        message=f"{type(exc).__name__}: {exc}",
                        details={
                            "order_id": order_id,
                            "action_id": source.action_id,
                            "matched_quantity": str(quantity),
                            "matched_notional_usd": str(notional),
                            "remaining_quantity": str(remaining),
                        },
                    )
                    results.append(
                        {
                            "terminal_status": "PENDING",
                            "reason": "INVALID_RECORDED_BUY_CASH_BOUND",
                        }
                    )
                    continue
            if buy_price_improvement_surplus and (
                source.side != "BUY" or maximum_buy_notional is None
            ):
                reason = "OFFICIAL_FILL_EXCEEDS_RECORDED_TARGET"
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category="INTERNAL_OFFICIAL_ASSOCIATED_TRADE_RECONCILIATION",
                    message=reason,
                    details={
                        "order_id": order_id,
                        "action_id": source.action_id,
                        "matched_quantity": str(quantity),
                        "expected_quantity": str(expected),
                        "remaining_quantity": str(remaining),
                    },
                )
                results.append({"terminal_status": "PENDING", "reason": reason})
                continue
            terminal = (
                "FILLED"
                if quantity >= remaining or buy_cash_order_complete
                else "PARTIAL"
                if execution_order_type in {"GTD", "GTC_ACTIVE_CANCEL"}
                else "PARTIAL" if target is None else "PARTIAL_PENDING"
            )
            reason = (
                "OFFICIAL_ASSOCIATED_TRADE_BUY_PRICE_IMPROVEMENT_FILL"
                if buy_price_improvement_surplus
                else "OFFICIAL_ASSOCIATED_TRADE_BUY_CASH_ORDER_COMPLETE"
                if buy_cash_order_complete
                else ""
                if terminal == "FILLED"
                else "GTC_ACTIVE_CANCEL_PARTIAL_FILL"
                if execution_order_type == "GTC_ACTIVE_CANCEL"
                else "GTD_EXPIRED_PARTIAL_FILL"
                if execution_order_type == "GTD"
                else "FAK_PARTIAL_FILL"
            )
            partial_remainder_evidence = None
            if target is not None and terminal == "PARTIAL_PENDING":
                partial_remainder_evidence = (
                    _partial_remainder_below_recorded_minimum(
                        remaining_quantity=(
                            Decimal(str(target["remaining_quantity"])) - quantity
                        ),
                        original_minimum_order_size=(
                            store.original_submission_minimum_order_size(
                                action_id=source.action_id
                            )
                        ),
                    )
                )
                if partial_remainder_evidence is not None:
                    terminal = "EXTERNAL_UNFILLABLE"
                    reason = "PARTIAL_REMAINDER_BELOW_ORIGINAL_MARKET_MINIMUM"
            store.apply_fill_and_finalize(
                source=source,
                quantity=quantity,
                price=vwap,
                notional_usd=notional,
                fee_usd=fee,
                terminal_status=terminal,
                reason=reason,
                created_at_ms=now_ms(),
                details={
                    "order_id": order_id,
                    "execution_order_type": str(
                        details.get("execution_order_type") or "UNKNOWN_LEGACY"
                    ),
                    "order": order,
                    "matched_quantity": str(quantity),
                    "matched_quantity_encoding": str(
                        official["quantity_encoding"]
                    ),
                    "matched_notional_usd": str(notional),
                    "matched_price": str(vwap),
                    "fee_usd": str(fee),
                    "official_associated_trades": official["trade_evidence"],
                    "receipt_evidence": receipt_evidence,
                    "attempt_id": str(details.get("attempt_id") or ""),
                    "attempt_number": details.get("attempt_number"),
                    "planned_requested_quantity": str(expected),
                    "planned_buy_notional_usd": (
                        None
                        if maximum_buy_notional is None
                        else str(maximum_buy_notional)
                    ),
                    "maximum_buy_total_cost_usd": (
                        None
                        if maximum_buy_total_cost is None
                        else str(maximum_buy_total_cost)
                    ),
                    "buy_price_improvement_share_surplus": str(
                        max(quantity - remaining, ZERO)
                    ),
                    "buy_cash_order_share_shortfall": str(
                        max(remaining - quantity, ZERO)
                        if buy_cash_order_complete
                        else ZERO
                    ),
                    "partial_remainder_evidence": partial_remainder_evidence,
                },
                maximum_buy_notional_usd=(
                    maximum_buy_notional
                    if buy_price_improvement_surplus
                    or buy_cash_order_complete
                    else None
                ),
                maximum_buy_total_cost_usd=(
                    maximum_buy_total_cost
                    if buy_price_improvement_surplus
                    or buy_cash_order_complete
                    else None
                ),
                buy_cash_order_complete=buy_cash_order_complete,
            )
            results.append({"terminal_status": terminal, "reason": reason})
            continue
        if status in {
            "ORDER_STATUS_INVALID",
            "ORDER_STATUS_CANCELED",
            "ORDER_STATUS_CANCELED_MARKET_RESOLVED",
            "INVALID",
            "CANCELED",
        }:
            liquidity_policy = store.liquidity_retry_policy_for_source(source)
            bounded_retry = (
                None
                if liquidity_policy is not None
                else store.bounded_retry_policy_for_source(source)
            )
            reason = (
                "GTC_ACTIVE_CANCEL_ZERO_FILL"
                if execution_order_type == "GTC_ACTIVE_CANCEL"
                else "GTD_EXPIRED_ZERO_FILL"
                if execution_order_type == "GTD"
                else
                "OFFICIAL_ORDER_ZERO_FILL_WITHOUT_FINALIZED_CHAIN_PROOF"
                if liquidity_policy is not None
                else "FAK_ZERO_FILL_RETRYABLE"
                if bounded_retry is not None
                else "FAK_ZERO_FILL_NOT_REOPENED"
            )
            target_state = (
                "EXTERNAL_UNFILLABLE"
                if execution_order_type in {"GTD", "GTC_ACTIVE_CANCEL"}
                or liquidity_policy is not None
                else "PENDING_LIQUIDITY"
                if bounded_retry is not None
                else "EXTERNAL_UNFILLABLE"
            )
            attempt_id = str(details.get("attempt_id") or "")
            finalized_at_ms = now_ms()
            store.release_reservation_and_finalize(
                source=source,
                terminal_status=target_state,
                reason=reason,
                created_at_ms=finalized_at_ms,
                details={
                    "order_id": order_id,
                    "order": order,
                    "attempt_id": attempt_id,
                    "attempt_number": details.get("attempt_number"),
                    "bounded_retry": bounded_retry,
                    "liquidity_retry_policy": liquidity_policy,
                    "termination_reason": None,
                },
                attempt_id=attempt_id or None,
                attempt_state="NO_FILL" if attempt_id else None,
                attempt_response=order if attempt_id else None,
            )
            results.append(
                {"terminal_status": target_state, "reason": reason}
            )
            continue
        results.append({"terminal_status": "PENDING", "reason": "ORDER_NOT_FINAL"})
    return results


def _quantity_to_token_raw(quantity: Decimal) -> int:
    """Convert a local CLOB share quantity to the token's exact 6-decimal raw unit."""

    if quantity < ZERO:
        raise LiveConfigurationError("NEGATIVE_LOCAL_CONDITION_INVENTORY")
    raw = quantity * TOKEN_SCALE
    if raw != raw.to_integral_value():
        raise LiveConfigurationError("NONINTEGRAL_LOCAL_TOKEN_RAW_BALANCE")
    return int(raw)


def _nonnegative_raw_balance(value: Any, *, label: str) -> int:
    try:
        raw = int(value)
    except (TypeError, ValueError) as exc:
        raise LiveConfigurationError(f"INVALID_{label}_RAW_BALANCE") from exc
    if raw < 0:
        raise LiveConfigurationError(f"NEGATIVE_{label}_RAW_BALANCE")
    return raw


def _verified_resolution(
    *,
    adapter: Any,
    condition_id: str,
    primary_token_id: str,
    secondary_token_id: str,
) -> str | None:
    """Return the official winner token only after a strict closed-state check."""

    resolution = adapter.condition_resolution(condition_id)
    if not isinstance(resolution, dict):
        raise LiveConfigurationError("INVALID_OFFICIAL_CONDITION_RESOLUTION")
    if str(resolution.get("condition_id", "")).lower() != str(condition_id).lower():
        raise LiveConfigurationError("CONDITION_RESOLUTION_ID_MISMATCH")
    if resolution.get("closed") is not True:
        return None
    raw_winner = resolution.get("winner_token_id")
    winner = "" if raw_winner is None else str(raw_winner).strip()
    if not winner and "winner_index" in resolution:
        try:
            winner_index = int(resolution["winner_index"])
        except (TypeError, ValueError) as exc:
            raise LiveConfigurationError("INVALID_OFFICIAL_WINNER_INDEX") from exc
        if winner_index == 0:
            winner = str(primary_token_id)
        elif winner_index == 1:
            winner = str(secondary_token_id)
        else:
            raise LiveConfigurationError("INVALID_OFFICIAL_WINNER_INDEX")
    if winner not in {str(primary_token_id), str(secondary_token_id)}:
        raise LiveConfigurationError("INVALID_OFFICIAL_WINNER_TOKEN")
    return winner


def _condition_local_and_onchain_raw(
    *,
    adapter: Any,
    inventory: Mapping[str, Any],
    wallet_address: str,
) -> dict[str, int]:
    """Return both ledgers without allowing aggregate/netting shortcuts."""

    primary_local_raw = _quantity_to_token_raw(
        Decimal(str(inventory["primary_quantity"]))
    )
    secondary_local_raw = _quantity_to_token_raw(
        Decimal(str(inventory["secondary_quantity"]))
    )
    primary_onchain_raw = _nonnegative_raw_balance(
        adapter.outcome_token_balance_raw(
            wallet_address=wallet_address,
            token_id=str(inventory["primary_token_id"]),
        ),
        label="PRIMARY_ONCHAIN",
    )
    secondary_onchain_raw = _nonnegative_raw_balance(
        adapter.outcome_token_balance_raw(
            wallet_address=wallet_address,
            token_id=str(inventory["secondary_token_id"]),
        ),
        label="SECONDARY_ONCHAIN",
    )
    return {
        "primary_local_raw": primary_local_raw,
        "secondary_local_raw": secondary_local_raw,
        "primary_onchain_raw": primary_onchain_raw,
        "secondary_onchain_raw": secondary_onchain_raw,
    }


def _condition_inventory_is_exact(raw: Mapping[str, int]) -> bool:
    return (
        raw["primary_local_raw"] == raw["primary_onchain_raw"]
        and raw["secondary_local_raw"] == raw["secondary_onchain_raw"]
    )


def _confirmed_redemption_collateral_payout_raw(
    *,
    adapter: Any,
    transaction_hash: str,
    wallet_address: str,
) -> int:
    """Read the exact USDC transfer into the authenticated wallet for one tx."""

    normalized_hash = str(transaction_hash or "").strip().lower()
    if not normalized_hash.startswith("0x") or len(normalized_hash) != 66:
        raise LiveConfigurationError("INVALID_REDEMPTION_TRANSACTION_HASH")
    reader = getattr(adapter, "confirmed_redemption_collateral_payout_raw", None)
    if not callable(reader):
        raise LiveConfigurationError(
            "REDEMPTION_ONCHAIN_COLLATERAL_PAYOUT_READER_UNAVAILABLE"
        )
    return _nonnegative_raw_balance(
        reader(
            transaction_hash=normalized_hash,
            wallet_address=str(wallet_address).lower(),
        ),
        label="CONFIRMED_REDEMPTION_COLLATERAL_PAYOUT",
    )


def _redemption_result(condition_id: str, state: str) -> dict[str, str]:
    return {"condition_id": str(condition_id).lower(), "state": str(state)}


def reconcile_platform_settled_winners(
    *,
    store: LiveStore,
    adapter: Any,
    observed_collateral_usd: Decimal,
    created_at_ms: int,
    coordinator: SharedWalletCoordinator | None = None,
    profile_key: str | None = None,
    exclude_condition_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Reconcile platform-credit settlements without treating them as a top-up.

    The direct funder ERC-1155 balance is an ownership check, while the
    authenticated CLOB collateral balance is the cash authority.  Neither one
    alone proves a CD90 redemption.  This routine accepts the platform path
    only if all eligible resolved winners have zero direct outcome balances and
    their combined exact payout is the entire observed cash delta.
    """

    observed = Decimal(str(observed_collateral_usd))
    if not observed.is_finite() or observed < ZERO:
        raise LiveConfigurationError("INVALID_PLATFORM_SETTLEMENT_COLLATERAL")
    normalized_wallet = str(adapter.wallet_address).lower()
    candidates: list[dict[str, Any]] = []
    external_errors = 0
    allowed_prior_states = {
        "NOT_SUBMITTED_RETRYABLE",
        "BLOCK_PRE_SUBMISSION_REVALIDATION",
        "BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY",
        "UNKNOWN_SUBMISSION",
    }
    excluded = {
        str(condition).lower() for condition in (exclude_condition_ids or set())
    }
    for inventory in store.condition_inventory():
        condition_id = str(inventory["condition_id"]).lower()
        if condition_id in excluded:
            continue
        receipt = store.redemption_receipt(condition_id)
        if receipt is not None:
            prior_state = str(receipt["state"])
            if prior_state not in allowed_prior_states:
                continue
            if receipt["transaction_id"] is not None or receipt["transaction_hash"] is not None:
                continue
        try:
            winner_token_id = _verified_resolution(
                adapter=adapter,
                condition_id=condition_id,
                primary_token_id=str(inventory["primary_token_id"]),
                secondary_token_id=str(inventory["secondary_token_id"]),
            )
            if winner_token_id is None:
                continue
            raw = _condition_local_and_onchain_raw(
                adapter=adapter,
                inventory=inventory,
                wallet_address=normalized_wallet,
            )
        except Exception as exc:
            external_errors += 1
            store.append_runtime_error(
                occurred_at_ms=int(created_at_ms),
                category="EXTERNAL_PLATFORM_SETTLEMENT_EVIDENCE",
                message=f"{type(exc).__name__}: {exc}",
                details={"condition_id": condition_id},
            )
            continue
        if raw["primary_onchain_raw"] != 0 or raw["secondary_onchain_raw"] != 0:
            continue
        winner_local_raw = (
            raw["primary_local_raw"]
            if winner_token_id == str(inventory["primary_token_id"])
            else raw["secondary_local_raw"]
        )
        payout = Decimal(winner_local_raw) / TOKEN_SCALE
        if payout <= ZERO:
            continue
        candidates.append(
            {
                "condition_id": condition_id,
                "winner_token_id": winner_token_id,
                "payout_usd": payout,
                "onchain_outcome_balances_zero": True,
                **raw,
            }
        )

    if not candidates:
        return {
            "state": "EXTERNAL_EVIDENCE_UNAVAILABLE"
            if external_errors
            else "NO_ELIGIBLE_WINNERS",
            "condition_count": 0,
            "external_error_count": external_errors,
        }
    account = store.account_snapshot()
    ledger_cash = account["cash_usd"]
    wallet_cash_before = ledger_cash
    if coordinator is not None:
        if not str(profile_key or "").strip():
            raise LiveConfigurationError("MISSING_SHARED_WALLET_PROFILE_KEY")
        snapshot = coordinator.authenticated_account_cash_snapshot(
            authenticated_collateral_usd=observed,
        )
        if snapshot.active_buy_reservations_usd != ZERO:
            return {
                "state": "BLOCK_ACTIVE_WALLET_RESERVATIONS",
                "condition_count": len(candidates),
                "aggregate_active_buy_reservations_usd": str(
                    snapshot.active_buy_reservations_usd
                ),
                "external_error_count": external_errors,
            }
        wallet_cash_before = snapshot.expected_accounting_cash_high_usd
    expected_payout = sum(
        (Decimal(str(candidate["payout_usd"])) for candidate in candidates), ZERO
    )
    collateral_delta = observed - wallet_cash_before
    if collateral_delta < ZERO:
        return {
            "state": "BLOCK_COLLATERAL_BELOW_LEDGER",
            "condition_count": len(candidates),
            "expected_payout_usd": str(expected_payout),
            "collateral_delta_usd": str(collateral_delta),
            "external_error_count": external_errors,
        }
    if collateral_delta == ZERO:
        return {
            "state": "NO_CASH_DELTA",
            "condition_count": len(candidates),
            "expected_payout_usd": str(expected_payout),
            "collateral_delta_usd": str(collateral_delta),
            "external_error_count": external_errors,
        }
    if collateral_delta != expected_payout:
        return {
            "state": "BLOCK_DELTA_MISMATCH",
            "condition_count": len(candidates),
            "expected_payout_usd": str(expected_payout),
            "collateral_delta_usd": str(collateral_delta),
            "external_error_count": external_errors,
        }
    settled = store.settle_platform_credited_winners(
        candidates=candidates,
        observed_collateral_usd=observed,
        created_at_ms=created_at_ms,
        verified_wallet_cash_before_usd=wallet_cash_before,
    )
    return {
        "state": "RECONCILED",
        **{
            key: str(value) if isinstance(value, Decimal) else value
            for key, value in settled.items()
        },
        "external_error_count": external_errors,
    }


def _platform_settlement_error_category(state: str) -> str:
    """Keep a non-mutating official cash mismatch out of the internal-fault bucket."""

    if state == "BLOCK_ACTIVE_WALLET_RESERVATIONS":
        return "ACCOUNT_CASH_RECONCILIATION"
    return "EXTERNAL_ACCOUNT_CASH_RECONCILIATION"


def reconcile_official_redeem_activities(
    *,
    store: LiveStore,
    adapter: Any,
    wallet_address: str,
    official_activities: list[Mapping[str, Any]],
    created_at_ms: int,
    frozen_cash_baseline_at_ms: int | None = None,
    exclude_condition_ids: set[str] | None = None,
    quarantine_confirmed_cash_credit: bool = False,
) -> dict[str, Any]:
    """Reconcile local winners against exact official REDEEM activity rows.

    This path deliberately ignores aggregate wallet cash changes.  It accepts
    one local condition only when the official wallet, condition, payout and
    transaction are exact, the official resolution identifies the same winner,
    and both outcome-token balances are already zero.
    """

    normalized_wallet = str(wallet_address).strip().lower()
    if (
        not normalized_wallet.startswith("0x")
        or len(normalized_wallet) != 42
    ):
        raise LiveConfigurationError("INVALID_OFFICIAL_ACTIVITY_WALLET")
    baseline_at = None
    if frozen_cash_baseline_at_ms is not None:
        try:
            baseline_at = int(frozen_cash_baseline_at_ms)
        except (TypeError, ValueError) as exc:
            raise LiveConfigurationError("INVALID_FROZEN_CASH_BASELINE_TIME") from exc
        if baseline_at < 0:
            raise LiveConfigurationError("INVALID_FROZEN_CASH_BASELINE_TIME")

    activities_by_condition: dict[str, list[dict[str, Any]]] = {}
    seen_activity: set[tuple[str, str, str]] = set()
    invalid_activity_count = 0
    payout_proof_mismatch_count = 0
    for raw in official_activities:
        if not isinstance(raw, Mapping) or str(raw.get("type", "")) != "REDEEM":
            continue
        proxy_wallet = str(raw.get("proxyWallet", "")).strip().lower()
        if proxy_wallet != normalized_wallet:
            invalid_activity_count += 1
            continue
        condition_id = str(raw.get("conditionId", "")).strip().lower()
        transaction_hash = str(raw.get("transactionHash", "")).strip().lower()
        try:
            payout = Decimal(str(raw.get("usdcSize", "")))
            timestamp_seconds = int(raw.get("timestamp", -1))
            valid_transaction_hash = (
                transaction_hash.startswith("0x")
                and len(transaction_hash) == 66
                and int(transaction_hash[2:], 16) >= 0
            )
        except (InvalidOperation, TypeError, ValueError):
            invalid_activity_count += 1
            continue
        if (
            not condition_id.startswith("0x")
            or len(condition_id) != 66
            or not payout.is_finite()
            or payout <= ZERO
            or timestamp_seconds < 0
            or not valid_transaction_hash
        ):
            invalid_activity_count += 1
            continue
        identity = (condition_id, transaction_hash, str(payout))
        if identity in seen_activity:
            continue
        seen_activity.add(identity)
        normalized_activity = {
            "proxy_wallet": proxy_wallet,
            "condition_id": condition_id,
            "payout_usd": payout,
            "transaction_hash": transaction_hash,
            "official_activity_timestamp_ms": timestamp_seconds * 1000,
        }
        activities_by_condition.setdefault(condition_id, []).append(
            normalized_activity
        )

    excluded = {
        str(condition).lower() for condition in (exclude_condition_ids or set())
    }
    normalized_adapter_wallet = str(adapter.wallet_address).strip().lower()
    if normalized_adapter_wallet != normalized_wallet:
        raise LiveConfigurationError("OFFICIAL_ACTIVITY_ADAPTER_WALLET_MISMATCH")
    candidates: list[dict[str, Any]] = []
    external_errors = 0
    ambiguous_match_count = 0
    for inventory in store.condition_inventory():
        condition_id = str(inventory["condition_id"]).lower()
        if condition_id in excluded:
            continue
        activity_rows = activities_by_condition.get(condition_id, [])
        if not activity_rows:
            continue
        receipt = store.redemption_receipt(condition_id)
        if receipt is not None and str(receipt["state"]) in {
            "REDEEMED",
            "REDEEMED_EXTERNAL_VERIFIED",
            "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED",
            "REDEEMED_OFFICIAL_ACTIVITY_VERIFIED",
            "REDEEMED_SHARED_WALLET",
            "REDEEMED_SHARED_PLATFORM_SETTLEMENT",
            "LOSS_RESOLVED_NO_PAYOUT",
            "LOSS_RESOLVED_SHARED_WALLET",
            "BLOCK_OFFICIAL_REDEEM_ONCHAIN_COLLATERAL_PAYOUT_MISMATCH",
        }:
            continue
        try:
            winner_token_id = _verified_resolution(
                adapter=adapter,
                condition_id=condition_id,
                primary_token_id=str(inventory["primary_token_id"]),
                secondary_token_id=str(inventory["secondary_token_id"]),
            )
            if winner_token_id is None:
                continue
            raw = _condition_local_and_onchain_raw(
                adapter=adapter,
                inventory=inventory,
                wallet_address=normalized_wallet,
            )
        except Exception as exc:
            external_errors += 1
            store.append_runtime_error(
                occurred_at_ms=int(created_at_ms),
                category="EXTERNAL_OFFICIAL_REDEEM_EVIDENCE",
                message=f"{type(exc).__name__}: {exc}",
                details={"condition_id": condition_id},
            )
            continue
        if raw["primary_onchain_raw"] != 0 or raw["secondary_onchain_raw"] != 0:
            continue
        winner_local_raw = (
            raw["primary_local_raw"]
            if winner_token_id == str(inventory["primary_token_id"])
            else raw["secondary_local_raw"]
        )
        expected_payout = Decimal(winner_local_raw) / TOKEN_SCALE
        exact_matches = [
            row
            for row in activity_rows
            if Decimal(str(row["payout_usd"])) == expected_payout
        ]
        if len(exact_matches) != 1:
            ambiguous_match_count += 1
            continue
        match = exact_matches[0]
        match_transaction_hash = str(match["transaction_hash"]).lower()
        known_transaction_hash = (
            ""
            if receipt is None or receipt["transaction_hash"] is None
            else str(receipt["transaction_hash"]).strip().lower()
        )
        if (
            known_transaction_hash
            and known_transaction_hash != match_transaction_hash
        ):
            external_errors += 1
            store.append_runtime_error(
                occurred_at_ms=int(created_at_ms),
                category="EXTERNAL_OFFICIAL_REDEEM_TRANSACTION_IDENTITY",
                message="OFFICIAL_REDEEM_TRANSACTION_HASH_MISMATCH",
                details={
                    "condition_id": condition_id,
                    "known_transaction_hash": known_transaction_hash,
                    "official_activity_transaction_hash": match_transaction_hash,
                },
            )
            continue
        try:
            expected_payout_raw = _quantity_to_token_raw(expected_payout)
            observed_payout_raw = _confirmed_redemption_collateral_payout_raw(
                adapter=adapter,
                transaction_hash=match_transaction_hash,
                wallet_address=normalized_wallet,
            )
        except Exception as exc:
            external_errors += 1
            store.append_runtime_error(
                occurred_at_ms=int(created_at_ms),
                category="EXTERNAL_OFFICIAL_REDEEM_PAYOUT_PROOF",
                message=f"{type(exc).__name__}: {exc}",
                details={
                    "condition_id": condition_id,
                    "transaction_hash": match_transaction_hash,
                },
            )
            continue
        if observed_payout_raw != expected_payout_raw:
            payout_proof_mismatch_count += 1
            mismatch_state = (
                "BLOCK_OFFICIAL_REDEEM_ONCHAIN_COLLATERAL_PAYOUT_MISMATCH"
            )
            mismatch_details = {
                **raw,
                "winner_token_id": winner_token_id,
                "official_payout_usd": str(expected_payout),
                "expected_official_payout_raw": str(expected_payout_raw),
                "observed_payout_raw": str(observed_payout_raw),
                "official_activity": dict(match),
            }
            if receipt is None:
                store.record_redemption_terminal_without_submission(
                    condition_id=condition_id,
                    state=mismatch_state,
                    reason="EXACT_OFFICIAL_ACTIVITY_ONCHAIN_COLLATERAL_PAYOUT_MISMATCH",
                    expected_payout_usd=expected_payout,
                    created_at_ms=int(created_at_ms),
                    details=mismatch_details,
                    transaction_hash=match_transaction_hash,
                )
            else:
                store.mark_redemption_terminal(
                    condition_id=condition_id,
                    state=mismatch_state,
                    reason="EXACT_OFFICIAL_ACTIVITY_ONCHAIN_COLLATERAL_PAYOUT_MISMATCH",
                    created_at_ms=int(created_at_ms),
                    details=mismatch_details,
                    transaction_hash=match_transaction_hash,
                )
            store.append_runtime_error(
                occurred_at_ms=int(created_at_ms),
                category="EXTERNAL_OFFICIAL_REDEEM_PAYOUT_MISMATCH",
                message="OFFICIAL_REDEEM_ACTIVITY_WITHOUT_MATCHING_ONCHAIN_COLLATERAL_TRANSFER",
                details={
                    "condition_id": condition_id,
                    "transaction_hash": match_transaction_hash,
                    "expected_payout_raw": str(expected_payout_raw),
                    "observed_payout_raw": str(observed_payout_raw),
                },
            )
            continue
        activity_timestamp_ms = int(match["official_activity_timestamp_ms"])
        evidence_without_hash = {
            "proxy_wallet": normalized_wallet,
            "condition_id": condition_id,
            "payout_usd": str(expected_payout),
            "transaction_hash": str(match["transaction_hash"]),
            "official_activity_timestamp_ms": activity_timestamp_ms,
        }
        evidence_hash = hashlib.sha256(
            json.dumps(
                evidence_without_hash,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        candidates.append(
            {
                **evidence_without_hash,
                "official_activity_type": "REDEEM",
                "official_activity_evidence_hash": evidence_hash,
                "winner_token_id": winner_token_id,
                "onchain_outcome_balances_zero": True,
                "confirmed_collateral_payout_raw": str(observed_payout_raw),
                "cash_already_in_frozen_baseline": (
                    baseline_at is not None and activity_timestamp_ms <= baseline_at
                ),
                **raw,
            }
        )

    if not candidates:
        return {
            "state": (
                "BLOCK_ONCHAIN_COLLATERAL_PAYOUT_MISMATCH"
                if payout_proof_mismatch_count
                else "NO_EXACT_OFFICIAL_REDEEM_MATCH"
            ),
            "condition_count": 0,
            "official_redeem_activity_count": sum(
                len(rows) for rows in activities_by_condition.values()
            ),
            "invalid_activity_count": invalid_activity_count,
            "ambiguous_match_count": ambiguous_match_count,
            "payout_proof_mismatch_count": payout_proof_mismatch_count,
            "external_error_count": external_errors,
        }
    settled = store.settle_official_activity_redeemed_winners(
        candidates=candidates,
        created_at_ms=created_at_ms,
        quarantine_cash_credit_until_authenticated=(
            bool(quarantine_confirmed_cash_credit)
        ),
    )
    return {
        "state": "RECONCILED_OFFICIAL_ACTIVITY",
        **settled,
        "official_redeem_activity_count": sum(
            len(rows) for rows in activities_by_condition.values()
        ),
        "invalid_activity_count": invalid_activity_count,
        "ambiguous_match_count": ambiguous_match_count,
        "payout_proof_mismatch_count": payout_proof_mismatch_count,
        "external_error_count": external_errors,
    }


def auto_redeem_resolved_positions(
    *,
    store: LiveStore,
    adapter: Any,
    wallet_address: str,
    live_enabled: bool,
    exclude_condition_ids: set[str] | None = None,
) -> list[dict[str, str]]:
    """Submit one gasless redemption per fully verified CD90 condition.

    A SecureClient redemption call redeems the wallet's entire condition
    balance.  The pre-submit comparison therefore requires *both* outcome
    token balances to exactly equal isolated CD90 inventory; any manual or
    other-strategy inventory blocks the condition rather than risking it.
    Cash is not released in the sleeve ledger here: that happens only after
    the relayer reports ``STATE_CONFIRMED`` and the outcome balances are zero.
    """

    if not live_enabled:
        raise LiveDisabledError("automatic redemption requires live guard")
    normalized_wallet = str(wallet_address).lower()
    results: list[dict[str, str]] = []
    excluded = {
        str(condition).lower() for condition in (exclude_condition_ids or set())
    }
    for inventory in store.condition_inventory():
        condition_id = str(inventory["condition_id"]).lower()
        if condition_id in excluded:
            continue
        existing = store.redemption_receipt(condition_id)
        if existing is not None and str(existing["state"]) != "NOT_SUBMITTED_RETRYABLE":
            results.append(_redemption_result(condition_id, str(existing["state"])))
            continue

        try:
            winner_token_id = _verified_resolution(
                adapter=adapter,
                condition_id=condition_id,
                primary_token_id=str(inventory["primary_token_id"]),
                secondary_token_id=str(inventory["secondary_token_id"]),
            )
        except Exception as exc:
            store.append_runtime_error(
                occurred_at_ms=now_ms(),
                category="EXTERNAL_REDEMPTION_RESOLUTION",
                message=f"{type(exc).__name__}: {exc}",
                details={"condition_id": condition_id},
            )
            results.append(_redemption_result(condition_id, "PENDING_OFFICIAL_RESOLUTION"))
            continue
        if winner_token_id is None:
            results.append(_redemption_result(condition_id, "PENDING_OFFICIAL_RESOLUTION"))
            continue

        try:
            raw = _condition_local_and_onchain_raw(
                adapter=adapter,
                inventory=inventory,
                wallet_address=normalized_wallet,
            )
        except Exception as exc:
            store.append_runtime_error(
                occurred_at_ms=now_ms(),
                category="EXTERNAL_REDEMPTION_BALANCE_READ",
                message=f"{type(exc).__name__}: {exc}",
                details={"condition_id": condition_id},
            )
            results.append(_redemption_result(condition_id, "PENDING_ONCHAIN_BALANCE_READ"))
            continue
        if not _condition_inventory_is_exact(raw):
            store.record_redemption_terminal_without_submission(
                condition_id=condition_id,
                state="BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY",
                reason="ONCHAIN_BALANCE_NOT_EQUAL_TO_ISOLATED_CD90_LEDGER",
                expected_payout_usd=ZERO,
                created_at_ms=now_ms(),
                details={**raw, "winner_token_id": winner_token_id},
            )
            results.append(
                _redemption_result(condition_id, "BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY")
            )
            continue

        winner_local_raw = (
            raw["primary_local_raw"]
            if winner_token_id == str(inventory["primary_token_id"])
            else raw["secondary_local_raw"]
        )
        expected_payout_usd = Decimal(winner_local_raw) / TOKEN_SCALE
        if expected_payout_usd == ZERO:
            # Both balances have been checked against the wallet.  A losing
            # condition needs no transaction; it is an official zero payout.
            try:
                store.settle_losing_condition(
                    condition_id=condition_id,
                    created_at_ms=now_ms(),
                    details={**raw, "winner_token_id": winner_token_id},
                )
            except Exception as exc:
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category="INTERNAL_REDEMPTION_LEDGER",
                    message=f"{type(exc).__name__}: {exc}",
                    details={"condition_id": condition_id},
                )
                results.append(_redemption_result(condition_id, "ERROR"))
                continue
            results.append(_redemption_result(condition_id, "LOSS_RESOLVED_NO_PAYOUT"))
            continue

        locked = store.start_redemption_submission(
            condition_id=condition_id,
            expected_payout_usd=expected_payout_usd,
            created_at_ms=now_ms(),
            details={**raw, "winner_token_id": winner_token_id},
        )
        if not locked:
            existing = store.redemption_receipt(condition_id)
            results.append(
                _redemption_result(
                    condition_id,
                    "ERROR" if existing is None else str(existing["state"]),
                )
            )
            continue

        # Re-read the official resolution and both balances *after* the
        # durable submission lock and immediately before the wallet call.
        try:
            before_submit_winner = _verified_resolution(
                adapter=adapter,
                condition_id=condition_id,
                primary_token_id=str(inventory["primary_token_id"]),
                secondary_token_id=str(inventory["secondary_token_id"]),
            )
            before_submit_raw = _condition_local_and_onchain_raw(
                adapter=adapter,
                inventory=inventory,
                wallet_address=normalized_wallet,
            )
            if (
                before_submit_winner != winner_token_id
                or not _condition_inventory_is_exact(before_submit_raw)
            ):
                store.mark_redemption_terminal(
                    condition_id=condition_id,
                    state="BLOCK_PRE_SUBMISSION_REVALIDATION",
                    reason="RESOLUTION_OR_ONCHAIN_INVENTORY_CHANGED",
                    created_at_ms=now_ms(),
                    details={
                        **before_submit_raw,
                        "winner_token_id": before_submit_winner,
                    },
                )
                results.append(
                    _redemption_result(condition_id, "BLOCK_PRE_SUBMISSION_REVALIDATION")
                )
                continue
        except Exception as exc:
            store.mark_redemption_terminal(
                condition_id=condition_id,
                state="BLOCK_PRE_SUBMISSION_REVALIDATION",
                reason=f"{type(exc).__name__}: {exc}",
                created_at_ms=now_ms(),
                details={"condition_id": condition_id},
            )
            results.append(_redemption_result(condition_id, "BLOCK_PRE_SUBMISSION_REVALIDATION"))
            continue
        try:
            submitted = adapter.submit_redeem(condition_id=condition_id)
        except RedemptionNotSubmittedError as exc:
            store.mark_redemption_terminal(
                condition_id=condition_id,
                state="NOT_SUBMITTED_RETRYABLE",
                reason=f"{type(exc).__name__}: {exc}",
                created_at_ms=now_ms(),
                details={"condition_id": condition_id},
            )
            results.append(_redemption_result(condition_id, "NOT_SUBMITTED_RETRYABLE"))
            continue
        except Exception as exc:
            # A transport exception can occur after the relayer accepted the
            # transaction.  Preserve the uncertain side effect; never post a
            # second redemption automatically.
            store.mark_redemption_terminal(
                condition_id=condition_id,
                state="UNKNOWN_SUBMISSION",
                reason=f"{type(exc).__name__}: {exc}",
                created_at_ms=now_ms(),
                details={"condition_id": condition_id},
            )
            results.append(_redemption_result(condition_id, "UNKNOWN_SUBMISSION"))
            continue
        transaction_id = "" if not isinstance(submitted, dict) else str(submitted.get("transaction_id", ""))
        if not transaction_id:
            store.mark_redemption_terminal(
                condition_id=condition_id,
                state="UNKNOWN_SUBMISSION",
                reason="MISSING_REDEMPTION_TRANSACTION_ID",
                created_at_ms=now_ms(),
                details={"response": submitted},
            )
            results.append(_redemption_result(condition_id, "UNKNOWN_SUBMISSION"))
            continue
        transaction_hash = submitted.get("transaction_hash") if isinstance(submitted, dict) else None
        store.mark_redemption_submission(
            condition_id=condition_id,
            transaction_id=transaction_id,
            transaction_hash=None if transaction_hash is None else str(transaction_hash),
            created_at_ms=now_ms(),
        )
        results.append(_redemption_result(condition_id, "SUBMITTED_UNRECONCILED"))
    return results


def reconcile_redemption_submissions(
    *,
    store: LiveStore,
    adapter: Any,
    wallet_address: str,
    exclude_condition_ids: set[str] | None = None,
    quarantine_confirmed_cash_credit: bool = False,
) -> list[dict[str, str]]:
    """Reconcile relayer state without misallocating a newly shared condition.

    ``exclude_condition_ids`` blocks local cash settlement and new local
    ownership, but it must not suppress read-only reconciliation of a local
    transaction that was submitted before another sleeve acquired exposure.
    A confirmed overlapping local transaction is quarantined for audited
    wallet-wide attribution instead of crediting one sleeve with the whole
    physical redemption.
    """

    normalized_wallet = str(wallet_address).lower()
    results: list[dict[str, str]] = []
    excluded = {
        str(condition).lower() for condition in (exclude_condition_ids or set())
    }
    for receipt in store.redemption_receipts_pending_reconciliation():
        condition_id = str(receipt["condition_id"]).lower()
        shared_overlap = condition_id in excluded
        transaction_id = str(receipt.get("transaction_id") or "")
        if not transaction_id:
            store.mark_redemption_terminal(
                condition_id=condition_id,
                state="UNKNOWN_SUBMISSION",
                reason="PENDING_REDEMPTION_WITHOUT_TRANSACTION_ID",
                created_at_ms=now_ms(),
                details={},
            )
            results.append(_redemption_result(condition_id, "UNKNOWN_SUBMISSION"))
            continue
        try:
            status = adapter.redemption_transaction_status(transaction_id)
        except Exception as exc:
            store.append_runtime_error(
                occurred_at_ms=now_ms(),
                category="EXTERNAL_REDEMPTION_STATUS",
                message=f"{type(exc).__name__}: {exc}",
                details={"condition_id": condition_id, "transaction_id": transaction_id},
            )
            results.append(_redemption_result(condition_id, "PENDING"))
            continue
        if not isinstance(status, dict) or not str(status.get("state", "")).strip():
            store.mark_redemption_terminal(
                condition_id=condition_id,
                state="ERROR",
                reason="INVALID_RELAYER_STATUS_RESPONSE",
                created_at_ms=now_ms(),
                details={"response": status},
            )
            results.append(_redemption_result(condition_id, "ERROR"))
            continue
        state = str(status["state"]).upper()
        transaction_hash = status.get("transaction_hash")
        if state == "STATE_CONFIRMED":
            if shared_overlap:
                store.mark_redemption_terminal(
                    condition_id=condition_id,
                    state="ERROR",
                    reason="ERROR_LOCAL_REDEMPTION_CROSSES_SHARED_INVENTORY",
                    created_at_ms=now_ms(),
                    details={
                        "relayer_status": status,
                        "cash_mutation_blocked": True,
                        "shared_wallet_attribution_required": True,
                    },
                    transaction_hash=(
                        None if transaction_hash is None else str(transaction_hash)
                    ),
                )
                results.append(_redemption_result(condition_id, "ERROR"))
                continue
            normalized_transaction_hash = str(transaction_hash or "").strip().lower()
            if not normalized_transaction_hash:
                store.mark_redemption_terminal(
                    condition_id=condition_id,
                    state="PENDING",
                    reason="CONFIRMED_REDEMPTION_AWAITING_TRANSACTION_HASH",
                    created_at_ms=now_ms(),
                    details={"relayer_status": status},
                )
                results.append(_redemption_result(condition_id, "PENDING"))
                continue
            official_reader = getattr(
                adapter, "official_redemption_activity_for_transaction", None
            )
            if not callable(official_reader):
                store.mark_redemption_terminal(
                    condition_id=condition_id,
                    state="PENDING",
                    reason="OFFICIAL_REDEMPTION_ACTIVITY_READER_UNAVAILABLE",
                    created_at_ms=now_ms(),
                    details={"relayer_status": status},
                    transaction_hash=normalized_transaction_hash,
                )
                results.append(_redemption_result(condition_id, "PENDING"))
                continue
            try:
                official_activity = official_reader(
                    condition_id=condition_id,
                    transaction_hash=normalized_transaction_hash,
                )
            except Exception as exc:
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category="EXTERNAL_OFFICIAL_REDEMPTION_ACTIVITY",
                    message=f"{type(exc).__name__}: {exc}",
                    details={
                        "condition_id": condition_id,
                        "transaction_id": transaction_id,
                        "transaction_hash": normalized_transaction_hash,
                    },
                )
                results.append(_redemption_result(condition_id, "PENDING"))
                continue
            if official_activity is None:
                store.mark_redemption_terminal(
                    condition_id=condition_id,
                    state="PENDING",
                    reason="CONFIRMED_REDEMPTION_AWAITING_OFFICIAL_ACTIVITY",
                    created_at_ms=now_ms(),
                    details={"relayer_status": status},
                    transaction_hash=normalized_transaction_hash,
                )
                results.append(_redemption_result(condition_id, "PENDING"))
                continue
            if (
                not isinstance(official_activity, Mapping)
                or str(official_activity.get("official_activity_type", ""))
                != "REDEEM"
                or str(official_activity.get("condition_id", "")).lower()
                != condition_id
                or str(official_activity.get("transaction_hash", "")).lower()
                != normalized_transaction_hash
            ):
                store.mark_redemption_terminal(
                    condition_id=condition_id,
                    state="ERROR",
                    reason="INVALID_EXACT_OFFICIAL_REDEMPTION_ACTIVITY",
                    created_at_ms=now_ms(),
                    details={"official_activity": official_activity},
                    transaction_hash=normalized_transaction_hash,
                )
                results.append(_redemption_result(condition_id, "ERROR"))
                continue
            try:
                official_payout_usd = Decimal(
                    str(official_activity.get("payout_usd", ""))
                )
            except (InvalidOperation, TypeError, ValueError):
                official_payout_usd = Decimal("NaN")
            if not official_payout_usd.is_finite() or official_payout_usd < ZERO:
                store.mark_redemption_terminal(
                    condition_id=condition_id,
                    state="ERROR",
                    reason="INVALID_EXACT_OFFICIAL_REDEMPTION_PAYOUT",
                    created_at_ms=now_ms(),
                    details={"official_activity": official_activity},
                    transaction_hash=normalized_transaction_hash,
                )
                results.append(_redemption_result(condition_id, "ERROR"))
                continue
            inventory_rows = store.condition_inventory(condition_id)
            if len(inventory_rows) != 1:
                store.mark_redemption_terminal(
                    condition_id=condition_id,
                    state="ERROR",
                    reason="MISSING_LOCAL_CONDITION_INVENTORY_AT_CONFIRMATION",
                    created_at_ms=now_ms(),
                    details={"relayer_status": status},
                    transaction_hash=None if transaction_hash is None else str(transaction_hash),
                )
                results.append(_redemption_result(condition_id, "ERROR"))
                continue
            inventory = inventory_rows[0]
            try:
                winner_token_id = _verified_resolution(
                    adapter=adapter,
                    condition_id=condition_id,
                    primary_token_id=str(inventory["primary_token_id"]),
                    secondary_token_id=str(inventory["secondary_token_id"]),
                )
                raw = _condition_local_and_onchain_raw(
                    adapter=adapter,
                    inventory=inventory,
                    wallet_address=normalized_wallet,
                )
                collateral_raw = _nonnegative_raw_balance(
                    adapter.collateral_balance_raw(wallet_address=normalized_wallet),
                    label="COLLATERAL_ONCHAIN",
                )
            except Exception as exc:
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category="EXTERNAL_REDEMPTION_CONFIRMATION_READ",
                    message=f"{type(exc).__name__}: {exc}",
                    details={"condition_id": condition_id, "transaction_id": transaction_id},
                )
                results.append(_redemption_result(condition_id, "PENDING"))
                continue
            if winner_token_id is None or raw["primary_onchain_raw"] != 0 or raw["secondary_onchain_raw"] != 0:
                store.mark_redemption_terminal(
                    condition_id=condition_id,
                    state="ERROR",
                    reason="CONFIRMED_REDEMPTION_ONCHAIN_RECONCILIATION_MISMATCH",
                    created_at_ms=now_ms(),
                    details={
                        **raw,
                        "winner_token_id": winner_token_id,
                        "collateral_onchain_raw": collateral_raw,
                        "relayer_status": status,
                    },
                    transaction_hash=None if transaction_hash is None else str(transaction_hash),
                )
                results.append(_redemption_result(condition_id, "ERROR"))
                continue
            expected_payout_usd = Decimal(str(receipt["expected_payout_usd"]))
            winner_local_raw = (
                raw["primary_local_raw"]
                if winner_token_id == str(inventory["primary_token_id"])
                else raw["secondary_local_raw"]
            )
            try:
                official_payout_raw = _quantity_to_token_raw(official_payout_usd)
                observed_payout_raw = _confirmed_redemption_collateral_payout_raw(
                    adapter=adapter,
                    transaction_hash=normalized_transaction_hash,
                    wallet_address=normalized_wallet,
                )
            except Exception as exc:
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category="EXTERNAL_REDEMPTION_PAYOUT_PROOF",
                    message=f"{type(exc).__name__}: {exc}",
                    details={
                        "condition_id": condition_id,
                        "transaction_id": transaction_id,
                        "transaction_hash": normalized_transaction_hash,
                    },
                )
                results.append(_redemption_result(condition_id, "PENDING"))
                continue
            if observed_payout_raw != official_payout_raw:
                store.mark_redemption_terminal(
                    condition_id=condition_id,
                    state="PENDING",
                    reason="CONFIRMED_REDEMPTION_ONCHAIN_COLLATERAL_PAYOUT_MISMATCH",
                    created_at_ms=now_ms(),
                    details={
                        **raw,
                        "winner_token_id": winner_token_id,
                        "official_payout_usd": str(official_payout_usd),
                        "expected_official_payout_raw": str(official_payout_raw),
                        "observed_payout_raw": str(observed_payout_raw),
                        "collateral_onchain_raw": collateral_raw,
                        "relayer_status": status,
                    },
                    transaction_hash=normalized_transaction_hash,
                )
                results.append(_redemption_result(condition_id, "PENDING"))
                continue
            try:
                store.settle_redeemed_condition(
                    condition_id=condition_id,
                    payout_usd=official_payout_usd,
                    created_at_ms=now_ms(),
                    details={
                        **raw,
                        "winner_token_id": winner_token_id,
                        "predicted_winner_payout_usd": str(
                            Decimal(winner_local_raw) / TOKEN_SCALE
                        ),
                        "submission_expected_payout_usd": str(
                            expected_payout_usd
                        ),
                        "official_redemption_activity": dict(
                            official_activity
                        ),
                        "confirmed_collateral_payout_raw": str(
                            observed_payout_raw
                        ),
                        "collateral_onchain_raw": collateral_raw,
                        "relayer_status": status,
                    },
                    transaction_hash=normalized_transaction_hash,
                    quarantine_cash_credit_until_authenticated=(
                        bool(quarantine_confirmed_cash_credit)
                    ),
                )
            except Exception as exc:
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category="INTERNAL_REDEMPTION_LEDGER",
                    message=f"{type(exc).__name__}: {exc}",
                    details={"condition_id": condition_id},
                )
                results.append(_redemption_result(condition_id, "ERROR"))
                continue
            results.append(
                _redemption_result(
                    condition_id,
                    "REDEEMED"
                    if official_payout_usd > ZERO
                    else "LOSS_RESOLVED_NO_PAYOUT",
                )
            )
            continue
        if state in {"STATE_FAILED", "STATE_INVALID", "STATE_REJECTED", "STATE_EXPIRED"}:
            store.mark_redemption_terminal(
                condition_id=condition_id,
                state="ERROR",
                reason="RELAYER_REDEMPTION_" + state,
                created_at_ms=now_ms(),
                details={"relayer_status": status},
                transaction_hash=None if transaction_hash is None else str(transaction_hash),
            )
            results.append(_redemption_result(condition_id, "ERROR"))
            continue
        if str(receipt["state"]) != "PENDING":
            store.mark_redemption_terminal(
                condition_id=condition_id,
                state="PENDING",
                reason="RELAYER_" + state,
                created_at_ms=now_ms(),
                details={"relayer_status": status},
                transaction_hash=None if transaction_hash is None else str(transaction_hash),
            )
        results.append(_redemption_result(condition_id, "PENDING"))
    return results


def backfill_redeemed_cash_credit_permanent_blocks_from_chain(
    *,
    store: LiveStore,
    adapter: Any,
    wallet_address: str,
    created_at_ms: int,
) -> list[dict[str, str]]:
    """Append shared-cash exclusions for legacy credits disproved by their tx.

    This is deliberately forward-only.  It preserves the original terminal
    redemption, account balance, PnL, and positions, then gives the shared
    coordinator exact chain evidence to exclude only unavailable cash from
    every future BUY calculation.
    """

    if int(created_at_ms) < 0:
        raise LiveConfigurationError("INVALID_REDEMPTION_PAYOUT_BACKFILL_TIME")
    results: list[dict[str, str]] = []
    for receipt in store.redemption_receipts_with_state("REDEEMED"):
        try:
            payout_usd = Decimal(str(receipt["expected_payout_usd"]))
            expected_raw = _quantity_to_token_raw(payout_usd)
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise LiveConfigurationError(
                "INVALID_LEGACY_REDEMPTION_PAYOUT"
            ) from exc
        if payout_usd <= ZERO:
            continue
        transaction_hash = str(receipt.get("transaction_hash") or "").strip()
        if not transaction_hash:
            # The existing ordinary quarantine remains the conservative path
            # for a terminal credit with no transaction identity to prove or
            # disprove.  This routine must never invent one.
            continue
        observed_raw = _confirmed_redemption_collateral_payout_raw(
            adapter=adapter,
            transaction_hash=transaction_hash,
            wallet_address=wallet_address,
        )
        if observed_raw == expected_raw:
            continue
        condition_id = str(receipt["condition_id"]).lower()
        inserted = store.record_redeemed_cash_credit_permanent_block(
            condition_id=condition_id,
            payout_usd=payout_usd,
            transaction_hash=transaction_hash,
            expected_payout_raw=expected_raw,
            observed_payout_raw=observed_raw,
            created_at_ms=int(created_at_ms),
            details={
                "source": "CONFIRMED_REDEMPTION_TRANSACTION_RECEIPT",
                "historical_ledger_rewritten": False,
                "positions_rewritten": False,
                "settlement_rewritten": False,
            },
        )
        if inserted:
            store.append_runtime_error(
                occurred_at_ms=int(created_at_ms),
                category="INTERNAL_REDEMPTION_CASH_CREDIT_MISMATCH",
                message="CONFIRMED_REDEMPTION_ONCHAIN_COLLATERAL_PAYOUT_MISMATCH",
                details={
                    "condition_id": condition_id,
                    "transaction_hash": str(transaction_hash).lower(),
                    "expected_payout_raw": str(expected_raw),
                    "observed_payout_raw": str(observed_raw),
                    "cash_availability_only": True,
                },
            )
        results.append(_redemption_result(condition_id, "PERMANENT_CASH_EXCLUSION"))
    return results


def _shared_condition_local_and_onchain_raw(
    *,
    adapter: Any,
    inventory: Mapping[str, Any],
    wallet_address: str,
) -> dict[str, int]:
    primary_local_raw = _quantity_to_token_raw(
        Decimal(str(inventory["primary_quantity"]))
    )
    secondary_local_raw = _quantity_to_token_raw(
        Decimal(str(inventory["secondary_quantity"]))
    )
    primary_onchain_raw = _nonnegative_raw_balance(
        adapter.outcome_token_balance_raw(
            wallet_address=str(wallet_address).lower(),
            token_id=str(inventory["primary_token_id"]),
        ),
        label="SHARED_PRIMARY_ONCHAIN",
    )
    secondary_onchain_raw = _nonnegative_raw_balance(
        adapter.outcome_token_balance_raw(
            wallet_address=str(wallet_address).lower(),
            token_id=str(inventory["secondary_token_id"]),
        ),
        label="SHARED_SECONDARY_ONCHAIN",
    )
    return {
        "primary_local_raw": primary_local_raw,
        "secondary_local_raw": secondary_local_raw,
        "primary_onchain_raw": primary_onchain_raw,
        "secondary_onchain_raw": secondary_onchain_raw,
    }


def _shared_raw_inventory_is_exact(raw: Mapping[str, int]) -> bool:
    return (
        int(raw["primary_local_raw"]) == int(raw["primary_onchain_raw"])
        and int(raw["secondary_local_raw"])
        == int(raw["secondary_onchain_raw"])
    )


def _apply_shared_redemption_allocations(
    *,
    coordinator: SharedWalletCoordinator,
    condition_id: str,
    local_terminal_state: str,
    coordinator_terminal_state: str,
    created_at_ms: int,
) -> dict[str, Any]:
    receipt = coordinator.shared_redemption_receipt(condition_id)
    if receipt is None:
        raise LiveConfigurationError("MISSING_SHARED_REDEMPTION_RECEIPT")
    allocations = coordinator.shared_condition_allocations(condition_id)
    if len(allocations) < 2:
        raise LiveConfigurationError("MISSING_SHARED_REDEMPTION_ALLOCATIONS")
    applied_profiles: list[str] = []
    for allocation in allocations:
        profile = str(allocation["profile_key"])
        if str(allocation["apply_state"]) == "APPLIED":
            applied_profiles.append(profile)
            continue
        ledger = LiveStore(Path(str(allocation["ledger_path"])))
        ledger.apply_shared_condition_settlement(
            condition_id=condition_id,
            terminal_state=local_terminal_state,
            allocation={
                **allocation,
                "primary_token_id": receipt["primary_token_id"],
                "secondary_token_id": receipt["secondary_token_id"],
                "inventory_hash": receipt["inventory_hash"],
            },
            transaction_hash=receipt.get("transaction_hash") or None,
            created_at_ms=created_at_ms,
        )
        coordinator.mark_shared_allocation_applied(
            condition_id=condition_id,
            profile_key=profile,
            created_at_ms=created_at_ms,
        )
        applied_profiles.append(profile)
    complete = coordinator.complete_shared_redemption_distribution(
        condition_id=condition_id,
        terminal_state=coordinator_terminal_state,
        created_at_ms=created_at_ms,
        details={"applied_profile_keys": sorted(applied_profiles)},
    )
    return {
        "condition_id": str(condition_id).lower(),
        "state": coordinator_terminal_state if complete else "DISTRIBUTING",
        "applied_profile_keys": sorted(applied_profiles),
    }


def process_shared_condition_redemptions(
    *,
    coordinator: SharedWalletCoordinator,
    adapter: Any,
    wallet_address: str,
    observed_collateral_usd: Decimal | None = None,
) -> list[dict[str, Any]]:
    """Redeem one physical condition once and allocate it to every sleeve.

    The caller must already hold the process-shared wallet lock.  This routine
    never reposts an uncertain redemption and never lets a per-sleeve local
    redemption touch a condition managed here.
    """

    observed_at_ms = now_ms()
    normalized_wallet = str(wallet_address).lower()
    conditions = coordinator.shared_managed_condition_ids()
    results: list[dict[str, Any]] = []
    platform_candidates: list[dict[str, Any]] = []

    for condition_id in sorted(conditions):
        receipt = coordinator.shared_redemption_receipt(condition_id)
        if receipt is not None:
            continue
        try:
            inventory = coordinator.shared_condition_inventory(condition_id)
            if int(inventory["active_order_reservation_count"]) != 0:
                raise SharedWalletCoordinatorError(
                    "ACTIVE_SHARED_CONDITION_ORDER_RESERVATION"
                )
            if int(inventory["active_local_redemption_count"]) != 0:
                raise SharedWalletCoordinatorError(
                    "ACTIVE_LOCAL_REDEMPTION_BLOCKS_SHARED_HANDOFF"
                )
            winner_token_id = _verified_resolution(
                adapter=adapter,
                condition_id=condition_id,
                primary_token_id=str(inventory["primary_token_id"]),
                secondary_token_id=str(inventory["secondary_token_id"]),
            )
            if winner_token_id is None:
                results.append(
                    _redemption_result(
                        condition_id, "PENDING_OFFICIAL_RESOLUTION"
                    )
                )
                continue
            raw = _shared_condition_local_and_onchain_raw(
                adapter=adapter,
                inventory=inventory,
                wallet_address=normalized_wallet,
            )
        except Exception as exc:
            results.append(
                {
                    "condition_id": condition_id,
                    "state": "PENDING_SHARED_EVIDENCE",
                    "reason": f"{type(exc).__name__}",
                }
            )
            continue
        winner_local_raw = (
            raw["primary_local_raw"]
            if winner_token_id == str(inventory["primary_token_id"])
            else raw["secondary_local_raw"]
        )
        expected_payout = Decimal(winner_local_raw) / TOKEN_SCALE
        if _shared_raw_inventory_is_exact(raw):
            receipt = coordinator.freeze_shared_condition_redemption(
                condition_id=condition_id,
                winner_token_id=winner_token_id,
                created_at_ms=observed_at_ms,
            )
            if expected_payout == ZERO:
                coordinator.mark_shared_redemption_state(
                    condition_id=condition_id,
                    state="LOSS_DISTRIBUTING",
                    reason="OFFICIAL_ZERO_PAYOUT_WITH_EXACT_AGGREGATE_INVENTORY",
                    created_at_ms=observed_at_ms,
                    details=raw,
                )
                results.append(
                    _apply_shared_redemption_allocations(
                        coordinator=coordinator,
                        condition_id=condition_id,
                        local_terminal_state="LOSS_RESOLVED_SHARED_WALLET",
                        coordinator_terminal_state="LOSS_RESOLVED_NO_PAYOUT",
                        created_at_ms=observed_at_ms,
                    )
                )
            continue
        if raw["primary_onchain_raw"] == 0 and raw["secondary_onchain_raw"] == 0:
            platform_candidates.append(
                {
                    "condition_id": condition_id,
                    "inventory": inventory,
                    "winner_token_id": winner_token_id,
                    "expected_payout_usd": expected_payout,
                    "raw": raw,
                }
            )
            continue
        results.append(
            {
                "condition_id": condition_id,
                "state": "BLOCK_SHARED_ONCHAIN_INVENTORY_MISMATCH",
                **raw,
            }
        )

    if platform_candidates:
        expected_total = sum(
            (
                Decimal(str(candidate["expected_payout_usd"]))
                for candidate in platform_candidates
            ),
            ZERO,
        )
        platform_proven = False
        if observed_collateral_usd is not None and expected_total > ZERO:
            observed = Decimal(str(observed_collateral_usd))
            cash_before = coordinator.authenticated_account_cash_snapshot(
                authenticated_collateral_usd=observed,
            ).expected_accounting_cash_high_usd
            platform_proven = observed - cash_before == expected_total
        if platform_proven:
            for candidate in platform_candidates:
                condition_id = str(candidate["condition_id"])
                coordinator.freeze_shared_condition_redemption(
                    condition_id=condition_id,
                    winner_token_id=str(candidate["winner_token_id"]),
                    created_at_ms=observed_at_ms,
                )
                coordinator.mark_shared_redemption_state(
                    condition_id=condition_id,
                    state="PLATFORM_DISTRIBUTING",
                    reason="EXACT_AUTHENTICATED_CASH_DELTA_AND_ZERO_OUTCOME_BALANCES",
                    created_at_ms=observed_at_ms,
                    details=candidate["raw"],
                )
                results.append(
                    _apply_shared_redemption_allocations(
                        coordinator=coordinator,
                        condition_id=condition_id,
                        local_terminal_state=(
                            "REDEEMED_SHARED_PLATFORM_SETTLEMENT"
                        ),
                        coordinator_terminal_state=(
                            "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED"
                        ),
                        created_at_ms=observed_at_ms,
                    )
                )
        else:
            results.extend(
                {
                    "condition_id": str(candidate["condition_id"]),
                    "state": "PENDING_SHARED_PLATFORM_CASH_PROOF",
                }
                for candidate in platform_candidates
            )

    for receipt in coordinator.shared_redemption_receipts():
        condition_id = str(receipt["condition_id"])
        state = str(receipt["state"])
        if state in {
            "REDEEMED",
            "LOSS_RESOLVED_NO_PAYOUT",
            "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED",
            "ERROR",
        }:
            continue
        if state == "LOSS_DISTRIBUTING":
            results.append(
                _apply_shared_redemption_allocations(
                    coordinator=coordinator,
                    condition_id=condition_id,
                    local_terminal_state="LOSS_RESOLVED_SHARED_WALLET",
                    coordinator_terminal_state="LOSS_RESOLVED_NO_PAYOUT",
                    created_at_ms=observed_at_ms,
                )
            )
            continue
        if state == "DISTRIBUTING":
            results.append(
                _apply_shared_redemption_allocations(
                    coordinator=coordinator,
                    condition_id=condition_id,
                    local_terminal_state="REDEEMED_SHARED_WALLET",
                    coordinator_terminal_state="REDEEMED",
                    created_at_ms=observed_at_ms,
                )
            )
            continue
        if state == "PLATFORM_DISTRIBUTING":
            results.append(
                _apply_shared_redemption_allocations(
                    coordinator=coordinator,
                    condition_id=condition_id,
                    local_terminal_state="REDEEMED_SHARED_PLATFORM_SETTLEMENT",
                    coordinator_terminal_state=(
                        "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED"
                    ),
                    created_at_ms=observed_at_ms,
                )
            )
            continue
        if state == "SUBMIT_STARTED":
            coordinator.mark_shared_redemption_state(
                condition_id=condition_id,
                state="UNKNOWN_SUBMISSION",
                reason="RECOVERY_UNKNOWN_AFTER_SHARED_SUBMIT_STARTED",
                created_at_ms=observed_at_ms,
            )
            results.append(_redemption_result(condition_id, "UNKNOWN_SUBMISSION"))
            continue
        transaction_id = str(receipt.get("transaction_id") or "")
        if state == "UNKNOWN_SUBMISSION" and not transaction_id:
            try:
                winner = _verified_resolution(
                    adapter=adapter,
                    condition_id=condition_id,
                    primary_token_id=receipt["primary_token_id"],
                    secondary_token_id=receipt["secondary_token_id"],
                )
                frozen_inventory = {
                    "primary_token_id": receipt["primary_token_id"],
                    "secondary_token_id": receipt["secondary_token_id"],
                    "primary_quantity": receipt["primary_quantity"],
                    "secondary_quantity": receipt["secondary_quantity"],
                }
                raw = _shared_condition_local_and_onchain_raw(
                    adapter=adapter,
                    inventory=frozen_inventory,
                    wallet_address=normalized_wallet,
                )
                expected_payout = Decimal(str(receipt["expected_payout_usd"]))
                allocations = coordinator.shared_condition_allocations(condition_id)
                cash_proven = False
                if (
                    observed_collateral_usd is not None
                    and expected_payout > ZERO
                    and len(allocations) >= 2
                ):
                    observed = Decimal(str(observed_collateral_usd))
                    cash_before = coordinator.authenticated_account_cash_snapshot(
                        authenticated_collateral_usd=observed,
                    ).expected_accounting_cash_high_usd
                    cash_proven = observed - cash_before == expected_payout
                if (
                    winner == receipt["winner_token_id"]
                    and raw["primary_onchain_raw"] == 0
                    and raw["secondary_onchain_raw"] == 0
                    and cash_proven
                ):
                    coordinator.mark_shared_redemption_state(
                        condition_id=condition_id,
                        state="PLATFORM_DISTRIBUTING",
                        reason=(
                            "UNKNOWN_SUBMISSION_RESOLVED_BY_EXACT_CASH_AND_ZERO_BALANCES"
                        ),
                        created_at_ms=observed_at_ms,
                        details=raw,
                    )
                    results.append(
                        _apply_shared_redemption_allocations(
                            coordinator=coordinator,
                            condition_id=condition_id,
                            local_terminal_state=(
                                "REDEEMED_SHARED_PLATFORM_SETTLEMENT"
                            ),
                            coordinator_terminal_state=(
                                "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED"
                            ),
                            created_at_ms=observed_at_ms,
                        )
                    )
                    continue
            except Exception:
                pass
            results.append(_redemption_result(condition_id, state))
            continue
        if state in {"SUBMITTED_UNRECONCILED", "PENDING", "UNKNOWN_SUBMISSION"}:
            if not transaction_id:
                coordinator.mark_shared_redemption_state(
                    condition_id=condition_id,
                    state="UNKNOWN_SUBMISSION",
                    reason="MISSING_SHARED_REDEMPTION_TRANSACTION_ID",
                    created_at_ms=observed_at_ms,
                )
                results.append(
                    _redemption_result(condition_id, "UNKNOWN_SUBMISSION")
                )
                continue
            try:
                relayer_status = adapter.redemption_transaction_status(transaction_id)
            except Exception as exc:
                results.append(
                    {
                        "condition_id": condition_id,
                        "state": "PENDING",
                        "reason": f"{type(exc).__name__}",
                    }
                )
                continue
            relayer_state = str(relayer_status.get("state") or "").upper()
            transaction_hash = relayer_status.get("transaction_hash")
            if relayer_state == "STATE_CONFIRMED":
                allocations = coordinator.shared_condition_allocations(condition_id)
                frozen_inventory = {
                    "primary_token_id": receipt["primary_token_id"],
                    "secondary_token_id": receipt["secondary_token_id"],
                    "primary_quantity": receipt["primary_quantity"],
                    "secondary_quantity": receipt["secondary_quantity"],
                }
                winner = _verified_resolution(
                    adapter=adapter,
                    condition_id=condition_id,
                    primary_token_id=receipt["primary_token_id"],
                    secondary_token_id=receipt["secondary_token_id"],
                )
                raw = _shared_condition_local_and_onchain_raw(
                    adapter=adapter,
                    inventory=frozen_inventory,
                    wallet_address=normalized_wallet,
                )
                if (
                    winner != receipt["winner_token_id"]
                    or raw["primary_onchain_raw"] != 0
                    or raw["secondary_onchain_raw"] != 0
                    or len(allocations) < 2
                ):
                    coordinator.mark_shared_redemption_state(
                        condition_id=condition_id,
                        state="ERROR",
                        reason="CONFIRMED_SHARED_REDEMPTION_EVIDENCE_MISMATCH",
                        created_at_ms=observed_at_ms,
                        details=raw,
                        transaction_hash=(
                            None
                            if transaction_hash is None
                            else str(transaction_hash)
                        ),
                    )
                    results.append(_redemption_result(condition_id, "ERROR"))
                    continue
                normalized_transaction_hash = str(transaction_hash or "").strip().lower()
                try:
                    expected_payout_raw = _quantity_to_token_raw(
                        Decimal(str(receipt["expected_payout_usd"]))
                    )
                    observed_payout_raw = (
                        _confirmed_redemption_collateral_payout_raw(
                            adapter=adapter,
                            transaction_hash=normalized_transaction_hash,
                            wallet_address=normalized_wallet,
                        )
                    )
                except Exception as exc:
                    coordinator.mark_shared_redemption_state(
                        condition_id=condition_id,
                        state="PENDING",
                        reason=(
                            "CONFIRMED_SHARED_REDEMPTION_AWAITING_ONCHAIN_"
                            "COLLATERAL_PAYOUT_PROOF:"
                            + type(exc).__name__
                        ),
                        created_at_ms=observed_at_ms,
                        details={**raw, "winner_token_id": winner},
                        transaction_hash=(
                            None if transaction_hash is None else str(transaction_hash)
                        ),
                    )
                    results.append(_redemption_result(condition_id, "PENDING"))
                    continue
                if observed_payout_raw != expected_payout_raw:
                    coordinator.mark_shared_redemption_state(
                        condition_id=condition_id,
                        state="PENDING",
                        reason=(
                            "CONFIRMED_SHARED_REDEMPTION_ONCHAIN_COLLATERAL_"
                            "PAYOUT_MISMATCH"
                        ),
                        created_at_ms=observed_at_ms,
                        details={
                            **raw,
                            "winner_token_id": winner,
                            "expected_payout_raw": str(expected_payout_raw),
                            "observed_payout_raw": str(observed_payout_raw),
                        },
                        transaction_hash=normalized_transaction_hash,
                    )
                    results.append(_redemption_result(condition_id, "PENDING"))
                    continue
                coordinator.mark_shared_redemption_state(
                    condition_id=condition_id,
                    state="DISTRIBUTING",
                    reason=(
                        "RELAYER_CONFIRMED_WITH_EXACT_ONCHAIN_"
                        "COLLATERAL_PAYOUT"
                    ),
                    created_at_ms=observed_at_ms,
                    details={
                        **raw,
                        "winner_token_id": winner,
                        "confirmed_collateral_payout_raw": str(
                            observed_payout_raw
                        ),
                    },
                    transaction_hash=normalized_transaction_hash,
                )
                results.append(
                    _apply_shared_redemption_allocations(
                        coordinator=coordinator,
                        condition_id=condition_id,
                        local_terminal_state="REDEEMED_SHARED_WALLET",
                        coordinator_terminal_state="REDEEMED",
                        created_at_ms=observed_at_ms,
                    )
                )
                continue
            if relayer_state in {
                "STATE_FAILED",
                "STATE_INVALID",
                "STATE_REJECTED",
                "STATE_EXPIRED",
            }:
                coordinator.mark_shared_redemption_state(
                    condition_id=condition_id,
                    state="ERROR",
                    reason="RELAYER_SHARED_REDEMPTION_" + relayer_state,
                    created_at_ms=observed_at_ms,
                    transaction_hash=(
                        None if transaction_hash is None else str(transaction_hash)
                    ),
                )
                results.append(_redemption_result(condition_id, "ERROR"))
                continue
            if state != "PENDING":
                coordinator.mark_shared_redemption_state(
                    condition_id=condition_id,
                    state="PENDING",
                    reason="RELAYER_" + (relayer_state or "UNKNOWN"),
                    created_at_ms=observed_at_ms,
                    transaction_hash=(
                        None if transaction_hash is None else str(transaction_hash)
                    ),
                )
            results.append(_redemption_result(condition_id, "PENDING"))
            continue
        if state not in {"READY", "NOT_SUBMITTED_RETRYABLE"}:
            results.append(_redemption_result(condition_id, state))
            continue
        try:
            inventory = coordinator.verify_shared_condition_inventory(condition_id)
            winner = _verified_resolution(
                adapter=adapter,
                condition_id=condition_id,
                primary_token_id=receipt["primary_token_id"],
                secondary_token_id=receipt["secondary_token_id"],
            )
            raw = _shared_condition_local_and_onchain_raw(
                adapter=adapter,
                inventory=inventory,
                wallet_address=normalized_wallet,
            )
            if winner != receipt["winner_token_id"] or not _shared_raw_inventory_is_exact(raw):
                raise LiveConfigurationError(
                    "SHARED_PRE_SUBMISSION_EVIDENCE_CHANGED"
                )
        except Exception as exc:
            results.append(
                {
                    "condition_id": condition_id,
                    "state": "BLOCK_SHARED_PRE_SUBMISSION_REVALIDATION",
                    "reason": f"{type(exc).__name__}",
                }
            )
            continue
        if not coordinator.start_shared_redemption_submission(
            condition_id=condition_id,
            created_at_ms=observed_at_ms,
        ):
            results.append(
                _redemption_result(
                    condition_id,
                    str(coordinator.shared_redemption_receipt(condition_id)["state"]),
                )
            )
            continue
        try:
            inventory = coordinator.verify_shared_condition_inventory(condition_id)
            before_submit_winner = _verified_resolution(
                adapter=adapter,
                condition_id=condition_id,
                primary_token_id=receipt["primary_token_id"],
                secondary_token_id=receipt["secondary_token_id"],
            )
            before_submit_raw = _shared_condition_local_and_onchain_raw(
                adapter=adapter,
                inventory=inventory,
                wallet_address=normalized_wallet,
            )
            if (
                before_submit_winner != receipt["winner_token_id"]
                or not _shared_raw_inventory_is_exact(before_submit_raw)
            ):
                raise LiveConfigurationError(
                    "SHARED_PRE_SUBMISSION_EVIDENCE_CHANGED"
                )
        except Exception as exc:
            coordinator.mark_shared_redemption_state(
                condition_id=condition_id,
                state="NOT_SUBMITTED_RETRYABLE",
                reason="SHARED_PRE_SUBMISSION_REVALIDATION_RETRYABLE:"
                + type(exc).__name__,
                created_at_ms=observed_at_ms,
            )
            results.append(
                _redemption_result(condition_id, "NOT_SUBMITTED_RETRYABLE")
            )
            continue
        try:
            submitted = adapter.submit_redeem(condition_id=condition_id)
        except RedemptionNotSubmittedError:
            coordinator.mark_shared_redemption_state(
                condition_id=condition_id,
                state="NOT_SUBMITTED_RETRYABLE",
                reason="RELAYER_PROVED_NOT_SUBMITTED",
                created_at_ms=observed_at_ms,
            )
            results.append(
                _redemption_result(condition_id, "NOT_SUBMITTED_RETRYABLE")
            )
            continue
        except Exception as exc:
            coordinator.mark_shared_redemption_state(
                condition_id=condition_id,
                state="UNKNOWN_SUBMISSION",
                reason="SHARED_SUBMISSION_TRANSPORT_UNKNOWN:"
                + type(exc).__name__,
                created_at_ms=observed_at_ms,
            )
            results.append(_redemption_result(condition_id, "UNKNOWN_SUBMISSION"))
            continue
        transaction_id = (
            ""
            if not isinstance(submitted, Mapping)
            else str(submitted.get("transaction_id") or "")
        )
        if not transaction_id:
            coordinator.mark_shared_redemption_state(
                condition_id=condition_id,
                state="UNKNOWN_SUBMISSION",
                reason="MISSING_SHARED_REDEMPTION_TRANSACTION_ID",
                created_at_ms=observed_at_ms,
            )
            results.append(_redemption_result(condition_id, "UNKNOWN_SUBMISSION"))
            continue
        transaction_hash = submitted.get("transaction_hash")
        coordinator.mark_shared_redemption_submission(
            condition_id=condition_id,
            transaction_id=transaction_id,
            transaction_hash=(
                None if transaction_hash is None else str(transaction_hash)
            ),
            created_at_ms=observed_at_ms,
        )
        results.append(
            _redemption_result(condition_id, "SUBMITTED_UNRECONCILED")
        )
    return results


def _is_retryable_external_error(exc: BaseException) -> bool:
    """Keep a healthy process on its current watermark for transient endpoints.

    Retrying in-process avoids a supervisor restart advancing the forward-only
    watermark and then attempting to infer a missed action from a later book.
    Deterministic decode, ledger, and configuration failures remain fatal.
    """

    external_exception_names = {
        "RpcError",
        "PublicReadError",
        "BoundedHttpError",
        "HTTPError",
        "URLError",
        "ConnectError",
        "ReadTimeout",
        "WriteTimeout",
        "NetworkError",
        "TransportError",
    }
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        # A local SQLite failure can arise while handling an earlier endpoint
        # exception (for example, while persisting its immutable receipt).
        # Its implicit exception context must not turn a ledger/status I/O
        # failure into a retryable network event: that would keep a real-money
        # daemon accepting new heads without surfacing its local evidence path.
        if isinstance(current, sqlite3.Error):
            return False
        if isinstance(current, (ConnectionError, TimeoutError, OSError)):
            return True
        if (
            isinstance(current, RuntimeError)
            and str(current).strip() in RETRYABLE_EMPTY_BOOK_LEVEL_ERRORS
        ):
            return True
        if type(current).__name__ == "PolyApiException":
            status = getattr(current, "status_code", None)
            try:
                status_code = None if status is None else int(status)
            except (TypeError, ValueError):
                return False
            if status_code is None or status_code == 429 or status_code >= 500:
                return True
            message = str(
                getattr(current, "error_msg", getattr(current, "error_message", ""))
            ).lower()
            return status_code == 404 and "no orderbook exists" in message
        if type(current).__name__ == "InvalidStatus":
            response = getattr(current, "response", None)
            status = getattr(response, "status_code", None)
            try:
                status_code = int(status)
            except (TypeError, ValueError):
                return False
            return status_code == 429 or status_code >= 500
        if type(current).__name__ in external_exception_names:
            return True
        if type(current).__module__.startswith(("httpx", "requests", "urllib")):
            return True
        current = current.__cause__ or current.__context__
    return False


def _ws_connect_exception_decision(
    exc: Exception,
    *,
    default_process_exception: Callable[[Exception], Exception | None],
) -> Exception | None:
    """Override the websocket library only for measured transient failures."""

    if _is_retryable_external_error(exc):
        return None
    return default_process_exception(exc)


def _repair_window_recovery_manager(
    *, store: LiveStore, follower: Any | None = None
) -> Any:
    """Return the one process-local manager backed by the live SQLite ledger."""

    if follower is not None:
        cached = getattr(follower, "_repair_window_recovery_manager", None)
        if cached is not None:
            return cached
    from repair_window_recovery import RepairWindowRecoveryManager

    manager = RepairWindowRecoveryManager(store)
    if follower is not None:
        try:
            setattr(follower, "_repair_window_recovery_manager", manager)
        except (AttributeError, TypeError):
            pass
    return manager


def _process_live_ws_head(
    *,
    store: LiveStore,
    runtime_dir: Path,
    follower: LiveSourceFollower,
    execution: Any,
    head: int,
    start_redemption_cycle: Callable[[], None],
) -> bool:
    """Process one announced head without tearing down a healthy websocket.

    A retryable source-discovery failure before durable action observation
    leaves the source cursor untouched for exact block replay.  A per-action
    external failure with an explicit durable pending state advances the source
    cursor and is retried from that queue on a later head.  Non-durable internal
    failures still escape to the session supervisor as fatal failures.
    """

    announced_head = int(head)
    processable_head = max(0, announced_head - 1)
    cursor_before_raw = store.runtime_value("last_processed_block")
    cursor_before = (
        None if cursor_before_raw is None else int(cursor_before_raw)
    )
    has_new_processable_block = (
        cursor_before is None or processable_head > cursor_before
    )
    recovery_internal_error: str | None = None
    persisted_recovery_raw = store.runtime_value(
        "repair_recovery_last_cycle_json"
    )
    persisted_recovery = (
        {}
        if not persisted_recovery_raw
        else json.loads(persisted_recovery_raw)
    )
    recovery_code_repair_required = (
        isinstance(persisted_recovery, Mapping)
        and persisted_recovery.get("state") == "CODE_REPAIR_REQUIRED"
    )
    selector_code_repair_required = (
        store.runtime_value("retryable_target_selector_state")
        == "CODE_REPAIR_REQUIRED"
    )
    redemption_maintenance_error: str | None = None
    store.set_runtime("last_announced_head", announced_head)
    store.set_runtime("processing_target_head", processable_head)
    try:
        wallet_lock_path = getattr(follower, "wallet_lock_path", None)
        coordinator = getattr(follower, "coordinator", None)
        profile_key = getattr(follower, "profile_key", None)
        source_error: Exception | None = None
        source_traceback = None
        current_intake_action_ids: set[str] = set()
        current_intake_token_ids: set[str] = set()
        if has_new_processable_block:
            cancellation_at_ms = now_ms()
            if wallet_lock_path is None:
                cancel_due_active_gtd_orders(
                    store=store,
                    execution=execution,
                    due_at_ms=cancellation_at_ms,
                )
            else:
                with _shared_wallet_submission_lock(wallet_lock_path):
                    cancel_due_active_gtd_orders(
                        store=store,
                        execution=execution,
                        due_at_ms=cancellation_at_ms,
                    )
        try:
            source_cycle_result = follower.run_cycle_to_head(
                head=processable_head,
                execution=execution,
                live_enabled=True,
            )
            if not isinstance(source_cycle_result, Mapping) or not {
                "source_action_count",
                "source_action_ids",
                "last_processed_block",
            } <= set(source_cycle_result):
                raise LiveConfigurationError("INVALID_SOURCE_CYCLE_RESULT")
            raw_source_action_ids = source_cycle_result["source_action_ids"]
            if not isinstance(raw_source_action_ids, list):
                raise LiveConfigurationError("INVALID_SOURCE_CYCLE_RESULT")
            current_intake_action_ids = {
                str(action_id).strip() for action_id in raw_source_action_ids
            }
            if (
                "" in current_intake_action_ids
                or len(current_intake_action_ids) != len(raw_source_action_ids)
                or int(source_cycle_result["source_action_count"])
                != len(raw_source_action_ids)
            ):
                raise LiveConfigurationError("INVALID_SOURCE_CYCLE_RESULT")
            if current_intake_action_ids:
                placeholders = ",".join("?" for _ in current_intake_action_ids)
                with store.connect() as connection:
                    current_receipts = connection.execute(
                        f"""
                        SELECT action_id, token_id
                        FROM action_receipts
                        WHERE action_id IN ({placeholders})
                        """,
                        tuple(sorted(current_intake_action_ids)),
                    ).fetchall()
                if len(current_receipts) != len(current_intake_action_ids):
                    raise LiveConfigurationError(
                        "SOURCE_CYCLE_ACTION_RECEIPT_NOT_PERSISTED"
                    )
                current_intake_token_ids = {
                    str(row["token_id"]) for row in current_receipts
                }
            reported_source_cursor = int(
                source_cycle_result["last_processed_block"]
            )
            persisted_source_cursor_raw = store.runtime_value(
                "last_processed_block"
            )
            if (
                persisted_source_cursor_raw is None
                or int(persisted_source_cursor_raw) != reported_source_cursor
            ):
                raise LiveConfigurationError(
                    "SOURCE_CYCLE_CURSOR_NOT_PERSISTED"
                )
            expected_source_cursor = (
                processable_head
                if cursor_before is None
                else max(cursor_before, processable_head)
            )
            if reported_source_cursor != expected_source_cursor:
                raise LiveConfigurationError("SOURCE_CYCLE_CURSOR_MISMATCH")
        except Exception as exc:
            source_error = exc
            source_traceback = exc.__traceback__

        # The bounded due-check still runs after a failed source read so an
        # external RPC incident cannot permanently starve wallet maintenance.
        # When both paths fail, retain the source failure as the head outcome
        # and append the simultaneous maintenance fault without replacing it.
        redemption_error: Exception | None = None
        try:
            start_redemption_cycle()
        except Exception as exc:
            redemption_error = exc
        if redemption_error is not None:
            redemption_message = _redact_sensitive_text(
                f"{type(redemption_error).__name__}: {redemption_error}"
            )
            redemption_details = {
                "announced_head": announced_head,
                "processable_head": processable_head,
                "source_failure_retained": source_error is not None,
                "source_batch_cursor_retained": source_error is None,
            }
            if source_error is not None:
                redemption_details["source_exception_type"] = type(
                    source_error
                ).__name__
            try:
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category=(
                        "EXTERNAL_REDEMPTION_MAINTENANCE"
                        if _is_retryable_external_error(redemption_error)
                        else "INTERNAL_REDEMPTION_MAINTENANCE"
                    ),
                    message=redemption_message,
                    details=redemption_details,
                )
            except Exception as evidence_exc:
                evidence_failure = LiveConfigurationError(
                    "REDEMPTION_MAINTENANCE_EVIDENCE_WRITE_FAILED"
                )
                evidence_failure.add_note(
                    "redemption exception retained: "
                    + redemption_message
                )
                if source_error is not None:
                    evidence_failure.add_note(
                        "source exception retained: "
                        + _redact_sensitive_text(
                            f"{type(source_error).__name__}: {source_error}"
                        )
                    )
                raise evidence_failure from evidence_exc
            if source_error is None:
                redemption_maintenance_error = redemption_message
        if source_error is not None:
            raise source_error.with_traceback(source_traceback)

        if has_new_processable_block:
            recovery_manager = _repair_window_recovery_manager(
                store=store,
                follower=follower,
            )
            recovery_manager.claim_new_causal_dependents(
                claimed_at_ms=now_ms()
            )
            current_attempts = {
                str(details.get("attempt_id") or "")
                for _source, details in store.unreconciled_submissions()
                if str(details.get("attempt_id") or "")
            }
            _reconcile_submissions_and_refresh_cash(
                store=store,
                execution=execution,
                wallet_lock_path=wallet_lock_path,
                coordinator=coordinator,
                profile_key=profile_key,
                attempt_ids=current_attempts,
            )
            # Reconciliation may expose an immutable terminal transition while
            # leaving its mutable target in a retryable state.  Align the two
            # before constructing the single ordinary retry slot so a terminal
            # ghost cannot consume the durable rotation cursor.
            store.repair_retryable_targets_with_terminal_latest_transition(
                changed_at_ms=now_ms()
            )
            attempts_before_history = {
                str(details.get("attempt_id") or "")
                for _source, details in store.unreconciled_submissions()
                if str(details.get("attempt_id") or "")
            }
            action_scope = getattr(follower, "action_scope", None)
            frozen_lifecycle_resolver = getattr(
                action_scope, "resolve_retry_lifecycle", None
            )
            lifecycle_resolver = None
            if callable(frozen_lifecycle_resolver):

                def lifecycle_resolver(source: SourceAction) -> Any:
                    frozen = store.frozen_action_metadata(source.action_id)
                    if frozen is None:
                        raise LiveConfigurationError(
                            "RETRY_ACTION_MISSING_FROZEN_MARKET_METADATA"
                        )
                    return frozen_lifecycle_resolver(
                        source,
                        dict(frozen["metadata"]),
                    )

            else:
                candidate_resolver = getattr(action_scope, "resolve_action", None)
                if callable(candidate_resolver):
                    lifecycle_resolver = candidate_resolver
            recovery_attempt_count_before = store.submission_attempt_count()
            try:
                recovery_result = recovery_manager.run_cycle(
                    processable_head=processable_head,
                    execution=execution,
                    wallet_lock_path=wallet_lock_path,
                    coordinator=coordinator,
                    profile_key=str(profile_key or ""),
                    exclude_action_ids=current_intake_action_ids,
                    market_lifecycle_resolver=(
                        lifecycle_resolver
                        if callable(lifecycle_resolver)
                        else None
                    ),
                )
            except Exception as exc:
                # The dedicated repair queue is isolated from ordinary
                # forward actions.  Keep its exact claims fail-closed, retain
                # the forward cursor already processed above, and surface the
                # internal fault without creating a restart gap.
                if _is_retryable_external_error(exc) or isinstance(
                    exc,
                    (sqlite3.DatabaseError, OSError, SharedWalletCoordinatorError),
                ):
                    raise
                recovery_internal_error = _redact_sensitive_text(
                    str(exc)
                    if isinstance(exc, LiveConfigurationError)
                    else f"{type(exc).__name__}: {exc}"
                )
                recovery_attempt_count_after = store.submission_attempt_count()
                store.append_runtime_error(
                    occurred_at_ms=now_ms(),
                    category="INTERNAL_REPAIR_WINDOW_RECOVERY",
                    message=recovery_internal_error,
                    details={
                        "processable_head": processable_head,
                        "claimed_actions_remain_excluded": True,
                        "submission_attempt_count_before": (
                            recovery_attempt_count_before
                        ),
                        "submission_attempt_count_after": (
                            recovery_attempt_count_after
                        ),
                        "new_submission_attempt_count": (
                            recovery_attempt_count_after
                            - recovery_attempt_count_before
                        ),
                        "order_submission_inferred_from_attempt_count": False,
                    },
                )
                recovery_result = {
                    "state": "CODE_REPAIR_REQUIRED",
                    "reason": recovery_internal_error,
                    "processable_head": processable_head,
                }
            store.set_runtime(
                "repair_recovery_last_cycle_json",
                json.dumps(
                    recovery_result,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
            )
            recovery_code_repair_required = (
                isinstance(recovery_result, Mapping)
                and recovery_result.get("state") == "CODE_REPAIR_REQUIRED"
            )
            claimed_after_source_discovery = recovery_manager.claimed_action_ids()
            prior_sources = store.retryable_prior_actions(
                processed_through_block=cursor_before
            )
            selector_code_repair_required = (
                store.runtime_value("retryable_target_selector_state")
                == "CODE_REPAIR_REQUIRED"
            )
            prior_sources = [
                source
                for source in prior_sources
                if source.action_id not in claimed_after_source_discovery
                and source.action_id not in current_intake_action_ids
                and source.token_id not in current_intake_token_ids
            ]
            observed_processor = getattr(
                follower, "_process_observed_action_safely", None
            )
            process_prior_action = None
            if prior_sources and not callable(observed_processor):
                raise LiveConfigurationError(
                    "MISSING_UNIFIED_PRIOR_ACTION_PROCESSOR"
                )
            if prior_sources:

                def process_prior_action(source: SourceAction) -> dict[str, Any]:
                    return observed_processor(
                        action=source,
                        execution=execution,
                        live_enabled=True,
                    )

            retry_pending_actions(
                store=store,
                execution=execution,
                wallet_lock_path=wallet_lock_path,
                coordinator=coordinator,
                profile_key=profile_key,
                sources=prior_sources,
                market_lifecycle_resolver=(
                    lifecycle_resolver if callable(lifecycle_resolver) else None
                ),
                process_action=process_prior_action,
            )
            attempts_after_cycle = {
                str(details.get("attempt_id") or "")
                for _source, details in store.unreconciled_submissions()
                if str(details.get("attempt_id") or "")
            }
            _reconcile_submissions_and_refresh_cash(
                store=store,
                execution=execution,
                wallet_lock_path=wallet_lock_path,
                coordinator=coordinator,
                profile_key=profile_key,
                attempt_ids=attempts_after_cycle - attempts_before_history,
            )
    except LiveConfigurationError as exc:
        # A mismatch between the independently observed public wallet page
        # and the chain-maker action is an internal evidence discrepancy.  It
        # must retain the cursor and remain visible, but it must not tear down
        # the websocket session and manufacture a restart gap.
        if str(exc) != "PUBLIC_CHAIN_MAKER_ACTION_RECONCILIATION_MISMATCH":
            raise
        store.append_runtime_error(
            occurred_at_ms=now_ms(),
            category="INTERNAL_SOURCE_RECONCILIATION",
            message=str(exc),
            details={"head": announced_head, "cursor_retained": True},
        )
        store.set_runtime(
            "last_cycle_outcome", "INTERNAL_SOURCE_RECONCILIATION_PENDING"
        )
        write_status_files_best_effort(store, runtime_dir)
        return False
    except SharedWalletCoordinatorError as exc:
        store.append_runtime_error(
            occurred_at_ms=now_ms(),
            category="INTERNAL_SHARED_WALLET_INVARIANT",
            message=f"{type(exc).__name__}: {exc}",
            details={"head": announced_head, "cursor_retained": True},
        )
        store.set_runtime("last_cycle_outcome", "INTERNAL_WALLET_INVARIANT_PENDING")
        write_status_files_best_effort(store, runtime_dir)
        return False
    except Exception as exc:
        if not _is_retryable_external_error(exc):
            raise
        store.record_external_head_incident(
            occurred_at_ms=now_ms(),
            head=announced_head,
            message=f"{type(exc).__name__}: {exc}",
        )
        store.set_runtime("last_cycle_outcome", "EXTERNAL_HEAD_RETRY_PENDING")
        write_status_files_best_effort(store, runtime_dir)
        return False
    completed_at_ms = now_ms()
    store.recover_external_head_incident(
        recovered_at_ms=completed_at_ms,
        processed_head=processable_head,
    )
    store.set_runtime("last_successful_cycle_at_ms", str(completed_at_ms))
    # The second reconciliation pass can be the first observer of a corrupt
    # reservation/attempt pair created later in this same head.  Read the
    # durable state again instead of relying on the pre-history cache.
    submission_code_repair_required = (
        store.runtime_value("submission_reconciliation_state")
        == "CODE_REPAIR_REQUIRED"
    )
    store.set_runtime(
        "last_cycle_outcome",
        (
            "SUCCESS_FORWARD_RECOVERY_CODE_REPAIR_REQUIRED"
            if (
                recovery_internal_error is not None
                or recovery_code_repair_required
                or selector_code_repair_required
                or submission_code_repair_required
            )
            else "SUCCESS_REDEMPTION_MAINTENANCE_PENDING"
            if redemption_maintenance_error is not None
            else "SUCCESS"
        ),
    )
    write_status_files_best_effort(store, runtime_dir)
    return True


def _process_hot_standby_ws_head(
    *,
    store: LiveStore,
    runtime_dir: Path,
    follower: LiveSourceFollower,
    execution: Any,
    head: int,
    start_redemption_cycle: Callable[[], None],
) -> bool:
    """Process only after the primary runtime owner has exited.

    The primary owns ``live.lock`` for its full daemon lifetime.  A standby
    keeps its subscription alive, but while the lock is held it only observes
    heads.  After an owner exit, the next confirmed head processes the exact
    persisted cursor-to-head range without a replacement watermark.
    """

    if (
        str(store.runtime_value("operator_planned_resume_state") or "").upper()
        == "PENDING"
    ):
        store.set_runtime("hot_standby_last_observed_head", int(head))
        return True

    try:
        with _exclusive_runtime_lock(_profile_runtime_lock_path(runtime_dir)):
            takeover_at_ms = now_ms()
            store.set_runtime("hot_standby_takeover_started_at_ms", takeover_at_ms)
            store.set_runtime("hot_standby_last_takeover_head", int(head))
            store.set_runtime(
                "hot_standby_takeover_count",
                int(store.runtime_value("hot_standby_takeover_count") or "0")
                + 1,
            )
            result = _process_live_ws_head(
                store=store,
                runtime_dir=runtime_dir,
                follower=follower,
                execution=execution,
                head=head,
                start_redemption_cycle=start_redemption_cycle,
            )
            store.set_runtime("hot_standby_takeover_finished_at_ms", now_ms())
            return result
    except LiveConfigurationError as exc:
        if str(exc) != "CD90_LIVE_ALREADY_RUNNING":
            raise
        store.set_runtime(
            "hot_standby_primary_runtime_lock_seen_at_ms", now_ms()
        )
        store.set_runtime("hot_standby_last_observed_head", int(head))
        return True


def _live_env(env: Mapping[str, str]) -> dict[str, str]:
    if str(env.get("POLYMARKET_LIVE_TRADING", "")) != "1":
        raise LiveDisabledError("missing_POLYMARKET_LIVE_TRADING_1")
    if str(env.get("I_UNDERSTAND_THIS_PLACES_A_REAL_ORDER", "")) != "1":
        raise LiveDisabledError("missing_real_order_confirmation")
    missing = [name for name in LIVE_REQUIRED_ENV if not str(env.get(name, ""))]
    if missing:
        raise LiveConfigurationError("missing_live_configuration:" + ",".join(missing))
    try:
        allocation = Decimal(str(env["CD90_ALLOCATION_USD"]))
    except (InvalidOperation, ValueError) as exc:
        raise LiveConfigurationError("invalid_CD90_ALLOCATION_USD") from exc
    if not allocation.is_finite() or allocation < ZERO:
        raise LiveConfigurationError("negative_CD90_ALLOCATION_USD")
    try:
        int(str(env["POLYMARKET_SIGNATURE_TYPE"]))
    except ValueError as exc:
        raise LiveConfigurationError("invalid_POLYMARKET_SIGNATURE_TYPE") from exc
    if not Path(str(env["POLYMARKET_SHARED_WALLET_LOCK_PATH"])).is_absolute():
        raise LiveConfigurationError("SHARED_WALLET_LOCK_PATH_NOT_ABSOLUTE")
    if not Path(
        str(env["POLYMARKET_SHARED_WALLET_COORDINATOR_PATH"])
    ).is_absolute():
        raise LiveConfigurationError(
            "SHARED_WALLET_COORDINATOR_PATH_NOT_ABSOLUTE"
        )
    return {key: str(env[key]) for key in LIVE_REQUIRED_ENV}


def build_authenticated_live_client(env: Mapping[str, str]) -> Any:
    """Build the authenticated client only after both live guards are set."""

    values = _live_env(env)
    try:
        from py_clob_client_v2 import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds
    except ImportError as exc:
        raise LiveConfigurationError("py_clob_client_v2_unavailable") from exc
    credentials = ApiCreds(
        api_key=values["POLYMARKET_API_KEY"],
        api_secret=values["POLYMARKET_API_SECRET"],
        api_passphrase=values["POLYMARKET_API_PASSPHRASE"],
    )
    return ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137,
        key=values["POLYMARKET_PRIVATE_KEY"],
        creds=credentials,
        signature_type=int(values["POLYMARKET_SIGNATURE_TYPE"]),
        funder=values["POLYMARKET_FUNDER_ADDRESS"],
        use_server_time=True,
    )


class CD90RedemptionAdapter:
    """Official SecureClient bridge for the isolated CD90 redemption worker.

    The CLOB L2 key is deliberately not used as a gasless-wallet key.  This
    adapter receives a separately-created Relayer API key and uses the
    official SDK for the transaction and authenticated relayer-status read.
    """

    def __init__(self, *, secure_client: Any) -> None:
        self.secure_client = secure_client
        self.wallet_address = str(secure_client._ctx.wallet).lower()

    def official_redemption_activities(self) -> list[dict[str, Any]]:
        """Return this authenticated wallet's public official REDEEM rows."""

        return fetch_official_redemption_activities(self.wallet_address)

    def official_redemption_activity_for_transaction(
        self, *, condition_id: str, transaction_hash: str
    ) -> dict[str, Any] | None:
        """Read the exact official payout for one confirmed relayer transaction."""

        return _exact_official_redemption_activity(
            official_activities=self.official_redemption_activities(),
            wallet_address=self.wallet_address,
            condition_id=condition_id,
            transaction_hash=transaction_hash,
        )

    @staticmethod
    def _read_uint_call(*, client: Any, to: str, signature: str, types: list[str], values: list[Any]) -> int:
        try:
            from eth_abi import encode
            from eth_utils import keccak
        except ImportError as exc:
            raise LiveConfigurationError("REDEMPTION_ABI_CODEC_UNAVAILABLE") from exc
        selector = keccak(text=signature)[:4]
        data = "0x" + (selector + encode(types, values)).hex()
        raw = client._ctx.rpc.eth_call(to=str(to), data=data)
        if not isinstance(raw, str) or not raw.startswith("0x"):
            raise LiveConfigurationError("INVALID_REDEMPTION_RPC_RESULT")
        try:
            return int(raw, 16)
        except ValueError as exc:
            raise LiveConfigurationError("INVALID_REDEMPTION_RPC_UINT") from exc

    def condition_mapping_for_token(self, token_id: str) -> dict[str, str]:
        # This endpoint is the documented token -> condition / two-token map.
        url = "https://clob.polymarket.com/markets-by-token/" + str(token_id)
        payload = _bounded_public_json(url)
        if not isinstance(payload, dict):
            raise LiveConfigurationError("INVALID_TOKEN_CONDITION_MAPPING_RESPONSE")
        condition_id = str(payload.get("condition_id", "")).lower()
        primary_token_id = str(payload.get("primary_token_id", ""))
        secondary_token_id = str(payload.get("secondary_token_id", ""))
        LiveStore._validate_condition_mapping(
            token_id=str(token_id),
            condition_id=condition_id,
            primary_token_id=primary_token_id,
            secondary_token_id=secondary_token_id,
        )
        clob_token_set = {primary_token_id, secondary_token_id}
        official_markets: list[Any] = []
        for closed in (False, True):
            paginator = self.secure_client.list_markets(
                condition_ids=[condition_id], closed=closed, page_size=1
            )
            iterator = (
                paginator.iter_items()
                if callable(getattr(paginator, "iter_items", None))
                else iter(paginator)
            )
            official_markets = list(iterator)
            official_markets = [
                market
                for market in official_markets
                if str(getattr(market, "condition_id", "")).lower() == condition_id
            ]
            if official_markets:
                break
        if len(official_markets) != 1:
            raise LiveConfigurationError("OFFICIAL_CONDITION_MARKET_NOT_UNIQUE")
        outcomes = getattr(official_markets[0], "outcomes", None)
        yes_token_id = str(getattr(getattr(outcomes, "yes", None), "token_id", ""))
        no_token_id = str(getattr(getattr(outcomes, "no", None), "token_id", ""))
        if {yes_token_id, no_token_id} != clob_token_set:
            raise LiveConfigurationError("OFFICIAL_CONDITION_TOKEN_SET_MISMATCH")
        return {
            "condition_id": condition_id,
            "primary_token_id": yes_token_id,
            "secondary_token_id": no_token_id,
        }

    def condition_resolution(self, condition_id: str) -> dict[str, Any]:
        """Read final payout numerators from Conditional Tokens on Polygon.

        Gamma/CLOB metadata is useful for mapping token IDs, but the payout
        denominator and numerators are the authoritative settlement state. A
        split/ambiguous payout is deliberately blocked instead of guessed.
        """

        normalized = str(condition_id).lower()
        if not normalized.startswith("0x") or len(normalized) != 66:
            raise LiveConfigurationError("INVALID_CONDITION_ID")
        try:
            condition_bytes = bytes.fromhex(normalized[2:])
        except ValueError as exc:
            raise LiveConfigurationError("INVALID_CONDITION_ID") from exc
        env = self.secure_client._ctx.environment
        denominator = self._read_uint_call(
            client=self.secure_client,
            to=str(env.conditional_tokens),
            signature="payoutDenominator(bytes32)",
            types=["bytes32"],
            values=[condition_bytes],
        )
        if denominator == 0:
            return {
                "condition_id": normalized,
                "closed": False,
                "winner_token_id": None,
            }
        primary_numerator = self._read_uint_call(
            client=self.secure_client,
            to=str(env.conditional_tokens),
            signature="payoutNumerators(bytes32,uint256)",
            types=["bytes32", "uint256"],
            values=[condition_bytes, 0],
        )
        secondary_numerator = self._read_uint_call(
            client=self.secure_client,
            to=str(env.conditional_tokens),
            signature="payoutNumerators(bytes32,uint256)",
            types=["bytes32", "uint256"],
            values=[condition_bytes, 1],
        )
        # The automated CD90 sleeve only handles the normal binary full-payout
        # shape. Anything else remains an auditable manual-review block.
        if primary_numerator == denominator and secondary_numerator == 0:
            winner_index = 0
        elif secondary_numerator == denominator and primary_numerator == 0:
            winner_index = 1
        else:
            raise LiveConfigurationError("AMBIGUOUS_OR_SPLIT_ONCHAIN_PAYOUT")
        market = _bounded_public_json(
            "https://clob.polymarket.com/markets/" + normalized
        )
        if (
            not isinstance(market, dict)
            or str(market.get("condition_id", "")).lower() != normalized
        ):
            raise LiveConfigurationError("INVALID_CANONICAL_CONDITION_MARKET")
        tokens = market.get("tokens")
        if not isinstance(tokens, list) or len(tokens) != 2 or any(
            not isinstance(token, dict) for token in tokens
        ):
            raise LiveConfigurationError("INVALID_CANONICAL_CONDITION_TOKENS")
        token_ids = [str(token.get("token_id", "")).strip() for token in tokens]
        if not all(token_ids) or len(set(token_ids)) != 2:
            raise LiveConfigurationError("INVALID_CANONICAL_CONDITION_TOKENS")
        market_winner_indices = [
            index for index, token in enumerate(tokens) if token.get("winner") is True
        ]
        if market_winner_indices and market_winner_indices != [winner_index]:
            raise LiveConfigurationError("CLOB_ONCHAIN_WINNER_MISMATCH")
        return {
            "condition_id": normalized,
            "closed": True,
            "winner_index": winner_index,
            "winner_token_id": token_ids[winner_index],
        }

    def outcome_token_balance_raw(self, *, wallet_address: str, token_id: str) -> int:
        try:
            from eth_utils import to_checksum_address
        except ImportError as exc:
            raise LiveConfigurationError("REDEMPTION_ABI_CODEC_UNAVAILABLE") from exc
        env = self.secure_client._ctx.environment
        return self._read_uint_call(
            client=self.secure_client,
            to=str(env.conditional_tokens),
            signature="balanceOf(address,uint256)",
            types=["address", "uint256"],
            values=[to_checksum_address(str(wallet_address)), int(str(token_id))],
        )

    def collateral_balance_raw(self, *, wallet_address: str) -> int:
        try:
            from eth_utils import to_checksum_address
        except ImportError as exc:
            raise LiveConfigurationError("REDEMPTION_ABI_CODEC_UNAVAILABLE") from exc
        env = self.secure_client._ctx.environment
        return self._read_uint_call(
            client=self.secure_client,
            to=str(env.collateral_token),
            signature="balanceOf(address)",
            types=["address"],
            values=[to_checksum_address(str(wallet_address))],
        )

    def confirmed_redemption_collateral_payout_raw(
        self, *, transaction_hash: str, wallet_address: str
    ) -> int:
        """Sum exact collateral-token transfers to this wallet in one mined tx.

        Relayer state and public activity rows are useful discovery evidence,
        but only this transaction receipt proves that the authenticated wallet
        actually received the claimed collateral.  Transfers to another
        address, outgoing transfers, and unrelated token logs are excluded.
        """

        normalized_hash = str(transaction_hash or "").strip().lower()
        normalized_wallet = str(wallet_address or "").strip().lower()
        if (
            not normalized_hash.startswith("0x")
            or len(normalized_hash) != 66
            or not normalized_wallet.startswith("0x")
            or len(normalized_wallet) != 42
        ):
            raise LiveConfigurationError("INVALID_REDEMPTION_TRANSACTION_IDENTITY")
        try:
            from eth_utils import keccak
        except ImportError as exc:
            raise LiveConfigurationError(
                "REDEMPTION_ABI_CODEC_UNAVAILABLE"
            ) from exc
        receipt = self.secure_client._ctx.rpc.eth_get_transaction_receipt(
            normalized_hash
        )
        if not isinstance(receipt, Mapping):
            raise LiveConfigurationError("INVALID_REDEMPTION_TRANSACTION_RECEIPT")
        status = receipt.get("status")
        try:
            status_value = (
                int(str(status), 16)
                if isinstance(status, str) and str(status).startswith("0x")
                else int(status)
            )
        except (TypeError, ValueError) as exc:
            raise LiveConfigurationError(
                "INVALID_REDEMPTION_TRANSACTION_RECEIPT_STATUS"
            ) from exc
        if status_value != 1:
            raise LiveConfigurationError("REDEMPTION_TRANSACTION_NOT_SUCCEEDED")
        logs = receipt.get("logs")
        if not isinstance(logs, list):
            raise LiveConfigurationError("INVALID_REDEMPTION_TRANSACTION_LOGS")
        collateral_token = str(
            self.secure_client._ctx.environment.collateral_token
        ).lower()
        transfer_topic = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
        recipient_topic = "0x" + ("0" * 24) + normalized_wallet[2:]
        payout_raw = 0
        for log in logs:
            if not isinstance(log, Mapping):
                continue
            topics = log.get("topics")
            if (
                str(log.get("address") or "").lower() != collateral_token
                or not isinstance(topics, list)
                or len(topics) < 3
                or str(topics[0]).lower() != transfer_topic
                or str(topics[2]).lower() != recipient_topic
            ):
                continue
            raw_data = str(log.get("data") or "")
            if not raw_data.startswith("0x") or len(raw_data) != 66:
                raise LiveConfigurationError(
                    "INVALID_REDEMPTION_COLLATERAL_TRANSFER_LOG"
                )
            try:
                amount = int(raw_data, 16)
            except ValueError as exc:
                raise LiveConfigurationError(
                    "INVALID_REDEMPTION_COLLATERAL_TRANSFER_LOG"
                ) from exc
            if amount < 0:
                raise LiveConfigurationError(
                    "NEGATIVE_REDEMPTION_COLLATERAL_TRANSFER"
                )
            payout_raw += amount
        return payout_raw

    def submit_redeem(self, *, condition_id: str) -> dict[str, str | None]:
        try:
            handle = self.secure_client.redeem_positions(
                condition_id=str(condition_id),
                metadata="CD90 automatic redemption",
            )
        except Exception as exc:
            if (
                type(exc).__name__ == "UserInputError"
                and str(exc).startswith("No market found for condition")
            ):
                raise RedemptionNotSubmittedError(str(exc)) from exc
            raise
        transaction_id = str(getattr(handle, "transaction_id", ""))
        if not transaction_id:
            raise LiveConfigurationError("MISSING_SDK_REDEMPTION_TRANSACTION_ID")
        transaction_hash = getattr(handle, "transaction_hash", None)
        return {
            "transaction_id": transaction_id,
            "transaction_hash": None if transaction_hash is None else str(transaction_hash),
        }

    def redemption_transaction_status(self, transaction_id: str) -> dict[str, str | None]:
        try:
            from polymarket._internal.actions.relayer.poll import fetch_gasless_transaction_sync
        except ImportError as exc:
            raise LiveConfigurationError("RELAYER_STATUS_CLIENT_UNAVAILABLE") from exc
        transaction = fetch_gasless_transaction_sync(
            self.secure_client._ctx.relayer,
            transaction_id=str(transaction_id),
        )
        state = getattr(transaction, "state", None)
        value = getattr(state, "value", state)
        if not value:
            raise LiveConfigurationError("INVALID_RELAYER_TRANSACTION_STATE")
        transaction_hash = getattr(transaction, "transaction_hash", None)
        return {
            "state": str(value),
            "transaction_hash": None if transaction_hash is None else str(transaction_hash),
        }


def build_redemption_adapter(env: Mapping[str, str]) -> CD90RedemptionAdapter | None:
    """Create an official gasless adapter only with an explicit enable flag."""

    if str(env.get(AUTO_REDEMPTION_ENV, "")) != "1":
        return None
    values = _live_env(env)
    missing = [name for name in REDEMPTION_REQUIRED_ENV if not str(env.get(name, ""))]
    if missing:
        raise LiveConfigurationError("missing_redemption_configuration:" + ",".join(missing))
    try:
        from eth_account import Account
        from polymarket import RelayerApiKey, SecureClient
        from polymarket.models.clob.api_key import ApiKeyCreds
    except ImportError as exc:
        raise LiveConfigurationError("official_redemption_sdk_unavailable") from exc
    signer_address = str(Account.from_key(values["POLYMARKET_PRIVATE_KEY"]).address).lower()
    relayer_address = str(env["POLYMARKET_RELAYER_API_KEY_ADDRESS"]).lower()
    if signer_address != relayer_address:
        raise LiveConfigurationError("RELAYER_SIGNER_ADDRESS_MISMATCH")
    credentials = ApiKeyCreds(
        apiKey=values["POLYMARKET_API_KEY"],
        secret=values["POLYMARKET_API_SECRET"],
        passphrase=values["POLYMARKET_API_PASSPHRASE"],
    )
    secure_client = SecureClient.create(
        private_key=values["POLYMARKET_PRIVATE_KEY"],
        wallet=values["POLYMARKET_FUNDER_ADDRESS"],
        credentials=credentials,
        api_key=RelayerApiKey(
            key=str(env["POLYMARKET_RELAYER_API_KEY"]),
            address=relayer_address,
        ),
    )
    if str(secure_client._ctx.wallet).lower() != values["POLYMARKET_FUNDER_ADDRESS"].lower():
        secure_client.close()
        raise LiveConfigurationError("REDEMPTION_WALLET_BINDING_MISMATCH")
    if str(secure_client._ctx.wallet_type) != "DEPOSIT_WALLET":
        secure_client.close()
        raise LiveConfigurationError("REDEMPTION_WALLET_TYPE_NOT_DEPOSIT")
    return CD90RedemptionAdapter(secure_client=secure_client)


def refresh_condition_mappings(
    *, store: LiveStore, adapter: CD90RedemptionAdapter
) -> list[dict[str, str]]:
    """Bind only current CD90-held tokens to the immutable official market map."""

    results: list[dict[str, str]] = []
    for token_id in store.unmapped_position_token_ids():
        mapping = adapter.condition_mapping_for_token(token_id)
        store.bind_condition_for_token(
            token_id=token_id,
            condition_id=str(mapping["condition_id"]),
            primary_token_id=str(mapping["primary_token_id"]),
            secondary_token_id=str(mapping["secondary_token_id"]),
            observed_at_ms=now_ms(),
        )
        results.append({"token_id": str(token_id), "condition_id": str(mapping["condition_id"])})
    return results


def run_redemption_cycle(
    *,
    store: LiveStore,
    adapter: CD90RedemptionAdapter,
    execution: Any | None = None,
    wallet_lock_path: Path | None = None,
    coordinator: SharedWalletCoordinator | None = None,
    profile_key: str | None = None,
) -> dict[str, Any]:
    """One wallet-maintenance pass serialized with CLOB submissions."""

    if wallet_lock_path is not None:
        with _shared_wallet_submission_lock(wallet_lock_path):
            return _run_redemption_cycle_locked(
                store=store,
                adapter=adapter,
                execution=execution,
                coordinator=coordinator,
                profile_key=profile_key,
            )
    return _run_redemption_cycle_locked(
        store=store,
        adapter=adapter,
        execution=execution,
        coordinator=coordinator,
        profile_key=profile_key,
    )


def _run_redemption_cycle_locked(
    *,
    store: LiveStore,
    adapter: CD90RedemptionAdapter,
    execution: Any | None,
    coordinator: SharedWalletCoordinator | None,
    profile_key: str | None,
) -> dict[str, Any]:
    """Perform redemption work after the authenticated-wallet lock is held."""

    mappings = refresh_condition_mappings(store=store, adapter=adapter)
    platform_settlement: dict[str, Any] | None = None
    observed_collateral: Decimal | None = None
    if execution is not None:
        sampled_at_ms = now_ms()
        try:
            observed_collateral = Decimal(str(execution.collateral_balance_usd()))
            if not observed_collateral.is_finite() or observed_collateral < ZERO:
                raise LiveConfigurationError("INVALID_AUTHENTICATED_COLLATERAL")
        except Exception as exc:
            store.append_runtime_error(
                occurred_at_ms=sampled_at_ms,
                category="EXTERNAL_AUTHENTICATED_COLLATERAL",
                message=f"{type(exc).__name__}: {exc}",
            )
            platform_settlement = {
                "state": "EXTERNAL_COLLATERAL_UNAVAILABLE",
                "condition_count": 0,
            }
        else:
            _persist_authenticated_collateral_observation(
                store=store,
                observed_collateral_usd=observed_collateral,
                observed_at_ms=sampled_at_ms,
                coordinator=coordinator,
                profile_key=profile_key,
            )

    shared_decisions: list[dict[str, Any]] = []
    shared_conditions: set[str] = set()
    if coordinator is not None:
        shared_conditions = coordinator.shared_managed_condition_ids()
        shared_decisions = process_shared_condition_redemptions(
            coordinator=coordinator,
            adapter=adapter,
            wallet_address=adapter.wallet_address,
            observed_collateral_usd=observed_collateral,
        )
        # A new central receipt can be created by the call above.  Refresh the
        # exclusion set before any legacy single-sleeve reconciliation or
        # submission path runs.
        shared_conditions = coordinator.shared_managed_condition_ids()

    official_settlement: dict[str, Any] | None = None
    terminal_payout_corrections: dict[str, Any] | None = None
    official_activity_reader = getattr(
        adapter, "official_redemption_activities", None
    )
    if callable(official_activity_reader):
        sampled_at_ms = now_ms()
        try:
            official_activities = official_activity_reader()
            terminal_payout_corrections = reconcile_terminal_redemption_payouts(
                store=store,
                wallet_address=adapter.wallet_address,
                official_activities=official_activities,
                created_at_ms=sampled_at_ms,
            )
            frozen_cash_baseline_at_ms = None
            if coordinator is not None:
                frozen_cash_baseline_at_ms = int(
                    coordinator.migration_receipt()["observed_at_ms"]
                )
            official_settlement = reconcile_official_redeem_activities(
                store=store,
                adapter=adapter,
                wallet_address=adapter.wallet_address,
                official_activities=official_activities,
                created_at_ms=sampled_at_ms,
                frozen_cash_baseline_at_ms=frozen_cash_baseline_at_ms,
                exclude_condition_ids=shared_conditions,
                quarantine_confirmed_cash_credit=coordinator is not None,
            )
            store.set_runtime(
                "official_activity_settlement_reconciliation_state",
                str(official_settlement["state"]),
            )
            store.set_runtime(
                "official_activity_settlement_reconciliation_at_ms",
                str(sampled_at_ms),
            )
        except Exception as exc:
            store.append_runtime_error(
                occurred_at_ms=sampled_at_ms,
                category="EXTERNAL_OFFICIAL_REDEMPTION_ACTIVITY",
                message=f"{type(exc).__name__}: {exc}",
            )
            official_settlement = {
                "state": "EXTERNAL_OFFICIAL_ACTIVITY_UNAVAILABLE",
                "condition_count": 0,
            }

    reconciled = reconcile_redemption_submissions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        exclude_condition_ids=shared_conditions,
        quarantine_confirmed_cash_credit=coordinator is not None,
    )
    try:
        official_cash_credit = (
            official_settlement is not None
            and Decimal(str(official_settlement.get("cash_credited_usd", "0")))
            > ZERO
        )
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise LiveConfigurationError("INVALID_OFFICIAL_REDEEM_CASH_CREDIT") from exc
    if execution is not None and (
        official_cash_credit
        or any(result.get("state") == "REDEEMED" for result in reconciled)
    ):
        # A relayer confirmation or an exact official activity can precede
        # the CLOB collateral credit.  Take a fresh authenticated sample while
        # the wallet lock is still held; the coordinator keeps any shortfall
        # quarantined instead of exposing it as spendable sleeve cash.
        _refresh_authenticated_collateral_after_cash_mutation(
            store=store,
            execution=execution,
            coordinator=coordinator,
            profile_key=profile_key,
            force=True,
        )
    if execution is not None and observed_collateral is not None:
        sampled_at_ms = now_ms()
        try:
            platform_settlement = reconcile_platform_settled_winners(
                store=store,
                adapter=adapter,
                observed_collateral_usd=observed_collateral,
                created_at_ms=sampled_at_ms,
                coordinator=coordinator,
                profile_key=profile_key,
                exclude_condition_ids=shared_conditions,
            )
            store.set_runtime(
                "platform_settlement_reconciliation_state",
                str(platform_settlement["state"]),
            )
            store.set_runtime(
                "platform_settlement_reconciliation_at_ms", str(sampled_at_ms)
            )
            if str(platform_settlement["state"]).startswith("BLOCK_"):
                store.append_runtime_error(
                    occurred_at_ms=sampled_at_ms,
                    category=_platform_settlement_error_category(
                        str(platform_settlement["state"])
                    ),
                    message=str(platform_settlement["state"]),
                    details=platform_settlement,
                )
        except Exception as exc:
            store.append_runtime_error(
                occurred_at_ms=sampled_at_ms,
                category="INTERNAL_PLATFORM_SETTLEMENT_RECONCILIATION",
                message=f"{type(exc).__name__}: {exc}",
            )
            raise
    decisions = auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
        exclude_condition_ids=shared_conditions,
    )
    outcome = {
        "mappings": mappings,
        "reconciled": reconciled,
        "decisions": decisions,
    }
    if coordinator is not None:
        outcome["shared_conditions"] = sorted(shared_conditions)
        outcome["shared_decisions"] = shared_decisions
    if platform_settlement is not None:
        outcome["platform_settlement"] = platform_settlement
    if official_settlement is not None:
        outcome["official_activity_settlement"] = official_settlement
    if terminal_payout_corrections is not None:
        outcome["terminal_payout_corrections"] = terminal_payout_corrections
    return outcome


def parse_source_open_position_value(payload: Any) -> Decimal:
    """Parse the documented public value response without guessing its shape.

    The endpoint currently returns a single-item array containing ``value``.
    A direct object is also accepted for backward-compatible API responses.
    Multiple entries cannot be silently summed because that would alter the
    fixed-scale denominator and therefore the live sleeve's size.
    """

    if isinstance(payload, dict):
        row = payload
    elif isinstance(payload, list) and len(payload) == 1 and isinstance(payload[0], dict):
        row = payload[0]
    else:
        raise LiveConfigurationError("unexpected_source_sleeve_value_shape")
    try:
        value = Decimal(str(row["value"]))
    except (KeyError, InvalidOperation, ValueError) as exc:
        raise LiveConfigurationError("invalid_source_sleeve_value") from exc
    if not value.is_finite() or value <= ZERO:
        raise LiveConfigurationError("nonpositive_source_sleeve_value")
    return value


def fetch_source_open_position_value_usd(source_wallet: str) -> Decimal:
    """Read the public current value of the source's open position sleeve."""

    normalized = str(source_wallet or "").lower()
    if not normalized.startswith("0x") or len(normalized) != 42:
        raise LiveConfigurationError("invalid_CD90_SOURCE_WALLET")
    url = "https://data-api.polymarket.com/value?" + urlencode({"user": normalized})
    try:
        payload = _bounded_public_json(url)
    except Exception as exc:
        raise LiveConfigurationError(f"source_sleeve_value_unavailable:{type(exc).__name__}") from exc
    return parse_source_open_position_value(payload)


def _decimal_config(env: Mapping[str, str], key: str) -> Decimal:
    try:
        value = Decimal(str(env[key]))
    except (KeyError, InvalidOperation, ValueError) as exc:
        raise LiveConfigurationError(f"invalid_{key}") from exc
    if value <= ZERO:
        raise LiveConfigurationError(f"nonpositive_{key}")
    return value


def _nonnegative_decimal_config(env: Mapping[str, str], key: str) -> Decimal:
    try:
        value = Decimal(str(env[key]))
    except (KeyError, InvalidOperation, ValueError) as exc:
        raise LiveConfigurationError(f"invalid_{key}") from exc
    if not value.is_finite() or value < ZERO:
        raise LiveConfigurationError(f"negative_{key}")
    return value


def _status_payload(store: LiveStore) -> dict[str, Any]:
    store.initialize()
    with store.connect() as connection:
        runtime_rows = connection.execute(
            "SELECT key, value FROM runtime_state ORDER BY key"
        ).fetchall()
        latest_rows = connection.execute(
            """
            SELECT t.status, COUNT(*) AS count
            FROM action_receipts AS a
            JOIN action_transitions AS t
              ON t.id = (
                SELECT id FROM action_transitions
                WHERE action_id = a.action_id
                ORDER BY id DESC LIMIT 1
              )
            GROUP BY t.status
            ORDER BY t.status
            """
        ).fetchall()
        positions = connection.execute(
            """
            SELECT token_id, quantity, cost_basis_usd
            FROM positions
            WHERE CAST(quantity AS REAL) != 0
            ORDER BY token_id
            """
        ).fetchall()
        errors = connection.execute(
            "SELECT COUNT(*) AS count FROM runtime_errors"
        ).fetchone()
        redemption_rows = connection.execute(
            """
            SELECT state, COUNT(*) AS count
            FROM redemption_receipts
            GROUP BY state
            ORDER BY state
            """
        ).fetchall()
        redemption_total = connection.execute(
            "SELECT COUNT(*) AS count FROM redemption_receipts"
        ).fetchone()
    runtime = {str(row["key"]): str(row["value"]) for row in runtime_rows}
    account: dict[str, str] | None
    try:
        account = {key: str(value) for key, value in store.account_snapshot().items()}
        # The sleeve ledger remains an attribution record.  It is deliberately
        # not surfaced as cash that can fund the next BUY.
        account["strategy_attribution_cash_usd"] = account["cash_usd"]
        coordinator_path = store.config("shared_wallet_coordinator_path")
        profile_key = store.config("profile_key")
        coordinator_instance: SharedWalletCoordinator | None = None
        coordinator_observation_error: Exception | None = None
        authenticated = runtime.get("last_authenticated_collateral_usd")
        if coordinator_path and profile_key:
            coordinator_instance = SharedWalletCoordinator(Path(coordinator_path))
            try:
                shared_observation = (
                    coordinator_instance.latest_authenticated_collateral_observation()
                )
            except (sqlite3.Error, SharedWalletCoordinatorError) as exc:
                coordinator_observation_error = exc
            else:
                if shared_observation is not None:
                    authenticated = shared_observation[
                        "authenticated_collateral_usd"
                    ]
                    account["authenticated_cash_observer_profile_key"] = (
                        shared_observation["profile_key"]
                    )
                    account["authenticated_cash_observed_at_ms"] = (
                        shared_observation["observed_at_ms"]
                    )
        account["authenticated_clob_collateral_usd"] = (
            authenticated if authenticated is not None else "not yet sampled"
        )
        if not coordinator_path:
            if authenticated is None:
                account["available_cash_usd"] = "NOT_YET_AUTHENTICATED"
                account["available_cash_authority"] = (
                    "AUTHENTICATED_COLLATERAL_REQUIRED"
                )
            else:
                try:
                    physical_cash = Decimal(str(authenticated))
                    account["available_cash_usd"] = str(
                        max(
                            physical_cash - store.active_buy_reservations_usd(),
                            ZERO,
                        )
                    )
                    account["available_cash_authority"] = (
                        "AUTHENTICATED_SINGLE_ACCOUNT_COLLATERAL_"
                        "MINUS_ACTIVE_BUY_RESERVATIONS"
                    )
                except (InvalidOperation, ValueError):
                    account["available_cash_usd"] = "INVALID_AUTHENTICATED_SAMPLE"
                    account["available_cash_authority"] = (
                        "AUTHENTICATED_COLLATERAL_REQUIRED"
                    )
        if authenticated is None:
            account["cash_reconciliation_delta_usd"] = "not yet sampled"
        else:
            try:
                account["cash_reconciliation_delta_usd"] = str(
                    Decimal(authenticated) - Decimal(account["cash_usd"])
                )
            except (InvalidOperation, ValueError):
                account["cash_reconciliation_delta_usd"] = "invalid authenticated sample"
        account["cash_reconciliation_state"] = runtime.get(
            "platform_settlement_reconciliation_state", "NOT_SAMPLED"
        )
        if coordinator_path and authenticated is not None:
            account["strategy_attribution_cash_usd"] = account["cash_usd"]
            try:
                if coordinator_observation_error is not None:
                    raise coordinator_observation_error
                physical_cash = Decimal(str(authenticated))
                if coordinator_instance is None:
                    raise SharedWalletCoordinatorError(
                        "MISSING_SHARED_WALLET_COORDINATOR"
                    )
                snapshot = coordinator_instance.authenticated_account_cash_snapshot(
                    authenticated_collateral_usd=physical_cash,
                )
            except (
                InvalidOperation,
                ValueError,
                sqlite3.Error,
                SharedWalletCoordinatorError,
            ) as exc:
                account["available_cash_usd"] = "UNAVAILABLE_COORDINATOR_ERROR"
                account["shared_wallet_cash_state"] = (
                    f"ERROR:{type(exc).__name__}:{exc}"
                )
            else:
                account["authenticated_account_active_buy_reservations_usd"] = str(
                    snapshot.active_buy_reservations_usd
                )
                account["coordinated_redeemed_cash_credit_quarantine_usd"] = str(
                    snapshot.redeemed_cash_credit_quarantine_usd
                )
                account["coordinated_permanent_redeemed_cash_credit_block_usd"] = str(
                    snapshot.permanent_redeemed_cash_credit_block_usd
                )
                account["available_cash_usd"] = str(
                    snapshot.available_for_new_buy_usd
                )
                account["available_cash_authority"] = (
                    "AUTHENTICATED_ACCOUNT_COLLATERAL_"
                    "MINUS_ACTIVE_BUY_RESERVATIONS"
                )
                account["shared_wallet_cash_state"] = snapshot.state
                account["account_expected_cash_low_usd"] = str(
                    snapshot.expected_accounting_cash_low_usd
                )
                account["account_expected_cash_high_usd"] = str(
                    snapshot.expected_accounting_cash_high_usd
                )
                account["unallocated_authenticated_account_cash_usd"] = str(
                    snapshot.unallocated_account_cash_usd
                )
                account["cash_reconciliation_delta_usd"] = str(
                    physical_cash - snapshot.expected_accounting_cash_high_usd
                )
                account["cash_reconciliation_state"] = snapshot.state
    except ScaleInputError:
        account = None
    unacknowledged_topic_alerts = store.source_topic_alerts(
        unacknowledged_only=True
    )
    repair_window_recovery = _repair_window_recovery_manager(
        store=store
    ).status_payload()
    return {
        "mode": "CASH_LIVE",
        "paper_only": False,
        "real_order_submission_enabled": store.runtime_value("real_order_submission_enabled") == "true",
        "real_order_submitted": store.runtime_value("real_order_submitted") == "true",
        "generated_at_ms": now_ms(),
        "config": {
            "profile_key": store.config("profile_key"),
            "profile_scope": store.config("profile_scope"),
            "source_action_detection_contract": store.config(
                "source_action_detection_contract"
            ),
            "shared_wallet_coordinator_path": store.config(
                "shared_wallet_coordinator_path"
            ),
            "shared_wallet_migration_receipt_hash": store.config(
                "shared_wallet_migration_receipt_hash"
            ),
            "allocation_usd": store.config("allocation_usd"),
            "source_open_position_value_usd": store.config("source_open_position_value_usd"),
            "source_sleeve_observed_at_ms": store.config("source_sleeve_observed_at_ms"),
            "fixed_share_scale": store.config("fixed_share_scale"),
            "scale_basis": store.config("scale_basis"),
            "minimum_marketable_buy_notional_usd": store.config(
                "minimum_marketable_buy_notional_usd"
            ),
            "minimum_marketable_buy_notional_source": store.config(
                "minimum_marketable_buy_notional_source"
            ),
            "minimum_size_policy": store.config("minimum_size_policy"),
            "allocation_role": "FIXED_SHARE_SCALE_INPUT_ONLY",
            "immediate_order_type": IMMEDIATE_ORDER_TYPE,
            "latest_scale_rebase": store.latest_scale_rebase(),
        },
        "account": account,
        "positions": [dict(row) for row in positions],
        "action_terminal_counts": {str(row["status"]): int(row["count"]) for row in latest_rows},
        "action_fidelity": store.action_fidelity_summary(),
        "liquidity_retry": store.liquidity_retry_summary(),
        "bounded_retry_history": {
            **store.bounded_retry_summary(),
            "current_policy": False,
        },
        "decision_units": store.decision_unit_summary(),
        "source_topic_alerts": {
            "unacknowledged_count": len(unacknowledged_topic_alerts),
            "unacknowledged": unacknowledged_topic_alerts,
        },
        "execution_drift_monitor": {
            "mode": "MONITOR_ONLY_NO_EXECUTION_GATE",
            "historical_reference_evidence": (
                "UNREPRODUCED_MISSING_RAW_CUTOFF_AND_HASH"
            ),
        },
        "repair_window_recovery": repair_window_recovery,
        "action_receipt_count": store.action_receipt_count(),
        "runtime_gap_receipt_count": store.runtime_gap_receipt_count(),
        "unpriced_gap_action_count": store.unpriced_gap_action_count(),
        "lossless_handoff_failure_action_count": (
            store.lossless_handoff_failure_action_count()
        ),
        "unresolved_lossless_handoff_action_count": (
            store.unresolved_lossless_handoff_action_count()
        ),
        "automatic_redemption": {
            "enabled": store.runtime_value("auto_redemption_enabled") == "true",
            "worker_state": store.runtime_value("auto_redemption_worker_state"),
            "last_cycle_at_ms": store.runtime_value("auto_redemption_last_cycle_at_ms"),
            "last_cycle_summary": store.runtime_value("auto_redemption_last_cycle_summary"),
            "receipt_count": int(redemption_total["count"] if redemption_total is not None else 0),
            "terminal_counts": {
                str(row["state"]): int(row["count"]) for row in redemption_rows
            },
            "unmapped_position_token_ids": store.unmapped_position_token_ids(),
        },
        "runtime": runtime,
        "runtime_error_count": int(errors["count"] if errors is not None else 0),
    }


def _atomic_write(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(body, encoding="utf-8")
    os.replace(temporary, path)


def write_status_files(store: LiveStore, runtime_dir: Path) -> dict[str, Any]:
    payload = _status_payload(store)
    _atomic_write(
        runtime_dir / "status.json",
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    account = payload.get("account") or {}
    rows = "".join(
        "<tr><td>{}</td><td>{}</td><td>{}</td></tr>".format(
            escape(str(row["token_id"])),
            escape(str(row["quantity"])),
            escape(str(row["cost_basis_usd"])),
        )
        for row in payload["positions"]
    ) or "<tr><td colspan='3'>No CD90 live inventory</td></tr>"
    html = """<!doctype html><meta charset='utf-8'><title>CD90 Live Copy</title>
<style>body{{font-family:-apple-system,BlinkMacSystemFont,sans-serif;margin:32px;color:#172033}}table{{border-collapse:collapse}}th,td{{border:1px solid #dce1ea;padding:8px;text-align:left}}</style>
<h1>CD90 cash live copy</h1>
<p><strong>CD90 strategy attribution</strong> &mdash; there is one authenticated trading account, not a separate CD90 wallet.</p>
<p>scale reference: {allocation} USD (not spendable cash) &nbsp; fixed share scale: {scale} &nbsp; immediate order: {order_type} &nbsp; below-minimum policy: {minimum_size_policy}</p>
<p>authenticated-account cash available for a new BUY: {wallet_buy_cash} USD &nbsp; active BUY reservations: {active_buy_reservations} USD</p>
<p>latest fixed-scale change (future actions only): {scale_rebase}</p>
<p>authenticated CLOB cash: {authenticated_cash} USD &nbsp; strategy attribution cash: {ledger_cash} USD &nbsp; cash reconciliation delta: {cash_delta} USD &nbsp; reconciliation state: {cash_reconciliation_state}</p>
<p>new-BUY cash authority: {cash} USD ({cash_authority}) &nbsp; redeemed cash awaiting authenticated collateral: {redeemed_cash_quarantine} USD &nbsp; permanently excluded false redemption cash: {permanent_redeemed_cash_block} USD &nbsp; realized P&amp;L: {pnl} USD &nbsp; runtime errors: {errors}</p>
<h2>Current CD90 sleeve inventory</h2><table><tr><th>Token</th><th>Shares</th><th>Cost basis (USD)</th></tr>{rows}</table>
<h2>Action fidelity</h2><pre>{action_fidelity}</pre>
<h2>Controlled liquidity retry</h2><pre>{liquidity_retry}</pre>
<h2>Decision units</h2><pre>{decision_units}</pre>
<h2>Non-Netflix source alerts</h2><pre>{source_topic_alerts}</pre>
<h2>Execution drift monitor</h2><pre>{drift_monitor}</pre>
<h2>Repair-window delayed recovery</h2><pre>{repair_window_recovery}</pre>
<h2>Action terminal counts</h2><pre>{counts}</pre>
<h2>Automatic redemption</h2><pre>{redemptions}</pre>
<h2>Runtime</h2><pre>{runtime}</pre>
""".format(
        allocation=escape(str(payload["config"]["allocation_usd"] or "uninitialized")),
        scale=escape(str(payload["config"]["fixed_share_scale"] or "uninitialized")),
        order_type=escape(str(payload["config"]["immediate_order_type"])),
        minimum_size_policy=escape(
            str(payload["config"]["minimum_size_policy"] or "uninitialized")
        ),
        wallet_buy_cash=escape(
            str(account.get("available_cash_usd", "not yet sampled"))
        ),
        active_buy_reservations=escape(
            str(account.get("authenticated_account_active_buy_reservations_usd", "0"))
        ),
        scale_rebase=escape(
            json.dumps(payload["config"]["latest_scale_rebase"], ensure_ascii=False, sort_keys=True)
            if payload["config"]["latest_scale_rebase"] is not None
            else "none"
        ),
        authenticated_cash=escape(
            str(account.get("authenticated_clob_collateral_usd", "not yet sampled"))
        ),
        ledger_cash=escape(str(account.get("cash_usd", "uninitialized"))),
        cash_delta=escape(
            str(account.get("cash_reconciliation_delta_usd", "not yet sampled"))
        ),
        cash_reconciliation_state=escape(
            str(account.get("cash_reconciliation_state", "NOT_SAMPLED"))
        ),
        cash=escape(str(account.get("available_cash_usd", "uninitialized"))),
        cash_authority=escape(
            str(
                account.get(
                    "available_cash_authority",
                    "AUTHENTICATED_ACCOUNT_COLLATERAL_MINUS_ACTIVE_BUY_RESERVATIONS",
                )
            )
        ),
        redeemed_cash_quarantine=escape(
            str(account.get("coordinated_redeemed_cash_credit_quarantine_usd", "0"))
        ),
        permanent_redeemed_cash_block=escape(
            str(
                account.get(
                    "coordinated_permanent_redeemed_cash_credit_block_usd",
                    "0",
                )
            )
        ),
        pnl=escape(str(account.get("realized_pnl_usd", "uninitialized"))),
        errors=payload["runtime_error_count"],
        rows=rows,
        action_fidelity=escape(
            json.dumps(payload["action_fidelity"], ensure_ascii=False, indent=2)
        ),
        liquidity_retry=escape(
            json.dumps(payload["liquidity_retry"], ensure_ascii=False, indent=2)
        ),
        decision_units=escape(
            json.dumps(payload["decision_units"], ensure_ascii=False, indent=2)
        ),
        source_topic_alerts=escape(
            json.dumps(
                payload["source_topic_alerts"],
                ensure_ascii=False,
                indent=2,
            )
        ),
        drift_monitor=escape(
            json.dumps(
                payload["execution_drift_monitor"],
                ensure_ascii=False,
                indent=2,
            )
        ),
        repair_window_recovery=escape(
            json.dumps(
                payload["repair_window_recovery"],
                ensure_ascii=False,
                indent=2,
            )
        ),
        counts=escape(json.dumps(payload["action_terminal_counts"], ensure_ascii=False, indent=2)),
        redemptions=escape(json.dumps(payload["automatic_redemption"], ensure_ascii=False, indent=2)),
        runtime=escape(json.dumps(payload["runtime"], ensure_ascii=False, indent=2)),
    )
    _atomic_write(runtime_dir / "status.html", html)
    return payload


def write_status_files_best_effort(store: LiveStore, runtime_dir: Path) -> dict[str, Any] | None:
    """Do not let a non-order status snapshot terminate the live WS worker."""

    try:
        return write_status_files(store, runtime_dir)
    except Exception as exc:
        print(
            "STATUS_SNAPSHOT_WRITE_FAILED:"
            f"{type(exc).__name__}:{_redact_sensitive_text(str(exc))}",
            file=sys.stderr,
            flush=True,
        )
        return None


@contextmanager
def _exclusive_runtime_lock(
    path: Path, *, wait_for_release: bool = False
) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            flags = fcntl.LOCK_EX
            if not wait_for_release:
                flags |= fcntl.LOCK_NB
            fcntl.flock(handle.fileno(), flags)
        except BlockingIOError as exc:
            raise LiveConfigurationError("CD90_LIVE_ALREADY_RUNNING") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _profile_runtime_lock_path(runtime_dir: Path) -> Path:
    """Place the profile lock under the protected shared runtime parent."""

    runtime = Path(runtime_dir)
    return runtime.with_name(f"{runtime.name}.lock")


@contextmanager
def _primary_runtime_lock(path: Path) -> Iterator[None]:
    """Wait for a transient hot-standby handoff without spawning a duplicate."""

    with _exclusive_runtime_lock(path, wait_for_release=True):
        yield


def arm_pre_repair_forward_recovery(
    *,
    store: LiveStore,
    change_id: str,
    reason: str,
    armed_at_ms: int,
) -> dict[str, Any]:
    """Record an operator-approved boundary before abandoning a stale gap.

    The subsequent initial websocket bootstrap is still responsible for
    reading the exact chain gap.  If source actions are found, it records an
    immutable internal no-action-time-book failure and advances only then.
    No order, position, cash, or historical price is created here.
    """

    last_raw = store.runtime_value("last_processed_block")
    if last_raw is None:
        raise LiveConfigurationError("FORWARD_WATERMARK_NOT_ESTABLISHED")
    try:
        last_processed_block = int(last_raw)
    except ValueError as exc:
        raise LiveConfigurationError("FORWARD_WATERMARK_NOT_ESTABLISHED") from exc
    result = store.arm_planned_operator_resume(
        resume_from_block=last_processed_block,
        change_id=str(change_id),
        reason=str(reason),
        armed_at_ms=int(armed_at_ms),
    )
    store.set_runtime("operator_pre_repair_forward_recovery_armed", "true")
    store.set_runtime(
        "operator_pre_repair_forward_recovery_change_id", str(change_id)
    )
    store.set_runtime(
        "operator_pre_repair_forward_recovery_reason", str(reason)
    )
    return result


def _arm_runtime(
    *,
    store: LiveStore,
    adapter: CLOBExecutionAdapter,
    env: Mapping[str, str],
    minimum_size_policy: str = MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    source_action_detection_contract: str = (
        SOURCE_ACTION_DETECTION_CONTRACT_CHAIN_MAKER_ONLY
    ),
) -> None:
    values = _live_env(env)
    source_wallet = str(values["CD90_SOURCE_WALLET"]).lower()
    if re.fullmatch(r"0x[a-f0-9]{40}", source_wallet) is None:
        raise LiveConfigurationError("INVALID_SOURCE_WALLET")
    if minimum_size_policy not in MINIMUM_SIZE_POLICIES:
        raise LiveConfigurationError(
            f"UNSUPPORTED_MINIMUM_SIZE_POLICY:{minimum_size_policy}"
        )
    allocation = _nonnegative_decimal_config(values, "CD90_ALLOCATION_USD")
    minimum_buy_notional = _decimal_config(
        values, "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD"
    )
    if adapter.minimum_marketable_buy_notional_usd != minimum_buy_notional:
        raise LiveConfigurationError("ADAPTER_BUY_MIN_NOTIONAL_MISMATCH")
    store.lock_config_once(
        "minimum_marketable_buy_notional_usd", minimum_buy_notional
    )
    store.lock_config_once(
        "minimum_marketable_buy_notional_source",
        "OFFICIAL_CLOB_REJECTION_EMPIRICAL_RUNTIME_AUDIT",
    )
    store.lock_config_once("source_wallet", source_wallet)
    store.migrate_source_action_detection_contract(
        expected_contract=source_action_detection_contract,
        changed_at_ms=now_ms(),
    )
    store.migrate_legacy_minimum_size_policy(
        expected_policy=minimum_size_policy,
        changed_at_ms=now_ms(),
    )
    existing_scale = store.config("fixed_share_scale")
    if existing_scale is None:
        if allocation <= ZERO:
            raise LiveConfigurationError(
                "POSITIVE_ALLOCATION_REQUIRED_TO_DERIVE_UNINITIALIZED_SCALE"
            )
        collateral = Decimal(str(adapter.collateral_balance_usd()))
        if not collateral.is_finite() or collateral < ZERO:
            raise LiveConfigurationError("INVALID_AUTHENTICATED_ACCOUNT_COLLATERAL")
        source_value = fetch_source_open_position_value_usd(values["CD90_SOURCE_WALLET"])
        store.initialize_scale_once(
            allocation_usd=allocation,
            source_open_position_value_usd=source_value,
            observed_at_ms=now_ms(),
        )
    else:
        stored_allocation = Decimal(str(store.config("allocation_usd")))
        if stored_allocation != allocation:
            raise LiveConfigurationError("IMMUTABLE_SCALE_ALLOCATION_MISMATCH")
    store.set_runtime("live_enabled", "true")
    store.set_runtime("paper_only", "false")
    store.set_runtime("real_order_submission_enabled", "true")
    if store.runtime_value("real_order_submitted") is None:
        store.set_runtime("real_order_submitted", "false")
def _ws_rpc_url(env: Mapping[str, str]) -> str:
    configured = str(env.get("CD90_WS_RPC_URL", DEFAULT_WS_RPC_URL)).strip()
    if configured not in ALLOWED_WS_RPC_URLS:
        raise LiveConfigurationError("UNAPPROVED_CD90_WS_RPC_URL")
    return configured


def _bootstrap_ws_connection(
    *, follower: LiveSourceFollower, initial_connection: bool
) -> dict[str, int | bool | None]:
    """Create one session watermark, then preserve it across WS reconnects.

    A transient WebSocket/RPC reconnect is not a daemon restart.  Rebuilding
    the watermark on every reconnect converts a recoverable cursor into an
    unpriced gap and can discard newly observed source actions.  Only the
    initial connection of a process session establishes the forward-only
    restart watermark; later subscriptions resume the persisted cursor.
    """

    if initial_connection:
        planned_resume_raw = follower.store.runtime_value(
            "operator_planned_resume_from_block"
        )
        if planned_resume_raw is not None and str(planned_resume_raw).strip():
            last_raw = follower.store.runtime_value("last_processed_block")
            if last_raw is None or int(planned_resume_raw) != int(last_raw):
                raise LiveConfigurationError(
                    "OPERATOR_PLANNED_RESUME_CURSOR_MISMATCH"
                )
            pre_repair_recovery_armed = (
                follower.store.runtime_value(
                    "operator_pre_repair_forward_recovery_armed"
                )
                == "true"
            )
            result = follower.establish_forward_watermark(
                advance_after_recorded_internal_repair_gap=(
                    pre_repair_recovery_armed
                )
            )
            if pre_repair_recovery_armed:
                follower.store.set_runtime(
                    "operator_pre_repair_forward_recovery_armed", "false"
                )
            follower.store.set_runtime(
                "ws_resume_mode", "OPERATOR_PLANNED_FORWARD_WATERMARK"
            )
            follower._complete_planned_operator_resume(
                processed_to_block=int(result["start_head"])
            )
            return {**result, "resumed_planned_operator_change": True}
        result = follower.establish_forward_watermark()
        return result
    last_raw = follower.store.runtime_value("last_processed_block")
    if last_raw is None:
        raise RuntimeError("FORWARD_WATERMARK_NOT_ESTABLISHED")
    last = int(last_raw)
    follower.store.set_runtime("ws_resume_mode", "IN_PROCESS_CURSOR_RESUME")
    follower.store.set_runtime("heartbeat_at_ms", follower.clock_ms())
    return {
        "previous_head": last,
        "start_head": last,
        "skipped_block_count": 0,
        "resumed_in_process": True,
    }


def _join_hot_standby_at_existing_cursor(
    *, follower: LiveSourceFollower
) -> dict[str, int | bool | None]:
    """Attach a second executor without scanning or advancing the cursor.

    A moving RPC tip is not a safe join precondition: a healthy websocket
    primary can be more than one block behind it.  The standby therefore
    subscribes at the persisted cursor and only processes after the primary's
    runtime lock releases.
    """

    last_raw = follower.store.runtime_value("last_processed_block")
    if last_raw is None:
        raise LiveConfigurationError("HOT_STANDBY_CURSOR_NOT_ESTABLISHED")
    last = int(last_raw)
    result = _bootstrap_ws_connection(follower=follower, initial_connection=False)
    follower.store.set_runtime("ws_resume_mode", "HOT_STANDBY_EXISTING_CURSOR")
    follower.store.set_runtime("hot_standby_joined_at_ms", follower.clock_ms())
    follower.store.set_runtime("hot_standby_joined_cursor", last)
    follower.store.set_runtime(
        "hot_standby_joined_head",
        follower.store.runtime_value("current_head") or last,
    )
    return {**result, "hot_standby_joined": True}


def _call_event_loop_callback_and_wait(
    *,
    event_loop: asyncio.AbstractEventLoop,
    callback: Callable[[], Any],
) -> Any:
    """Run one loop-owned callback and return its result to the worker."""

    outcome: Future[Any] = Future()

    def invoke() -> None:
        try:
            result = callback()
        except BaseException as exc:
            outcome.set_exception(exc)
        else:
            outcome.set_result(result)

    event_loop.call_soon_threadsafe(invoke)
    return outcome.result()


async def _run_ws_new_head_service(
    *,
    runtime_dir: Path,
    store: LiveStore,
    follower: LiveSourceFollower,
    execution: CLOBExecutionAdapter,
    ws_rpc_url: str,
    redemption: CD90RedemptionAdapter | None,
    initial_connection: bool = True,
    hot_standby: bool = False,
) -> None:
    """Follow only post-subscription Polygon heads, without a timed poll loop."""

    try:
        import websockets
    except ImportError as exc:
        raise LiveConfigurationError("WEBSOCKET_CLIENT_UNAVAILABLE") from exc

    redemption_task: asyncio.Task[dict[str, Any]] | None = None

    def _start_redemption_cycle() -> None:
        nonlocal redemption_task
        if redemption is None or redemption_task is not None:
            return
        if not redemption_maintenance_due(
            store=store,
            observed_at_ms=now_ms(),
        ):
            return
        store.set_runtime("auto_redemption_worker_state", "RUNNING")
        store.set_runtime(
            "auto_redemption_maintenance_interval_ms",
            str(REDEMPTION_MAINTENANCE_INTERVAL_MS),
        )
        redemption_task = asyncio.create_task(
            asyncio.to_thread(
                run_redemption_cycle,
                store=store,
                adapter=redemption,
                execution=execution,
                wallet_lock_path=follower.wallet_lock_path,
                coordinator=follower.coordinator,
                profile_key=follower.profile_key,
            )
        )

    def _collect_redemption_cycle() -> None:
        nonlocal redemption_task
        if redemption_task is None or not redemption_task.done():
            return
        try:
            outcome = redemption_task.result()
        except Exception as exc:
            retryable_external = _is_retryable_external_error(exc)
            store.append_runtime_error(
                occurred_at_ms=now_ms(),
                category=(
                    "EXTERNAL_REDEMPTION_CYCLE"
                    if retryable_external
                    else "INTERNAL_REDEMPTION_CYCLE"
                ),
                message=f"{type(exc).__name__}: {exc}",
            )
            store.set_runtime(
                "auto_redemption_worker_state",
                "EXTERNAL_RETRY" if retryable_external else "CODE_REPAIR_REQUIRED",
            )
        else:
            store.set_runtime("auto_redemption_worker_state", "IDLE")
            store.set_runtime("auto_redemption_last_cycle_at_ms", str(now_ms()))
            store.set_runtime(
                "auto_redemption_last_cycle_summary",
                json.dumps(outcome, sort_keys=True, default=str),
            )
        redemption_task = None

    try:
        from websockets.asyncio.client import process_exception as default_process_exception
        from websockets.exceptions import ConnectionClosed
    except ImportError as exc:
        raise LiveConfigurationError("WEBSOCKET_RECONNECT_API_UNAVAILABLE") from exc

    fatal_recorded = False
    establishing_initial_connection = bool(initial_connection)

    def _record_stream_failure(exc: Exception, *, retryable: bool, phase: str) -> None:
        nonlocal fatal_recorded
        store.append_runtime_error(
            occurred_at_ms=now_ms(),
            category="EXTERNAL_WS_STREAM" if retryable else "INTERNAL_RUNTIME",
            message=f"{type(exc).__name__}: {exc}",
            details={"phase": phase, "retry_in_process": retryable},
        )
        store.set_runtime("ws_subscription_active", "false")
        if retryable:
            reconnects = int(store.runtime_value("ws_reconnect_count") or "0") + 1
            store.set_runtime("ws_reconnect_count", reconnects)
            store.set_runtime("last_cycle_outcome", "EXTERNAL_WS_RECONNECTING")
        else:
            fatal_recorded = True
            store.set_runtime("last_cycle_outcome", "FATAL_INTERNAL")
        write_status_files_best_effort(store, runtime_dir)

    def _process_connect_exception(exc: Exception) -> Exception | None:
        decision = _ws_connect_exception_decision(
            exc,
            default_process_exception=default_process_exception,
        )
        _record_stream_failure(
            exc,
            retryable=decision is None,
            phase="CONNECT",
        )
        return decision

    try:
        async for websocket in websockets.connect(
            ws_rpc_url,
            proxy=None,
            process_exception=_process_connect_exception,
        ):
            try:
                await websocket.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_subscribe",
                            "params": ["newHeads"],
                        },
                        separators=(",", ":"),
                    )
                )
                try:
                    acknowledgement = json.loads(
                        await _receive_ws_message_with_liveness(
                            websocket,
                            timeout_seconds=WS_NEW_HEAD_LIVENESS_TIMEOUT_SECONDS,
                        )
                    )
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise RuntimeError("INVALID_WS_SUBSCRIPTION_ACK") from exc
                subscription_id = parse_ws_subscription_ack(acknowledgement)

                # A new daemon establishes a forward-only waterline once.
                # Later WebSocket reconnects belong to the same live session
                # and must retain that cursor rather than manufacture a new
                # unpriced restart gap for a transient provider failure.
                _bootstrap_ws_connection(
                    follower=follower,
                    initial_connection=establishing_initial_connection,
                )
                establishing_initial_connection = False
                store.set_runtime("transport_mode", "WS_NEW_HEADS_ACTIVE")
                store.set_runtime("ws_rpc_url", ws_rpc_url)
                store.set_runtime("ws_subscription_active", "true")
                store.set_runtime("ws_subscription_started_at_ms", str(now_ms()))
                if redemption is not None:
                    store.set_runtime("auto_redemption_enabled", "true")
                    store.set_runtime("auto_redemption_worker_state", "READY")
                elif store.runtime_value("auto_redemption_enabled") is None:
                    store.set_runtime("auto_redemption_enabled", "false")
                    store.set_runtime(
                        "auto_redemption_worker_state", "DISABLED_NO_RELAYER_AUTH"
                    )
                write_status_files_best_effort(store, runtime_dir)

                event_loop = asyncio.get_running_loop()

                def _schedule_redemption_cycle_from_worker() -> None:
                    _call_event_loop_callback_and_wait(
                        event_loop=event_loop,
                        callback=_start_redemption_cycle,
                    )

                pending_head: int | None = None
                while True:
                    if pending_head is None:
                        raw_message = await _receive_ws_message_with_liveness(
                            websocket,
                            timeout_seconds=WS_NEW_HEAD_LIVENESS_TIMEOUT_SECONDS,
                        )
                        head = extract_ws_new_head_number(
                            raw_message, subscription_id=subscription_id
                        )
                        if head is None:
                            continue
                    else:
                        head = pending_head
                        pending_head = None
                    _collect_redemption_cycle()
                    store.set_runtime("last_ws_head_received_at_ms", str(now_ms()))

                    async def _process_head(candidate_head: int) -> bool:
                        process_head = (
                            _process_hot_standby_ws_head
                            if hot_standby
                            else _process_live_ws_head
                        )
                        return await asyncio.to_thread(
                            process_head,
                            store=store,
                            runtime_dir=runtime_dir,
                            follower=follower,
                            execution=execution,
                            head=candidate_head,
                            start_redemption_cycle=(
                                _schedule_redemption_cycle_from_worker
                            ),
                        )

                    _handled, pending_head, buffered_count = (
                        await _process_head_while_coalescing_notifications(
                            websocket=websocket,
                            subscription_id=subscription_id,
                            head=head,
                            process_head=_process_head,
                        )
                    )
                    if buffered_count:
                        total = int(
                            store.runtime_value("ws_buffered_heads_coalesced_total")
                            or "0"
                        ) + int(buffered_count)
                        store.set_runtime(
                            "ws_buffered_heads_coalesced_total", str(total)
                        )
                        store.set_runtime(
                            "last_ws_head_received_at_ms", str(now_ms())
                        )
                        store.set_runtime("last_coalesced_from_head", str(head))
                        store.set_runtime(
                            "last_coalesced_to_head", str(pending_head)
                        )
            except Exception as exc:
                retryable_external = isinstance(exc, ConnectionClosed) or (
                    _is_retryable_external_error(exc)
                )
                _record_stream_failure(
                    exc,
                    retryable=retryable_external,
                    phase="ACTIVE_SESSION",
                )
                if retryable_external:
                    continue
                raise
        raise RuntimeError("WS_RECONNECT_ITERATOR_STOPPED")
    except Exception as exc:
        if not fatal_recorded:
            _record_stream_failure(exc, retryable=False, phase="FATAL_EXIT")
        raise


def run_live_service(
    *,
    runtime_dir: Path,
    env: Mapping[str, str],
    profile_key: str,
    action_scope: Any | None = None,
    hot_standby: bool = False,
) -> None:
    if action_scope is None and profile_key == LIVE_PROFILE_CD90:
        from live_copy_profiles import FullWalletEventScope

        action_scope = FullWalletEventScope(_bounded_public_json)
    minimum_size_policy = minimum_size_policy_for_profile(profile_key)
    values = _live_env(env)
    store = LiveStore(runtime_dir / "live.sqlite3")
    coordinator = SharedWalletCoordinator(
        Path(values["POLYMARKET_SHARED_WALLET_COORDINATOR_PATH"])
    )
    wallet_lock_path = coordinator.lock_submission_path(
        Path(values["POLYMARKET_SHARED_WALLET_LOCK_PATH"])
    )
    coordinator_receipt = coordinator.migration_receipt()
    if coordinator_receipt["funder_address"] != values[
        "POLYMARKET_FUNDER_ADDRESS"
    ].lower():
        raise LiveConfigurationError("SHARED_WALLET_COORDINATOR_FUNDER_MISMATCH")
    registered = coordinator.registered_sleeve(profile_key)
    if Path(registered["ledger_path"]) != store.path.resolve():
        raise LiveConfigurationError("SHARED_WALLET_COORDINATOR_LEDGER_MISMATCH")
    store.lock_config_once("profile_key", profile_key)
    store.lock_config_once(
        "shared_wallet_coordinator_path", coordinator.path
    )
    store.migrate_shared_wallet_migration_receipt_hash(
        expected_receipt_hash=coordinator_receipt["migration_receipt_hash"],
        receipt_history=coordinator.receipt_history(),
        changed_at_ms=now_ms(),
    )
    rpc = RpcClient()
    client = build_authenticated_live_client(env)
    adapter = CLOBExecutionAdapter(
        client,
        minimum_marketable_buy_notional_usd=_decimal_config(
            values, "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD"
        ),
        receipt_reader=rpc,
    )
    _arm_runtime(
        store=store,
        adapter=adapter,
        env=env,
        minimum_size_policy=minimum_size_policy,
        source_action_detection_contract=(
            source_action_detection_contract_for_profile(profile_key)
        ),
    )
    store.ensure_bounded_retry_policy_at_current_cursor(
        activated_at_ms=now_ms(),
        change_id="bounded-live-retry-release-v1",
    )
    store.ensure_liquidity_retry_policy_at_current_cursor(
        activated_at_ms=now_ms(),
        change_id="liquidity-only-retry-v2-release",
    )
    redemption: CD90RedemptionAdapter | None
    if hot_standby:
        # The standby owns source-action continuity only.  A second redemption
        # worker against the same wallet would violate the ownership boundary.
        redemption = None
    else:
        try:
            redemption = build_redemption_adapter(env)
        except Exception as exc:
            # A missing/bad redemption credential must not interrupt the user’s
            # separately-authorized CLOB copier.  It remains a visible internal
            # configuration issue until fixed, with no redemption side effect.
            store.append_runtime_error(
                occurred_at_ms=now_ms(),
                category="INTERNAL_REDEMPTION_CONFIGURATION",
                message=f"{type(exc).__name__}: {exc}",
            )
            store.set_runtime("auto_redemption_enabled", "false")
            store.set_runtime("auto_redemption_worker_state", "BLOCK_CONFIGURATION")
            redemption = None
        else:
            if redemption is None:
                store.set_runtime("auto_redemption_enabled", "false")
                store.set_runtime("auto_redemption_worker_state", "DISABLED_NO_RELAYER_AUTH")
    follower = LiveSourceFollower(
        store=store,
        rpc=rpc,
        source_wallet=values["CD90_SOURCE_WALLET"],
        clock_ms=now_ms,
        action_scope=action_scope,
        public_get_json=(
            _bounded_public_json if profile_key == LIVE_PROFILE_CD90 else None
        ),
        wallet_lock_path=wallet_lock_path,
        coordinator=coordinator,
        profile_key=profile_key,
    )
    if hot_standby:
        _join_hot_standby_at_existing_cursor(follower=follower)
        asyncio.run(
            _run_ws_new_head_service(
                runtime_dir=runtime_dir,
                store=store,
                follower=follower,
                execution=adapter,
                ws_rpc_url=_ws_rpc_url(env),
                redemption=None,
                initial_connection=False,
                hot_standby=True,
            )
        )
        return
    with _primary_runtime_lock(_profile_runtime_lock_path(runtime_dir)):
        asyncio.run(
            _run_ws_new_head_service(
                runtime_dir=runtime_dir,
                store=store,
                follower=follower,
                execution=adapter,
                ws_rpc_url=_ws_rpc_url(env),
                redemption=redemption,
            )
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cd90-live-copy")
    parser.add_argument("--runtime-dir", type=Path, default=DEFAULT_RUNTIME_DIR)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--run", action="store_true")
    modes.add_argument("--run-hot-standby", action="store_true")
    modes.add_argument("--status", action="store_true")
    modes.add_argument("--preflight", action="store_true")
    modes.add_argument("--reconcile-unresolved-only", action="store_true")
    modes.add_argument("--rebase-fixed-share-scale-multiplier", metavar="MULTIPLIER")
    modes.add_argument("--arm-planned-cursor-resume", action="store_true")
    modes.add_argument("--arm-pre-repair-forward-recovery", action="store_true")
    modes.add_argument("--establish-forward-watermark", action="store_true")
    parser.add_argument("--scale-change-id")
    parser.add_argument("--operator-resume-change-id")
    parser.add_argument("--operator-resume-reason")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    runtime_dir = Path(args.runtime_dir).resolve()
    store = LiveStore(runtime_dir / "live.sqlite3")
    if args.status:
        print(json.dumps(write_status_files(store, runtime_dir), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if args.establish_forward_watermark:
        source_wallet = str(os.environ.get("CD90_SOURCE_WALLET") or "")
        with _exclusive_runtime_lock(_profile_runtime_lock_path(runtime_dir)):
            follower = LiveSourceFollower(
                store=store,
                rpc=RpcClient(),
                source_wallet=source_wallet,
                clock_ms=now_ms,
            )
            result = follower.establish_forward_watermark()
            store.set_runtime("operator_planned_resume_from_block", "")
            store.set_runtime(
                "operator_planned_resume_state",
                "FORWARD_WATERMARK_ESTABLISHED_NO_REPLAY",
            )
            payload = write_status_files(store, runtime_dir)
        print(
            json.dumps(
                {"forward_watermark": result, "runtime": payload["runtime"]},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    if args.preflight:
        values = _live_env(os.environ)
        client = build_authenticated_live_client(os.environ)
        adapter = CLOBExecutionAdapter(
            client,
            minimum_marketable_buy_notional_usd=_decimal_config(
                values, "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD"
            ),
        )
        _arm_runtime(
            store=store,
            adapter=adapter,
            env=os.environ,
            minimum_size_policy=minimum_size_policy_for_profile(LIVE_PROFILE_CD90),
            source_action_detection_contract=(
                source_action_detection_contract_for_profile(LIVE_PROFILE_CD90)
            ),
        )
        payload = write_status_files(store, runtime_dir)
        print(json.dumps({"preflight": "PASS", "allocation_usd": payload["config"]["allocation_usd"], "fixed_share_scale": payload["config"]["fixed_share_scale"]}, ensure_ascii=False))
        return 0
    if args.reconcile_unresolved_only:
        values = _live_env(os.environ)
        client = build_authenticated_live_client(os.environ)
        adapter = CLOBExecutionAdapter(
            client,
            minimum_marketable_buy_notional_usd=_decimal_config(
                values, "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD"
            ),
            receipt_reader=RpcClient(),
        )
        with _exclusive_runtime_lock(_profile_runtime_lock_path(runtime_dir)):
            results = _reconcile_submissions_and_refresh_cash(
                store=store,
                execution=adapter,
                wallet_lock_path=None,
                coordinator=None,
                profile_key=None,
            )
            payload = write_status_files(store, runtime_dir)
        print(json.dumps({"reconciliation": results, "runtime": payload["runtime"]}, ensure_ascii=False, sort_keys=True))
        return 0
    if args.rebase_fixed_share_scale_multiplier is not None:
        if not str(args.scale_change_id or "").strip():
            raise LiveConfigurationError("MISSING_SCALE_REBASE_CHANGE_ID")
        try:
            multiplier = Decimal(str(args.rebase_fixed_share_scale_multiplier))
        except (InvalidOperation, ValueError) as exc:
            raise LiveConfigurationError("INVALID_SCALE_REBASE_MULTIPLIER") from exc
        # This is a real-sleeve configuration change.  Reuse the same live
        # guards as order submission, but never instantiate a client or send
        # an order here.
        _live_env(os.environ)
        with _exclusive_runtime_lock(_profile_runtime_lock_path(runtime_dir)):
            last_raw = store.runtime_value("last_processed_block")
            current_raw = store.runtime_value("current_head")
            if last_raw is None or current_raw is None:
                raise LiveConfigurationError(
                    "SCALE_REBASE_REQUIRES_CAUGHT_UP_CHAIN_CURSOR"
                )
            try:
                last_processed_block = int(last_raw)
                current_head = int(current_raw)
            except ValueError as exc:
                raise LiveConfigurationError(
                    "SCALE_REBASE_REQUIRES_CAUGHT_UP_CHAIN_CURSOR"
                ) from exc
            if current_head < last_processed_block:
                raise LiveConfigurationError(
                    "SCALE_REBASE_REQUIRES_CAUGHT_UP_CHAIN_CURSOR"
                )
            result = store.rebase_fixed_share_scale(
                multiplier=multiplier,
                change_id=str(args.scale_change_id),
                effective_after_block=current_head,
                resume_from_block=last_processed_block,
                requested_at_ms=now_ms(),
            )
            payload = write_status_files(store, runtime_dir)
        print(
            json.dumps(
                {
                    "scale_rebase": {
                        key: str(value) if isinstance(value, Decimal) else value
                        for key, value in result.items()
                    },
                    "config": payload["config"],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    if args.arm_planned_cursor_resume:
        if not str(args.operator_resume_change_id or "").strip():
            raise LiveConfigurationError("MISSING_OPERATOR_PLANNED_RESUME_CHANGE_ID")
        if not str(args.operator_resume_reason or "").strip():
            raise LiveConfigurationError("MISSING_OPERATOR_PLANNED_RESUME_REASON")
        values = _live_env(os.environ)
        source_wallet = str(values["CD90_SOURCE_WALLET"]).lower()
        if re.fullmatch(r"0x[a-f0-9]{40}", source_wallet) is None:
            raise LiveConfigurationError("INVALID_SOURCE_WALLET")
        with _exclusive_runtime_lock(_profile_runtime_lock_path(runtime_dir)):
            store.lock_config_once("source_wallet", source_wallet)
            last_raw = store.runtime_value("last_processed_block")
            if last_raw is None:
                raise LiveConfigurationError("FORWARD_WATERMARK_NOT_ESTABLISHED")
            try:
                last_processed_block = int(last_raw)
            except ValueError as exc:
                raise LiveConfigurationError("FORWARD_WATERMARK_NOT_ESTABLISHED") from exc
            result = store.arm_planned_operator_resume(
                resume_from_block=last_processed_block,
                change_id=str(args.operator_resume_change_id),
                reason=str(args.operator_resume_reason),
                armed_at_ms=now_ms(),
            )
            payload = write_status_files(store, runtime_dir)
        print(
            json.dumps(
                {"operator_planned_resume": result, "runtime": payload["runtime"]},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    if args.arm_pre_repair_forward_recovery:
        if not str(args.operator_resume_change_id or "").strip():
            raise LiveConfigurationError("MISSING_OPERATOR_PLANNED_RESUME_CHANGE_ID")
        if not str(args.operator_resume_reason or "").strip():
            raise LiveConfigurationError("MISSING_OPERATOR_PLANNED_RESUME_REASON")
        values = _live_env(os.environ)
        source_wallet = str(values["CD90_SOURCE_WALLET"]).lower()
        if re.fullmatch(r"0x[a-f0-9]{40}", source_wallet) is None:
            raise LiveConfigurationError("INVALID_SOURCE_WALLET")
        with _exclusive_runtime_lock(_profile_runtime_lock_path(runtime_dir)):
            store.lock_config_once("source_wallet", source_wallet)
            result = arm_pre_repair_forward_recovery(
                store=store,
                change_id=str(args.operator_resume_change_id),
                reason=str(args.operator_resume_reason),
                armed_at_ms=now_ms(),
            )
            payload = write_status_files(store, runtime_dir)
        print(
            json.dumps(
                {
                    "pre_repair_forward_recovery": result,
                    "runtime": payload["runtime"],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    run_live_service(
        runtime_dir=runtime_dir,
        env=os.environ,
        profile_key=LIVE_PROFILE_CD90,
        hot_standby=bool(args.run_hot_standby),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
