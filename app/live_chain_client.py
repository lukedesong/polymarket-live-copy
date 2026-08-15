#!/usr/bin/env python3
"""Shared read-only Polygon and public Polymarket clients for live followers."""

from __future__ import annotations

import json
import re
import subprocess
import time
from decimal import Decimal
from http.client import RemoteDisconnected
from typing import Any, Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen


D = Decimal
ZERO = D("0")

# Polygon and official Polymarket deployment constraints.
CHAIN_ID = 137
V2_EXCHANGE_ADDRESSES = (
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
)
ORDER_FILLED_TOPIC = (
    "0xd543adfd945773f1a62f74f0ee55a5e3"
    "b9b1a28262980ba90b1a89f2ea84d8ee"
)
RPC_MAX_LOG_BLOCK_RANGE = 10_000
TOKEN_SCALE = D("1000000")

# Public read endpoints already used by the live follower.
DEFAULT_RPC_URLS = (
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
)
HTTP_TIMEOUT_SECONDS = 10
PUBLIC_GET_MAX_ATTEMPTS = 2
CURL_BINARY = "/usr/bin/curl"
_CURL_STATUS_MARKER = b"\n__POLYMARKET_CURL_STATUS__"
CURL_PARENT_GRACE_SECONDS = 1.0
_curl_run = subprocess.run
_DEFAULT_URLOPEN = urlopen

RPC_READ_METHODS = frozenset(
    {
        "eth_chainId",
        "eth_blockNumber",
        "eth_getBlockByNumber",
        "eth_getLogs",
        "eth_getTransactionReceipt",
    }
)
RPC_ALLOWED_HOSTS = frozenset(urlparse(url).hostname for url in DEFAULT_RPC_URLS)


class ReadOnlyViolation(RuntimeError):
    """Raised if a caller attempts a non-public or state-changing operation."""


class RpcError(RuntimeError):
    """Raised when every configured public RPC endpoint fails."""


class PublicReadError(RuntimeError):
    """Raised when a public endpoint cannot provide a usable read response."""

    def __init__(self, *, url: str, reason: str) -> None:
        self.url = str(url)
        self.reason = str(reason)
        super().__init__(f"public read failed for {self.url}: {self.reason}")


class BoundedHttpError(RuntimeError):
    """A bounded public HTTP request could not return usable JSON."""

    def __init__(
        self,
        *,
        url: str,
        status: int | None,
        reason: str,
    ) -> None:
        self.url = str(url)
        self.status = status
        self.reason = str(reason)
        status_text = f"HTTP {status}" if status is not None else "transport"
        super().__init__(
            f"bounded public read failed for {self.url} ({status_text}): "
            f"{self.reason}"
        )


def _curl_timeout_text(timeout: float | int) -> str:
    timeout_seconds = float(timeout)
    if timeout_seconds <= 0:
        raise ValueError("public HTTP timeout must be positive")
    return format(timeout_seconds, "g")


def _bounded_curl_json(
    url: str,
    *,
    method: str,
    headers: dict[str, str],
    data: bytes | None,
    timeout: float | int,
) -> Any:
    """Read public JSON in a killable child process, not a daemon thread."""
    timeout_text = _curl_timeout_text(timeout)
    command = [
        CURL_BINARY,
        "--silent",
        "--show-error",
        "--location",
        "--request",
        str(method).upper(),
        "--connect-timeout",
        timeout_text,
        "--max-time",
        timeout_text,
    ]
    for name, value in sorted(headers.items()):
        command.extend(["--header", f"{name}: {value}"])
    if data is not None:
        command.extend(["--data-binary", "@-"])
    command.extend(
        [
            "--write-out",
            _CURL_STATUS_MARKER.decode("ascii") + "%{http_code}",
            str(url),
        ]
    )
    try:
        completed = _curl_run(
            command,
            input=data,
            capture_output=True,
            check=False,
            timeout=float(timeout) + CURL_PARENT_GRACE_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise BoundedHttpError(
            url=url,
            status=None,
            reason=(
                "wall-clock timeout after "
                f"{_curl_timeout_text(timeout)} seconds"
            ),
        ) from exc
    except OSError as exc:
        raise BoundedHttpError(
            url=url,
            status=None,
            reason=f"curl transport startup failed: {exc}",
        ) from exc

    stdout = bytes(completed.stdout or b"")
    body, marker, raw_status = stdout.rpartition(_CURL_STATUS_MARKER)
    if not marker:
        raise BoundedHttpError(
            url=url,
            status=None,
            reason="curl response omitted the terminal HTTP status marker",
        )
    try:
        status = int(raw_status.decode("ascii").strip())
    except (TypeError, ValueError, UnicodeDecodeError) as exc:
        raise BoundedHttpError(
            url=url,
            status=None,
            reason="curl response had an invalid terminal HTTP status marker",
        ) from exc
    stderr = bytes(completed.stderr or b"").decode("utf-8", "replace").strip()
    if int(completed.returncode) != 0:
        raise BoundedHttpError(
            url=url,
            status=status if status > 0 else None,
            reason=(
                stderr
                or f"curl exited with status {int(completed.returncode)}"
            ),
        )
    if not 200 <= status < 300:
        body_reason = ""
        try:
            parsed_error = json.loads(body.decode("utf-8"))
            body_reason = json_text(parsed_error)
        except (UnicodeDecodeError, json.JSONDecodeError):
            body_reason = body.decode("utf-8", "replace").strip()
        raise BoundedHttpError(
            url=url,
            status=status,
            reason=(stderr or body_reason[:1000] or "non-success HTTP response"),
        )
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BoundedHttpError(
            url=url,
            status=status,
            reason=f"invalid JSON: {exc}",
        ) from exc


def bounded_json_request(
    url: str,
    *,
    method: str,
    headers: dict[str, str],
    data: bytes | None = None,
    timeout: float | int = HTTP_TIMEOUT_SECONDS,
    opener: Callable[..., Any] | None = None,
) -> Any:
    """Return JSON with a hard production deadline and injectable test IO."""
    if opener is not None:
        request = Request(
            str(url),
            data=data,
            headers=headers,
            method=str(method).upper(),
        )
        with opener(request, timeout=timeout) as response:
            return json.load(response)
    return _bounded_curl_json(
        str(url),
        method=method,
        headers=headers,
        data=data,
        timeout=timeout,
    )


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def normalize_address(value: str) -> str:
    raw = str(value or "").lower()
    if not raw.startswith("0x") or len(raw) != 42:
        raise ValueError(f"invalid address: {value}")
    int(raw[2:], 16)
    return raw


def address_topic(address: str) -> str:
    return "0x" + ("0" * 24) + normalize_address(address)[2:]


def topic_address(topic: str) -> str:
    raw = str(topic or "").lower().removeprefix("0x")
    if len(raw) != 64:
        raise ValueError("address topic must contain 32 bytes")
    int(raw, 16)
    return "0x" + raw[-40:]


def hex_int(value: str | int) -> int:
    if isinstance(value, int):
        return value
    return int(str(value), 16)


def is_hex_quantity(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return hex_int(value) >= 0
    except (TypeError, ValueError):
        return False


def split_words(data: str) -> list[str]:
    raw = str(data or "").removeprefix("0x")
    if len(raw) % 64:
        raise ValueError("ABI data is not aligned to 32-byte words")
    return ["0x" + raw[index : index + 64] for index in range(0, len(raw), 64)]


def decode_order_filled(
    log: dict[str, Any],
    source_wallet: str,
) -> dict[str, Any] | None:
    """Decode one official V2 OrderFilled log without guessing counterparty side."""
    topics = [str(value).lower() for value in log.get("topics", [])]
    if len(topics) != 4 or topics[0] != ORDER_FILLED_TOPIC:
        return None
    maker = topic_address(topics[2])
    taker = topic_address(topics[3])
    source = normalize_address(source_wallet)
    if source not in {maker, taker}:
        return None

    words = split_words(str(log.get("data", "")))
    if len(words) != 7:
        raise ValueError(f"OrderFilled expected 7 data words, got {len(words)}")
    side_code = hex_int(words[0])
    if side_code not in {0, 1}:
        raise ValueError(f"unsupported Side enum: {side_code}")
    token_id = hex_int(words[1])
    maker_amount_raw = hex_int(words[2])
    taker_amount_raw = hex_int(words[3])
    fee_raw = hex_int(words[4])
    source_role = "maker" if maker == source else "taker"
    source_order = source_role == "maker"

    side = "UNKNOWN"
    quantity: Decimal | None = None
    notional: Decimal | None = None
    price: Decimal | None = None
    if source_order:
        side = "BUY" if side_code == 0 else "SELL"
        if side == "BUY":
            notional = D(maker_amount_raw) / TOKEN_SCALE
            quantity = D(taker_amount_raw) / TOKEN_SCALE
        else:
            quantity = D(maker_amount_raw) / TOKEN_SCALE
            notional = D(taker_amount_raw) / TOKEN_SCALE
        if quantity > ZERO:
            price = notional / quantity

    return {
        "chain_id": CHAIN_ID,
        "exchange_address": normalize_address(str(log.get("address", ""))),
        "block_number": hex_int(log.get("blockNumber", "0x0")),
        "block_hash": str(log.get("blockHash", "")).lower(),
        "transaction_hash": str(log.get("transactionHash", "")).lower(),
        "log_index": hex_int(log.get("logIndex", "0x0")),
        "order_hash": topics[1],
        "maker": maker,
        "taker": taker,
        "source_role": source_role,
        "source_order": source_order,
        "side_code": side_code,
        "side": side,
        "token_id": str(token_id),
        "maker_amount_raw": str(maker_amount_raw),
        "taker_amount_raw": str(taker_amount_raw),
        "fee_raw": str(fee_raw),
        "quantity": quantity,
        "notional": notional,
        "price": price,
        "builder": words[5].lower(),
        "metadata": words[6].lower(),
        "raw_log": log,
    }


def validate_rpc_request(url: str, method: str) -> None:
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in RPC_ALLOWED_HOSTS
        or parsed.username
        or parsed.password
        or method not in RPC_READ_METHODS
    ):
        raise ReadOnlyViolation(f"RPC request rejected: {method} {url}")


def validate_public_get(url: str) -> None:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    token_id = parsed.path.removeprefix("/markets-by-token/")
    exact_token_mapping = (
        parsed.hostname == "clob.polymarket.com"
        and parsed.path.startswith("/markets-by-token/")
        and "/" not in token_id
        and token_id.isdecimal()
        and not query
    )
    condition_market_id = parsed.path.removeprefix("/markets/")
    exact_condition_market = (
        parsed.hostname == "clob.polymarket.com"
        and parsed.path.startswith("/markets/")
        and re.fullmatch(r"0x[a-fA-F0-9]{64}", condition_market_id) is not None
        and not query
    )
    value_user = query.get("user")
    exact_account_value = (
        parsed.hostname == "data-api.polymarket.com"
        and parsed.path == "/value"
        and set(query) == {"user"}
        and value_user is not None
        and len(value_user) == 1
        and re.fullmatch(r"0x[a-fA-F0-9]{40}", value_user[0]) is not None
    )
    positions_user = query.get("user")
    positions_limit = query.get("limit")
    positions_offset = query.get("offset")
    exact_positions_page = (
        parsed.hostname == "data-api.polymarket.com"
        and parsed.path == "/positions"
        and set(query) == {"user", "limit", "offset"}
        and positions_user is not None
        and len(positions_user) == 1
        and re.fullmatch(r"0x[a-fA-F0-9]{40}", positions_user[0]) is not None
        and positions_limit is not None
        and len(positions_limit) == 1
        and positions_limit[0].isdecimal()
        and int(positions_limit[0]) > 0
        and positions_offset is not None
        and len(positions_offset) == 1
        and positions_offset[0].isdecimal()
    )
    activity_user = query.get("user")
    activity_type = query.get("type")
    activity_start = query.get("start")
    activity_sort_by = query.get("sortBy")
    activity_sort_direction = query.get("sortDirection")
    activity_limit = query.get("limit")
    activity_offset = query.get("offset")
    exact_redeem_activity_page = (
        parsed.hostname == "data-api.polymarket.com"
        and parsed.path == "/activity"
        and set(query)
        == {
            "user",
            "type",
            "start",
            "sortBy",
            "sortDirection",
            "limit",
            "offset",
        }
        and activity_user is not None
        and len(activity_user) == 1
        and re.fullmatch(r"0x[a-fA-F0-9]{40}", activity_user[0]) is not None
        and activity_type == ["REDEEM"]
        and activity_start == ["1"]
        and activity_sort_by == ["TIMESTAMP"]
        and activity_sort_direction == ["ASC"]
        and activity_limit is not None
        and len(activity_limit) == 1
        and activity_limit[0].isdecimal()
        and 0 < int(activity_limit[0]) <= 500
        and activity_offset is not None
        and len(activity_offset) == 1
        and activity_offset[0].isdecimal()
        and int(activity_offset[0]) <= 5000
    )
    condition_ids = query.get("condition_ids")
    exact_gamma_condition = (
        parsed.hostname == "gamma-api.polymarket.com"
        and parsed.path == "/markets"
        and set(query) == {"condition_ids"}
        and condition_ids is not None
        and len(condition_ids) == 1
        and re.fullmatch(r"0x[a-fA-F0-9]{64}", condition_ids[0]) is not None
    )
    event_slugs = query.get("slug")
    exact_gamma_event_slug = (
        parsed.hostname == "gamma-api.polymarket.com"
        and parsed.path == "/events"
        and set(query) == {"slug"}
        and event_slugs is not None
        and len(event_slugs) == 1
        and re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", event_slugs[0])
        is not None
    )
    allowed = (
        parsed.scheme == "https"
        and not parsed.username
        and not parsed.password
        and (
            (
                parsed.hostname == "data-api.polymarket.com"
                and parsed.path == "/trades"
            )
            or (
                parsed.hostname == "clob.polymarket.com"
                and parsed.path == "/book"
            )
            # CD90 redemption resolves only the official condition mapped to a
            # local position token.  Keep these two extra read routes exact so
            # the shared public reader remains deny-by-default.
            or exact_token_mapping
            or exact_condition_market
            or exact_account_value
            or exact_redeem_activity_page
            # Prospective live profiles need only exact paginated position
            # reads for scale locking and one-condition Gamma metadata reads
            # for scope verification.  Both remain shape-checked here.
            or exact_positions_page
            or exact_gamma_condition
            or exact_gamma_event_slug
        )
    )
    if not allowed:
        raise ReadOnlyViolation(f"public GET rejected: {url}")


class RpcClient:
    """Minimal allow-listed JSON-RPC client with endpoint failover."""

    def __init__(
        self,
        urls: Iterable[str] = DEFAULT_RPC_URLS,
        *,
        timeout: int = HTTP_TIMEOUT_SECONDS,
    ):
        self.urls = tuple(urls)
        if not self.urls:
            raise ValueError("at least one RPC URL is required")
        self.timeout = timeout
        self.last_url: str | None = None
        self.last_latency_ms: int | None = None
        self._split_log_filter_urls: set[str] = set()

    def _call_url(
        self,
        url: str,
        method: str,
        params: list[Any],
        *,
        result_validator: Callable[[Any], bool] | None = None,
    ) -> tuple[Any, int]:
        validate_rpc_request(url, method)
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }
        ).encode("utf-8")
        started = now_ms()
        body = bounded_json_request(
            url,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "User-Agent": "polymarket-live-chain-client/1.0",
            },
            data=payload,
            timeout=self.timeout,
            opener=(
                urlopen if urlopen is not _DEFAULT_URLOPEN else None
            ),
        )
        if body.get("error"):
            raise RpcError(json_text(body["error"]))
        result = body.get("result")
        if result_validator is not None and not result_validator(result):
            raise RpcError(f"{method} returned an unavailable result")
        return result, now_ms() - started

    def call(
        self,
        method: str,
        params: list[Any],
        *,
        result_validator: Callable[[Any], bool] | None = None,
    ) -> Any:
        errors: list[str] = []
        for url in self.urls:
            try:
                result, latency_ms = self._call_url(
                    url,
                    method,
                    params,
                    result_validator=result_validator,
                )
                self.last_url = url
                self.last_latency_ms = latency_ms
                return result
            except Exception as exc:
                errors.append(f"{url}: {type(exc).__name__}: {exc}")
        raise RpcError("; ".join(errors))

    def latest_block_number(self) -> int:
        return hex_int(
            self.call(
                "eth_blockNumber",
                [],
                result_validator=is_hex_quantity,
            )
        )

    def finalized_block_number(self) -> int:
        block = self.call(
            "eth_getBlockByNumber",
            ["finalized", False],
            result_validator=lambda value: isinstance(value, dict),
        )
        if not isinstance(block, dict) or not is_hex_quantity(block.get("number")):
            raise RpcError("finalized block unavailable")
        return hex_int(block["number"])

    def get_block(self, block_number: int) -> dict[str, Any]:
        result = self.call(
            "eth_getBlockByNumber",
            [hex(block_number), False],
            result_validator=lambda value: isinstance(value, dict),
        )
        if not isinstance(result, dict):
            raise RpcError(f"block {block_number} not found")
        return result

    def get_receipt(self, transaction_hash: str) -> dict[str, Any]:
        result = self.call(
            "eth_getTransactionReceipt",
            [transaction_hash],
            result_validator=lambda value: isinstance(value, dict),
        )
        if not isinstance(result, dict):
            raise RpcError(f"receipt {transaction_hash} not found")
        return result

    def source_logs_range(
        self,
        from_block: int,
        to_block: int,
        source_wallet: str,
        role: str,
    ) -> list[dict[str, Any]]:
        if role not in {"maker", "taker"}:
            raise ValueError(f"invalid indexed role: {role}")
        if from_block > to_block:
            return []
        topics: list[str | None] = [ORDER_FILLED_TOPIC, None]
        if role == "maker":
            topics.append(address_topic(source_wallet))
        else:
            topics.extend([None, address_topic(source_wallet)])
        shared_filter = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": topics,
        }
        errors: list[str] = []
        for url in self.urls:
            if url not in self._split_log_filter_urls:
                try:
                    result, latency_ms = self._call_url(
                        url,
                        "eth_getLogs",
                        [{**shared_filter, "address": list(V2_EXCHANGE_ADDRESSES)}],
                        result_validator=lambda value: isinstance(value, list),
                    )
                    self.last_url = url
                    self.last_latency_ms = latency_ms
                    return [row for row in result if isinstance(row, dict)]
                except Exception as exc:
                    # Cache the endpoint's proven request-shape constraint so
                    # every later head does not repeat a request known to fail.
                    if "invalid block range params" not in str(exc).lower():
                        errors.append(f"{url}: {type(exc).__name__}: {exc}")
                        continue
                    self._split_log_filter_urls.add(url)
            rows_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
            try:
                latency_ms = 0
                for exchange_address in V2_EXCHANGE_ADDRESSES:
                    partial, latency_ms = self._call_url(
                        url,
                        "eth_getLogs",
                        [{**shared_filter, "address": exchange_address}],
                        result_validator=lambda value: isinstance(value, list),
                    )
                    for row in partial:
                        if not isinstance(row, dict):
                            continue
                        identity = (
                            str(row.get("transactionHash", "")).lower(),
                            str(row.get("logIndex", "")).lower(),
                        )
                        if not identity[0] or not identity[1]:
                            raise RpcError("source log lacks transaction/log identity")
                        rows_by_identity[identity] = row
                self.last_url = url
                self.last_latency_ms = latency_ms
                return [rows_by_identity[key] for key in sorted(rows_by_identity)]
            except Exception as split_exc:
                errors.append(
                    f"{url}: split-filter {type(split_exc).__name__}: {split_exc}"
                )
        raise RpcError("; ".join(errors))

    def order_fill_logs_range(
        self,
        *,
        from_block: int,
        to_block: int,
        order_id: str,
    ) -> list[dict[str, Any]]:
        """Read exact ``OrderFilled`` logs for one immutable order hash."""
        if int(from_block) < 0 or int(to_block) < int(from_block):
            raise ValueError("invalid order-fill block range")
        normalized_order = str(order_id).lower()
        if re.fullmatch(r"0x[0-9a-f]{64}", normalized_order) is None:
            raise ValueError("invalid order-fill order id")
        errors: list[str] = []
        for url in self.urls:
            try:
                rows: list[dict[str, Any]] = []
                start = int(from_block)
                while start <= int(to_block):
                    end = min(start + RPC_MAX_LOG_BLOCK_RANGE - 1, int(to_block))
                    result, latency_ms = self._call_url(
                        url,
                        "eth_getLogs",
                        [{
                            "fromBlock": hex(start), "toBlock": hex(end),
                            "topics": [ORDER_FILLED_TOPIC, normalized_order],
                            "address": list(V2_EXCHANGE_ADDRESSES),
                        }],
                        result_validator=lambda value: isinstance(value, list),
                    )
                    rows.extend(row for row in result if isinstance(row, dict))
                    self.last_url = url
                    self.last_latency_ms = latency_ms
                    start = end + 1
                return rows
            except Exception as exc:
                array_error = f"{url}: {type(exc).__name__}: {exc}"
            try:
                rows_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
                latency_ms = 0
                for exchange_address in V2_EXCHANGE_ADDRESSES:
                    start = int(from_block)
                    while start <= int(to_block):
                        end = min(
                            start + RPC_MAX_LOG_BLOCK_RANGE - 1,
                            int(to_block),
                        )
                        partial, latency_ms = self._call_url(
                            url,
                            "eth_getLogs",
                            [{
                                "fromBlock": hex(start),
                                "toBlock": hex(end),
                                "topics": [ORDER_FILLED_TOPIC, normalized_order],
                                "address": exchange_address,
                            }],
                            result_validator=lambda value: isinstance(value, list),
                        )
                        for row in partial:
                            if not isinstance(row, dict):
                                continue
                            identity = (
                                str(row.get("transactionHash", "")).lower(),
                                str(row.get("logIndex", "")).lower(),
                            )
                            if not identity[0] or not identity[1]:
                                raise RpcError(
                                    "order-fill log lacks transaction/log identity"
                                )
                            rows_by_identity[identity] = row
                        start = end + 1
                self.last_url = url
                self.last_latency_ms = latency_ms
                return [rows_by_identity[key] for key in sorted(rows_by_identity)]
            except Exception as split_exc:
                errors.append(
                    f"{array_error}; split-filter "
                    f"{type(split_exc).__name__}: {split_exc}"
                )
        raise RpcError("; ".join(errors))


class PublicPolymarketClient:
    """Read-only public CLOB/Data API client."""

    def __init__(self, *, timeout: int = HTTP_TIMEOUT_SECONDS):
        self.timeout = timeout

    def get_json(self, url: str) -> Any:
        validate_public_get(url)
        transport_errors: list[str] = []
        for attempt in range(PUBLIC_GET_MAX_ATTEMPTS):
            try:
                return bounded_json_request(
                    url,
                    method="GET",
                    headers={"User-Agent": "polymarket-live-chain-client/1.0"},
                    timeout=self.timeout,
                    opener=(
                        urlopen if urlopen is not _DEFAULT_URLOPEN else None
                    ),
                )
            except BoundedHttpError as exc:
                if exc.status is not None:
                    raise PublicReadError(
                        url=url,
                        reason=f"HTTP {exc.status}: {exc.reason}",
                    ) from exc
                transport_errors.append(f"transport error: {exc.reason}")
                if attempt + 1 < PUBLIC_GET_MAX_ATTEMPTS:
                    continue
                raise PublicReadError(
                    url=url,
                    reason="; ".join(transport_errors),
                ) from exc
            except HTTPError as exc:
                raise PublicReadError(
                    url=url,
                    reason=f"HTTP {exc.code}: {exc.reason}",
                ) from exc
            except (URLError, RemoteDisconnected) as exc:
                transport_errors.append(
                    "transport error: "
                    f"{getattr(exc, 'reason', exc)}"
                )
                if attempt + 1 < PUBLIC_GET_MAX_ATTEMPTS:
                    continue
                raise PublicReadError(
                    url=url,
                    reason="; ".join(transport_errors),
                ) from exc
            except TimeoutError as exc:
                transport_errors.append(f"timeout: {exc}")
                if attempt + 1 < PUBLIC_GET_MAX_ATTEMPTS:
                    continue
                raise PublicReadError(
                    url=url,
                    reason="; ".join(transport_errors),
                ) from exc
            except json.JSONDecodeError as exc:
                raise PublicReadError(
                    url=url,
                    reason=f"invalid JSON: {exc}",
                ) from exc
        raise AssertionError("public GET retry loop exhausted without result")
