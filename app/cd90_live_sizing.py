"""Pure, side-effect-free sizing rules for the CD90 live copy account.

The allocation is a user-specified cash cap.  The resulting fixed share scale
is based on the *publicly observable open-position sleeve*, not an assertion
about the source wallet's total equity (its unobserved cash is not public).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


ZERO = Decimal("0")
TEN_THOUSAND = Decimal("10000")


class ScaleInputError(ValueError):
    """Raised when a claimed proportional-scale denominator is unusable."""


@dataclass(frozen=True)
class ActionPlan:
    """A non-mutating plan for one exact source BUY or SELL action."""

    terminal_status: str
    reason: str
    side: str
    proportional_quantity: Decimal
    requested_quantity: Decimal
    order_amount_usd: Decimal
    worst_price: Decimal
    reserved_cash_usd: Decimal


def _decimal(value: Decimal | str | int | float) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ScaleInputError(f"invalid decimal input: {value!r}") from exc
    if not result.is_finite():
        raise ScaleInputError(f"non-finite decimal input: {value!r}")
    return result


def maximum_buy_fee_usd(
    *,
    order_amount_usd: Decimal,
    taker_fee_bps: Decimal,
    fee_exponent: Decimal,
    minimum_fill_price: Decimal | None,
    maximum_fill_price: Decimal,
) -> Decimal:
    """Return a formula-derived upper fee bound for a fixed-cash BUY.

    The official curve is ``shares * rate * (p * (1-p))**e``.  A market BUY
    fixes collateral, not shares, so substituting ``shares = cash / p`` gives
    ``cash * rate * p**(e-1) * (1-p)**e``.  Maximise that expression over the
    executable price interval instead of assuming the worst-price limit is
    also the worst fee price.
    """

    amount = _decimal(order_amount_usd)
    fee_bps = _decimal(taker_fee_bps)
    exponent = _decimal(fee_exponent)
    high = _decimal(maximum_fill_price)
    if amount < ZERO or fee_bps < ZERO:
        raise ScaleInputError("buy amount and fee cannot be negative")
    if exponent < ZERO or exponent != exponent.to_integral_value():
        raise ScaleInputError("unsupported nonintegral fee exponent")
    if high <= ZERO or high > Decimal("1"):
        raise ScaleInputError("maximum fill price must be within (0, 1]")
    if amount == ZERO or fee_bps == ZERO:
        return ZERO
    rate = fee_bps / TEN_THOUSAND
    exponent_int = int(exponent)
    if minimum_fill_price is None:
        if exponent_int < 1:
            raise ScaleInputError(
                "minimum fill price is required for zero fee exponent"
            )
        # With no verified lower tick, p**(e-1)*(1-p)**e <= 1 for e >= 1.
        return amount * rate
    low = _decimal(minimum_fill_price)
    if low <= ZERO or low > high:
        raise ScaleInputError("minimum fill price must be within the order limit")
    if exponent_int == 0:
        maximizing_price = low
    elif exponent_int == 1:
        maximizing_price = low
    else:
        critical = Decimal(exponent_int - 1) / Decimal(2 * exponent_int - 1)
        maximizing_price = min(max(critical, low), high)
    fee_factor = (
        maximizing_price ** (exponent_int - 1)
        * (Decimal("1") - maximizing_price) ** exponent_int
    )
    return amount * rate * fee_factor


def derive_fixed_share_scale(
    *,
    allocation_usd: Decimal,
    source_open_position_value_usd: Decimal,
) -> Decimal:
    """Return the immutable per-share scale for this isolated live sleeve.

    This is a formula-derived observable-sleeve scale:

        allocation_usd / source_open_position_value_usd

    It must never be described as a source-wallet-equity scale because public
    positions do not expose any separate source-wallet cash balance.
    """

    allocation = _decimal(allocation_usd)
    denominator = _decimal(source_open_position_value_usd)
    if allocation <= ZERO:
        raise ScaleInputError("allocation_usd must be positive")
    if denominator <= ZERO:
        raise ScaleInputError("source_open_position_value_usd must be positive")
    return allocation / denominator


def _skipped(
    *,
    reason: str,
    side: str,
    proportional_quantity: Decimal,
    requested_quantity: Decimal,
    best_price: Decimal,
    reserved_cash_usd: Decimal = ZERO,
) -> ActionPlan:
    return ActionPlan(
        terminal_status="SKIPPED",
        reason=reason,
        side=side,
        proportional_quantity=proportional_quantity,
        requested_quantity=requested_quantity,
        order_amount_usd=ZERO,
        worst_price=best_price,
        reserved_cash_usd=reserved_cash_usd,
    )


def _pending(
    *,
    status: str,
    reason: str,
    side: str,
    proportional_quantity: Decimal,
    requested_quantity: Decimal,
    best_price: Decimal,
    reserved_cash_usd: Decimal = ZERO,
) -> ActionPlan:
    return ActionPlan(
        terminal_status=status,
        reason=reason,
        side=side,
        proportional_quantity=proportional_quantity,
        requested_quantity=requested_quantity,
        order_amount_usd=ZERO,
        worst_price=best_price,
        reserved_cash_usd=reserved_cash_usd,
    )


def plan_action(
    *,
    side: str,
    source_quantity: Decimal,
    scale: Decimal,
    held_quantity: Decimal,
    minimum_order_size: Decimal,
    minimum_marketable_buy_notional_usd: Decimal,
    best_price: Decimal,
    minimum_fill_price: Decimal | None = None,
    visible_best_level_size: Decimal,
    taker_fee_bps: int | Decimal,
    available_cash: Decimal,
    fee_exponent: int | Decimal = Decimal("1"),
    allow_minimum_upscale: bool = False,
    max_buy_notional_usd: Decimal | None = None,
) -> ActionPlan:
    """Plan a FAK action under an explicit market-minimum policy.

    ``best_price`` is the action-time worst executable price: best Ask for a
    BUY and best Bid for a SELL.  For a BUY, cash reserves the notional plus a
    conservative protocol-fee bound.  The fixed share-scale is preserved whenever
    it is tradable.  Per the user's execution instruction, a smaller scaled
    amount can either be raised once to the largest current executable
    minimum, or be skipped without changing the fixed proportional quantity.
    It is never deferred to a newer price; cash, depth, and sell-inventory
    still apply to a tradable target.
    """

    normalized_side = str(side).upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise ScaleInputError(f"unsupported side: {side!r}")
    quantity = _decimal(source_quantity)
    share_scale = _decimal(scale)
    held = _decimal(held_quantity)
    minimum = _decimal(minimum_order_size)
    minimum_buy_notional = _decimal(minimum_marketable_buy_notional_usd)
    price = _decimal(best_price)
    visible = _decimal(visible_best_level_size)
    fee_bps = _decimal(taker_fee_bps)
    exponent = _decimal(fee_exponent)
    cash = _decimal(available_cash)
    if quantity <= ZERO or share_scale <= ZERO:
        raise ScaleInputError("source_quantity and scale must be positive")
    if (
        held < ZERO
        or minimum <= ZERO
        or minimum_buy_notional < ZERO
        or price <= ZERO
        or visible < ZERO
    ):
        raise ScaleInputError("position, minimum, price, and depth must be valid")
    if fee_bps < ZERO or cash < ZERO:
        raise ScaleInputError("fee and available cash cannot be negative")
    if exponent < ZERO or exponent != exponent.to_integral_value():
        # The CLOB V2 fee curve is supplied by the official market metadata.
        # This copier only permits an exact integral exponent; silently using a
        # floating-point approximation would change a live cash reservation.
        raise ScaleInputError("unsupported nonintegral fee exponent")

    proportional = quantity * share_scale
    requested_basis = proportional
    notional_capped = False
    buy_notional_cap = (
        None if max_buy_notional_usd is None else _decimal(max_buy_notional_usd)
    )
    if buy_notional_cap is not None:
        if buy_notional_cap <= ZERO:
            raise ScaleInputError("max_buy_notional_usd must be positive")
        if normalized_side == "BUY":
            cap_quantity = buy_notional_cap / price
            if requested_basis > cap_quantity:
                requested_basis = cap_quantity
                notional_capped = True
    buy_notional_minimum_quantity = (
        minimum_buy_notional / price if normalized_side == "BUY" else ZERO
    )
    if not allow_minimum_upscale:
        if requested_basis < minimum:
            return _skipped(
                reason=(
                    "NON_TENNIS_COPY_NOTIONAL_CAP_BELOW_MARKET_MINIMUM"
                    if notional_capped
                    else "PROPORTIONAL_QUANTITY_BELOW_MARKET_MINIMUM"
                ),
                side=normalized_side,
                proportional_quantity=proportional,
                requested_quantity=requested_basis,
                best_price=price,
            )
        if (
            normalized_side == "BUY"
            and requested_basis < buy_notional_minimum_quantity
        ):
            return _skipped(
                reason=(
                    "NON_TENNIS_COPY_NOTIONAL_CAP_BELOW_MARKETABLE_MINIMUM"
                    if notional_capped
                    else "PROPORTIONAL_BUY_NOTIONAL_BELOW_MARKETABLE_MINIMUM"
                ),
                side=normalized_side,
                proportional_quantity=proportional,
                requested_quantity=requested_basis,
                best_price=price,
            )
        requested = requested_basis
        share_minimum_override = False
        buy_notional_override = False
    elif normalized_side == "BUY":
        requested = max(requested_basis, minimum, buy_notional_minimum_quantity)
        if notional_capped:
            requested = requested_basis
        share_minimum_override = (
            (not notional_capped)
            and requested_basis < minimum
            and minimum >= buy_notional_minimum_quantity
        )
        buy_notional_override = (
            (not notional_capped)
            and normalized_side == "BUY"
            and requested_basis < buy_notional_minimum_quantity
            and buy_notional_minimum_quantity > minimum
        )
    else:
        # The user authorized minimum-size upscaling only for BUY.  Raising a
        # SELL would dispose of more inventory than the scaled source action,
        # even when this sleeve happens to hold enough shares.
        requested = proportional
        share_minimum_override = False
        buy_notional_override = False
    inventory_capped = False
    if normalized_side == "SELL":
        # A prior FOK can reconcile as a partial fill.  A later source SELL
        # must unwind what this sleeve actually owns, not skip it forever or
        # round it up into an unauthorised short position.
        target_before_inventory_cap = requested
        requested = min(requested, held)
        inventory_capped = requested < target_before_inventory_cap
        if requested < minimum:
            return _pending(
                status="PENDING_MINIMUM_UNWIND",
                reason=(
                    "PROPORTIONAL_SELL_QUANTITY_BELOW_MARKET_MINIMUM"
                    if proportional < minimum and held >= proportional
                    else "INSUFFICIENT_LOCAL_INVENTORY_BELOW_MARKET_MINIMUM"
                ),
                side=normalized_side,
                proportional_quantity=proportional,
                requested_quantity=requested,
                best_price=price,
            )
    # A valid minimum-size FAK may legitimately receive a partial fill.
    # Displayed depth below the target says only that a full immediate fill is
    # unavailable; it is not a reason to turn a FAK into a local pre-submit
    # skip.  For a SELL, retain the minimum-depth guard: submitting when even
    # the visible best bid is below the market minimum has no verified
    # executable minimum leg.
    shallow_fak_attempt = visible < requested
    if normalized_side == "SELL" and visible < minimum:
        return _pending(
            status="PENDING_LIQUIDITY",
            reason="INSUFFICIENT_BEST_LEVEL_DEPTH",
            side=normalized_side,
            proportional_quantity=proportional,
            requested_quantity=requested,
            best_price=price,
        )
    amount = requested * price
    # External Polymarket V2 fee formula: C * feeRate * (p * (1-p))^e.
    # ``feeRate`` is converted from the official market-info rate to bps by
    # the adapter; ``e`` is the official fee-curve exponent.
    fee_bound = (
        maximum_buy_fee_usd(
            order_amount_usd=amount,
            taker_fee_bps=fee_bps,
            fee_exponent=exponent,
            minimum_fill_price=minimum_fill_price,
            maximum_fill_price=price,
        )
        if normalized_side == "BUY"
        else requested
        * (fee_bps / TEN_THOUSAND)
        * (price * (Decimal("1") - price)) ** int(exponent)
    )
    reserved = amount + fee_bound
    if normalized_side == "BUY" and reserved > cash:
        return _pending(
            status="PENDING_CAPITAL",
            reason="INSUFFICIENT_AVAILABLE_CASH",
            side=normalized_side,
            proportional_quantity=proportional,
            requested_quantity=requested,
            best_price=price,
            reserved_cash_usd=reserved,
        )
    return ActionPlan(
        terminal_status="READY",
        reason=(
            "FAK_PARTIAL_ATTEMPT"
            if shallow_fak_attempt
            else "SELL_AVAILABLE_INVENTORY_CAP"
            if inventory_capped
            else "MINIMUM_MARKETABLE_BUY_NOTIONAL_UPSCALE"
            if buy_notional_override
            else "MINIMUM_ORDER_SIZE_UPSCALE"
            if share_minimum_override
            else "NON_TENNIS_COPY_NOTIONAL_CAP"
            if notional_capped
            else ""
        ),
        side=normalized_side,
        proportional_quantity=proportional,
        requested_quantity=requested,
        order_amount_usd=amount,
        worst_price=price,
        reserved_cash_usd=reserved if normalized_side == "BUY" else ZERO,
    )
