"""Stop Zockdo soccer BUY copies. SELL still unwinds inventory.

League identity is the official Gamma `/sports` `sport` code plus a hyphen,
the same prefix rule tennis uses for atp-/wta-/itf-. Tag 100350 is soccer.
Fetched 2026-08-29. New soccer leagues added later are not copied until this
tuple is updated. Empty slug is not soccer: missing metadata must not kill
a tennis BUY.
"""

from __future__ import annotations

from typing import Any, Mapping

from live_copy_profiles import ScopeDecision


ZOCKDO_SOCCER_SLEEVE_STOPPED = "ZOCKDO_SOCCER_SLEEVE_STOPPED"

# Official gamma-api.polymarket.com/sports rows whose tags include 100350.
SOCCER_EVENT_SLUG_PREFIXES = (
    "acle-", "afc-", "afcl-", "afwq-", "arg-", "argcopa-", "argpn-", "asean-",
    "aseanc-", "aseanw-", "aswq-", "atc-", "auc-", "aus-", "aut-", "aze1-", "aze2-",
    "azec-", "bel1-", "bel2-", "bl2-", "blr1-", "bol1-", "bra-", "bra2-", "bra3-",
    "brcm-", "brco-", "bul-", "bun-", "caf-", "cafcl-", "canpl-", "ccc-", "ccup-",
    "cde-", "cdr-", "chfa-", "chi-", "chi1-", "chi2-", "chl2-", "clf-", "cof-", "col-",
    "col1-", "col2-", "con-", "conl-", "copaam-", "cwc-", "cze1-", "den-", "dfb-",
    "ecs-", "ecu1-", "efa-", "efl-", "egy1-", "el1-", "el2-", "elc-", "enl-", "epl-",
    "ere-", "es2-", "est1-", "euc-", "ewq-", "fif-", "fifaw-", "fifwc-", "fin1-",
    "fl1-", "fpd-", "fr2-", "fro1-", "frtc-", "geo1-", "grc-", "gre1-", "gsc-", "gtm-",
    "hr1-", "hun-", "icwq-", "idn1-", "idn2-", "ire-", "irl1-", "isc-", "isl1-", "isp-",
    "isr-", "itc-", "itsb-", "j1100-", "j2100-", "ja2-", "jap-", "kaz1-", "kor-",
    "kor2-", "lal-", "lec-", "lib-", "ltu1-", "lva1-", "mar1-", "mex-", "mls-", "nawq-",
    "ncag-", "ned2-", "nirl1-", "nlc-", "nor-", "nor2-", "nwsl-", "ofc-", "owq-",
    "par1-", "per1-", "pol-", "por-", "ptc-", "ptsc-", "qat1-", "rou1-", "rus-",
    "saf1-", "sawq-", "sclc-", "scoc-", "scop-", "sea-", "skc-", "slo-", "spl-", "srb-",
    "ssc-", "sud-", "sui-", "svk1-", "swe-", "swe2-", "tha1-", "tpe1-", "tpew-",
    "trsk-", "tur-", "tur2-", "uae1-", "ucl-", "uef-", "uel-", "ueq-", "ukr1-", "unl-",
    "uru1-", "usc-", "usl1-", "uslc-", "usoc-", "uwcl-", "uzb1-", "ven1-", "weuc-",
    "wsl-", "wwcquefa-",
)


def is_soccer_event_slug(event_slug: str) -> bool:
    slug = str(event_slug or "").strip().lower()
    if not slug:
        return False
    return slug.startswith(SOCCER_EVENT_SLUG_PREFIXES)


class ZockdoEventScope:
    """Follow the full Zockdo wallet except new soccer BUYs."""

    def __init__(self, inner: Any):
        self._inner = inner

    def resolve(self, token_id: str) -> ScopeDecision:
        return self._inner.resolve(token_id)

    def resolve_action(self, action: Any) -> ScopeDecision:
        resolver = getattr(self._inner, "resolve_action", None)
        if callable(resolver):
            decision = resolver(action)
        else:
            decision = self._inner.resolve(getattr(action, "token_id", ""))
        side = str(getattr(action, "side", "") or "").strip().upper()
        if side != "BUY":
            return decision
        if not decision.follow:
            return decision
        slug = str((decision.metadata or {}).get("event_slug") or "")
        if not is_soccer_event_slug(slug):
            return decision
        return ScopeDecision(
            False,
            ZOCKDO_SOCCER_SLEEVE_STOPPED,
            decision.metadata,
        )

    def resolve_retry_lifecycle(
        self,
        action: Any,
        frozen_metadata: Mapping[str, Any],
    ) -> ScopeDecision:
        return self._inner.resolve_retry_lifecycle(action, frozen_metadata)
