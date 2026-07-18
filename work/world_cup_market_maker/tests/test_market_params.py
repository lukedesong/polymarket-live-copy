import json
from decimal import Decimal

import pytest

from world_cup_mm.market_params import ClobMarketParamsClient, parse_clob_market_info


class JsonResponse:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self):
        return self.payload


def test_parse_clob_market_info_uses_official_minimum_and_maker_fee():
    result = parse_clob_market_info(
        {"mos": 5, "mbf": 0, "mts": 0.01, "t": [{"t": "yes", "o": "Yes"}]}
    )

    assert result.minimum_order_size == Decimal("5")
    assert result.maker_fee_bps == 0
    assert result.tick_size == Decimal("0.01")
    assert result.outcomes == {"yes": "Yes"}


@pytest.mark.parametrize(
    "payload,error",
    [
        ({"mos": 0, "mbf": 0, "mts": 0.01, "t": []}, "invalid_minimum_order_size"),
        ({"mos": 5, "mbf": 0, "mts": 0, "t": []}, "invalid_tick_size"),
        ({"mos": 5, "mbf": 1, "mts": 0.01, "t": []}, "nonzero_maker_fee"),
    ],
)
def test_invalid_or_nonzero_maker_parameters_fail_closed(payload, error):
    with pytest.raises(ValueError, match=error):
        parse_clob_market_info(payload)


def test_client_uses_public_clob_market_endpoint_and_json_headers(monkeypatch):
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return JsonResponse({"mos": 5, "mbf": 0, "mts": 0.01, "t": []})

    monkeypatch.setattr("world_cup_mm.market_params.urlopen", fake_urlopen)

    result = ClobMarketParamsClient().fetch("condition-a")

    assert result.minimum_order_size == Decimal("5")
    assert captured["request"].full_url.endswith("/clob-markets/condition-a")
    assert captured["request"].get_header("Accept") == "application/json"
    assert captured["request"].get_header("User-agent") == "world-cup-mm/0.1"
