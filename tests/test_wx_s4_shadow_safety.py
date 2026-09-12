from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_s4_daemon_is_record_only_and_excludes_dead_leftover():
    source = (ROOT / "app" / "wx_s4_shadow.py").read_text(encoding="utf-8")
    assert "real_order_submitted" in source
    assert "poly_live_trading_armed" in source
    assert "False" in source
    assert "dead_leftover" not in source
    assert "forecast_nowcast" in source
    assert "POST" not in source
    assert "/order" not in source
    assert "CLOBExecutionAdapter" not in source
    unit = (ROOT / "deploy" / "wx-s4" / "com.luke.wx-s4-shadow.service").read_text(encoding="utf-8")
    assert "UnsetEnvironment=" in unit
    assert "POLYMARKET_LIVE_TRADING" in unit
    assert "wx_s4_shadow.py" in unit
    assert "wx-s3-shadow" not in unit.split("ExecStart")[1]


def test_hourly_timer_does_not_arm_trading():
    unit = (ROOT / "deploy" / "wx-s4" / "com.luke.ensemble-hourly-collect.service").read_text(
        encoding="utf-8"
    )
    assert "UnsetEnvironment=" in unit
    assert "POLYMARKET_LIVE_TRADING" in unit
    assert "Type=oneshot" in unit
