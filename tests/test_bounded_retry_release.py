import ast
import importlib.util
from types import SimpleNamespace

import live_release_transaction as release
import stat

from cd90_live_copy import LiveStore, SourceAction


def test_release_controller_registers_copy_executors_and_residual_ledger():
    assert release.PROFILE_KEYS == (
        "cd90",
        "zockdo_full_wallet",
        "wallet_9506_full_wallet",
    )
    assert len(release.EXECUTOR_UNITS) == 4
    assert release.RETIRED_CD90_PRIMARY_UNIT not in release.EXECUTOR_UNITS
    assert release.RETIRED_CD90_STANDBY_UNIT not in release.EXECUTOR_UNITS
    cd90 = next(spec for spec in release.PROFILE_SPECS if spec.key == "cd90")
    assert cd90.has_executor is False
    assert {spec.key for spec in release.executor_specs()} == {
        "zockdo_full_wallet",
        "wallet_9506_full_wallet",
    }
    assert release.EXPECTED_SLEEVE_ROLES == {
        "cd90": "RESIDUAL",
        "zockdo_full_wallet": "RESERVED",
        "wallet_9506_full_wallet": "RESERVED",
    }
    assert "ops/polymarket-deadman-alerter.py" in release.REQUIRED_ASSETS
    assert (
        "systemd/com.luke.polymarket.deadman-alerter.service"
        in release.REQUIRED_ASSETS
    )
    assert "systemd/com.luke.polymarket.cd90-live.service" not in release.REQUIRED_ASSETS


def test_pre_stop_health_strips_retired_cd90_from_n_minus_one_payload():
    payload = {
        "overall_state": "INTERNAL_DEGRADED",
        "profiles": {
            "cd90": {"paused": True, "status_issues": ["CD90_HOT_STANDBY_INACTIVE"]},
            "zockdo_full_wallet": {"paused": False, "status_issues": []},
            "wallet_9506_full_wallet": {"paused": False, "status_issues": []},
        },
        "services": [
            {"unit": release.RETIRED_CD90_PRIMARY_UNIT, "ActiveState": "inactive"},
            {"unit": release.RETIRED_CD90_STANDBY_UNIT, "ActiveState": "inactive"},
            {"unit": release.EXECUTOR_UNITS[0], "ActiveState": "active"},
            {"unit": release.EXECUTOR_UNITS[1], "ActiveState": "active"},
            {"unit": release.EXECUTOR_UNITS[2], "ActiveState": "active"},
            {"unit": release.EXECUTOR_UNITS[3], "ActiveState": "active"},
        ],
        "paused_profiles": ["cd90"],
        "service_paused_units": [
            release.RETIRED_CD90_PRIMARY_UNIT,
            release.RETIRED_CD90_STANDBY_UNIT,
        ],
        "service_inactive_units": [release.RETIRED_CD90_PRIMARY_UNIT],
        "failed_polymarket_units": [release.RETIRED_CD90_STANDBY_UNIT],
        "failed_polymarket_unit_count": 1,
        "monitored_profile_count": 3,
        "service_expected_count": 6,
    }

    stripped = release.strip_retired_profiles_from_health_payload(payload)

    assert set(stripped["profiles"]) == {
        "zockdo_full_wallet",
        "wallet_9506_full_wallet",
    }
    assert {item["unit"] for item in stripped["services"]} == set(release.EXECUTOR_UNITS)
    assert stripped["paused_profiles"] == []
    assert stripped["service_paused_units"] == []
    assert stripped["service_inactive_units"] == []
    assert stripped["failed_polymarket_units"] == []
    assert stripped["failed_polymarket_unit_count"] == 0
    assert stripped["monitored_profile_count"] == 2
    assert stripped["service_expected_count"] == 4
    assert stripped["service_active_count"] == 4


def test_release_controller_does_not_treat_deleted_cd90_as_an_executor(tmp_path):
    """Deleting the CD90 follower must not leave a startable executor pair."""

    transaction = release.ReleaseTransaction(
        release.TransactionConfig(
            new_release=tmp_path / "candidate",
            expected_manifest_digest="0" * 64,
            change_id="deleted-cd90-coverage",
            snapshot=tmp_path / "snapshot",
            production=False,
        )
    )
    cd90 = next(spec for spec in release.PROFILE_SPECS if spec.key == "cd90")
    transaction._validate_original_executor_policy()

    assert transaction._profile_original_mode(cd90) == "RETIRED"
    assert set(transaction._original_active_executor_units()) == set(
        release.EXECUTOR_UNITS
    )


def test_release_controller_does_not_restore_the_retired_deadman_bridge():
    source = release.Path(release.__file__).read_text(encoding="utf-8")

    assert "DEADMAN_BRIDGE" not in source
    assert 'Path("/usr/local/sbin") / DEADMAN_BRIDGE' not in source


def test_release_always_contains_canonical_version_authority_verifier():
    relative = "tools/verify_repair_version_authority.py"

    assert relative in release.REQUIRED_ASSETS
    assert (
        release.Path(release.__file__).resolve().parent.parent / relative
    ).is_file()


def test_version_verifier_uses_noninteractive_sudo_only_after_permission_denied(
    monkeypatch,
):
    verifier_path = (
        release.Path(release.__file__).resolve().parent.parent
        / "tools/verify_repair_version_authority.py"
    )
    spec = importlib.util.spec_from_file_location("repair_version_verifier", verifier_path)
    verifier = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(verifier)

    class DeniedTimeline:
        def read_text(self, **_kwargs):
            raise PermissionError("private runtime")

        def __str__(self):
            return "/private/repair_version_timeline.jsonl"

    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(stdout='{"semantic_repair_version":"3.9"}\n')

    monkeypatch.setattr(verifier.subprocess, "run", fake_run)

    assert "3.9" in verifier.read_timeline_text(DeniedTimeline())
    assert calls == [
        (
            [
                "/usr/bin/sudo",
                "-n",
                "/usr/bin/cat",
                "/private/repair_version_timeline.jsonl",
            ],
            {"check": True, "capture_output": True, "text": True},
        )
    ]


def test_release_controller_accepts_liquidity_retry_states():
    assert {
        "PENDING_CONFIRMED_ZERO_FILL",
        "PENDING_PRICE_PROTECTION",
        "EXPIRED_RETRY_WINDOW",
    } <= release.ACTION_TRANSITION_ALLOWLIST


def test_release_controller_accepts_account_cash_reconciliation_as_external():
    category = "EXTERNAL_ACCOUNT_CASH_RECONCILIATION"
    row = {
        "release_runtime_error_audit": {
            "event_count": 1,
            "internal_event_count": 0,
            "external_event_count": 1,
            "code_repair_event_count": 0,
            "category_counts": {category: 1},
            "latest_category": category,
            "state": "ERRORS_OBSERVED",
        }
    }

    assert category in release.EXTERNAL_RUNTIME_CATEGORIES
    assert release._validated_health_runtime_audit(
        row, profile_key="zockdo_full_wallet"
    )["external_count"] == 1


def test_release_controller_knows_every_external_runtime_category_in_shared_core():
    tree = ast.parse(
        (
            release.Path(release.__file__).resolve().parent.parent
            / "app/cd90_live_copy.py"
        ).read_text(encoding="utf-8")
    )
    emitted = {
        str(value.value)
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        for keyword in call.keywords
        if keyword.arg == "category"
        for value in ast.walk(keyword.value)
        if isinstance(value, ast.Constant)
        and isinstance(value.value, str)
        and value.value.startswith("EXTERNAL_")
    }

    assert emitted <= release.EXTERNAL_RUNTIME_CATEGORIES


def test_offline_release_stage_maps_all_profiles_and_activates_at_cursor():
    program = release.ReleaseTransaction._offline_migration_program
    assert callable(program)
    source = release.Path(release.__file__).read_text(encoding="utf-8")
    assert '"database": f"{spec.key}.sqlite3"' in source
    assert "ensure_liquidity_retry_policy_at_current_cursor" in source
    assert "offline stage liquidity retry cursor ahead" in source
    assert "ensure_bounded_retry_policy_at_current_cursor" in source
    assert "offline stage bounded retry cursor ahead" in source


def test_offline_release_stage_does_not_arm_the_deleted_cd90_follower(tmp_path):
    transaction = release.ReleaseTransaction(
        release.TransactionConfig(
            new_release=tmp_path / "candidate",
            expected_manifest_digest="0" * 64,
            change_id="retired-cd90-stage",
            snapshot=tmp_path / "snapshot",
            production=False,
        )
    )

    program = transaction._offline_migration_program()
    source = release.Path(release.__file__).read_text(encoding="utf-8")

    assert "if item['resume']:" in program
    assert "'key': 'cd90', 'database': 'cd90.sqlite3', 'change_env': 'CHANGE_ID_CD90', 'resume': False" in program
    assert "'resume': True" in program
    assert "paused candidate resume evidence drift" in source


def test_live_store_repairs_database_mode_without_replacing_ledger(tmp_path):
    database = tmp_path / "live.sqlite3"
    store = LiveStore(database)
    store.initialize()
    original_inode = database.stat().st_ino
    database.chmod(0o644)

    store.config("missing")

    status = database.stat()
    assert status.st_ino == original_inode
    assert stat.S_IMODE(status.st_mode) == 0o600


def test_bounded_retry_summary_uses_action_target_derived_remainder(tmp_path):
    """The deployed target schema stores no physical remainder column."""

    store = LiveStore(tmp_path / "live.sqlite3")
    store.initialize()

    summary = store.bounded_retry_summary()

    assert summary["target_conservation_violation_count"] == 0


def test_bounded_retry_summary_ignores_pre_policy_target_overshoot(tmp_path):
    """Prospective retry activation must not relabel immutable old fills."""

    store = LiveStore(tmp_path / "live.sqlite3")
    store.initialize()
    action = SourceAction(
        transaction_hash="0x" + "1" * 64,
        token_id="1",
        side="BUY",
        source_quantity=release.Decimal("1"),
        source_notional=release.Decimal("0.5"),
        source_timestamp=1,
        block_number=99,
        block_hash="0x" + "2" * 64,
        source_role="TAKER",
        order_hash="0x" + "3" * 64,
        discovered_at_ms=1,
        log_index=0,
    )
    store.record_action_receipt(action)
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO action_targets(
                action_id,proportional_quantity,target_quantity,
                cumulative_filled_quantity,state,reason,created_at_ms,updated_at_ms
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (action.action_id, "1", "1", "1.1", "FILLED", "OLD_FILL", 1, 1),
        )
    store.activate_bounded_retry_policy(
        effective_after_block=99,
        activated_at_ms=2,
        change_id="prospective-test",
    )

    assert store.bounded_retry_summary()["target_conservation_violation_count"] == 0


def test_release_lock_inventory_does_not_require_unused_legacy_locks():
    source = release.Path(release.__file__).read_text(encoding="utf-8")
    assert "if legacy.exists():" in source
    assert "paths[f\"legacy-profile:{spec.key}\"] = legacy" in source
    assert "locks = [(\"candidate\", self._candidate_profile_lock(spec))]" in source
