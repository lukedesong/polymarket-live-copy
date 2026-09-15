import ast
import importlib.util
from types import SimpleNamespace

import live_release_transaction as release
import stat

from cd90_live_copy import LiveStore, SourceAction


def test_pre_stop_health_allows_authorized_liquidity_retry_json_payload():
    row = {
        "external_limitation_count": 1,
        "external_limitations": [
            'WALLET_42B79C_FULL_WALLET_LIQUIDITY_RETRY_PENDING:'
            '{"action_id":"abc","state":"PENDING_CONFIRMED_ZERO_FILL"}'
        ],
        "latest_runtime_error_category": "",
    }
    audit = {
        "event_count": 0,
        "internal_count": 0,
        "external_count": 0,
        "code_repair_count": 0,
        "latest": "",
        "categories": {},
    }
    count = release._validate_health_external_limitations(
        row,
        profile_key="wallet_42b79c_full_wallet",
        outcome="SUCCESS",
        audit=audit,
    )
    assert count == 1


def test_release_controller_registers_copy_executors_and_residual_ledger():
    assert release.PROFILE_KEYS == (
        "cd90",
        "zockdo_full_wallet",
        "wallet_9506_full_wallet",
        "wallet_42b79c_full_wallet",
        "albumista_full_wallet",
        "mauricebishop_full_wallet",
    )
    assert len(release.EXECUTOR_UNITS) == 4
    assert release.RETIRED_CD90_PRIMARY_UNIT not in release.EXECUTOR_UNITS
    assert release.RETIRED_CD90_STANDBY_UNIT not in release.EXECUTOR_UNITS
    assert release.RETIRED_9506_PRIMARY_UNIT not in release.EXECUTOR_UNITS
    assert release.RETIRED_9506_STANDBY_UNIT not in release.EXECUTOR_UNITS
    cd90 = next(spec for spec in release.PROFILE_SPECS if spec.key == "cd90")
    wallet9506 = next(
        spec for spec in release.PROFILE_SPECS if spec.key == "wallet_9506_full_wallet"
    )
    wallet42b79c = next(
        spec for spec in release.PROFILE_SPECS if spec.key == "wallet_42b79c_full_wallet"
    )
    assert cd90.has_executor is False
    assert wallet9506.has_executor is False
    assert wallet42b79c.has_executor is True
    albumista = next(
        spec for spec in release.PROFILE_SPECS if spec.key == "albumista_full_wallet"
    )
    maurice = next(
        spec for spec in release.PROFILE_SPECS if spec.key == "mauricebishop_full_wallet"
    )
    assert albumista.has_executor is False
    assert maurice.has_executor is False
    assert release.MASK_ON_CUTOVER_KEYS == frozenset(
        {"cd90", "wallet_9506_full_wallet"}
    )
    assert release.cutover_requires_observed_drain(albumista, cutover=True) is False
    assert release.cutover_requires_observed_drain(maurice, cutover=True) is False
    zock = next(
        spec for spec in release.PROFILE_SPECS if spec.key == "zockdo_full_wallet"
    )
    assert release.cutover_requires_observed_drain(zock, cutover=True) is True
    assert release.cutover_requires_observed_drain(zock, cutover=False) is False
    assert albumista.primary_unit not in release.retired_follower_units()
    assert maurice.primary_unit not in release.retired_follower_units()
    assert release.RETIRED_CD90_PRIMARY_UNIT in release.retired_follower_units()
    assert {spec.key for spec in release.executor_specs()} == {
        "zockdo_full_wallet",
        "wallet_42b79c_full_wallet",
    }
    assert "systemd/com.luke.polymarket.wallet-9506-live.service" not in release.REQUIRED_ASSETS
    assert (
        "systemd/com.luke.polymarket.wallet-9506-live-hot-standby.service"
        not in release.REQUIRED_ASSETS
    )
    assert "app/wallet42b79c_live_copy.py" in release.REQUIRED_ASSETS
    source = release.Path(release.__file__).read_text(encoding="utf-8")
    assert "apply_twenty_percent_scale_rebase" in source
    assert "grand scale rebase requires cursor" in source
    assert "fixed_share_scale" in release.MIGRATION_CONFIG_KEYS
    assert "scale_basis" in release.MIGRATION_CONFIG_KEYS
    assert "systemd/com.luke.polymarket.wallet-42b79c-live.service" in release.REQUIRED_ASSETS
    assert (
        "systemd/com.luke.polymarket.wallet-42b79c-live-hot-standby.service"
        in release.REQUIRED_ASSETS
    )
    assert "wallet-9506-live.env" in release.retired_follower_env_names()
    assert "zockdo-live.env" not in release.retired_follower_env_names()
    assert release.RETIRED_9506_PRIMARY_UNIT in release.retired_follower_units()
    assert release.RETIRED_9506_PRIMARY_UNIT not in release.EXECUTOR_UNITS
    assert release.EXPECTED_SLEEVE_ROLES == {
        "cd90": "RESIDUAL",
        "zockdo_full_wallet": "RESERVED",
        "wallet_9506_full_wallet": "RESERVED",
        "wallet_42b79c_full_wallet": "RESERVED",
        "albumista_full_wallet": "RESERVED",
        "mauricebishop_full_wallet": "RESERVED",
    }
    assert "ops/polymarket-deadman-alerter.py" in release.REQUIRED_ASSETS
    assert (
        "systemd/com.luke.polymarket.deadman-alerter.service"
        in release.REQUIRED_ASSETS
    )
    assert "systemd/com.luke.polymarket.cd90-live.service" not in release.REQUIRED_ASSETS


def test_pre_stop_health_strips_retired_followers_from_n_minus_one_payload():
    payload = {
        "overall_state": "INTERNAL_DEGRADED",
        "profiles": {
            "cd90": {"paused": True, "status_issues": ["CD90_HOT_STANDBY_INACTIVE"]},
            "zockdo_full_wallet": {"paused": False, "status_issues": []},
            "wallet_9506_full_wallet": {
                "paused": True,
                "status_issues": ["WALLET_9506_FULL_WALLET_HOT_STANDBY_INACTIVE"],
            },
        },
        "services": [
            {"unit": release.RETIRED_CD90_PRIMARY_UNIT, "ActiveState": "inactive"},
            {"unit": release.RETIRED_CD90_STANDBY_UNIT, "ActiveState": "inactive"},
            {"unit": release.RETIRED_9506_PRIMARY_UNIT, "ActiveState": "inactive"},
            {"unit": release.RETIRED_9506_STANDBY_UNIT, "ActiveState": "inactive"},
            {"unit": release.EXECUTOR_UNITS[0], "ActiveState": "active"},
            {"unit": release.EXECUTOR_UNITS[1], "ActiveState": "active"},
        ],
        "paused_profiles": ["cd90", "wallet_9506_full_wallet"],
        "service_paused_units": [
            release.RETIRED_CD90_PRIMARY_UNIT,
            release.RETIRED_CD90_STANDBY_UNIT,
            release.RETIRED_9506_PRIMARY_UNIT,
            release.RETIRED_9506_STANDBY_UNIT,
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
    }
    assert {item["unit"] for item in stripped["services"]} == {
        release.PRIMARY_UNITS[0],
        release.STANDBY_UNITS[0],
    }
    assert stripped["paused_profiles"] == []
    assert stripped["service_paused_units"] == []
    assert stripped["service_inactive_units"] == []
    assert stripped["failed_polymarket_units"] == []
    assert stripped["failed_polymarket_unit_count"] == 0
    assert stripped["monitored_profile_count"] == 1
    assert stripped["service_expected_count"] == 2
    assert stripped["service_active_count"] == 2


def test_grand_vista_is_new_executor_when_absent_from_old_release(tmp_path):
    transaction = release.ReleaseTransaction(
        release.TransactionConfig(
            new_release=tmp_path / "candidate",
            expected_manifest_digest="0" * 64,
            change_id="grand-vista-half",
            snapshot=tmp_path / "snapshot",
            production=False,
        )
    )
    transaction.old_release = tmp_path / "old"
    (transaction.old_release / "systemd").mkdir(parents=True)
    grand = next(
        spec
        for spec in release.PROFILE_SPECS
        if spec.key == "wallet_42b79c_full_wallet"
    )
    zock = next(
        spec for spec in release.PROFILE_SPECS if spec.key == "zockdo_full_wallet"
    )
    (transaction.old_release / "systemd" / zock.primary_unit).write_text("x")
    assert transaction._executor_introduced_this_release(grand) is True
    assert transaction._executor_introduced_this_release(zock) is False


def test_pre_stop_health_scope_keeps_installed_zockdo_when_new_executor_missing():
    payload = {
        "profiles": {"zockdo_full_wallet": {"paused": False}},
        "services": [
            {"unit": release.PRIMARY_UNITS[0]},
            {"unit": release.STANDBY_UNITS[0]},
        ],
    }
    specs, units = release._health_executor_scope(payload, pre_stop=True)
    assert {spec.key for spec in specs} == {"zockdo_full_wallet"}
    assert set(units) == {release.PRIMARY_UNITS[0], release.STANDBY_UNITS[0]}


def test_health_scope_lists_grand_vista_after_cutover():
    specs, units = release._health_executor_scope({"profiles": {}}, pre_stop=False)
    assert {spec.key for spec in specs} == {
        "zockdo_full_wallet",
        "wallet_42b79c_full_wallet",
    }
    assert set(units) == set(release.EXECUTOR_UNITS)


def test_release_controller_does_not_treat_deleted_weather_followers_as_executors(
    tmp_path,
):
    """Deleting CD90/9506 followers must not leave a startable executor pair."""

    old = tmp_path / "old"
    (old / "systemd").mkdir(parents=True)
    zock = next(
        spec for spec in release.PROFILE_SPECS if spec.key == "zockdo_full_wallet"
    )
    grand = next(
        spec
        for spec in release.PROFILE_SPECS
        if spec.key == "wallet_42b79c_full_wallet"
    )
    (old / "systemd" / zock.primary_unit).write_text("x")
    current_link = tmp_path / "current"
    current_link.symlink_to(old)
    transaction = release.ReleaseTransaction(
        release.TransactionConfig(
            new_release=tmp_path / "candidate",
            expected_manifest_digest="0" * 64,
            change_id="deleted-cd90-coverage",
            snapshot=tmp_path / "snapshot",
            production=False,
            current_link=current_link,
        )
    )
    cd90 = next(spec for spec in release.PROFILE_SPECS if spec.key == "cd90")
    wallet9506 = next(
        spec for spec in release.PROFILE_SPECS if spec.key == "wallet_9506_full_wallet"
    )
    transaction._validate_original_executor_policy()

    assert transaction._profile_original_mode(cd90) == "RETIRED"
    assert transaction._profile_original_mode(wallet9506) == "RETIRED"
    assert transaction._profile_original_mode(grand) == "NEW"
    assert set(transaction._original_active_executor_units()) == {
        zock.primary_unit,
        zock.standby_unit,
        grand.primary_unit,
    }


def test_pre_stop_health_filters_new_sleeve_open_failures_when_n_minus_one_omits_new_units(
    tmp_path,
):
    from cd90_live_copy import LiveStore

    old = tmp_path / "old"
    (old / "systemd").mkdir(parents=True)
    zock = next(
        spec for spec in release.PROFILE_SPECS if spec.key == "zockdo_full_wallet"
    )
    (old / "systemd" / zock.primary_unit).write_text("x")
    current_link = tmp_path / "current"
    current_link.symlink_to(old)
    runtime_root = tmp_path / "runtime"
    store = LiveStore(runtime_root / "zockdo_live" / "live.sqlite3")
    store.append_runtime_error(
        occurred_at_ms=50,
        category="INTERNAL_REDEMPTION_CYCLE",
        message="OperationalError: unable to open database file",
    )
    transaction = release.ReleaseTransaction(
        release.TransactionConfig(
            new_release=tmp_path / "candidate",
            expected_manifest_digest="0" * 64,
            change_id="grand-vista-pre-stop",
            snapshot=tmp_path / "snapshot",
            production=False,
            current_link=current_link,
            runtime_root=runtime_root,
        )
    )
    grand = next(
        spec
        for spec in release.PROFILE_SPECS
        if spec.key == "wallet_42b79c_full_wallet"
    )
    transaction.original_activity = {
        zock.primary_unit: "active",
        zock.standby_unit: "active",
        grand.primary_unit: "inactive",
        grand.standby_unit: "inactive",
    }
    transaction.original_enablement = {
        zock.primary_unit: "enabled",
        zock.standby_unit: "enabled",
        grand.primary_unit: "disabled",
        grand.standby_unit: "disabled",
        release.HEALTH_TIMER: "disabled",
    }
    payload = {
        "overall_state": "INTERNAL_DEGRADED",
        "profiles": {
            "zockdo_full_wallet": {
                "paused": False,
                "status_issues": [],
                "status_issue_count": 0,
                "last_cycle_outcome": "SUCCESS",
                "sqlite_integrity": {"state": "OK"},
                "external_limitations": [],
                "external_limitation_count": 0,
                "release_runtime_error_audit": {
                    "state": "ERRORS_OBSERVED",
                    "release_started_at_ms": 0,
                    "event_count": 1,
                    "internal_event_count": 1,
                    "external_event_count": 0,
                    "code_repair_event_count": 0,
                    "latest_category": "INTERNAL_REDEMPTION_CYCLE",
                    "category_counts": {"INTERNAL_REDEMPTION_CYCLE": 1},
                },
            }
        },
        "services": [
            {"unit": release.PRIMARY_UNITS[0], "ActiveState": "active"},
            {"unit": release.STANDBY_UNITS[0], "ActiveState": "active"},
        ],
        "coordinator": {
            "state": "INVALID",
            "issues": ["UNMONITORED_COORDINATOR_PROFILES:wallet_42b79c_full_wallet"],
        },
        "failed_polymarket_units": [],
        "failed_polymarket_unit_count": 0,
        "service_inactive_units": [],
    }

    normalized = transaction._normalized_pre_stop_health_payload(payload)

    zock_row = normalized["profiles"]["zockdo_full_wallet"]
    audit = zock_row["release_runtime_error_audit"]
    assert audit["internal_event_count"] == 0
    assert zock_row["runtime_internal_error_count"] == 0
    assert normalized["coordinator"]["state"] == "OK"


def test_redemption_worker_code_repair_latch_does_not_block_storage_gate():
    assert release.blocking_code_repair_runtime_keys(()) == ()
    assert release.blocking_code_repair_runtime_keys(
        (("auto_redemption_worker_state", "CODE_REPAIR_REQUIRED"),)
    ) == ()
    assert release.blocking_code_repair_runtime_keys(
        (
            ("auto_redemption_worker_state", "CODE_REPAIR_REQUIRED"),
            ("status", "CODE_REPAIR_REQUIRED"),
        )
    ) == ("status",)


def test_new_sleeve_ledger_open_blocked_is_the_protectsystem_gap():
    assert release.new_sleeve_ledger_open_blocked(
        "INTERNAL_REDEMPTION_CYCLE",
        "OperationalError: unable to open database file",
    )
    assert not release.new_sleeve_ledger_open_blocked(
        "INTERNAL_REDEMPTION_CYCLE",
        "IntegrityError: UNIQUE constraint failed",
    )
    assert not release.new_sleeve_ledger_open_blocked(
        "INTERNAL_RUNTIME",
        "OperationalError: unable to open database file",
    )


def test_new_executor_planned_resume_must_be_idle_not_completed():
    assert release.new_executor_planned_resume_violation({}) == ""
    assert (
        release.new_executor_planned_resume_violation(
            {
                "operator_planned_resume_from_block": "",
                "operator_pre_repair_forward_recovery_armed": "false",
                "operator_planned_resume_state": "",
            }
        )
        == ""
    )
    assert (
        release.new_executor_planned_resume_violation(
            {"operator_planned_resume_from_block": "92580000"}
        )
        == "operator_planned_resume_from_block"
    )
    assert (
        release.new_executor_planned_resume_violation(
            {"operator_pre_repair_forward_recovery_armed": "true"}
        )
        == "operator_pre_repair_forward_recovery_armed"
    )
    assert (
        release.new_executor_planned_resume_violation(
            {"operator_planned_resume_state": "PENDING"}
        )
        == "operator_planned_resume_state"
    )


def test_new_executor_leftover_enabled_units_are_not_partial_policy(tmp_path):
    old = tmp_path / "old"
    (old / "systemd").mkdir(parents=True)
    zock = next(
        spec for spec in release.PROFILE_SPECS if spec.key == "zockdo_full_wallet"
    )
    grand = next(
        spec
        for spec in release.PROFILE_SPECS
        if spec.key == "wallet_42b79c_full_wallet"
    )
    (old / "systemd" / zock.primary_unit).write_text("x")
    current_link = tmp_path / "current"
    current_link.symlink_to(old)
    transaction = release.ReleaseTransaction(
        release.TransactionConfig(
            new_release=tmp_path / "candidate",
            expected_manifest_digest="0" * 64,
            change_id="grand-vista-retry",
            snapshot=tmp_path / "snapshot",
            production=False,
            current_link=current_link,
        )
    )
    transaction.original_activity = {
        zock.primary_unit: "active",
        zock.standby_unit: "active",
        grand.primary_unit: "inactive",
        grand.standby_unit: "inactive",
    }
    transaction.original_enablement = {
        zock.primary_unit: "enabled",
        zock.standby_unit: "enabled",
        grand.primary_unit: "enabled",
        grand.standby_unit: "enabled",
        release.HEALTH_TIMER: "disabled",
    }
    transaction._validate_original_executor_policy()
    assert transaction._profile_original_mode(grand) == "NEW"


def test_release_controller_does_not_restore_the_retired_deadman_bridge():
    source = release.Path(release.__file__).read_text(encoding="utf-8")

    assert "DEADMAN_BRIDGE" not in source
    assert 'Path("/usr/local/sbin") / DEADMAN_BRIDGE' not in source


def test_release_includes_zockdo_soccer_stop_modules():
    for relative in (
        "app/zockdo_nontennis_cap.py",
        "app/zockdo_soccer_sleeve.py",
    ):
        assert relative in release.REQUIRED_ASSETS
        assert (
            release.Path(release.__file__).resolve().parent.parent / relative
        ).is_file()


def test_release_always_contains_canonical_version_authority_verifier():
    relative = "tools/verify_repair_version_authority.py"

    assert relative in release.REQUIRED_ASSETS
    assert (
        release.Path(release.__file__).resolve().parent.parent / relative
    ).is_file()


def test_release_inventory_carries_tempx_runtime_and_regressions():
    expected = {
        "tempx/temperature_executor.py",
        "tempx/temperature_signal_engine.py",
        "tempx/temperature_book_capture.py",
        "tempx/bin_decision_engine.py",
        "tempx/hightemptation_ladder_econ.py",
        "tempx/hightemptation_obs_alignment.py",
        "tempx/tempx_write_status.py",
        "systemd/com.luke.polymarket.tempx-executor.service",
        "systemd/com.luke.polymarket.tempx-signal.service",
        "systemd/com.luke.polymarket.tempx-universe.service",
        "systemd/com.luke.polymarket.tempx-universe.timer",
        "systemd/com.luke.polymarket.tempx-status.service",
        "systemd/com.luke.polymarket.tempx-status.timer",
        "tests/test_tempx_architecture.py",
        "tests/test_bin_decision_engine.py",
    }

    assert expected <= release.REQUIRED_ASSETS


def test_release_quiescence_allows_only_exact_tempx_unit_processes():
    python = release.Path("/opt/polymarket-live/venv/bin/python")
    workdir = release.Path("/opt/polymarket-tempx")
    script = "/opt/polymarket-tempx/temperature_signal_engine.py"
    cgroup = "0::/system.slice/com.luke.polymarket.tempx-signal.service\n"
    argv = (str(python), "-u", script, "--interval", "10")

    assert release.is_allowlisted_tempx_process(
        cgroup_text=cgroup,
        cwd=workdir,
        executable=python,
        argv=argv,
        expected_python=python,
    )
    assert not release.is_allowlisted_tempx_process(
        cgroup_text="0::/system.slice/com.luke.polymarket.zockdo-live.service\n",
        cwd=workdir,
        executable=python,
        argv=argv,
        expected_python=python,
    )
    assert not release.is_allowlisted_tempx_process(
        cgroup_text=cgroup,
        cwd=release.Path("/srv/polymarket-live/runtime"),
        executable=python,
        argv=argv,
        expected_python=python,
    )
    assert not release.is_allowlisted_tempx_process(
        cgroup_text=cgroup,
        cwd=workdir,
        executable=python,
        argv=(str(python), "-u", "/tmp/untrusted.py"),
        expected_python=python,
    )


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
    assert (
        "'key': 'wallet_9506_full_wallet', 'database': 'wallet_9506_full_wallet.sqlite3', "
        "'change_env': 'CHANGE_ID_WALLET_9506_FULL_WALLET', 'resume': False"
    ) in program
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


def test_frozen_sleeves_may_lag_on_historical_migration_tip():
    source = release.Path(release.__file__).read_text(encoding="utf-8")
    assert "known_migration_tips" in source
    assert "local_tip not in known_migration_tips" in source
    assert 'config.get("shared_wallet_migration_receipt_hash") != tip' not in source
