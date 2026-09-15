"""Contract checks on the unit files this repo ships for the live copiers.

These units are what the server actually runs, so a unit that points at a
script the release no longer carries, or that sandboxes away the runtime
directory it was told to write, is a start-time failure that no Python test
would otherwise notice.  The repo is the review surface for the deployment
definition, so the checks live here rather than only on the server.
"""

import re
from pathlib import Path

import pytest

import live_release_transaction as release

REPO_ROOT = Path(__file__).resolve().parents[1]
SYSTEMD_DIR = REPO_ROOT / "systemd"
APP_DIR = REPO_ROOT / "app"

RELEASE_APP_PREFIX = "/opt/polymarket-live/current/app/"
SERVER_HEALTH_SCRIPT = SYSTEMD_DIR / "polymarket-server-health"

# Sleeves that run beside the release-transaction profile table.  They are not
# part of PROFILE_SPECS because the release machinery does not stage them, but
# their unit still has to point at a script this repo carries.
STANDALONE_SLEEVE_UNITS = {
    "com.luke.polymarket.mean-impediment-live.service": "mean_impediment_live_copy.py",
}


def unit_files() -> list[Path]:
    return sorted(
        path
        for path in SYSTEMD_DIR.iterdir()
        if path.is_file() and path.suffix in {".service", ".timer"}
    )


def service_units() -> list[Path]:
    return [path for path in unit_files() if path.suffix == ".service"]


def unit_ids(paths):
    return [path.name for path in paths]


def directives(path: Path, key: str) -> list[str]:
    """Return every value assigned to `key`, in file order."""

    values = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith(f"{key}="):
            values.append(stripped.split("=", 1)[1].strip())
    return values


def exec_start_app_script(path: Path) -> str | None:
    """The app script an ExecStart runs out of the release, if any."""

    for value in directives(path, "ExecStart"):
        match = re.search(rf"{re.escape(RELEASE_APP_PREFIX)}(\S+\.py)", value)
        if match:
            return match.group(1)
    return None


def exec_start_runtime_dir(path: Path) -> str | None:
    for value in directives(path, "ExecStart"):
        match = re.search(r"--runtime-dir\s+(\S+)", value)
        if match:
            return match.group(1)
    return None


def spec_for_unit(unit_name: str) -> release.ProfileSpec | None:
    for spec in release.PROFILE_SPECS:
        if unit_name in {spec.primary_unit, spec.standby_unit}:
            return spec
    return None


@pytest.mark.parametrize("unit", service_units(), ids=unit_ids(service_units()))
def test_unit_app_script_exists_unless_its_profile_is_retired(unit):
    """A live unit may not point at a script the release does not carry."""

    script = exec_start_app_script(unit)
    if script is None:
        return

    if unit.name in STANDALONE_SLEEVE_UNITS:
        assert script == STANDALONE_SLEEVE_UNITS[unit.name]
        assert (APP_DIR / script).is_file()
        return

    spec = spec_for_unit(unit.name)
    assert spec is not None, (
        f"{unit.name} runs {script} but belongs to no ProfileSpec, so the "
        "release transaction would never stage or verify its ledger"
    )
    assert script == spec.app_script

    if spec.has_executor:
        assert (APP_DIR / script).is_file(), (
            f"{unit.name} is a live executor but app/{script} is missing"
        )
    else:
        # A retired profile keeps its unit on disk so an operator can see it is
        # deliberately stopped.  It must stay out of the live executor set, and
        # its absent script is what makes an accidental start fail loudly.
        assert unit.name not in release.EXECUTOR_UNITS


@pytest.mark.parametrize("unit", service_units(), ids=unit_ids(service_units()))
def test_unit_may_write_the_runtime_directory_it_was_given(unit):
    runtime_dir = exec_start_runtime_dir(unit)
    if runtime_dir is None:
        return
    writable = {
        entry
        for value in directives(unit, "ReadWritePaths")
        for entry in value.split()
    }
    assert runtime_dir in writable, (
        f"{unit.name} runs with --runtime-dir {runtime_dir} but ProtectSystem="
        "strict does not expose that path for writing"
    )


@pytest.mark.parametrize("unit_name", release.EXECUTOR_UNITS)
def test_every_live_executor_unit_is_kept_in_the_repo(unit_name):
    assert (SYSTEMD_DIR / unit_name).is_file()


def test_retired_profiles_still_ship_their_stopped_units():
    """Retiring a profile must not silently drop its unit from review."""

    for spec in release.PROFILE_SPECS:
        if spec.has_executor:
            continue
        for unit_name in (spec.primary_unit, spec.standby_unit):
            assert (SYSTEMD_DIR / unit_name).is_file(), (
                f"{spec.key} is retired but {unit_name} is not in the repo, so "
                "the stopped unit on the server has no review surface"
            )


def server_health_registrations() -> dict[str, tuple[str, str, str]]:
    body = SERVER_HEALTH_SCRIPT.read_text(encoding="utf-8")
    registrations = {}
    for value in re.findall(r"--live-profile\s+(\S+)", body):
        profile_key, primary, runtime_dir, standby = value.split("=")
        registrations[profile_key] = (primary, runtime_dir, standby)
    return registrations


def test_server_health_watches_every_wallet_unit_the_repo_ships():
    registrations = server_health_registrations()
    assert registrations, "server-health registers no live profile at all"

    for profile_key, (primary, runtime_dir, standby) in sorted(registrations.items()):
        primary_unit = SYSTEMD_DIR / primary
        standby_unit = SYSTEMD_DIR / standby
        assert primary_unit.is_file(), f"{profile_key}: {primary} missing from repo"
        assert standby_unit.is_file(), f"{profile_key}: {standby} missing from repo"
        assert exec_start_runtime_dir(primary_unit) == runtime_dir, (
            f"{profile_key}: server-health watches {runtime_dir} but the unit "
            "runs against a different runtime directory"
        )


def test_server_health_covers_the_units_the_release_treats_as_live():
    watched_primaries = {
        primary for primary, _runtime, _standby in server_health_registrations().values()
    }
    assert set(release.PRIMARY_UNITS) <= watched_primaries, (
        "a live executor is not watched by the server-health heartbeat"
    )


def test_deadman_timer_fires_on_a_calendar_so_a_failed_run_cannot_stop_it():
    timer = SYSTEMD_DIR / "com.luke.polymarket.deadman-alerter.timer"
    body = timer.read_text(encoding="utf-8")
    assert "OnCalendar=" in body
    assert "OnUnitActiveSec" not in body
    assert "Persistent=true" in body
