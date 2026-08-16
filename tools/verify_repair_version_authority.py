#!/usr/bin/env python3
"""Resolve the server's canonical repair version or fail closed."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path


TIMELINE_PATH = Path(
    "/srv/polymarket-live/runtime/server_health/repair_version_timeline.jsonl"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_timeline_text(path: Path = TIMELINE_PATH) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except PermissionError:
        completed = subprocess.run(
            ["/usr/bin/sudo", "-n", "/usr/bin/cat", str(path)],
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout


def main() -> int:
    current = Path("/opt/polymarket-live/current").resolve(strict=True)
    index_path = Path("/opt/polymarket-live/CURRENT_REPAIR_VERSION.json")
    try:
        index = json.loads(index_path.read_text(encoding="utf-8"))
        receipt_path = Path(index["source_commit_receipt"])
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        timeline = json.loads(
            [
                line
                for line in read_timeline_text().splitlines()
                if line.strip()
            ][-1]
        )
        version = str(index["semantic_repair_version"])
        checks = (
            Path(index["release"]) == current,
            index["version_status"] == "VERIFIED_FIXED",
            receipt_path == current / "COMMITTED.json",
            index["source_commit_receipt_sha256"] == sha256(receipt_path),
            receipt["release"] == str(current),
            str(receipt["semantic_repair_version"]) == version,
            receipt["result"] == "VERIFIED_FIXED",
            timeline["release"] == str(current),
            str(timeline["semantic_repair_version"]) == version,
            timeline["receipt_sha256"] == sha256(receipt_path),
            timeline["classification"] == "VERIFIED_FIXED",
        )
        if not all(checks):
            raise ValueError("VERSION_IDENTITY_MISMATCH")
    except (
        IndexError,
        KeyError,
        OSError,
        ValueError,
        json.JSONDecodeError,
        subprocess.SubprocessError,
    ) as exc:
        print(
            json.dumps(
                {"state": "BLOCK_VERSION_AUTHORITY", "reason": str(exc)},
                sort_keys=True,
            )
        )
        return 1
    print(
        json.dumps(
            {
                "state": "VERIFIED_FIXED",
                "semantic_repair_version": version,
                "release": str(current),
                "checks_passed": len(checks),
                "checks_total": len(checks),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
