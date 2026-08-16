#!/bin/sh
set -eu
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
new_release=${1:?usage: deploy_three_wallet_core_hotfix_release.sh NEW_RELEASE_PATH EXPECTED_MANIFEST_SHA256 CHANGE_ID SNAPSHOT_PATH}
expected_manifest_digest=${2:?usage: deploy_three_wallet_core_hotfix_release.sh NEW_RELEASE_PATH EXPECTED_MANIFEST_SHA256 CHANGE_ID SNAPSHOT_PATH}
entrypoint=$(/usr/bin/readlink -f -- "$0")
expected_entrypoint=$new_release/tools/deploy_three_wallet_core_hotfix_release.sh
test "$entrypoint" = "$expected_entrypoint"
/opt/polymarket-live/venv/bin/python -I -c '
import hashlib
import re
import sys
from pathlib import Path
release, expected_digest, wrapper = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
if release.parent != Path("/opt/polymarket-live/releases") or release.is_symlink():
    raise SystemExit("candidate release path is not immutable")
for path in (release, *release.rglob("*")):
    status = path.lstat()
    if path.is_symlink() or status.st_uid != 0 or status.st_gid != 0 or status.st_mode & 0o022:
        raise SystemExit("candidate release tree is not root immutable")
manifest = release / "MANIFEST.sha256"
if not manifest.is_file() or manifest.is_symlink():
    raise SystemExit("candidate manifest is not regular")
if re.fullmatch(r"[0-9a-f]{64}", expected_digest) is None:
    raise SystemExit("invalid caller manifest digest")
if hashlib.sha256(manifest.read_bytes()).hexdigest() != expected_digest:
    raise SystemExit("caller manifest digest mismatch")
wanted = {
    "tools/deploy_three_wallet_core_hotfix_release.sh": wrapper,
    "tools/live_release_transaction.py": release / "tools/live_release_transaction.py",
}
found = {}
for line in manifest.read_text(encoding="utf-8").splitlines():
    fields = line.split(maxsplit=1)
    if len(fields) != 2:
        raise SystemExit("invalid manifest record")
    digest, relative = fields[0], fields[1].lstrip("*")
    if relative.startswith("./"):
        relative = relative[2:]
    if relative in wanted:
        if relative in found:
            raise SystemExit("duplicate bootstrap manifest record")
        found[relative] = digest
if set(found) != set(wanted):
    raise SystemExit("bootstrap manifest record missing")
for relative, path in wanted.items():
    if not path.is_file() or path.is_symlink():
        raise SystemExit("bootstrap asset is not regular")
    if hashlib.sha256(path.read_bytes()).hexdigest() != found[relative]:
        raise SystemExit("bootstrap asset digest mismatch")
print("BOOTSTRAP_RELEASE_MANIFEST_VERIFIED")
' "$new_release" "$expected_manifest_digest" "$entrypoint"
exec /usr/bin/sudo -n /usr/bin/env -i \
    PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUTF8=1 PYTHONDONTWRITEBYTECODE=1 \
    RELEASE_WRAPPER="$entrypoint" \
    /opt/polymarket-live/venv/bin/python -I \
    "$new_release/tools/live_release_transaction.py" "$@"
