#!/usr/bin/env bash
#
# sync_purge_remote.sh -- delete one date= partition of Parquet data from the VPS,
# but ONLY after re-verifying that the local copy matches the VPS by checksum.
# This is the explicit, separate deletion step referenced by sync_pull.sh. It
# never runs automatically and refuses to delete anything unless verification is
# clean and --confirm is passed.
#
# Usage:
#   ./sync_purge_remote.sh YYYY-MM-DD --confirm
#
# Configure via the same environment variables as sync_pull.sh:
#   VPS_HOST, REMOTE_DATA_DIR, LOCAL_DATA_DIR, SSH_OPTS
#
set -euo pipefail

DATE="${1:-}"
CONFIRM="${2:-}"
VPS_HOST="${VPS_HOST:-kalshi@vps}"
REMOTE_DATA_DIR="${REMOTE_DATA_DIR:-/mnt/kalshi/data}"
LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-./data}"
SSH_OPTS="${SSH_OPTS:-}"

if [[ ! "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "usage: $0 YYYY-MM-DD --confirm" >&2
  exit 2
fi

REMOTE_PARQUET="${REMOTE_DATA_DIR%/}/parquet/"
LOCAL_PARQUET="${LOCAL_DATA_DIR%/}/parquet/"

# Re-verify: pull-direction checksum dry-run scoped to this date must be empty,
# i.e. everything on the VPS for date=DATE already exists locally, byte-verified.
echo "==> Re-verifying date=${DATE} local vs VPS by checksum (dry-run)..."
FILTERS=(
  --include='*/'
  --include="date=${DATE}/"
  --include="date=${DATE}/**"
  --exclude='*'
)
DIFF="$(rsync -a -n --checksum --out-format='%n' \
  -e "ssh ${SSH_OPTS}" \
  "${FILTERS[@]}" \
  "${VPS_HOST}:${REMOTE_PARQUET}" "${LOCAL_PARQUET}" | grep -vE '/$' || true)"

if [[ -n "$DIFF" ]]; then
  echo "ABORT: local copy does NOT match the VPS for date=${DATE}; nothing deleted." >&2
  echo "Files not yet safely synced locally:" >&2
  echo "$DIFF" >&2
  echo "Run ./sync_pull.sh ${DATE} first." >&2
  exit 1
fi
echo "==> Verified: every date=${DATE} file on the VPS exists locally."

if [[ "$CONFIRM" != "--confirm" ]]; then
  echo "Verification passed but --confirm was not given; nothing deleted." >&2
  echo "Re-run: $0 ${DATE} --confirm" >&2
  exit 3
fi

# Delete only the date= partition, only under the parquet type=* datasets on the
# VPS. Uses a bounded find so a bad DATE can never expand to the whole tree.
echo "==> Deleting date=${DATE} partitions on ${VPS_HOST}..."
# shellcheck disable=SC2029  # we intentionally expand DATE locally into the remote command
ssh ${SSH_OPTS} "${VPS_HOST}" \
  "find '${REMOTE_DATA_DIR%/}/parquet' -type d -name 'date=${DATE}' -prune -print -exec rm -rf {} +"
echo "==> Done. Removed date=${DATE} from the VPS parquet tree (local copy retained)."
