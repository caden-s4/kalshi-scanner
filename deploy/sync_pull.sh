#!/usr/bin/env bash
#
# sync_pull.sh -- pull Parquet data from the Kalshi VPS to a local path, verified
# by checksum. This script ONLY copies; it never deletes anything, on either end.
# Deleting the synced data from the VPS is a separate, explicit step
# (sync_purge_remote.sh), which re-verifies before it removes anything.
#
# Usage:
#   ./sync_pull.sh [DATE]
#     DATE   optional YYYY-MM-DD; restrict to that date= partition across all
#            type=* datasets. Omit to sync the entire parquet tree.
#
# Configure via environment (defaults shown):
#   VPS_HOST=kalshi@vps               # ssh destination (host or ~/.ssh/config alias)
#   REMOTE_DATA_DIR=/mnt/kalshi/data  # KALSHI_DATA_DIR on the VPS
#   LOCAL_DATA_DIR=./data             # local destination root
#   SSH_OPTS=                         # extra ssh options, e.g. "-i ~/.ssh/id_ed25519 -p 22"
#
set -euo pipefail

DATE="${1:-}"
VPS_HOST="${VPS_HOST:-kalshi@vps}"
REMOTE_DATA_DIR="${REMOTE_DATA_DIR:-/mnt/kalshi/data}"
LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-./data}"
SSH_OPTS="${SSH_OPTS:-}"

if [[ -n "$DATE" && ! "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "error: DATE must be YYYY-MM-DD, got '$DATE'" >&2
  exit 2
fi

REMOTE_PARQUET="${REMOTE_DATA_DIR%/}/parquet/"
LOCAL_PARQUET="${LOCAL_DATA_DIR%/}/parquet/"
mkdir -p "$LOCAL_PARQUET"

# --checksum forces content comparison (not size+mtime), which is the whole point
# of a verified pull. Build the filter set for an optional single date partition.
FILTERS=()
if [[ -n "$DATE" ]]; then
  FILTERS=(
    --include='*/'
    --include="date=${DATE}/"
    --include="date=${DATE}/**"
    --exclude='*'
  )
fi

RSYNC_COMMON=(
  -a --checksum --human-readable --partial
  -e "ssh ${SSH_OPTS}"
  "${FILTERS[@]}"
  "${VPS_HOST}:${REMOTE_PARQUET}" "${LOCAL_PARQUET}"
)

echo "==> Pulling ${VPS_HOST}:${REMOTE_PARQUET} -> ${LOCAL_PARQUET}"
[[ -n "$DATE" ]] && echo "    scoped to date=${DATE}"
rsync --info=progress2 "${RSYNC_COMMON[@]}"

# Verify: a second checksum pass in dry-run must find nothing left to transfer.
echo "==> Verifying by checksum (dry-run)..."
REMAINING="$(rsync -n --out-format='%n' "${RSYNC_COMMON[@]}" | grep -vE '/$' || true)"
if [[ -n "$REMAINING" ]]; then
  echo "VERIFY FAILED: files still differ after pull:" >&2
  echo "$REMAINING" >&2
  exit 1
fi
echo "==> Verified: local copy matches the VPS by checksum."
echo "    (No data was deleted. To delete the verified date on the VPS, run:"
echo "     ./sync_purge_remote.sh ${DATE:-YYYY-MM-DD} --confirm )"
