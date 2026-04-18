#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${CTO_BASE_URL:-}" || -z "${CTO_INGEST_TOKEN:-}" ]]; then
  echo "Missing CTO_BASE_URL or CTO_INGEST_TOKEN" >&2
  exit 1
fi

PAYLOAD='{"drop_digested_day":false,"prune_healthy_older_than_days":7,"prune_nonhealthy_older_than_days":30}'

curl -fsS -X POST "${CTO_BASE_URL%/}/api/cto/compact" \
  -H "Authorization: Bearer ${CTO_INGEST_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}"

echo "cto digest compaction ok @ $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
