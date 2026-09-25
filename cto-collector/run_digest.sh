#!/usr/bin/env bash
set -euo pipefail

# Nightly CTO log compaction, directly against Postgres (digest.py). The old
# /api/cto/compact path was capped at 1000 rows by PostgREST (PGRST_DB_MAX_ROWS),
# so each daily digest covered only the first ~22 minutes of the day.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${ROOT}/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ROOT}/.env"
  set +a
fi

if [[ -z "${CTO_DB_DSN:-}" ]]; then
  echo "Missing CTO_DB_DSN" >&2
  exit 1
fi

"${DIGEST_PYTHON:-/opt/labbit-ops/.venv/bin/python}" "${ROOT}/digest.py" "$@"

echo "cto digest compaction ok @ $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
