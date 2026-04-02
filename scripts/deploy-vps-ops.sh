#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash scripts/deploy-vps-ops.sh
#
# Optional env overrides:
#   OPS_DIR=/opt/labbit-ops
#   BRANCH=main
#   COLLECTOR_ENV_FILE=/opt/labbit-ops/cto-collector/.env
#   PM2_COLLECTOR_NAME=labbit-cto-collector
#   PM2_DIGEST_NAME=labbit-cto-digest
#   DIGEST_CRON="20 1 * * *"
#   START_PM2=1

OPS_DIR="${OPS_DIR:-/opt/labbit-ops}"
BRANCH="${BRANCH:-main}"
COLLECTOR_ENV_FILE="${COLLECTOR_ENV_FILE:-${OPS_DIR}/cto-collector/.env}"
PM2_COLLECTOR_NAME="${PM2_COLLECTOR_NAME:-labbit-cto-collector}"
PM2_DIGEST_NAME="${PM2_DIGEST_NAME:-labbit-cto-digest}"
DIGEST_CRON="${DIGEST_CRON:-20 1 * * *}"
START_PM2="${START_PM2:-1}"

echo "==> Deploying labbit-ops from ${OPS_DIR} (${BRANCH})"
cd "${OPS_DIR}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "❌ ${OPS_DIR} is not a git repository."
  exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
  echo "❌ Working tree is dirty in ${OPS_DIR}. Commit/stash/revert changes before deploy."
  git status --short
  exit 1
fi

echo "==> Pull latest"
git fetch origin "${BRANCH}"
git checkout "${BRANCH}"
git pull --ff-only origin "${BRANCH}"

if [ ! -f "${COLLECTOR_ENV_FILE}" ]; then
  echo "❌ Missing env file: ${COLLECTOR_ENV_FILE}"
  cat <<'ENV_SAMPLE'
Create it with at least:
  CTO_BASE_URL=https://lab.sdrc.in
  CTO_INGEST_TOKEN=...
  CTO_LAB_ID=...
  CTO_SOURCE=vps-hel1-1
ENV_SAMPLE
  exit 1
fi

echo "==> One-shot collector validation"
set -a
source "${COLLECTOR_ENV_FILE}"
set +a
python3 cto-collector/collector.py --once

if [ "${START_PM2}" != "1" ]; then
  echo "==> START_PM2=${START_PM2}; skipping PM2 process setup."
  exit 0
fi

echo "==> Start/restart PM2 collector (${PM2_COLLECTOR_NAME})"
pm2 delete "${PM2_COLLECTOR_NAME}" >/dev/null 2>&1 || true
pm2 start "python3 ${OPS_DIR}/cto-collector/collector.py" --name "${PM2_COLLECTOR_NAME}" --update-env

echo "==> Start/restart PM2 daily digest (${PM2_DIGEST_NAME})"
pm2 delete "${PM2_DIGEST_NAME}" >/dev/null 2>&1 || true
pm2 start "bash ${OPS_DIR}/cto-collector/run_digest.sh" \
  --name "${PM2_DIGEST_NAME}" \
  --cron "${DIGEST_CRON}" \
  --no-autorestart \
  --update-env

pm2 save
pm2 status

echo "✅ Ops deploy complete."
echo "Tip: pm2 logs ${PM2_COLLECTOR_NAME} --lines 120"
