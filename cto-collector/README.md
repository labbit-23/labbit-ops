# CTO Collector (VPS Ops)

This folder is intentionally separate from `labbit-py` runtime code.
It is an ops-side collector that posts:

- service snapshots to `POST /api/cto/ingest`
- curated incidents to `POST /api/cto/events`

It is designed to be lightweight and safe on production VPS.

## Files

- `collector.py`: main collector loop (PM2 + VPS host + Docker signals)
- `run_digest.sh`: daily digest trigger for `/api/cto/compact`
- `../scripts/run-ops-cleanup.sh`: daily maintenance cleanup (filesystem + optional DB staging tables)

## Required env vars

```bash
CTO_BASE_URL=https://lab.sdrc.in
CTO_INGEST_TOKEN=...
CTO_LAB_ID=b539c161-1e2b-480b-9526-d4b37bd37b1e
CTO_SOURCE=vps-hel1-1
```

## Optional env vars

```bash
CTO_INTERVAL_SECONDS=60
CTO_STATE_FILE=/var/tmp/labbit-cto-collector-state.json
CTO_PM2_BIN=pm2
CTO_DOCKER_BIN=docker
CTO_ENABLE_DOCKER=1
CTO_PM2_RESTART_STORM_DELTA=3
CTO_EVENT_COOLDOWN_SECONDS=600
CTO_HOST_MEM_WARN_PCT=80
CTO_HOST_MEM_CRITICAL_PCT=92
CTO_HOST_DISK_WARN_PCT=85
CTO_HOST_DISK_CRITICAL_PCT=95
CTO_HOST_SWAP_WARN_PCT=35
CTO_SCHEDULED_PM2_NAMES=labbit-cto-digest
CTO_SCHEDULED_JOB_WARN_AGE_SECONDS=108000
CTO_SCHEDULED_JOB_MAX_AGE_SECONDS=129600

# Cleanup job (script-level)
CLEANUP_DRY_RUN=1
CLEANUP_PY_REPORTS_DIR=/opt/labbit-py/reports
CLEANUP_PY_REPORTS_RETENTION_DAYS=7
CLEANUP_PY_COMBINED_CACHE_DIR=/opt/labbit-py/reports/_combined_cache
CLEANUP_PY_COMBINED_CACHE_RETENTION_DAYS=1
CLEANUP_DB_RETENTION_DAYS=3
# Explicit allowlist only, comma-separated
CLEANUP_DB_TABLES=public.some_staging_table
# Required only if CLEANUP_DB_TABLES is set
CTO_DB_DSN=postgresql://user:pass@host:5432/dbname
```


## Standard deploy (from labbit-ops repo)

```bash
cd /opt/labbit-ops
bash scripts/deploy-vps-ops.sh
```

## Run once (manual)

```bash
python3 /opt/labbit-ops/cto-collector/collector.py --once
```

## Run continuously (PM2)

```bash
cd /opt/labbit-ops
CTO_BASE_URL=https://lab.sdrc.in \
CTO_INGEST_TOKEN=... \
CTO_LAB_ID=... \
CTO_SOURCE=vps-hel1-1 \
pm2 start "python3 cto-collector/collector.py" --name labbit-cto-collector
pm2 save
```

## Daily digest compaction (PM2 cron)

```bash
cd /opt/labbit-ops
CTO_BASE_URL=https://lab.sdrc.in \
CTO_INGEST_TOKEN=... \
pm2 start "bash cto-collector/run_digest.sh" \
  --name labbit-cto-digest \
  --cron "20 1 * * *" \
  --no-autorestart
pm2 save
```

## Daily cleanup (PM2 cron, dry-run first)

```bash
cd /opt/labbit-ops
CLEANUP_DRY_RUN=1 \
pm2 start "bash scripts/run-ops-cleanup.sh" \
  --name labbit-ops-cleanup \
  --cron "35 1 * * *" \
  --no-autorestart
pm2 save
```

## Backfill digest once

Run SQL file:

- `../labbit-ops/sql/cto-digest-backfill.sql`
