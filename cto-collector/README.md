# CTO Collector (VPS Ops)

This folder is intentionally separate from `labbit-py` runtime code.
It is an ops-side collector that posts:

- service snapshots to `POST /api/cto/ingest`
- curated incidents to `POST /api/cto/events`

It is designed to be lightweight and safe on production VPS.

## Files

- `collector.py`: main collector loop (PM2 + Docker signals)
- `run_digest.sh`: daily digest trigger for `/api/cto/compact`

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

## Backfill digest once

Run SQL file:

- `../labbit-ops/sql/cto-digest-backfill.sql`

