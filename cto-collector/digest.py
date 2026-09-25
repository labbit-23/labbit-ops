#!/usr/bin/env python3
"""Nightly CTO log compaction, run directly against Postgres.

Replaces the /api/cto/compact call, which read through PostgREST and was silently
capped at PGRST_DB_MAX_ROWS=1000 rows, so every daily digest covered only the
first ~22 minutes of the day. Aggregation happens in SQL over the full UTC day.

Retention (unchanged): healthy rows > 7 days, non-healthy rows > 30 days.
Usage: digest.py [--day YYYY-MM-DD] [--dry-run] [--no-prune]
Needs CTO_DB_DSN in the environment.
"""
import argparse
import datetime as dt
import json
import os
import sys

import psycopg

AGG = """
WITH src AS (
  SELECT lab_id, service_key, category, label, source, checked_at, latency_ms,
         CASE WHEN status IN ('healthy','degraded','down','unknown') THEN status ELSE 'unknown' END AS st
  FROM public.cto_service_logs
  WHERE checked_at >= %(day)s::date AND checked_at < %(day)s::date + 1
), w AS (
  SELECT *, lag(st) OVER (PARTITION BY lab_id, service_key ORDER BY checked_at) AS prev_st FROM src
), agg AS (
  SELECT %(day)s::date AS day_date, lab_id, service_key,
    (array_agg(NULLIF(category,'') ORDER BY checked_at))[1] AS category,
    (array_agg(NULLIF(label,'')    ORDER BY checked_at))[1] AS label,
    (array_agg(NULLIF(source,'')   ORDER BY checked_at))[1] AS source,
    count(*)::int AS total_checks,
    (count(*) FILTER (WHERE st='healthy'))::int  AS healthy_count,
    (count(*) FILTER (WHERE st='degraded'))::int AS degraded_count,
    (count(*) FILTER (WHERE st='down'))::int     AS down_count,
    (count(*) FILTER (WHERE st='unknown'))::int  AS unknown_count,
    round(avg(latency_ms)::numeric, 2) AS avg_latency_ms,
    count(latency_ms)::int AS latency_sample_count,
    percentile_disc(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms,
    max(latency_ms) AS max_latency_ms,
    min(checked_at) AS first_checked_at, max(checked_at) AS last_checked_at,
    (count(*) FILTER (WHERE prev_st IS NOT NULL AND prev_st <> st))::int AS status_transitions,
    (array_agg(st ORDER BY checked_at DESC))[1] AS last_status
  FROM w GROUP BY lab_id, service_key
)
"""

UPSERT = AGG + """
INSERT INTO public.cto_service_daily_digest
  (day_date, lab_id, service_key, category, label, source, total_checks, healthy_count,
   degraded_count, down_count, unknown_count, avg_latency_ms, latency_sample_count,
   p95_latency_ms, max_latency_ms, first_checked_at, last_checked_at, status_transitions, last_status)
SELECT day_date, lab_id, service_key, category, label, source, total_checks, healthy_count,
   degraded_count, down_count, unknown_count, avg_latency_ms, latency_sample_count,
   p95_latency_ms, max_latency_ms, first_checked_at, last_checked_at, status_transitions, last_status
FROM agg
ON CONFLICT (day_date, lab_id, service_key) DO UPDATE SET
  category=EXCLUDED.category, label=EXCLUDED.label, source=EXCLUDED.source,
  total_checks=EXCLUDED.total_checks, healthy_count=EXCLUDED.healthy_count,
  degraded_count=EXCLUDED.degraded_count, down_count=EXCLUDED.down_count,
  unknown_count=EXCLUDED.unknown_count, avg_latency_ms=EXCLUDED.avg_latency_ms,
  latency_sample_count=EXCLUDED.latency_sample_count, p95_latency_ms=EXCLUDED.p95_latency_ms,
  max_latency_ms=EXCLUDED.max_latency_ms, first_checked_at=EXCLUDED.first_checked_at,
  last_checked_at=EXCLUDED.last_checked_at, status_transitions=EXCLUDED.status_transitions,
  last_status=EXCLUDED.last_status
"""

DRY = AGG + "SELECT count(*), coalesce(sum(total_checks), 0) FROM agg"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", help="UTC day to compact (default: yesterday)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-prune", action="store_true")
    ap.add_argument("--healthy-days", type=int, default=7)
    ap.add_argument("--nonhealthy-days", type=int, default=30)
    a = ap.parse_args()

    today = dt.datetime.now(dt.timezone.utc).date()
    day = dt.date.fromisoformat(a.day) if a.day else today - dt.timedelta(days=1)
    healthy_before = today - dt.timedelta(days=a.healthy_days)
    nonhealthy_before = today - dt.timedelta(days=a.nonhealthy_days)

    with psycopg.connect(os.environ["CTO_DB_DSN"]) as conn:
        cur = conn.cursor()
        out = {"message": "CTO logs compacted", "day": day.isoformat(), "dry_run": a.dry_run}
        if a.dry_run:
            cur.execute(DRY, {"day": day})
            out["digest_rows"], out["source_rows"] = cur.fetchone()
        else:
            cur.execute(UPSERT, {"day": day})
            out["digest_rows"] = cur.rowcount
            cur.execute("SELECT coalesce(sum(total_checks),0) FROM public.cto_service_daily_digest WHERE day_date = %s", (day,))
            out["source_rows"] = cur.fetchone()[0]
        if a.no_prune:
            out["pruned_rows"] = None
        else:
            healthy_sql = "FROM public.cto_service_logs WHERE status = 'healthy' AND checked_at < %s::date"
            other_sql = "FROM public.cto_service_logs WHERE status IN ('degraded','down','unknown') AND checked_at < %s::date"
            pruned = 0
            for sql, before in ((healthy_sql, healthy_before), (other_sql, nonhealthy_before)):
                if a.dry_run:
                    cur.execute("SELECT count(*) " + sql, (before,))
                    pruned += cur.fetchone()[0]
                else:
                    cur.execute("DELETE " + sql, (before,))
                    pruned += cur.rowcount
            out["pruned_rows"] = pruned
        if a.dry_run:
            conn.rollback()
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"digest failed: {exc}", file=sys.stderr)
        sys.exit(1)
