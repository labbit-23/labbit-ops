-- Backfill cto_service_daily_digest from existing cto_service_logs
-- Safe to run multiple times (uses upsert).
-- Uses UTC day buckets to align with API compaction/trends logic.

with base as (
  select
    (checked_at at time zone 'UTC')::date as day_date,
    lab_id,
    service_key,
    category,
    label,
    source,
    checked_at,
    status,
    latency_ms
  from public.cto_service_logs
),
sequenced as (
  select
    b.*,
    lag(status) over (
      partition by day_date, lab_id, service_key
      order by checked_at
    ) as prev_status,
    row_number() over (
      partition by day_date, lab_id, service_key
      order by checked_at desc
    ) as rn_desc
  from base b
),
aggregated as (
  select
    day_date,
    lab_id,
    service_key,
    min(category) as category,
    min(label) as label,
    min(source) as source,
    count(*)::int as total_checks,
    count(*) filter (where status = 'healthy')::int as healthy_count,
    count(*) filter (where status = 'degraded')::int as degraded_count,
    count(*) filter (where status = 'down')::int as down_count,
    count(*) filter (where status not in ('healthy','degraded','down'))::int as unknown_count,
    round(avg(latency_ms)::numeric, 2) as avg_latency_ms,
    count(latency_ms)::int as latency_sample_count,
    percentile_disc(0.95) within group (order by latency_ms)
      filter (where latency_ms is not null) as p95_latency_ms,
    max(latency_ms) as max_latency_ms,
    min(checked_at) as first_checked_at,
    max(checked_at) as last_checked_at,
    sum(
      case
        when prev_status is not null and status is distinct from prev_status then 1
        else 0
      end
    )::int as status_transitions
  from sequenced
  group by day_date, lab_id, service_key
),
last_status as (
  select
    day_date,
    lab_id,
    service_key,
    status as last_status
  from sequenced
  where rn_desc = 1
)
insert into public.cto_service_daily_digest (
  day_date,
  lab_id,
  service_key,
  category,
  label,
  source,
  total_checks,
  healthy_count,
  degraded_count,
  down_count,
  unknown_count,
  avg_latency_ms,
  latency_sample_count,
  p95_latency_ms,
  max_latency_ms,
  first_checked_at,
  last_checked_at,
  status_transitions,
  last_status,
  updated_at
)
select
  a.day_date,
  a.lab_id,
  a.service_key,
  a.category,
  a.label,
  a.source,
  a.total_checks,
  a.healthy_count,
  a.degraded_count,
  a.down_count,
  a.unknown_count,
  a.avg_latency_ms,
  a.latency_sample_count,
  a.p95_latency_ms,
  a.max_latency_ms,
  a.first_checked_at,
  a.last_checked_at,
  a.status_transitions,
  coalesce(ls.last_status, 'unknown') as last_status,
  now()
from aggregated a
left join last_status ls
  on ls.day_date = a.day_date
 and ls.lab_id = a.lab_id
 and ls.service_key = a.service_key
on conflict (day_date, lab_id, service_key) do update
set
  category = excluded.category,
  label = excluded.label,
  source = excluded.source,
  total_checks = excluded.total_checks,
  healthy_count = excluded.healthy_count,
  degraded_count = excluded.degraded_count,
  down_count = excluded.down_count,
  unknown_count = excluded.unknown_count,
  avg_latency_ms = excluded.avg_latency_ms,
  latency_sample_count = excluded.latency_sample_count,
  p95_latency_ms = excluded.p95_latency_ms,
  max_latency_ms = excluded.max_latency_ms,
  first_checked_at = excluded.first_checked_at,
  last_checked_at = excluded.last_checked_at,
  status_transitions = excluded.status_transitions,
  last_status = excluded.last_status,
  updated_at = now();

