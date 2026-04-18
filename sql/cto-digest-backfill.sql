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
    latency_ms,
    payload
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
    )::int as status_transitions,
    count(*) filter (
      where
        (payload ? 'memory_pct' or payload ? 'mem_pct' or payload ? 'memory_percent' or payload ? 'ram_used_pct')
        or (payload ? 'disk_pct' or payload ? 'disk_used_pct' or payload ? 'disk_percent' or payload ? 'root_disk_pct')
        or (payload ? 'swap_pct' or payload ? 'swap_used_pct' or payload ? 'swap_percent')
        or (payload ? 'load_1' or payload ? 'load1' or payload ? 'loadavg_1')
        or (payload ? 'load_1_per_core_pct' or payload ? 'load_per_core_pct')
    )::int as host_metric_samples,
    round(avg(
      coalesce(
        nullif(payload->>'memory_pct','')::numeric,
        nullif(payload->>'mem_pct','')::numeric,
        nullif(payload->>'memory_percent','')::numeric,
        nullif(payload->>'ram_used_pct','')::numeric
      )
    )::numeric, 2) as host_memory_avg_pct,
    max(
      coalesce(
        nullif(payload->>'memory_pct','')::numeric,
        nullif(payload->>'mem_pct','')::numeric,
        nullif(payload->>'memory_percent','')::numeric,
        nullif(payload->>'ram_used_pct','')::numeric
      )
    ) as host_memory_max_pct,
    round(avg(
      coalesce(
        nullif(payload->>'disk_pct','')::numeric,
        nullif(payload->>'disk_used_pct','')::numeric,
        nullif(payload->>'disk_percent','')::numeric,
        nullif(payload->>'root_disk_pct','')::numeric
      )
    )::numeric, 2) as host_disk_avg_pct,
    max(
      coalesce(
        nullif(payload->>'disk_pct','')::numeric,
        nullif(payload->>'disk_used_pct','')::numeric,
        nullif(payload->>'disk_percent','')::numeric,
        nullif(payload->>'root_disk_pct','')::numeric
      )
    ) as host_disk_max_pct,
    round(avg(
      coalesce(
        nullif(payload->>'swap_pct','')::numeric,
        nullif(payload->>'swap_used_pct','')::numeric,
        nullif(payload->>'swap_percent','')::numeric
      )
    )::numeric, 2) as host_swap_avg_pct,
    max(
      coalesce(
        nullif(payload->>'swap_pct','')::numeric,
        nullif(payload->>'swap_used_pct','')::numeric,
        nullif(payload->>'swap_percent','')::numeric
      )
    ) as host_swap_max_pct,
    round(avg(
      coalesce(
        nullif(payload->>'load_1','')::numeric,
        nullif(payload->>'load1','')::numeric,
        nullif(payload->>'loadavg_1','')::numeric
      )
    )::numeric, 2) as host_load1_avg,
    max(
      coalesce(
        nullif(payload->>'load_1','')::numeric,
        nullif(payload->>'load1','')::numeric,
        nullif(payload->>'loadavg_1','')::numeric
      )
    ) as host_load1_max,
    round(avg(
      coalesce(
        nullif(payload->>'load_1_per_core_pct','')::numeric,
        nullif(payload->>'load_per_core_pct','')::numeric
      )
    )::numeric, 2) as host_load_per_core_avg_pct,
    max(
      coalesce(
        nullif(payload->>'load_1_per_core_pct','')::numeric,
        nullif(payload->>'load_per_core_pct','')::numeric
      )
    ) as host_load_per_core_max_pct
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
  host_metric_samples,
  host_memory_avg_pct,
  host_memory_max_pct,
  host_disk_avg_pct,
  host_disk_max_pct,
  host_swap_avg_pct,
  host_swap_max_pct,
  host_load1_avg,
  host_load1_max,
  host_load_per_core_avg_pct,
  host_load_per_core_max_pct,
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
  a.host_metric_samples,
  a.host_memory_avg_pct,
  a.host_memory_max_pct,
  a.host_disk_avg_pct,
  a.host_disk_max_pct,
  a.host_swap_avg_pct,
  a.host_swap_max_pct,
  a.host_load1_avg,
  a.host_load1_max,
  a.host_load_per_core_avg_pct,
  a.host_load_per_core_max_pct,
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
  host_metric_samples = excluded.host_metric_samples,
  host_memory_avg_pct = excluded.host_memory_avg_pct,
  host_memory_max_pct = excluded.host_memory_max_pct,
  host_disk_avg_pct = excluded.host_disk_avg_pct,
  host_disk_max_pct = excluded.host_disk_max_pct,
  host_swap_avg_pct = excluded.host_swap_avg_pct,
  host_swap_max_pct = excluded.host_swap_max_pct,
  host_load1_avg = excluded.host_load1_avg,
  host_load1_max = excluded.host_load1_max,
  host_load_per_core_avg_pct = excluded.host_load_per_core_avg_pct,
  host_load_per_core_max_pct = excluded.host_load_per_core_max_pct,
  last_status = excluded.last_status,
  updated_at = now();
