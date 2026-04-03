#!/usr/bin/env bash
set -euo pipefail

# Daily ops cleanup job (safe by default: dry-run).
# - File cleanup for frontend report artifacts
# - Optional DB cleanup for explicitly configured staging tables
#
# Required for DB cleanup:
#   CTO_DB_DSN=postgresql://user:pass@host:5432/dbname
#   CLEANUP_DB_TABLES=public.some_staging_table,public.another_staging_table
#
# Optional:
#   CLEANUP_ENV_FILE=/opt/labbit-ops/cto-collector/.env
#   CLEANUP_DRY_RUN=1
#   CLEANUP_PY_REPORTS_DIR=/opt/labbit-py/reports
#   CLEANUP_PY_REPORTS_RETENTION_DAYS=7
#   CLEANUP_PY_COMBINED_CACHE_DIR=/opt/labbit-py/reports/_combined_cache
#   CLEANUP_PY_COMBINED_CACHE_RETENTION_DAYS=1
#   CLEANUP_DB_RETENTION_DAYS=3

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${CLEANUP_ENV_FILE:-${ROOT_DIR}/cto-collector/.env}"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ENV_FILE}"
  set +a
fi

DRY_RUN="${CLEANUP_DRY_RUN:-1}"
PY_REPORTS_DIR="${CLEANUP_PY_REPORTS_DIR:-/opt/labbit-py/reports}"
PY_REPORTS_RETENTION_DAYS="${CLEANUP_PY_REPORTS_RETENTION_DAYS:-7}"
PY_COMBINED_CACHE_DIR="${CLEANUP_PY_COMBINED_CACHE_DIR:-/opt/labbit-py/reports/_combined_cache}"
PY_COMBINED_CACHE_RETENTION_DAYS="${CLEANUP_PY_COMBINED_CACHE_RETENTION_DAYS:-1}"
DB_RETENTION_DAYS="${CLEANUP_DB_RETENTION_DAYS:-3}"
DB_TABLES_RAW="${CLEANUP_DB_TABLES:-}"
DB_DSN="${CTO_DB_DSN:-}"

log() {
  echo "[ops-cleanup] $*"
}

is_int() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

split_schema_table() {
  local input="$1"
  if [[ "${input}" == *.* ]]; then
    echo "${input%%.*} ${input#*.}"
  else
    echo "public ${input}"
  fi
}

cleanup_path() {
  local path="$1"
  local retention_days="$2"
  local label="$3"

  if ! is_int "${retention_days}"; then
    log "invalid retention for ${label}: ${retention_days}; skipping"
    return
  fi

  if [[ ! -d "${path}" ]]; then
    log "directory missing for ${label}: ${path} (skip)"
    return
  fi

  mapfile -t targets < <(find "${path}" -type f -mtime +"${retention_days}" 2>/dev/null || true)
  local count="${#targets[@]}"
  log "${label} cleanup candidates=${count} path=${path} older_than_days=${retention_days} dry_run=${DRY_RUN}"

  if [[ "${count}" -eq 0 ]]; then
    return
  fi

  for file in "${targets[@]}"; do
    log "${label}_file_candidate ${file}"
  done

  if [[ "${DRY_RUN}" != "1" ]]; then
    find "${path}" -type f -mtime +"${retention_days}" -print -delete
    log "${label} cleanup delete completed"
  fi
}

resolve_timestamp_column() {
  local schema="$1"
  local table="$2"
  local col=""
  for candidate in created_at updated_at inserted_at last_seen_at first_seen_at; do
    col="$(psql "${DB_DSN}" -Atqc \
      "select column_name
         from information_schema.columns
        where table_schema='${schema}'
          and table_name='${table}'
          and column_name='${candidate}'
        limit 1;" 2>/dev/null || true)"
    if [[ -n "${col}" ]]; then
      echo "${col}"
      return
    fi
  done
  echo ""
}

cleanup_db_tables() {
  if [[ -z "${DB_TABLES_RAW// }" ]]; then
    log "CLEANUP_DB_TABLES not set; skipping DB cleanup"
    return
  fi

  if [[ -z "${DB_DSN}" ]]; then
    log "CTO_DB_DSN not set; skipping DB cleanup"
    return
  fi

  if ! is_int "${DB_RETENTION_DAYS}"; then
    log "invalid CLEANUP_DB_RETENTION_DAYS=${DB_RETENTION_DAYS}; skipping DB cleanup"
    return
  fi

  IFS=',' read -r -a tables <<< "${DB_TABLES_RAW}"
  for raw in "${tables[@]}"; do
    local table_ref
    table_ref="$(echo "${raw}" | xargs)"
    [[ -z "${table_ref}" ]] && continue

    if [[ ! "${table_ref}" =~ ^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$ ]]; then
      log "skip unsafe table reference: ${table_ref}"
      continue
    fi

    local schema table
    read -r schema table < <(split_schema_table "${table_ref}")
    local ts_col
    ts_col="$(resolve_timestamp_column "${schema}" "${table}")"
    if [[ -z "${ts_col}" ]]; then
      log "skip ${schema}.${table}: no timestamp column found (created_at/updated_at/inserted_at/last_seen_at/first_seen_at)"
      continue
    fi

    local count_sql
    count_sql="select count(*) from ${schema}.${table} where ${ts_col} < now() - interval '${DB_RETENTION_DAYS} days';"
    local candidate_count
    candidate_count="$(psql "${DB_DSN}" -Atqc "${count_sql}" 2>/dev/null || echo "0")"
    log "db_cleanup_candidates table=${schema}.${table} ts_col=${ts_col} retention_days=${DB_RETENTION_DAYS} count=${candidate_count} dry_run=${DRY_RUN}"

    if [[ "${DRY_RUN}" != "1" ]]; then
      local delete_sql
      delete_sql="delete from ${schema}.${table} where ${ts_col} < now() - interval '${DB_RETENTION_DAYS} days';"
      psql "${DB_DSN}" -v ON_ERROR_STOP=1 -c "${delete_sql}"
      log "db_cleanup_delete_completed table=${schema}.${table}"
    fi
  done
}

main() {
  log "starting dry_run=${DRY_RUN}"
  cleanup_path "${PY_REPORTS_DIR}" "${PY_REPORTS_RETENTION_DAYS}" "py_reports"
  cleanup_path "${PY_COMBINED_CACHE_DIR}" "${PY_COMBINED_CACHE_RETENTION_DAYS}" "py_combined_cache"
  cleanup_db_tables
  log "completed"
}

main "$@"
