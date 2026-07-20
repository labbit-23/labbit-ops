#!/usr/bin/env python3
"""
Lightweight VPS collector for CTO metrics/events.
Posts to:
  - /api/cto/ingest (snapshot metrics)
  - /api/cto/events (curated incidents)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


RESTART_WINDOW_SECONDS = 24 * 60 * 60
MIN_ACTIONABLE_ERRORS_FOR_EVENT = 10  # Only log worker log error event when >5 actionable errors (avoid noise from 1-2 transient errors)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def getenv_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def run_cmd(args: List[str], timeout: int = 6) -> Tuple[int, str, str]:
    try:
        proc = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
            text=True,
        )
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
    except Exception as exc:
        return 1, "", str(exc)


def parse_csv_lower(value: str) -> List[str]:
    return [item.strip().lower() for item in str(value or "").split(",") if item.strip()]


def file_age_seconds(path: str) -> int | None:
    try:
        stat = os.stat(path)
        return max(0, int(time.time() - int(stat.st_mtime)))
    except Exception:
        return None


def normalize_key(raw: str) -> str:
    key = re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")
    return key or "unknown"


def with_node_suffix(base_key: str, node_suffix: str) -> str:
    return f"{base_key}__{normalize_key(node_suffix or 'vps')}"


def post_json(url: str, token: str, payload: Dict[str, Any], timeout: int = 8) -> Tuple[bool, str]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            if 200 <= status < 300:
                return True, f"{status}"
            return False, f"HTTP {status}"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}: {exc.reason}"
    except Exception as exc:
        return False, str(exc)


def initial_state() -> Dict[str, Any]:
    return {
        "pm2_restarts": {},
        "pm2_restart_samples": {},
        "event_last_sent_at": {},
    }


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return initial_state()
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        base = initial_state()
        base.update(loaded if isinstance(loaded, dict) else {})
        return base
    except Exception:
        return initial_state()


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")


def compute_restarts_24h(
    state: Dict[str, Any],
    restart_map: Dict[str, int],
    now_ts: int,
) -> Dict[str, int]:
    history = state.setdefault("pm2_restart_samples", {})
    cutoff = now_ts - RESTART_WINDOW_SECONDS
    restarts_24h: Dict[str, int] = {}

    for service_key, current in restart_map.items():
        samples = history.get(service_key, [])
        if not isinstance(samples, list):
            samples = []

        samples.append({"ts": int(now_ts), "value": int(current)})
        samples = [
            s
            for s in samples
            if isinstance(s, dict)
            and int(s.get("ts", 0)) >= cutoff
            and isinstance(s.get("value", 0), int)
        ]
        samples.sort(key=lambda item: int(item.get("ts", 0)))

        if samples:
            baseline = int(samples[0].get("value", current))
            delta = max(0, int(current) - baseline)
        else:
            delta = 0

        restarts_24h[service_key] = delta
        history[service_key] = samples[-2000:]

    for service_key in list(history.keys()):
        if service_key in restart_map:
            continue
        samples = history.get(service_key, [])
        if not isinstance(samples, list):
            del history[service_key]
            continue
        kept = [
            s for s in samples if isinstance(s, dict) and int(s.get("ts", 0)) >= cutoff
        ]
        if kept:
            history[service_key] = kept[-2000:]
        else:
            del history[service_key]

    return restarts_24h


def collect_pm2(
    pm2_bin: str, state: Dict[str, Any], config: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int], List[Dict[str, Any]]]:
    services: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    restart_map: Dict[str, int] = {}
    runtime_key = with_node_suffix("pm2_runtime", config["node_suffix"])

    code, out, err = run_cmd([pm2_bin, "jlist"], timeout=8)
    if code != 0:
        events.append(
            {
                "service_key": runtime_key,
                "event_type": "pm2_jlist_failed",
                "severity": "high",
                "message": f"pm2 jlist failed: {err[:180]}",
                "payload": {"stderr": err[:500]},
                "event_at": now_iso(),
            }
        )
        services.append(
            {
                "service_key": runtime_key,
                "category": "ops",
                "label": "PM2 Runtime",
                "status": "down",
                "latency_ms": None,
                "message": "Unable to read PM2 process list",
                "payload": {"error": err[:500]},
            }
        )
        return services, events, restart_map, []

    try:
        rows = json.loads(out or "[]")
    except Exception:
        rows = []

    row_infos: List[Dict[str, Any]] = []
    for row in rows:
        name = str(row.get("name") or "pm2_proc")
        key = with_node_suffix(normalize_key(name), config["node_suffix"])
        pm2_env = row.get("pm2_env") or {}
        monit = row.get("monit") or {}
        status = str(pm2_env.get("status") or "").lower()
        restarts = int(pm2_env.get("restart_time") or 0)
        restart_map[key] = restarts
        row_infos.append(
            {
                "name": name,
                "key": key,
                "status": status,
                "restarts": restarts,
                "cpu": monit.get("cpu"),
                "memory": monit.get("memory"),
                "out_log_path": str(pm2_env.get("pm_out_log_path") or ""),
                "cron_restart": str(pm2_env.get("cron_restart") or ""),
            }
        )

    restarts_24h_map = compute_restarts_24h(state, restart_map, int(time.time()))

    for info in row_infos:
        name = info["name"]
        key = info["key"]
        status = info["status"]
        restarts = int(info["restarts"])
        cpu = info["cpu"]
        mem = info["memory"]
        out_log_path = info.get("out_log_path") or ""
        cron_restart = info.get("cron_restart") or ""
        restarts_24h = int(restarts_24h_map.get(key, 0))
        scheduled_names = config.get("scheduled_pm2_names", [])
        is_scheduled_job = str(name or "").strip().lower() in scheduled_names

        if is_scheduled_job:
            log_age = file_age_seconds(out_log_path) if out_log_path else None
            max_ok = int(config.get("scheduled_job_max_age_seconds", 36 * 60 * 60))
            warn_age = int(config.get("scheduled_job_warn_age_seconds", 30 * 60 * 60))
            if status == "online":
                norm_status = "healthy"
            elif status == "stopped":
                if log_age is None:
                    norm_status = "degraded"
                elif log_age > max_ok:
                    norm_status = "down"
                elif log_age > warn_age:
                    norm_status = "degraded"
                else:
                    norm_status = "healthy"
            else:
                norm_status = "down"
        elif status == "online":
            norm_status = "healthy"
            if restarts_24h >= 3:
                norm_status = "down"
            elif restarts_24h >= 1:
                norm_status = "degraded"
        elif status in {"launching", "stopping", "errored"}:
            norm_status = "down"
        else:
            norm_status = "unknown"

        msg = f"pm2={status or 'unknown'} restarts_24h={restarts_24h} restarts_total={restarts}"
        if is_scheduled_job:
            log_age = file_age_seconds(out_log_path) if out_log_path else None
            if log_age is None:
                msg += " scheduled_job_log_age=unknown"
            else:
                msg += f" scheduled_job_log_age_min={log_age // 60}"
        if cpu is not None:
            msg += f" cpu={cpu}"
        if mem is not None:
            msg += f" mem={mem}"

        services.append(
            {
                "service_key": key,
                "category": "app",
                "label": name,
                "status": norm_status,
                "latency_ms": None,
                "message": msg[:240],
                "payload": {
                    "pm2_status": status,
                    "restarts": restarts,
                    "restarts_24h": restarts_24h,
                    "cpu": cpu,
                    "memory": mem,
                    "scheduled_job": is_scheduled_job,
                    "pm2_cron_restart": cron_restart or None,
                    "out_log_path": out_log_path or None,
                },
            }
        )

        if norm_status == "down":
            events.append(
                {
                    "service_key": key,
                    "event_type": "pm2_process_down" if status != "online" else "pm2_restart_storm_24h",
                    "severity": "high" if status != "online" else "critical",
                    "message": (
                        f"{name} is {status or 'down'}"
                        if status != "online"
                        else f"{name} restart volume high in 24h: {restarts_24h}"
                    ),
                    "payload": {
                        "pm2_status": status,
                        "restarts": restarts,
                        "restarts_24h": restarts_24h,
                    },
                    "event_at": now_iso(),
                }
            )

    return services, events, restart_map, rows


def read_meminfo() -> Dict[str, int]:
    data: Dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                parts = line.split(":", 1)
                if len(parts) != 2:
                    continue
                key = parts[0].strip()
                values = parts[1].strip().split()
                if not values:
                    continue
                data[key] = int(values[0])
    except Exception:
        return {}
    return data


def safe_read_float(path: str) -> float | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            parts = handle.read().strip().split()
            if not parts:
                return None
            value = float(parts[0])
            if value < 0:
                return None
            return value
    except Exception:
        return None


def collect_host_metrics(config: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    services: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    host_service_key = with_node_suffix("vps_host", config["node_suffix"])

    meminfo = read_meminfo()
    mem_total_kb = meminfo.get("MemTotal")
    mem_available_kb = meminfo.get("MemAvailable")
    mem_used_kb = None
    mem_pct = None
    if isinstance(mem_total_kb, int) and mem_total_kb > 0 and isinstance(mem_available_kb, int):
        mem_used_kb = max(0, mem_total_kb - mem_available_kb)
        mem_pct = (mem_used_kb / mem_total_kb) * 100.0

    swap_total_kb = meminfo.get("SwapTotal")
    swap_free_kb = meminfo.get("SwapFree")
    swap_used_kb = None
    swap_pct = None
    if isinstance(swap_total_kb, int) and swap_total_kb > 0 and isinstance(swap_free_kb, int):
        swap_used_kb = max(0, swap_total_kb - swap_free_kb)
        swap_pct = (swap_used_kb / swap_total_kb) * 100.0

    disk_total = None
    disk_used = None
    disk_pct = None
    try:
        vfs = os.statvfs("/")
        total = int(vfs.f_blocks) * int(vfs.f_frsize)
        free = int(vfs.f_bavail) * int(vfs.f_frsize)
        used = max(0, total - free)
        disk_total = total
        disk_used = used
        if total > 0:
            disk_pct = (used / total) * 100.0
    except Exception:
        pass

    load_1 = safe_read_float("/proc/loadavg")
    cpu_cores = os.cpu_count() or 1
    load_1_per_core_pct = None
    if isinstance(load_1, float) and cpu_cores > 0:
        load_1_per_core_pct = min(999.0, max(0.0, (load_1 / float(cpu_cores)) * 100.0))

    severity = "healthy"
    reasons: List[str] = []
    if isinstance(mem_pct, float) and mem_pct >= config["host_mem_critical_pct"]:
        severity = "down"
        reasons.append(f"memory high {mem_pct:.1f}%")
    elif isinstance(mem_pct, float) and mem_pct >= config["host_mem_warn_pct"]:
        severity = "degraded"
        reasons.append(f"memory elevated {mem_pct:.1f}%")

    if isinstance(disk_pct, float) and disk_pct >= config["host_disk_critical_pct"]:
        severity = "down"
        reasons.append(f"disk high {disk_pct:.1f}%")
    elif isinstance(disk_pct, float) and disk_pct >= config["host_disk_warn_pct"] and severity != "down":
        severity = "degraded"
        reasons.append(f"disk elevated {disk_pct:.1f}%")

    if isinstance(swap_pct, float) and swap_pct >= config["host_swap_warn_pct"] and severity != "down":
        severity = "degraded"
        reasons.append(f"swap high {swap_pct:.1f}%")

    if not reasons:
        reasons.append("host pressure nominal")

    message = " | ".join(reasons)

    payload = {
        "memory_pct": round(mem_pct, 2) if isinstance(mem_pct, float) else None,
        "memory_total_mb": round(mem_total_kb / 1024, 2) if isinstance(mem_total_kb, int) else None,
        "memory_used_mb": round(mem_used_kb / 1024, 2) if isinstance(mem_used_kb, int) else None,
        "swap_pct": round(swap_pct, 2) if isinstance(swap_pct, float) else None,
        "swap_total_mb": round(swap_total_kb / 1024, 2) if isinstance(swap_total_kb, int) else None,
        "swap_used_mb": round(swap_used_kb / 1024, 2) if isinstance(swap_used_kb, int) else None,
        "disk_pct": round(disk_pct, 2) if isinstance(disk_pct, float) else None,
        "disk_total_gb": round(disk_total / (1024 ** 3), 2) if isinstance(disk_total, int) else None,
        "disk_used_gb": round(disk_used / (1024 ** 3), 2) if isinstance(disk_used, int) else None,
        "load_1": round(load_1, 2) if isinstance(load_1, float) else None,
        "cpu_cores": int(cpu_cores),
        "load_1_per_core_pct": round(load_1_per_core_pct, 2) if isinstance(load_1_per_core_pct, float) else None,
    }

    services.append(
        {
            "service_key": host_service_key,
            "category": "ops",
            "label": "VPS Host",
            "status": severity,
            "latency_ms": None,
            "message": message[:240],
            "payload": payload,
        }
    )

    if severity in {"degraded", "down"}:
        events.append(
            {
                "service_key": host_service_key,
                "event_type": "vps_host_pressure",
                "severity": "high" if severity == "down" else "medium",
                "message": message[:240],
                "payload": payload,
                "event_at": now_iso(),
            }
        )

    return services, events


def collect_docker(
    docker_bin: str, enabled: bool, config: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not enabled:
        return [], []

    services: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    runtime_key = with_node_suffix("docker_runtime", config["node_suffix"])

    code, out, err = run_cmd([docker_bin, "ps", "--format", "{{.Names}}|{{.Status}}"], timeout=8)
    if code != 0:
        events.append(
            {
                "service_key": runtime_key,
                "event_type": "docker_ps_failed",
                "severity": "medium",
                "message": f"docker ps failed: {err[:180]}",
                "payload": {"stderr": err[:500]},
                "event_at": now_iso(),
            }
        )
        return services, events

    lines = [line for line in out.splitlines() if line.strip()]
    for line in lines:
        name, status_raw = (line.split("|", 1) + [""])[:2]
        key = with_node_suffix(f"docker_{normalize_key(name)}", config["node_suffix"])
        status_text = status_raw.lower()

        if "unhealthy" in status_text:
            norm = "down"
        elif "healthy" in status_text or "up " in status_text:
            norm = "healthy"
        else:
            norm = "degraded"

        services.append(
            {
                "service_key": key,
                "category": "database",
                "label": f"Docker {name}",
                "status": norm,
                "latency_ms": None,
                "message": status_raw[:240],
                "payload": {"container": name, "status": status_raw},
            }
        )

        if norm != "healthy":
            events.append(
                {
                    "service_key": key,
                    "event_type": "docker_container_unhealthy",
                    "severity": "high" if norm == "down" else "medium",
                    "message": f"Container {name}: {status_raw}",
                    "payload": {"container": name, "status": status_raw},
                    "event_at": now_iso(),
                }
            )

    return services, events


def read_recent_lines(path: str, max_lines: int = 160) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            lines = handle.readlines()
    except Exception:
        return []
    if max_lines <= 0:
        return lines
    return lines[-max_lines:]


def read_json_file(path: str) -> Dict[str, Any]:
    try:
        if not path:
            return {}
        with open(path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def extract_last_error_at(lines: List[str]) -> str | None:
    ts_re = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
    for raw in reversed(lines):
        line = str(raw or "")
        m = ts_re.search(line)
        if m:
            return m.group(1) + "Z"
    return None


def recent_error_signals(lines: List[str]) -> Dict[str, Any]:
    patterns = {
        "json_decode": re.compile(r"json\.decoder\.JSONDecodeError|JSONDecodeError", re.IGNORECASE),
        "http_error": re.compile(r"HTTPError|requests\.exceptions\.HTTPError|\b4\d\d Client Error\b|\b5\d\d Server Error\b", re.IGNORECASE),
        "traceback": re.compile(r"Traceback \(most recent call last\)", re.IGNORECASE),
        "exception": re.compile(r"\bException:\b|\berror\b", re.IGNORECASE),
    }
    counts = {key: 0 for key in patterns}
    sample = None
    actionable_sample = None
    actionable_total = 0
    for raw in lines:
        line = str(raw or "").strip()
        if not line:
            continue
        is_actionable_line = False
        for key, pat in patterns.items():
            if pat.search(line):
                counts[key] += 1
                if sample is None:
                    sample = line[:220]
                # Treat traceback header alone as non-actionable noise.
                if key != "traceback":
                    is_actionable_line = True
        if is_actionable_line:
            actionable_total += 1
            if actionable_sample is None:
                actionable_sample = line[:220]
    total = sum(counts.values())
    return {
        "total": total,
        "counts": counts,
        "sample": sample,
        "actionable_total": actionable_total,
        "actionable_sample": actionable_sample,
    }


def parse_worker_log_timestamp(line: str) -> datetime | None:
    match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", str(line or ""))
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def latest_worker_log_activity(lines: List[str]) -> Tuple[datetime | None, str | None]:
    for raw in reversed(lines):
        line = str(raw or "").strip()
        if not line:
            continue
        parsed = parse_worker_log_timestamp(line)
        if parsed:
            return parsed, line[:220]
    return None, None


def latest_logged_sleep_seconds(lines: List[str]) -> int | None:
    pattern = re.compile(r"Sleeping\s+(\d+)\s+seconds\s+before\s+next\s+enqueue\s+cycle", re.IGNORECASE)
    for raw in reversed(lines):
        line = str(raw or "")
        match = pattern.search(line)
        if not match:
            continue
        try:
            return int(match.group(1))
        except Exception:
            return None
    return None


def enqueue_expected_sleep_seconds(report_sender_cfg: Dict[str, Any]) -> int | None:
    enqueue_cfg = report_sender_cfg.get("enqueue") if isinstance(report_sender_cfg, dict) else {}
    if not isinstance(enqueue_cfg, dict):
        return None

    now = datetime.now()
    hhmm = now.hour * 100 + now.minute
    boundary = int(enqueue_cfg.get("poll_fast_after_hhmm") or enqueue_cfg.get("poll_post_boundary_hhmm") or 1000)
    key = "poll_seconds_pre_10am" if hhmm < boundary else "poll_seconds_post_10am"
    try:
        return int(enqueue_cfg.get(key))
    except Exception:
        return None


def sender_expected_sleep_seconds(report_sender_cfg: Dict[str, Any]) -> int | None:
    worker_cfg = report_sender_cfg.get("worker") if isinstance(report_sender_cfg, dict) else {}
    if not isinstance(worker_cfg, dict):
        return None
    try:
        return int(worker_cfg.get("poll_seconds"))
    except Exception:
        return None


def expected_worker_log_age_seconds(name_l: str, lines: List[str], report_sender_cfg: Dict[str, Any], grace_seconds: int) -> Tuple[int | None, Dict[str, Any]]:
    logged_sleep = latest_logged_sleep_seconds(lines) if name_l == "report-enqueue-watch" else None
    configured_sleep = (
        enqueue_expected_sleep_seconds(report_sender_cfg)
        if name_l == "report-enqueue-watch"
        else sender_expected_sleep_seconds(report_sender_cfg)
    )
    sleep_seconds = logged_sleep if isinstance(logged_sleep, int) and logged_sleep > 0 else configured_sleep
    if not isinstance(sleep_seconds, int) or sleep_seconds <= 0:
        return None, {
            "logged_sleep_seconds": logged_sleep,
            "configured_sleep_seconds": configured_sleep,
        }

    allowed = int(sleep_seconds + max(60, grace_seconds))
    return allowed, {
        "logged_sleep_seconds": logged_sleep,
        "configured_sleep_seconds": configured_sleep,
        "allowed_log_age_seconds": allowed,
        "stale_grace_seconds": max(60, grace_seconds),
    }


def collect_dispatch_worker_log_signals(config: Dict[str, Any], pm2_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    services: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    tracked = {"report-sender", "report-enqueue-watch"}
    report_sender_cfg = read_json_file(str(config.get("report_sender_config_path") or ""))
    stale_grace_seconds = int(config.get("worker_log_stale_grace_seconds", 300))

    for row in pm2_rows:
        name = str(row.get("name") or "").strip()
        name_l = name.lower()
        if name_l not in tracked:
            continue

        key = with_node_suffix(normalize_key(name), config["node_suffix"])
        pm2_env = row.get("pm2_env") or {}
        err_path = str(pm2_env.get("pm_err_log_path") or "")
        out_path = str(pm2_env.get("pm_out_log_path") or "")

        err_lines = read_recent_lines(err_path, 220) if err_path else []
        out_lines = read_recent_lines(out_path, 120) if out_path else []
        all_lines = err_lines + out_lines
        err_signal = recent_error_signals(err_lines)
        last_error_at = extract_last_error_at(err_lines)
        last_activity_at, last_activity_line = latest_worker_log_activity(all_lines)
        max_log_age_seconds, cadence_payload = expected_worker_log_age_seconds(
            name_l,
            all_lines,
            report_sender_cfg,
            stale_grace_seconds,
        )
        log_age_seconds = None
        if last_activity_at:
            log_age_seconds = max(0, int((datetime.now() - last_activity_at).total_seconds()))

        fetched_lines = [ln for ln in all_lines if "Fetched" in ln and "jobs" in ln]
        outside_window_lines = [ln for ln in all_lines if "Outside" in ln and "window" in ln]

        status = "healthy"
        message = "No hard errors in recent worker logs"
        severity = None

        if err_signal.get("actionable_total", 0) >= MIN_ACTIONABLE_ERRORS_FOR_EVENT:
            status = "degraded"
            severity = "medium"
            message = (
                f"{name} recent actionable_errors={err_signal.get('actionable_total', 0)} "
                f"raw_errors={err_signal.get('total', 0)} last_error_at={last_error_at or '-'} "
                f"sample={err_signal.get('actionable_sample') or err_signal.get('sample') or '-'}"
            )
        elif err_signal["total"] > 0:
            status = "healthy"
            message = (
                f"{name} non-actionable log noise raw_errors={err_signal.get('total', 0)} "
                f"last_error_at={last_error_at or '-'} sample={err_signal.get('sample') or '-'}"
            )
        elif fetched_lines and not outside_window_lines:
            status = "healthy"
            message = f"{name} active; fetched cycles={len(fetched_lines)}"

        if (
            status == "healthy"
            and isinstance(log_age_seconds, int)
            and isinstance(max_log_age_seconds, int)
            and log_age_seconds > max_log_age_seconds
        ):
            status = "down"
            severity = "high"
            message = (
                f"{name} stale log heartbeat age_min={log_age_seconds // 60} "
                f"allowed_min={max_log_age_seconds // 60}"
            )

        services.append({
            "service_key": with_node_suffix(f"{normalize_key(name)}_log_health", config["node_suffix"]),
            "category": "ops",
            "label": f"{name} Log Health",
            "status": status,
            "latency_ms": None,
            "message": message[:240],
            "payload": {
                "worker_service_key": key,
                "error_log_path": err_path or None,
                "out_log_path": out_path or None,
                "recent_error_total": err_signal["total"],
                "recent_error_counts": err_signal["counts"],
                "sample_error": err_signal["sample"],
                "recent_actionable_error_total": err_signal.get("actionable_total", 0),
                "sample_actionable_error": err_signal.get("actionable_sample"),
                "last_error_at": last_error_at,
                "recent_fetched_cycles": len(fetched_lines),
                "last_activity_at": last_activity_at.isoformat() if last_activity_at else None,
                "last_activity_line": last_activity_line,
                "log_age_seconds": log_age_seconds,
                "report_sender_config_path": str(config.get("report_sender_config_path") or ""),
                **cadence_payload,
            },
        })

        if severity:
            events.append({
                "service_key": key,
                "event_type": "worker_log_stale" if status == "down" and isinstance(log_age_seconds, int) else "worker_log_error_spike",
                "severity": severity,
                "message": message[:240],
                "payload": {
                    "service": name,
                    "error_total": err_signal.get("actionable_total", 0),
                    "raw_error_total": err_signal.get("total", 0),
                    "error_counts": err_signal["counts"],
                    "error_sample": err_signal.get("actionable_sample") or err_signal.get("sample"),
                    "last_error_at": last_error_at,
                    "last_activity_at": last_activity_at.isoformat() if last_activity_at else None,
                    "log_age_seconds": log_age_seconds,
                    **cadence_payload,
                },
                "event_at": now_iso(),
            })

    return services, events




def supabase_rest_get(base_url: str, key: str, table: str, params: Dict[str, str], timeout: int = 8) -> Tuple[bool, Any, str]:
    query = urllib.parse.urlencode(params, doseq=True)
    url = f"{base_url.rstrip('/')}/rest/v1/{table}"
    if query:
        url = f"{url}?{query}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(body or "[]")
            return True, data, f"{getattr(resp, 'status', 200)}"
    except urllib.error.HTTPError as exc:
        return False, None, f"HTTP {exc.code}: {exc.reason}"
    except Exception as exc:
        return False, None, str(exc)


def collect_dispatch_queue_signals(config: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    services: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []

    if not config.get("dispatch_checks_enabled"):
        return services, events

    base_url = str(config.get("supabase_url") or "").strip()
    key = str(config.get("supabase_key") or "").strip()
    jobs_table = str(config.get("dispatch_jobs_table") or "report_auto_dispatch_jobs").strip()
    events_table = str(config.get("dispatch_events_table") or "report_auto_dispatch_events").strip()
    if not base_url or not key:
        return services, events

    q_threshold = int(config.get("dispatch_queued_overdue_minutes", 30))
    wait_hours = float(config.get("dispatch_stuck_queued_wait_hours", 6))
    cool_hours = float(config.get("dispatch_stuck_cooling_off_hours", 2))
    scan_limit = int(config.get("dispatch_scan_limit", 500))

    now = datetime.now(timezone.utc)
    queued_cutoff = (now - timedelta(minutes=q_threshold)).isoformat()

    ok_q, queued_rows, err_q = supabase_rest_get(
        base_url, key, jobs_table,
        {
            "select": "id,reqno,status,is_paused,next_attempt_at,updated_at",
            "status": "eq.queued",
            "is_paused": "eq.false",
            "next_attempt_at": f"lte.{queued_cutoff}",
            "order": "next_attempt_at.asc",
            "limit": str(scan_limit),
        },
    )

    if not ok_q:
        events.append({
            "service_key": with_node_suffix("auto_dispatch_queue", config["node_suffix"]),
            "event_type": "dispatch_queue_check_failed",
            "severity": "medium",
            "message": f"queue overdue check failed: {err_q}",
            "payload": {"error": err_q},
            "event_at": now_iso(),
        })
        return services, events

    queued_rows = queued_rows or []

    ok_e, ev_rows, err_e = supabase_rest_get(
        base_url, key, events_table,
        {
            "select": "job_id,reqno,event_type,created_at",
            "event_type": "in.(queued_wait,cooling_off)",
            "order": "created_at.desc",
            "limit": str(scan_limit * 3),
        },
    )
    if not ok_e:
        ev_rows = []
        err_e = err_e

    latest_by_job = {}
    for r in (ev_rows or []):
        jid = r.get("job_id")
        if not jid or jid in latest_by_job:
            continue
        latest_by_job[jid] = r

    stale = []
    for ev in latest_by_job.values():
        created = ev.get("created_at")
        try:
            dt = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
        except Exception:
            continue
        age_h = (now - dt).total_seconds() / 3600.0
        et = str(ev.get("event_type") or "").lower()
        if (et == "queued_wait" and age_h >= wait_hours) or (et == "cooling_off" and age_h >= cool_hours):
            stale.append({"job_id": ev.get("job_id"), "reqno": ev.get("reqno"), "event_type": et, "age_hours": round(age_h, 2), "created_at": created})

    queue_status = "healthy" if len(queued_rows) == 0 else ("degraded" if len(queued_rows) < 10 else "down")
    wait_status = "healthy" if len(stale) == 0 else ("degraded" if len(stale) < 10 else "down")

    services.append({
        "service_key": with_node_suffix("auto_dispatch_queued_overdue", config["node_suffix"]),
        "category": "ops",
        "label": "Auto Dispatch Queued Overdue",
        "status": queue_status,
        "latency_ms": None,
        "message": f"{len(queued_rows)} queued jobs overdue > {q_threshold}m",
        "payload": {"count": len(queued_rows), "threshold_minutes": q_threshold, "samples": queued_rows[:25]},
    })

    services.append({
        "service_key": with_node_suffix("auto_dispatch_wait_state_stuck", config["node_suffix"]),
        "category": "ops",
        "label": "Auto Dispatch Wait-State Stuck",
        "status": wait_status,
        "latency_ms": None,
        "message": f"{len(stale)} wait-state jobs stale (queued_wait>{wait_hours}h, cooling_off>{cool_hours}h)",
        "payload": {"count": len(stale), "queued_wait_hours": wait_hours, "cooling_off_hours": cool_hours, "samples": stale[:25], "events_lookup_error": err_e if not ok_e else None},
    })

    if len(queued_rows) > 0:
        events.append({
            "service_key": with_node_suffix("auto_dispatch_queue", config["node_suffix"]),
            "event_type": "queued_overdue",
            "severity": "high" if len(queued_rows) >= 10 else "medium",
            "message": f"{len(queued_rows)} queued jobs overdue by > {q_threshold}m",
            "payload": {"count": len(queued_rows), "threshold_minutes": q_threshold, "samples": queued_rows[:20]},
            "event_at": now_iso(),
        })

    if len(stale) > 0:
        events.append({
            "service_key": with_node_suffix("auto_dispatch_queue", config["node_suffix"]),
            "event_type": "wait_state_stuck",
            "severity": "high" if len(stale) >= 10 else "medium",
            "message": f"{len(stale)} dispatch jobs stuck in queued_wait/cooling_off",
            "payload": {"count": len(stale), "queued_wait_hours": wait_hours, "cooling_off_hours": cool_hours, "samples": stale[:20]},
            "event_at": now_iso(),
        })

    return services, events


def apply_restart_storm_events(
    events: List[Dict[str, Any]],
    prev_restarts: Dict[str, int],
    current_restarts: Dict[str, int],
    threshold: int,
) -> None:
    for service_key, current in current_restarts.items():
        prev = int(prev_restarts.get(service_key, current))
        delta = current - prev
        if delta >= threshold:
            events.append(
                {
                    "service_key": service_key,
                    "event_type": "pm2_restart_storm",
                    "severity": "critical",
                    "message": f"Restart storm detected: +{delta} restarts",
                    "payload": {"delta_restarts": delta, "previous": prev, "current": current},
                    "event_at": now_iso(),
                }
            )


def dedupe_events(events: List[Dict[str, Any]], state: Dict[str, Any], cooldown_seconds: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    last_sent = state.setdefault("event_last_sent_at", {})
    now_ts = int(time.time())
    for event in events:
        key = "|".join(
            [
                str(event.get("service_key") or ""),
                str(event.get("event_type") or ""),
                str(event.get("severity") or ""),
                str(event.get("message") or ""),
            ]
        )
        prev = int(last_sent.get(key, 0))
        if now_ts - prev < cooldown_seconds:
            continue
        out.append(event)
        last_sent[key] = now_ts
    return out


def run_cycle(config: Dict[str, Any], state: Dict[str, Any]) -> int:
    services: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []

    pm2_services, pm2_events, restart_map, pm2_rows = collect_pm2(config["pm2_bin"], state, config)
    services.extend(pm2_services)
    events.extend(pm2_events)

    worker_log_services, worker_log_events = collect_dispatch_worker_log_signals(config, pm2_rows)
    services.extend(worker_log_services)
    events.extend(worker_log_events)

    dispatch_services, dispatch_events = collect_dispatch_queue_signals(config)
    services.extend(dispatch_services)
    events.extend(dispatch_events)

    host_services, host_events = collect_host_metrics(config)
    services.extend(host_services)
    events.extend(host_events)

    docker_services, docker_events = collect_docker(
        config["docker_bin"], config["enable_docker"], config
    )
    services.extend(docker_services)
    events.extend(docker_events)

    apply_restart_storm_events(
        events,
        state.get("pm2_restarts", {}),
        restart_map,
        config["restart_storm_delta"],
    )

    payload = {
        "lab_id": config["lab_id"],
        "source": config["source"],
        "checked_at": now_iso(),
        "services": services,
    }
    ingest_ok, ingest_msg = post_json(
        f"{config['base_url'].rstrip('/')}/api/cto/ingest",
        config["token"],
        payload,
    )

    filtered_events = dedupe_events(events, state, config["event_cooldown_seconds"])
    events_ok = True
    events_msg = "no-events"
    if filtered_events:
        event_payload = {
            "lab_id": config["lab_id"],
            "source": f"{config['source']}-collector",
            "events": filtered_events,
        }
        events_ok, events_msg = post_json(
            f"{config['base_url'].rstrip('/')}/api/cto/events",
            config["token"],
            event_payload,
        )

    state["pm2_restarts"] = restart_map

    summary = (
        f"[cto-collector] services={len(services)} events={len(filtered_events)} "
        f"ingest_ok={ingest_ok} ingest={ingest_msg} events_ok={events_ok} events={events_msg}"
    )
    print(summary, flush=True)

    if not ingest_ok:
        return 2
    if not events_ok:
        return 3
    return 0


def load_config() -> Dict[str, Any]:
    required = ["CTO_BASE_URL", "CTO_INGEST_TOKEN", "CTO_LAB_ID", "CTO_SOURCE"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    return {
        "base_url": os.environ["CTO_BASE_URL"],
        "token": os.environ["CTO_INGEST_TOKEN"],
        "lab_id": os.environ["CTO_LAB_ID"],
        "source": os.environ["CTO_SOURCE"],
        "node_suffix": normalize_key(os.getenv("CTO_NODE_SUFFIX", "vps")),
        "interval_seconds": int(os.getenv("CTO_INTERVAL_SECONDS", "60")),
        "state_file": Path(os.getenv("CTO_STATE_FILE", "/var/tmp/labbit-cto-collector-state.json")),
        "pm2_bin": os.getenv("CTO_PM2_BIN", "pm2"),
        "docker_bin": os.getenv("CTO_DOCKER_BIN", "docker"),
        "enable_docker": getenv_bool("CTO_ENABLE_DOCKER", True),
        "restart_storm_delta": int(os.getenv("CTO_PM2_RESTART_STORM_DELTA", "3")),
        "event_cooldown_seconds": int(os.getenv("CTO_EVENT_COOLDOWN_SECONDS", "600")),
        "host_mem_warn_pct": float(os.getenv("CTO_HOST_MEM_WARN_PCT", "80")),
        "host_mem_critical_pct": float(os.getenv("CTO_HOST_MEM_CRITICAL_PCT", "92")),
        "host_disk_warn_pct": float(os.getenv("CTO_HOST_DISK_WARN_PCT", "85")),
        "host_disk_critical_pct": float(os.getenv("CTO_HOST_DISK_CRITICAL_PCT", "95")),
        "host_swap_warn_pct": float(os.getenv("CTO_HOST_SWAP_WARN_PCT", "35")),
        "scheduled_pm2_names": parse_csv_lower(os.getenv("CTO_SCHEDULED_PM2_NAMES", "labbit-cto-digest")),
        "scheduled_job_warn_age_seconds": int(os.getenv("CTO_SCHEDULED_JOB_WARN_AGE_SECONDS", str(30 * 60 * 60))),
        "scheduled_job_max_age_seconds": int(os.getenv("CTO_SCHEDULED_JOB_MAX_AGE_SECONDS", str(36 * 60 * 60))),
        "report_sender_config_path": os.getenv("CTO_REPORT_SENDER_CONFIG_PATH", "/opt/py_utils/workers/report_sender/config/report_sender.json"),
        "worker_log_stale_grace_seconds": int(os.getenv("CTO_WORKER_LOG_STALE_GRACE_SECONDS", "300")),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run one collection cycle and exit")
    args = parser.parse_args()

    try:
        config = load_config()
    except Exception as exc:
        print(f"[cto-collector] config error: {exc}", file=sys.stderr)
        return 1

    state = load_state(config["state_file"])

    if args.once:
        code = run_cycle(config, state)
        save_state(config["state_file"], state)
        return code

    while True:
        code = run_cycle(config, state)
        save_state(config["state_file"], state)
        sleep_for = max(15, config["interval_seconds"])
        if code != 0:
            sleep_for = min(120, max(sleep_for, 45))
        time.sleep(sleep_for)


if __name__ == "__main__":
    raise SystemExit(main())
