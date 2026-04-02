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
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


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


def normalize_key(raw: str) -> str:
    key = re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")
    return key or "unknown"


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


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"pm2_restarts": {}, "event_last_sent_at": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"pm2_restarts": {}, "event_last_sent_at": {}}


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")


def collect_pm2(pm2_bin: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    services: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    restart_map: Dict[str, int] = {}

    code, out, err = run_cmd([pm2_bin, "jlist"], timeout=8)
    if code != 0:
        events.append(
            {
                "service_key": "pm2_runtime__vps",
                "event_type": "pm2_jlist_failed",
                "severity": "high",
                "message": f"pm2 jlist failed: {err[:180]}",
                "payload": {"stderr": err[:500]},
                "event_at": now_iso(),
            }
        )
        services.append(
            {
                "service_key": "pm2_runtime__vps",
                "category": "ops",
                "label": "PM2 Runtime",
                "status": "down",
                "latency_ms": None,
                "message": "Unable to read PM2 process list",
                "payload": {"error": err[:500]},
            }
        )
        return services, events, restart_map

    try:
        rows = json.loads(out or "[]")
    except Exception:
        rows = []

    for row in rows:
        name = str(row.get("name") or "pm2_proc")
        key = f"{normalize_key(name)}__vps"
        pm2_env = row.get("pm2_env") or {}
        monit = row.get("monit") or {}
        status = str(pm2_env.get("status") or "").lower()
        restarts = int(pm2_env.get("restart_time") or 0)
        restart_map[key] = restarts

        if status == "online":
            norm_status = "healthy"
            if restarts >= 20:
                norm_status = "degraded"
        elif status in {"launching", "stopping", "errored"}:
            norm_status = "down"
        else:
            norm_status = "unknown"

        cpu = monit.get("cpu")
        mem = monit.get("memory")
        msg = f"pm2={status or 'unknown'} restarts={restarts}"
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
                    "cpu": cpu,
                    "memory": mem,
                },
            }
        )

        if norm_status == "down":
            events.append(
                {
                    "service_key": key,
                    "event_type": "pm2_process_down",
                    "severity": "high",
                    "message": f"{name} is {status or 'down'}",
                    "payload": {"pm2_status": status, "restarts": restarts},
                    "event_at": now_iso(),
                }
            )

    return services, events, restart_map


def collect_docker(docker_bin: str, enabled: bool) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not enabled:
        return [], []

    services: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []

    code, out, err = run_cmd([docker_bin, "ps", "--format", "{{.Names}}|{{.Status}}"], timeout=8)
    if code != 0:
        events.append(
            {
                "service_key": "docker_runtime__vps",
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
        key = f"docker_{normalize_key(name)}__vps"
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

    pm2_services, pm2_events, restart_map = collect_pm2(config["pm2_bin"])
    services.extend(pm2_services)
    events.extend(pm2_events)

    docker_services, docker_events = collect_docker(config["docker_bin"], config["enable_docker"])
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
        "interval_seconds": int(os.getenv("CTO_INTERVAL_SECONDS", "60")),
        "state_file": Path(os.getenv("CTO_STATE_FILE", "/var/tmp/labbit-cto-collector-state.json")),
        "pm2_bin": os.getenv("CTO_PM2_BIN", "pm2"),
        "docker_bin": os.getenv("CTO_DOCKER_BIN", "docker"),
        "enable_docker": getenv_bool("CTO_ENABLE_DOCKER", True),
        "restart_storm_delta": int(os.getenv("CTO_PM2_RESTART_STORM_DELTA", "3")),
        "event_cooldown_seconds": int(os.getenv("CTO_EVENT_COOLDOWN_SECONDS", "600")),
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

