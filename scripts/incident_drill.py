#!/usr/bin/env python3
"""Incident drill: stop the public gateway, wait for the Slack alert, recover, and record evidence.

Standard library only, so it runs on the VM host. The gateway is always restarted, even if the
drill fails part-way. Run it once, after announcing it in the team Slack channel:

    sudo --preserve-env=DRILL_GRAFANA_PASSWORD python3 scripts/incident_drill.py --confirm-stop-gateway
"""
import argparse
import base64
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.request import Request, urlopen


def wait_until(check: Callable[[], bool], timeout: float, interval: float, *,
               sleep=time.sleep, clock=time.monotonic) -> float:
    """Poll `check` until it is true; return the seconds waited or raise TimeoutError."""
    started = clock()
    while True:
        if check():
            return clock() - started
        if clock() - started >= timeout:
            raise TimeoutError(f"condition not met within {timeout}s")
        sleep(interval)


def _fetch(url: str, headers: dict) -> bytes:
    with urlopen(Request(url, headers=headers), timeout=10) as response:
        return response.read()


def alert_state(grafana_url: str, alert_name: str, *, user: str, password: str, fetch=None) -> str:
    """Return "firing", "pending" or "inactive" for the named Grafana alert rule."""
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    url = grafana_url.rstrip("/") + "/api/prometheus/grafana/api/v1/rules"
    payload = json.loads((fetch or _fetch)(url, {"Authorization": f"Basic {token}"}))
    for group in payload["data"]["groups"]:
        for rule in group["rules"]:
            if rule["name"] == alert_name:
                return rule["state"] if rule["state"] in ("firing", "pending") else "inactive"
    raise LookupError(f"alert rule {alert_name!r} not found in Grafana")


def render_evidence(events: list[tuple[str, str]]) -> str:
    rows = ["| Time (UTC) | Event |", "| --- | --- |"]
    rows += [f"| {stamp} | {event} |" for stamp, event in events]
    return "\n".join(rows) + "\n"


@dataclass
class DrillDeps:
    stop_gateway: Callable[[], None]
    start_gateway: Callable[[], None]
    alert_state: Callable[[], str]
    gateway_ready: Callable[[], bool]
    recommendation_ok: Callable[[], bool]
    ask_receiver: Callable[[], str]
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    alert_timeout: float = 600
    recovery_timeout: float = 600
    interval: float = 10


@dataclass
class DrillResult:
    events: list[tuple[str, str]] = field(default_factory=list)
    success: bool = True


def run_drill(deps: DrillDeps) -> DrillResult:
    result = DrillResult()

    def note(text: str) -> None:
        stamp = deps.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        result.events.append((stamp, text))

    def wait(check, timeout, ok, failed) -> bool:
        try:
            waited = wait_until(check, timeout, deps.interval, sleep=deps.sleep, clock=deps.clock)
        except TimeoutError:
            note(failed)
            result.success = False
            return False
        note(ok.format(seconds=waited))
        return True

    note("drill started")
    deps.stop_gateway()
    note("gateway stopped")
    try:
        fired = wait(lambda: deps.alert_state() == "firing", deps.alert_timeout,
                     "alert firing ({seconds:.0f}s after the gateway stopped)",
                     f"alert did not fire within {deps.alert_timeout:.0f}s")
        if fired:
            receiver = deps.ask_receiver().strip()
            if receiver:
                note(f"Slack alert received by {receiver}")
            else:
                note("Slack alert receipt NOT confirmed by a teammate")
                result.success = False
    finally:
        deps.start_gateway()
        note("gateway started")
    wait(deps.gateway_ready, deps.recovery_timeout, "ready check passed ({seconds:.0f}s after start)",
         "ready check did not pass in time")
    wait(deps.recommendation_ok, deps.recovery_timeout, "recommendation check passed",
         "recommendation check did not pass in time")
    wait(lambda: deps.alert_state() == "inactive", deps.alert_timeout, "alert resolved ({seconds:.0f}s after the checks)",
         "alert did not resolve in time")
    return result


def _http_ok(url: str, *, need_body: bool = False) -> bool:
    try:
        with urlopen(url, timeout=5) as response:
            return response.status == 200 and (not need_body or bool(response.read().strip()))
    except OSError:
        return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--confirm-stop-gateway", action="store_true", help="required: this stops the production gateway")
    parser.add_argument("--grafana-url", default="http://127.0.0.1:3000")
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8082")
    parser.add_argument("--project", default="recommendationmodel", help="Compose project name")
    parser.add_argument("--alert-title", default="Gateway unreachable")
    parser.add_argument("--evidence-dir", type=Path, default=Path("verification"))
    parser.add_argument("--alert-timeout", type=float, default=600)
    parser.add_argument("--recovery-timeout", type=float, default=600)
    parser.add_argument("--interval", type=float, default=10)
    args = parser.parse_args(argv)
    if not args.confirm_stop_gateway:
        print("Refusing to run: pass --confirm-stop-gateway after announcing the drill in Slack.", file=sys.stderr)
        return 2
    password = os.environ.get("DRILL_GRAFANA_PASSWORD")
    if not password:
        print("Set DRILL_GRAFANA_PASSWORD (a Grafana viewer or admin password).", file=sys.stderr)
        return 2
    user = os.environ.get("DRILL_GRAFANA_USER", "admin")
    compose = ["docker", "compose", "--project-name", args.project]
    deps = DrillDeps(
        stop_gateway=lambda: subprocess.run([*compose, "stop", "gateway"], check=True),
        start_gateway=lambda: subprocess.run([*compose, "up", "-d", "--no-deps", "gateway"], check=True),
        alert_state=lambda: alert_state(args.grafana_url, args.alert_title, user=user, password=password),
        gateway_ready=lambda: _http_ok(args.gateway_url + "/health/ready"),
        recommendation_ok=lambda: _http_ok(args.gateway_url + "/recommend/1", need_body=True),
        ask_receiver=lambda: input("Who received the Slack alert (name, blank if nobody)? "),
        alert_timeout=args.alert_timeout, recovery_timeout=args.recovery_timeout, interval=args.interval,
    )
    result = run_drill(deps)
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    date = result.events[0][0][:10]
    path = args.evidence_dir / f"m3-drill-{date}.md"
    verdict = "PASSED" if result.success else "FAILED"
    path.write_text(f"# Incident drill {date}: {verdict}\n\nInjected failure: `docker compose stop gateway`.\n\n"
                    + render_evidence(result.events), encoding="utf-8")
    print(f"Drill {verdict}. Evidence written to {path}")
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
