#!/usr/bin/env python3
"""Incident drill: stop the public gateway, wait for the Slack alert, recover, and record evidence.

Standard library only, so it runs on the VM host. The gateway is restarted as soon as the alert has
fired, and also on any failure, Ctrl-C, SIGTERM or dropped SSH session. Run it once, inside tmux or
screen, after announcing it in the team Slack channel. The evidence file it writes
(`verification/m3-drill-<date>.md`: timestamps and a first name, no user data) is the only incident
file meant for Git; real incident timelines, postmortems and logs stay out of the repo
(see INCIDENT_RESPONSE.md):

    sudo --preserve-env=DRILL_GRAFANA_PASSWORD python3 scripts/incident_drill.py --confirm-stop-gateway
"""
import argparse
import base64
import json
import os
import signal
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


def gateway_command(project: str, action: str) -> list[str]:
    """`start` is the exact inverse of `stop`; it needs no compose file and never recreates the container."""
    if action not in ("stop", "start"):
        raise ValueError(f"unsupported gateway action {action!r}")
    return ["docker", "compose", "--project-name", project, action, "gateway"]


def install_signal_handlers() -> None:
    """Turn SIGHUP (dropped SSH session) and SIGTERM into SystemExit so the cleanup in run_drill runs."""
    def stop(signum, frame):
        raise SystemExit(128 + signum)

    for sig in (signal.SIGHUP, signal.SIGTERM):
        signal.signal(sig, stop)


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
        errors = []

        def safe() -> bool:
            try:
                return check()
            except Exception as error:  # Grafana restarting etc.: keep waiting; KeyboardInterrupt/SystemExit still propagate
                errors.append(type(error).__name__)
                return False

        try:
            waited = wait_until(safe, timeout, deps.interval, sleep=deps.sleep, clock=deps.clock)
        except TimeoutError:
            note(failed)
            result.success = False
            return False
        finally:
            if errors:
                note(f"{len(errors)} check error(s) retried ({errors[-1]})")
        note(ok.format(seconds=waited))
        return True

    note("drill started")
    # Fail before any outage if Grafana is unreachable, or if the alert is already firing (a false detection).
    state = deps.alert_state()
    if state != "inactive":
        note(f"aborted: alert is already {state}; nothing was stopped")
        result.success = False
        return result
    try:
        deps.stop_gateway()
        note("gateway stopped")
        fired = wait(lambda: deps.alert_state() == "firing", deps.alert_timeout,
                     "alert firing ({seconds:.0f}s after the gateway stopped)",
                     f"alert did not fire within {deps.alert_timeout:.0f}s")
    finally:
        # Restart as soon as detection is proven (or on any failure/interrupt); never wait for a human while down.
        deps.start_gateway()
        note("gateway started")
    if fired:
        try:
            receiver = deps.ask_receiver().strip()
        except EOFError:
            receiver = ""
        if receiver:
            note(f"Slack alert received by {receiver}")
        else:
            note("Slack alert receipt NOT confirmed by a teammate")
            result.success = False
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
    os.chdir(Path(__file__).resolve().parents[1])  # relative evidence path no longer depends on where this was run
    install_signal_handlers()
    deps = DrillDeps(
        stop_gateway=lambda: subprocess.run(gateway_command(args.project, "stop"), check=True),
        start_gateway=lambda: subprocess.run(gateway_command(args.project, "start"), check=True),
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
