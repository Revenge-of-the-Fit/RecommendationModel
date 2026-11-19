"""Incident drill helper: timing, alert polling, evidence, and the safety net."""
import io
import json
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import incident_drill as drill


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def rules_payload(state, title="Gateway unreachable"):
    return json.dumps({"data": {"groups": [{"name": "required", "rules": [
        {"name": "Some other rule", "state": "firing"},
        {"name": title, "state": state},
    ]}]}}).encode()


class WaitUntilTests(unittest.TestCase):
    def test_returns_seconds_waited_once_the_check_passes(self):
        fake = FakeClock()
        answers = iter([False, False, True])
        waited = drill.wait_until(lambda: next(answers), timeout=60, interval=5, sleep=fake.sleep, clock=fake.clock)
        self.assertEqual(waited, 10)
        self.assertEqual(fake.sleeps, [5, 5])

    def test_does_not_sleep_after_success(self):
        fake = FakeClock()
        drill.wait_until(lambda: True, timeout=60, interval=5, sleep=fake.sleep, clock=fake.clock)
        self.assertEqual(fake.sleeps, [])

    def test_raises_timeout_when_check_never_passes(self):
        fake = FakeClock()
        with self.assertRaises(TimeoutError):
            drill.wait_until(lambda: False, timeout=20, interval=5, sleep=fake.sleep, clock=fake.clock)
        self.assertLessEqual(fake.now, 25)


class AlertStateTests(unittest.TestCase):
    def state(self, raw_state, title="Gateway unreachable"):
        return drill.alert_state("http://grafana:3000", "Gateway unreachable", user="viewer", password="pw",
                                 fetch=lambda url, headers: rules_payload(raw_state, title))

    def test_maps_firing_and_pending(self):
        self.assertEqual(self.state("firing"), "firing")
        self.assertEqual(self.state("pending"), "pending")

    def test_everything_else_is_inactive(self):
        for raw in ("inactive", "normal", "unknown"):
            with self.subTest(raw=raw):
                self.assertEqual(self.state(raw), "inactive")

    def test_missing_rule_is_an_error_not_inactive(self):
        with self.assertRaises(LookupError):
            self.state("firing", title="Different rule")

    def test_sends_basic_auth_and_the_rules_url(self):
        seen = {}

        def fetch(url, headers):
            seen["url"], seen["headers"] = url, headers
            return rules_payload("inactive")

        drill.alert_state("http://grafana:3000/", "Gateway unreachable", user="viewer", password="pw", fetch=fetch)
        self.assertEqual(seen["url"], "http://grafana:3000/api/prometheus/grafana/api/v1/rules")
        self.assertTrue(seen["headers"]["Authorization"].startswith("Basic "))


class RenderEvidenceTests(unittest.TestCase):
    def test_table_lists_events_in_order_with_iso_utc(self):
        text = drill.render_evidence([("2026-11-20T10:00:00Z", "gateway stopped"), ("2026-11-20T10:03:10Z", "alert firing")])
        lines = [line for line in text.splitlines() if line.startswith("|")]
        self.assertEqual(lines[0], "| Time (UTC) | Event |")
        self.assertLess(text.index("gateway stopped"), text.index("alert firing"))
        self.assertIn("2026-11-20T10:03:10Z", text)


def make_deps(*, alert_fires=True, receiver="Sam", log=None):
    log = log if log is not None else []
    fake = FakeClock()
    start = datetime(2026, 11, 20, 10, 0, 0, tzinfo=timezone.utc)
    state = {"stopped": False, "alert": "inactive"}

    def stop():
        log.append("stop")
        state["stopped"] = True

    def start_gateway():
        log.append("start")
        state["stopped"] = False

    def alert_state():
        if state["stopped"] and alert_fires:
            return "firing"
        return "inactive"

    return drill.DrillDeps(
        stop_gateway=stop, start_gateway=start_gateway, alert_state=alert_state,
        gateway_ready=lambda: not state["stopped"], recommendation_ok=lambda: not state["stopped"],
        ask_receiver=lambda: receiver,
        now=lambda: start + timedelta(seconds=fake.now), sleep=fake.sleep, clock=fake.clock,
        alert_timeout=60, recovery_timeout=60, interval=5,
    ), log


class RunDrillTests(unittest.TestCase):
    def test_happy_path_records_detection_delivery_and_recovery_in_order(self):
        deps, log = make_deps()
        result = drill.run_drill(deps)
        self.assertTrue(result.success)
        self.assertEqual(log, ["stop", "start"])
        text = " | ".join(event for _, event in result.events)
        for expected in ("gateway stopped", "alert firing", "received by Sam", "gateway started", "ready check passed",
                         "recommendation check passed", "alert resolved"):
            with self.subTest(expected=expected):
                self.assertIn(expected, text)
        self.assertLess(text.index("alert firing"), text.index("gateway started"))
        self.assertTrue(all(stamp.endswith("Z") for stamp, _ in result.events))

    def test_gateway_is_restarted_even_when_the_alert_never_fires(self):
        deps, log = make_deps(alert_fires=False)
        result = drill.run_drill(deps)
        self.assertFalse(result.success)
        self.assertEqual(log, ["stop", "start"])
        self.assertIn("alert did not fire", " | ".join(event for _, event in result.events))

    def test_blank_receipt_fails_the_drill_but_still_recovers(self):
        deps, log = make_deps(receiver="")
        result = drill.run_drill(deps)
        self.assertFalse(result.success)
        self.assertEqual(log, ["stop", "start"])
        self.assertIn("NOT confirmed", " | ".join(event for _, event in result.events))

    def test_gateway_is_restarted_if_a_step_raises(self):
        deps, log = make_deps()
        deps.alert_state = lambda: (_ for _ in ()).throw(RuntimeError("grafana unreachable"))
        with self.assertRaises(RuntimeError):
            drill.run_drill(deps)
        self.assertEqual(log, ["stop", "start"])


class MainSafetyTests(unittest.TestCase):
    def test_refuses_to_stop_the_gateway_without_explicit_confirmation(self):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = drill.main([])
        self.assertEqual(code, 2)
        self.assertIn("--confirm-stop-gateway", err.getvalue())


if __name__ == "__main__":
    unittest.main()
