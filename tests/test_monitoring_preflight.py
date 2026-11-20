"""The pre-flight check that refuses to start Grafana without its private settings."""
import os
import subprocess
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "monitoring_preflight.sh"
REQUIRED = ("SLACK_WEBHOOK_URL", "GRAFANA_ADMIN_PASSWORD", "GRAFANA_ROOT_URL")


def run_preflight(**overrides):
    environment = {"PATH": os.environ["PATH"], **{name: "value" for name in REQUIRED}}
    environment.update(overrides)
    environment = {key: value for key, value in environment.items() if value is not None}
    return subprocess.run(["bash", str(SCRIPT)], env=environment, capture_output=True, text=True)


class MonitoringPreflightTests(unittest.TestCase):
    def test_passes_when_every_setting_is_present(self):
        result = run_preflight()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_fails_naming_each_missing_setting(self):
        for name in REQUIRED:
            with self.subTest(missing=name):
                result = run_preflight(**{name: None})
                self.assertEqual(result.returncode, 1)
                self.assertIn(name, result.stderr)

    def test_empty_value_counts_as_missing(self):
        result = run_preflight(GRAFANA_ADMIN_PASSWORD="")
        self.assertEqual(result.returncode, 1)
        self.assertIn("GRAFANA_ADMIN_PASSWORD", result.stderr)

    def test_never_prints_secret_values(self):
        result = run_preflight(SLACK_WEBHOOK_URL="https://example.invalid/secret-token", GRAFANA_ADMIN_PASSWORD="")
        self.assertNotIn("secret-token", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
