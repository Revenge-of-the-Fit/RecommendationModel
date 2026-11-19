"""Static checks of the monitoring stack configuration (compose, Prometheus, Grafana)."""
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
NEW_SERVICES = ("prometheus", "blackbox-exporter", "grafana")
MONITORING_SERVICES = {*NEW_SERVICES, "monitoring"}
SERVING_SERVICES = ("api", "api-green", "gateway", "kafka")


def load_yaml(relative_path: str):
    return yaml.safe_load((ROOT / relative_path).read_text(encoding="utf-8"))


def load_compose() -> dict:
    return load_yaml("compose.yaml")


def depends_on_names(service: dict) -> set[str]:
    depends = service.get("depends_on") or {}
    return set(depends) if isinstance(depends, (dict, list)) else set()


class ComposeMonitoringTests(unittest.TestCase):
    def setUp(self):
        self.services = load_compose()["services"]

    def test_monitoring_services_use_monitoring_profile(self):
        for name in NEW_SERVICES:
            with self.subTest(service=name):
                self.assertIn("monitoring", self.services[name].get("profiles", []))

    def test_serving_services_do_not_depend_on_monitoring(self):
        for name in SERVING_SERVICES:
            with self.subTest(service=name):
                self.assertFalse(depends_on_names(self.services[name]) & MONITORING_SERVICES)

    def test_monitoring_services_restart_and_limits(self):
        for name in NEW_SERVICES:
            with self.subTest(service=name):
                self.assertEqual(self.services[name].get("restart"), "unless-stopped")
                self.assertTrue(self.services[name].get("mem_limit"))

    def test_grafana_hardened(self):
        environment = self.services["grafana"]["environment"]
        self.assertEqual(str(environment["GF_AUTH_ANONYMOUS_ENABLED"]).lower(), "false")
        self.assertIn("${GRAFANA_ADMIN_PASSWORD:-", environment["GF_SECURITY_ADMIN_PASSWORD"])

    def test_named_volumes_declared(self):
        volumes = load_compose()["volumes"]
        self.assertIn("prometheus_data", volumes)
        self.assertIn("grafana_data", volumes)


class PrometheusConfigTests(unittest.TestCase):
    def setUp(self):
        config = load_yaml("monitoring/prometheus/prometheus.yml")
        self.jobs = {job["job_name"]: job for job in config["scrape_configs"]}

    def test_prometheus_scrapes_expected_jobs(self):
        gateway = self.jobs["gateway-ready"]
        self.assertEqual(gateway["metrics_path"], "/probe")
        self.assertEqual(gateway["params"]["module"], ["http_2xx"])
        self.assertEqual(gateway["scrape_interval"], "15s")
        self.assertEqual(gateway["static_configs"][0]["targets"], ["http://gateway:8082/health/ready"])
        self.assertEqual(self.jobs["mlip-monitoring"]["static_configs"][0]["targets"], ["monitoring:9091"])

    def test_probe_relabels_target_to_the_blackbox_exporter(self):
        replacements = {rule.get("target_label"): rule.get("replacement") for rule in self.jobs["gateway-ready"]["relabel_configs"]}
        self.assertEqual(replacements["__address__"], "blackbox-exporter:9115")

    def test_blackbox_defines_http_module(self):
        modules = load_yaml("monitoring/blackbox/blackbox.yml")["modules"]
        self.assertIn("http_2xx", modules)
        self.assertEqual(modules["http_2xx"]["prober"], "http")


if __name__ == "__main__":
    unittest.main()
