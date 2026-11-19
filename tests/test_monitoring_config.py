"""Static checks of the monitoring stack configuration (compose, Prometheus, Grafana)."""
import json
import re
import sys
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
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

    def test_grafana_refuses_to_start_without_an_admin_password(self):
        entrypoint = " ".join(self.services["grafana"]["entrypoint"])
        self.assertIn("GF_SECURITY_ADMIN_PASSWORD", entrypoint)
        self.assertIn("exit 1", entrypoint)
        self.assertIn("exec /run.sh", entrypoint)

    def test_grafana_root_url_is_configurable_for_alert_links(self):
        environment = self.services["grafana"]["environment"]
        self.assertIn("${GRAFANA_ROOT_URL:-", environment["GF_SERVER_ROOT_URL"])

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


PROVISIONING = "monitoring/grafana/provisioning"


def load_rules() -> dict[str, dict]:
    groups = load_yaml(f"{PROVISIONING}/alerting/rules.yml")["groups"]
    return {rule["uid"]: rule for group in groups for rule in group["rules"]}


def dashboard() -> dict:
    return json.loads((ROOT / PROVISIONING / "dashboards/m3-production.json").read_text(encoding="utf-8"))


def panel_titles(board: dict) -> list[str]:
    return [panel["title"] for panel in board["panels"]]


class GrafanaProvisioningTests(unittest.TestCase):
    def test_dashboard_metrics_exist_in_exporter(self):
        from monitoring_api import metric_lines

        exported = set(re.findall(r"^mlip_[a-z0-9_]+", metric_lines(None, max_age=600), re.MULTILINE))
        used = set(re.findall(r"mlip_[a-z0-9_]+", json.dumps(dashboard())))
        self.assertTrue(used)
        self.assertEqual(used - exported, set())

    def test_dashboard_has_all_four_categories(self):
        titles = " | ".join(panel_titles(dashboard()))
        for category in ("Availability", "Quality", "Cost", "Drift"):
            with self.subTest(category=category):
                self.assertIn(category, titles)

    def test_stat_panels_show_the_latest_sample_so_stale_data_is_visible(self):
        stats = [panel for panel in dashboard()["panels"] if panel["type"] == "stat"]
        self.assertTrue(stats)
        for panel in stats:
            with self.subTest(panel=panel["title"]):
                self.assertEqual(panel["options"]["reduceOptions"]["calcs"], ["last"])

    def test_dashboard_identity(self):
        board = dashboard()
        self.assertEqual(board["uid"], "m3-production")
        self.assertEqual(board["refresh"], "30s")

    def test_datasource_uid_matches_dashboard_and_rules(self):
        datasource = load_yaml(f"{PROVISIONING}/datasources/prometheus.yml")["datasources"][0]
        self.assertEqual(datasource["uid"], "prometheus")
        self.assertEqual(datasource["url"], "http://prometheus:9090")
        for panel in dashboard()["panels"]:
            for target in panel.get("targets", []):
                self.assertEqual(target["datasource"]["uid"], "prometheus")

    def test_required_alert_rule(self):
        rule = load_rules()["gateway-unreachable"]
        self.assertEqual(rule["for"], "2m")
        self.assertEqual(rule["noDataState"], "Alerting")
        self.assertEqual(rule["execErrState"], "Alerting")
        expressions = " ".join(item["model"].get("expr", "") for item in rule["data"])
        self.assertIn("probe_success", expressions)
        self.assertIn("gateway-ready", expressions)
        self.assertIn("INCIDENT_RESPONSE.md", rule["annotations"]["runbook_url"])

    def test_policy_repeat_interval_and_receiver(self):
        policy = load_yaml(f"{PROVISIONING}/alerting/policies.yml")["policies"][0]
        self.assertEqual(policy["receiver"], "slack-team")
        self.assertEqual(policy["repeat_interval"], "4h")

    def test_contact_point_sends_resolved_and_reads_webhook_from_env(self):
        receiver = load_yaml(f"{PROVISIONING}/alerting/contact-points.yml")["contactPoints"][0]["receivers"][0]
        self.assertEqual(receiver["type"], "slack")
        self.assertFalse(receiver.get("disableResolveMessage", False))
        self.assertEqual(receiver["settings"]["url"], "$__env{SLACK_WEBHOOK_URL}")

    def test_message_template_pings_channel(self):
        text = load_yaml(f"{PROVISIONING}/alerting/templates.yml")["templates"][0]["template"]
        self.assertIn("<!channel>", text)
        self.assertNotIn("@channel", text)
        self.assertIn("runbook_url", text)
        self.assertIn("/d/m3-production", text)
        # Grafana's ExternalURL ends in "/", so it must be trimmed to avoid "//d/...".
        self.assertIn('reReplaceAll "/+$" "" $.ExternalURL', text)

    def test_no_webhook_literal_in_repo(self):
        files = [ROOT / "compose.yaml", *(path for path in (ROOT / "monitoring").rglob("*") if path.is_file())]
        for path in files:
            with self.subTest(path=path.name):
                self.assertNotIn("hooks.slack.com/services/", path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
