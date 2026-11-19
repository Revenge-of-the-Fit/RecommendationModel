"""The incident response plan must keep the sections the course requires."""
import os
import re
import unittest
from pathlib import Path

PLAN = Path(__file__).resolve().parents[1] / "INCIDENT_RESPONSE.md"
HEADINGS = (
    "## Severity and scope",
    "## Detection",
    "## Urgent contact",
    "## Roles",
    "## Communication",
    "## Quick mitigations",
    "## Suspected Security Event",
    "## Recovery and verification",
    "## After the incident",
)


def section(text: str, heading: str) -> str:
    match = re.search(rf"^{re.escape(heading)}\n(.*?)(?=^## |\Z)", text, re.MULTILINE | re.DOTALL)
    return match.group(1) if match else ""


class IncidentPlanTests(unittest.TestCase):
    def setUp(self):
        self.text = PLAN.read_text(encoding="utf-8")

    def test_has_every_required_heading(self):
        for heading in HEADINGS:
            with self.subTest(heading=heading):
                self.assertRegex(self.text, rf"(?m)^{re.escape(heading)}$")

    def test_urgent_contact_names_slack_ping_and_a_fallback(self):
        contact = section(self.text, "## Urgent contact")
        self.assertRegex(contact, r"@channel|<!channel>")
        self.assertRegex(contact, r"(?i)fallback")

    def test_roles_cover_the_response_roles(self):
        roles = section(self.text, "## Roles").lower()
        for role in ("incident lead", "responding engineer"):
            with self.subTest(role=role):
                self.assertIn(role, roles)

    def test_mitigations_cover_rollback_and_gateway_restart(self):
        mitigations = section(self.text, "## Quick mitigations")
        self.assertIn("scripts/switch_model.sh", mitigations)
        self.assertRegex(mitigations, r"restart.*gateway|gateway.*restart")

    def test_document_header_names_owner_version_and_review_date(self):
        header = self.text.split("## ", 1)[0]
        for field in ("Owner:", "Version:", "Last reviewed:"):
            with self.subTest(field=field):
                self.assertIn(field, header)

    def test_severity_levels_include_security_events_and_recovery_target(self):
        severity = section(self.text, "## Severity and scope")
        for level in ("SEV1", "SEV2", "SEV3"):
            with self.subTest(level=level):
                self.assertIn(level, severity)
        self.assertRegex(severity, r"(?i)security")
        self.assertRegex(severity, r"(?i)recovery time|recover within")

    def test_backups_statement_is_honest_about_compose_settings(self):
        severity = section(self.text, "## Severity and scope")
        self.assertIn("--no-backups", severity)

    def test_secret_leak_playbook_rotates_every_credential(self):
        playbook = section(self.text, "## Suspected Security Event")
        for secret in ("OpenAI", "Slack webhook", "Grafana", "Kafka"):
            with self.subTest(secret=secret):
                self.assertIn(secret, playbook)
        self.assertRegex(playbook, r"(?i)evidence")

    def test_evidence_is_preserved_before_restarts_and_kept_out_of_git(self):
        mitigations = section(self.text, "## Quick mitigations")
        self.assertRegex(mitigations, r"(?i)preserve evidence")
        self.assertIn("docker compose --project-name recommendationmodel logs", mitigations)
        self.assertRegex(mitigations, r"(?i)not (in|into) Git|outside Git")

    def test_communication_forbids_pasting_secrets(self):
        communication = section(self.text, "## Communication")
        self.assertRegex(communication, r"(?i)never paste")
        self.assertRegex(communication, r"(?i)secret")

    def test_urgent_contact_lists_outside_parties_and_slack_outage(self):
        contact = section(self.text, "## Urgent contact")
        self.assertRegex(contact, r"(?i)course staff")
        self.assertRegex(contact, r"(?i)slack (itself )?is down")

    def test_incident_artifacts_go_to_a_slack_canvas_not_the_repo(self):
        after = section(self.text, "## After the incident")
        self.assertRegex(after, r"(?i)slack canvas")
        self.assertNotIn("verification/incident-", self.text)
        mitigations = section(self.text, "## Quick mitigations")
        self.assertNotIn("verification/", mitigations)
        self.assertRegex(mitigations, r"(?i)incident canvas")

    def test_drill_record_is_the_only_incident_file_meant_for_git(self):
        after = section(self.text, "## After the incident")
        self.assertRegex(after, r"(?is)drill record.*only incident file meant for Git")
        self.assertIn("verification/", after)

    def test_postmortem_actions_have_owner_and_due_date(self):
        after = section(self.text, "## After the incident")
        self.assertRegex(after, r"(?i)owner")
        self.assertRegex(after, r"(?i)due date")

    def test_llm_mitigation_clears_both_keys_and_restarts_one_slot_at_a_time(self):
        mitigations = section(self.text, "## Quick mitigations")
        self.assertIn("OPENAI_API_KEY", mitigations)
        self.assertIn("OPENAI_TOKEN", mitigations)
        self.assertRegex(mitigations, r"(?i)one slot at a time")

    def test_detection_links_the_alert_and_dashboard(self):
        detection = section(self.text, "## Detection")
        self.assertIn("Gateway unreachable", detection)
        self.assertIn("m3-production", detection)

    def test_placeholders_use_one_greppable_marker(self):
        self.assertNotRegex(self.text, r"(?i)\b(tbd|fixme)\b|<fill")

    @unittest.skipUnless(os.environ.get("INCIDENT_PLAN_FINAL"), "release gate: set INCIDENT_PLAN_FINAL=1 once the team filled in contacts")
    def test_no_team_placeholders_remain(self):
        self.assertNotIn("TODO(team)", self.text)


if __name__ == "__main__":
    unittest.main()
