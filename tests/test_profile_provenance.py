import copy
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
from openai import APIStatusError
from openai.types.responses import Response


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from preferences import ColdStartError, INSTRUCTIONS, LLM_MODEL, PreferenceInterpreter
from storage.database import StorageError
from storage.metadata import MetadataStore
from storage.profiles import ProfileStore, content_version
from storage.requests import RequestStore


class ProviderResponse(SimpleNamespace):
    def model_dump(self, **_):
        return copy.deepcopy(self.envelope)


class ProfileProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.cache = self.directory / "cache"
        self.database = self.directory / "events.sqlite3"
        self.interpreter = self.new_interpreter()
        self.profile = {
            "liked_genres": ["Crime", "Drama"], "excluded_genres": ["Horror"],
            "liked_titles": ["Movie A"], "disliked_titles": [],
            "likes_summary": "Detective investigations and crime dramas.",
            "dislikes_summary": "Horror.",
        }

    def tearDown(self):
        self.temporary.cleanup()

    def new_interpreter(self, genres=None):
        return PreferenceInterpreter(
            genres or ["Crime", "Drama", "Horror"], self.cache,
            self.directory / "unused.env", storage_path=self.database,
        )

    def response(self, *, status="completed", output_text=None, profile=None, **extension):
        text = json.dumps(profile or self.profile) if output_text is None else output_text
        usage = {
            "input_tokens": 17, "output_tokens": 23, "total_tokens": 40,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens_details": {"reasoning_tokens": 4},
        }
        envelope = {
            "id": "response-real-1", "model": "returned-model-snapshot", "status": status,
            "output": [{"type": "message", "content": [{"type": "output_text", "text": text}]}],
            "usage": usage, "error": None, "incomplete_details": None,
            "provider_extension": {"nullable": None, "enabled": False, "nested": [{"unknown": [1, 2]}]},
            **extension,
        }
        return ProviderResponse(
            id=envelope["id"], model=envelope["model"], status=status, output_text=text,
            usage=SimpleNamespace(model_dump=lambda **_: copy.deepcopy(usage)),
            envelope=envelope,
        )

    @contextmanager
    def sdk(self, response=None, *, side_effect=None):
        client = MagicMock()
        client.__enter__.return_value = client
        client.responses.create.return_value = response or self.response()
        client.responses.create.side_effect = side_effect
        with patch.dict("os.environ", {"OPENAI_API_KEY": "sdk-test-secret"}, clear=True):
            with patch("preferences.dotenv_values", return_value={}) as dotenv:
                with patch("preferences.OpenAI", return_value=client) as factory:
                    yield factory, client.responses.create, dotenv

    def rows(self, table):
        with closing(sqlite3.connect(self.database)) as connection:
            return [json.loads(row[0]) for row in connection.execute(f"SELECT record_json FROM {table}")]

    def write_legacy(self, likes="Crime dramas", dislikes="Horror", saved=None):
        user_input = json.dumps(
            {"likes": likes.strip(), "dislikes": dislikes.strip()}, ensure_ascii=False, sort_keys=True,
        )
        cache_input = json.dumps(
            [LLM_MODEL, INSTRUCTIONS, user_input, self.interpreter._response_schema()], ensure_ascii=False,
        )
        cache_key = hashlib.sha256(cache_input.encode()).hexdigest()
        self.cache.mkdir(parents=True, exist_ok=True)
        path = self.cache / f"{cache_key}.json"
        path.write_text(json.dumps(saved or {"model": LLM_MODEL, "profile": self.profile}), encoding="utf-8")
        return path

    def seed_snapshot(self, user_id, snapshot_id):
        record = {"user_id": user_id, "likes": "Crime dramas", "dislikes": "Horror", "unknown": None}
        with MetadataStore(self.database) as store:
            store.save_fetch({
                "fetch_id": f"metadata-fetch-{user_id}", "source_id": "course-api",
                "entity_type": "user", "requested_ids": [user_id],
                "started_at": "2026-10-09T12:00:00Z", "finished_at": "2026-10-09T12:00:01Z",
                "status": "success", "http_status": 200, "error_type": None,
                "response": {"body": record},
            }, [{
                "snapshot_id": snapshot_id, "source_id": "course-api", "entity_type": "user",
                "entity_id": user_id, "fetched_at": "2026-10-09T12:00:01Z", "record": record,
            }])

    def test_success_retains_provider_envelope_actual_model_and_usage_after_restart(self):
        response = self.response()
        with self.sdk(response) as (factory, create, _):
            saved, cached = self.interpreter.get_profile_record("  Crime dramas  ", " Horror ")
        self.assertFalse(cached)
        self.assertEqual(saved["profile"], self.profile)
        self.assertEqual(saved["model"], response.model)
        self.assertEqual(saved["response_id"], response.id)
        self.assertEqual(saved["usage"], response.envelope["usage"])
        self.assertEqual(factory.call_args.kwargs["max_retries"], 0)
        self.assertEqual(create.call_count, 1)
        self.assertEqual(json.loads(create.call_args.kwargs["input"]), {"likes": "Crime dramas", "dislikes": "Horror"})
        provenance = saved["_provenance"]
        for field in ("profile_id", "attempt_id", "cache_key", "prompt_version", "schema_version", "profile_version", "origin"):
            self.assertTrue(provenance[field])
        with ProfileStore(self.database) as reopened:
            attempt = reopened.get_attempt(provenance["attempt_id"])
            profile = reopened.get_profile(provenance["profile_id"])
        self.assertEqual(attempt["status"], "success")
        self.assertEqual(attempt["response"], response.envelope)
        self.assertIn("Crime dramas", json.dumps(attempt))
        self.assertIn(INSTRUCTIONS.strip(), json.dumps(attempt).replace("\\n", "\n"))
        self.assertIn("liked_genres", json.dumps(attempt))
        self.assertIn(self.profile, profile.values())
        self.assertNotIn("sdk-test-secret", json.dumps(attempt))

    def test_azure_request_records_deployment_and_retains_actual_model(self):
        response = self.response()
        azure = {
            "AZURE_OPENAI_ENDPOINT": "https://example.services.ai.azure.com",
            "AZURE_OPENAI_DEPLOYMENT": "class-mini", "AZURE_OPENAI_API_KEY": "azure-test-key",
        }
        with self.sdk(response) as (factory, create, _):
            with patch.dict("os.environ", azure):
                saved, cached = self.interpreter.get_profile_record("Crime dramas", "Horror")
        self.assertFalse(cached)
        self.assertEqual(factory.call_args.kwargs["base_url"], azure["AZURE_OPENAI_ENDPOINT"] + "/openai/v1/")
        self.assertEqual(factory.call_args.kwargs["api_key"], "azure-test-key")
        self.assertEqual(create.call_args.kwargs["model"], "class-mini")
        self.assertEqual(create.call_args.kwargs["reasoning"], {"effort": "minimal"})
        self.assertTrue(create.call_args.kwargs["text"]["format"]["strict"])
        self.assertFalse(create.call_args.kwargs["store"])
        attempt = self.rows("llm_attempts")[0]
        self.assertEqual(attempt["versions"]["requested_model"], "class-mini")
        self.assertEqual(attempt["request"]["model"], "class-mini")
        self.assertEqual(attempt["request"]["reasoning"], create.call_args.kwargs["reasoning"])
        self.assertEqual(saved["model"], response.model)
        self.assertNotIn("azure-test-key", json.dumps(attempt))

    def test_provider_switch_reuses_cache_and_durable_profile_without_relabeling(self):
        with self.sdk():
            original, _ = self.interpreter.get_profile_record("Crime dramas", "Horror")
        before = {table: self.rows(table) for table in ("llm_attempts", "preference_profiles")}
        azure = {
            "AZURE_OPENAI_ENDPOINT": "https://example.services.ai.azure.com/openai/v1",
            "AZURE_OPENAI_DEPLOYMENT": "gpt-5-mini", "AZURE_OPENAI_API_KEY": "azure-test-key",
        }
        path = self.cache / (original["_provenance"]["cache_key"] + ".json")
        for remove_cache in (False, True):
            with self.subTest(remove_cache=remove_cache):
                if remove_cache:
                    path.unlink()
                with patch.dict("os.environ", azure, clear=True):
                    with patch("preferences.dotenv_values", side_effect=AssertionError("Reuse read credentials")):
                        with patch("preferences.OpenAI", side_effect=AssertionError("Reuse called provider")):
                            reused, cached = self.new_interpreter().get_profile_record("Crime dramas", "Horror")
                self.assertTrue(cached)
                self.assertEqual(reused, original)
                self.assertEqual(before, {table: self.rows(table) for table in before})

    def test_pending_attempt_exists_before_api_and_response_is_saved_before_validation(self):
        observed = []

        def create(**_):
            attempts = self.rows("llm_attempts")
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "pending")
            self.assertEqual(self.rows("preference_profiles"), [])
            observed.append("pending")
            return self.response()

        original = self.interpreter._validate_profile

        def validate(profile):
            if "responded" not in observed:
                attempt = self.rows("llm_attempts")[0]
                self.assertEqual(attempt["status"], "responded")
                self.assertEqual(attempt["response"]["id"], "response-real-1")
                self.assertEqual(self.rows("preference_profiles"), [])
                observed.append("responded")
            return original(profile)

        with self.sdk(side_effect=create):
            with patch.object(self.interpreter, "_validate_profile", side_effect=validate):
                self.interpreter.get_profile_record("Crime dramas", "Horror")
        self.assertEqual(observed, ["pending", "responded"])

    def test_incomplete_malformed_and_invalid_profiles_retain_paid_responses(self):
        invalid_profile = {**self.profile, "liked_genres": ["Unrecognized"]}
        cases = [
            self.response(status="incomplete", incomplete_details={"reason": "max_output_tokens"}),
            self.response(output_text="not-json"),
            self.response(profile=invalid_profile),
            self.response(output_text="", output=[{"type": "message", "content": [{"type": "refusal", "refusal": "Unable to process"}]}]),
        ]
        for index, response in enumerate(cases):
            with self.subTest(status=response.status, text=response.output_text):
                with self.sdk(response):
                    with self.assertRaises(ColdStartError):
                        self.interpreter.get_profile_record(f"Crime dramas {index}", "Horror")
                attempts = self.rows("llm_attempts")
                self.assertEqual(len(attempts), index + 1)
                failed = next(attempt for attempt in attempts if attempt["response"] == response.envelope)
                self.assertEqual(failed["status"], "failed")
                self.assertTrue(failed["error_type"])
                self.assertEqual(self.rows("preference_profiles"), [])
                self.assertEqual(list(self.cache.glob("*.json")), [])

    def test_credential_redaction_covers_inputs_response_prose_error_body_and_headers(self):
        response = self.response(provider_extension={
            "password_hint": "response-field-secret", "description": "password=response-prose-secret",
            "nested": {"public": None, "input_tokens": 17},
        })
        with self.sdk(response):
            self.interpreter.get_profile_record("Crime dramas password=input-prose-secret", "Horror")
        body = {"error": {
            "message": "Incorrect API key provided: sdk-test-secret; token=error-body-secret",
            "extension": {"public": None, "password": "error-field-secret"},
        }}
        request = httpx.Request("POST", "https://api.openai.com/v1/responses")
        error_response = httpx.Response(400, json=body, request=request, headers={
            "authorization": "Bearer header-secret", "x-trace-id": "retained-trace",
        })
        error = APIStatusError("unfiltered-exception-message-secret", response=error_response, body=body)
        with self.sdk(side_effect=error):
            with self.assertRaises(ColdStartError) as raised:
                self.interpreter.get_profile_record("Different crime dramas", "Horror")
        encoded = json.dumps(self.rows("llm_attempts") + self.rows("preference_profiles") + self.rows("profile_uses"))
        for secret in (
            "response-field-secret", "response-prose-secret", "input-prose-secret", "error-body-secret",
            "error-field-secret", "header-secret", "sdk-test-secret", "unfiltered-exception-message-secret",
        ):
            self.assertNotIn(secret, encoded)
            self.assertNotIn(secret, str(raised.exception))
        self.assertIn("retained-trace", encoded)
        self.assertIn('"input_tokens": 17', encoded)
        self.assertIn('"public": null', encoded)

    def test_shared_cache_retains_distinct_users_and_source_snapshot_links(self):
        self.seed_snapshot(21, "source-snapshot-21")
        self.seed_snapshot(22, "source-snapshot-22")
        versions = {"dataset": "sha256:dataset-1", "model": "sha256:model-1"}
        with self.sdk() as (_, create, _):
            first, first_cached = self.interpreter.get_profile_record("Crime dramas", "Horror", context={
                "user_id": 21, "source_snapshot_id": "source-snapshot-21", "versions": versions,
            })
            second, second_cached = self.new_interpreter().get_profile_record("Crime dramas", "Horror", context={
                "user_id": 22, "source_snapshot_id": "source-snapshot-22", "versions": versions,
            })
        self.assertFalse(first_cached)
        self.assertTrue(second_cached)
        self.assertEqual(create.call_count, 1)
        self.assertEqual(first["_provenance"], second["_provenance"])
        uses = self.rows("profile_uses")
        contexts = [record.get("context", record) for record in uses]
        self.assertEqual({record["user_id"] for record in contexts}, {21, 22})
        with closing(sqlite3.connect(self.database)) as connection:
            for use, context in zip(uses, contexts):
                source = json.loads(connection.execute(
                    "SELECT record_json FROM metadata_snapshots WHERE snapshot_id=?",
                    (context["source_snapshot_id"],),
                ).fetchone()[0])
                self.assertEqual(int(source["entity_id"]), context["user_id"])
                self.assertEqual(context["versions"], versions)
                self.assertIn(first["_provenance"]["profile_id"], json.dumps(use))
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
        self.assertEqual(len(self.rows("llm_attempts")), 1)
        self.assertEqual(len(self.rows("preference_profiles")), 1)

    def test_offline_valid_legacy_cache_avoids_database_and_sdk_entirely(self):
        self.write_legacy()
        with patch("preferences.ProfileStore", side_effect=AssertionError("offline opened storage")):
            with patch("preferences.OpenAI", side_effect=AssertionError("offline called SDK")):
                with patch("preferences.dotenv_values", side_effect=AssertionError("offline read credentials")):
                    saved, cached = self.interpreter.get_profile_record("Crime dramas", "Horror", offline=True, context={"user_id": 21})
                    profile, cached_wrapper = self.interpreter.get_profile("Crime dramas", "Horror", offline=True)
        self.assertTrue(cached)
        self.assertTrue(cached_wrapper)
        self.assertEqual(saved["profile"], self.profile)
        self.assertEqual(profile, self.profile)
        self.assertFalse(self.database.exists())

    def test_offline_generated_cache_does_not_add_attempts_profiles_or_uses(self):
        with self.sdk():
            saved, _ = self.interpreter.get_profile_record("Crime dramas", "Horror", context={"user_id": 21})
        before = {table: self.rows(table) for table in ("llm_attempts", "preference_profiles", "profile_uses")}
        with patch("preferences.ProfileStore", side_effect=AssertionError("offline opened storage")):
            with patch("preferences.OpenAI", side_effect=AssertionError("offline called SDK")):
                reused, cached = self.new_interpreter().get_profile_record("Crime dramas", "Horror", offline=True, context={"user_id": 22})
        self.assertTrue(cached)
        self.assertEqual(reused, saved)
        self.assertEqual(before, {table: self.rows(table) for table in before})

    def test_missing_and_corrupt_offline_cache_never_create_storage_or_call_sdk(self):
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt):
                if corrupt:
                    self.write_legacy().write_text("{corrupt", encoding="utf-8")
                with patch("preferences.ProfileStore", side_effect=AssertionError("offline opened storage")):
                    with patch("preferences.OpenAI", side_effect=AssertionError("offline called SDK")):
                        with self.assertRaises(ColdStartError):
                            self.interpreter.get_profile_record("Crime dramas", "Horror", offline=True)
                self.assertFalse(self.database.exists())

    def test_legacy_import_is_durable_idempotent_and_does_not_fabricate_an_attempt(self):
        self.write_legacy(saved={"model": "legacy-model", "response_id": "historical-response", "usage": None, "profile": self.profile})
        with patch("preferences.OpenAI", side_effect=AssertionError("legacy import called SDK")):
            first, cached = self.interpreter.get_profile_record("Crime dramas", "Horror", context={"user_id": 21})
            second, second_cached = self.new_interpreter().get_profile_record("Crime dramas", "Horror", context={"user_id": 22})
        self.assertTrue(cached)
        self.assertTrue(second_cached)
        self.assertEqual(first["model"], "legacy-model")
        self.assertEqual(first["response_id"], "historical-response")
        self.assertEqual(first["_provenance"]["origin"], "legacy_cache")
        self.assertIsNone(first["_provenance"]["attempt_id"])
        self.assertEqual(first["_provenance"], second["_provenance"])
        self.assertEqual(self.rows("llm_attempts"), [])
        self.assertEqual(len(self.rows("preference_profiles")), 1)
        self.assertEqual(len(self.rows("profile_uses")), 2)
        with ProfileStore(self.database) as store:
            self.assertIsNotNone(store.get_profile(first["_provenance"]["profile_id"]))

    def test_corrupt_online_cache_is_replaced_by_a_new_audited_response(self):
        path = self.write_legacy()
        path.write_text("{corrupt", encoding="utf-8")
        with self.sdk() as (_, create, _):
            saved, cached = self.interpreter.get_profile_record("Crime dramas", "Horror")
        self.assertFalse(cached)
        self.assertEqual(create.call_count, 1)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), saved)
        self.assertEqual(len(self.rows("llm_attempts")), 1)
        self.assertEqual(len(self.rows("preference_profiles")), 1)

    def test_prompt_and_schema_changes_create_distinct_audited_profiles(self):
        with self.sdk() as (_, create, _):
            first, _ = self.interpreter.get_profile_record("Crime dramas", "Horror")
            with patch("preferences.INSTRUCTIONS", INSTRUCTIONS + "Additional deterministic instruction.\n"):
                second, second_cached = self.interpreter.get_profile_record("Crime dramas", "Horror")
            third, third_cached = self.new_interpreter(["Crime", "Drama", "Horror", "Science Fiction"]).get_profile_record("Crime dramas", "Horror")
        self.assertFalse(second_cached)
        self.assertFalse(third_cached)
        self.assertEqual(create.call_count, 3)
        a, b, c = [saved["_provenance"] for saved in (first, second, third)]
        self.assertNotEqual(a["cache_key"], b["cache_key"])
        self.assertNotEqual(a["cache_key"], c["cache_key"])
        self.assertNotEqual(a["prompt_version"], b["prompt_version"])
        self.assertEqual(a["schema_version"], b["schema_version"])
        self.assertNotEqual(a["schema_version"], c["schema_version"])
        self.assertEqual(a["profile_version"], b["profile_version"])
        self.assertEqual(len(self.rows("llm_attempts")), 3)

    def test_failed_cache_publication_retains_paid_response_and_records_safe_outcome(self):
        with self.sdk():
            with patch("preferences.os.replace", side_effect=OSError("private-cache-exception-secret")):
                try:
                    saved, cached = self.interpreter.get_profile_record("Crime dramas", "Horror", context={"user_id": 21})
                except ColdStartError as error:
                    self.assertNotIn("private-cache-exception-secret", str(error))
                else:
                    self.assertFalse(cached)
                    self.assertTrue(saved["_provenance"]["profile_id"])
        attempts = self.rows("llm_attempts")
        profiles = self.rows("preference_profiles")
        uses = self.rows("profile_uses")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["status"], "success")
        self.assertEqual(attempts[0]["response"]["id"], "response-real-1")
        self.assertEqual(len(profiles), 1)
        self.assertIn("cache_publish", json.dumps(uses))
        self.assertIn("failed", json.dumps(uses))
        self.assertNotIn("private-cache-exception-secret", json.dumps(attempts + profiles + uses))
        self.assertEqual(list(self.cache.iterdir()), [])

    def test_missing_cache_recovers_audited_profile_without_another_provider_request(self):
        with self.sdk():
            original, _ = self.interpreter.get_profile_record("Crime dramas", "Horror")
        path = self.cache / (original["_provenance"]["cache_key"] + ".json")
        path.unlink()
        with patch("preferences.OpenAI", side_effect=AssertionError("Recovery called provider")):
            recovered, cached = self.new_interpreter().get_profile_record("Crime dramas", "Horror")
        self.assertTrue(cached)
        self.assertEqual(recovered, original)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), original)
        self.assertEqual(len(self.rows("llm_attempts")), 1)
        self.assertEqual(len(self.rows("preference_profiles")), 1)

    def test_interrupted_cache_publication_recovers_the_successful_paid_response(self):
        with self.sdk() as (_, create, _):
            with patch("preferences.os.replace", side_effect=OSError("unpublished-cache")):
                with self.assertRaises(ColdStartError):
                    self.interpreter.get_profile_record("Crime dramas", "Horror", context={"user_id": 21})
            self.assertEqual(create.call_count, 1)
        with patch("preferences.OpenAI", side_effect=AssertionError("Recovery called provider")):
            recovered, cached = self.new_interpreter().get_profile_record(
                "Crime dramas", "Horror", context={"user_id": 21},
            )
        self.assertTrue(cached)
        self.assertEqual(recovered["response_id"], "response-real-1")
        self.assertEqual(len(self.rows("llm_attempts")), 1)
        self.assertEqual(len(self.rows("preference_profiles")), 1)
        self.assertEqual([row["outcome"] for row in self.rows("profile_uses")], ["cache_publish_failed", "available"])

    def test_profile_storage_upgrade_preserves_existing_requests(self):
        request = {"request_id": "existing-request", "started_at": "2026-10-09T12:00:00Z", "user_id": 21, "recommendations": []}
        with RequestStore(self.database) as store:
            store.save_request(request)
            baseline = store.get_request("existing-request")
        with self.sdk():
            saved, _ = self.interpreter.get_profile_record("Crime dramas", "Horror")
        with RequestStore(self.database) as reopened:
            self.assertEqual(reopened.get_request("existing-request"), baseline)
        with ProfileStore(self.database) as reopened:
            self.assertIsNotNone(reopened.get_attempt(saved["_provenance"]["attempt_id"]))
            self.assertIsNotNone(reopened.get_profile(saved["_provenance"]["profile_id"]))
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_completed_attempt_and_profile_replay_preserve_first_records_and_reject_conflicts(self):
        with self.sdk():
            saved, _ = self.interpreter.get_profile_record("Crime dramas", "Horror")
        with ProfileStore(self.database) as store:
            attempt = store.get_attempt(saved["_provenance"]["attempt_id"])
            profile = store.get_profile(saved["_provenance"]["profile_id"])
            self.assertFalse(store.update_attempt(attempt))
            store.save_profile(profile)
            with self.assertRaises(StorageError):
                store.update_attempt({**attempt, "response": {**attempt["response"], "model": "different-provider-model"}})
            changed = copy.deepcopy(profile)
            changed["profile"]["likes_summary"] = "A changed interpretation."
            changed["content_version"] = content_version(changed["profile"])
            with self.assertRaises(StorageError):
                store.save_profile(changed)
            self.assertEqual(store.get_attempt(attempt["attempt_id"]), attempt)
            self.assertEqual(store.get_profile(profile["profile_id"]), profile)
        self.assertEqual(len(self.rows("llm_attempts")), 1)
        self.assertEqual(len(self.rows("preference_profiles")), 1)

    def test_success_and_profile_insert_roll_back_together_when_profile_is_invalid(self):
        with self.sdk():
            saved, _ = self.interpreter.get_profile_record("Crime dramas", "Horror")
        with ProfileStore(self.database) as store:
            original = store.get_attempt(saved["_provenance"]["attempt_id"])
            profile = store.get_profile(saved["_provenance"]["profile_id"])
            pending = store.start_attempt({
                **original, "attempt_id": "rollback-attempt", "started_at": datetime.now(timezone.utc).isoformat(),
            })
            completed = {**pending, "status": "success", "finished_at": datetime.now(timezone.utc).isoformat()}
            invalid = {
                **profile, "profile_id": "rollback-profile", "attempt_id": "rollback-attempt",
                "created_at": completed["finished_at"], "content_version": "sha256:incorrect",
            }
            with self.assertRaises(ValueError):
                store.update_attempt(completed, invalid)
            self.assertEqual(store.get_attempt("rollback-attempt"), pending)
            self.assertIsNone(store.get_profile("rollback-profile"))
            self.assertEqual(store.get_attempt(original["attempt_id"]), original)
            self.assertEqual(store.get_profile(profile["profile_id"]), profile)

    def test_online_cache_rejects_reference_to_a_profile_generated_for_different_input(self):
        with self.sdk() as (_, create, _):
            first, _ = self.interpreter.get_profile_record("Crime dramas", "Horror")
            second, _ = self.interpreter.get_profile_record("Drama stories", "Horror")
            altered = copy.deepcopy(first)
            altered["_provenance"]["profile_id"] = second["_provenance"]["profile_id"]
            path = self.cache / f"{first['_provenance']['cache_key']}.json"
            path.write_text(json.dumps(altered), encoding="utf-8")
            repaired, cached = self.interpreter.get_profile_record("Crime dramas", "Horror")
        self.assertFalse(cached)
        self.assertEqual(create.call_count, 3)
        self.assertNotEqual(repaired["_provenance"]["profile_id"], second["_provenance"]["profile_id"])
        self.assertEqual(repaired["_provenance"]["cache_key"], first["_provenance"]["cache_key"])
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), repaired)
        with ProfileStore(self.database) as store:
            record = store.get_profile(first["_provenance"]["profile_id"])
            other = store.get_profile(second["_provenance"]["profile_id"])
            replacement = store.get_profile(repaired["_provenance"]["profile_id"])
            self.assertEqual(replacement["cache_key"], first["_provenance"]["cache_key"])
            self.assertEqual(replacement["content_version"], repaired["_provenance"]["profile_version"])
        self.assertEqual(record["cache_key"], first["_provenance"]["cache_key"])
        self.assertEqual(other["cache_key"], second["_provenance"]["cache_key"])
        self.assertEqual(len(self.rows("llm_attempts")), 3)
        self.assertEqual(len(self.rows("preference_profiles")), 3)

    def test_startup_import_skips_malformed_provenance_and_deep_json(self):
        path = self.write_legacy()
        cases = (
            json.dumps({"profile": self.profile, "_provenance": "broken"}),
            '{"profile":' + "[" * 1500 + "null" + "]" * 1500 + "}",
        )
        for raw in cases:
            with self.subTest(raw_size=len(raw)):
                path.write_text(raw, encoding="utf-8")
                with patch("preferences.OpenAI", side_effect=AssertionError("startup import called SDK")):
                    with self.assertLogs("preferences", level="WARNING"):
                        self.assertEqual(self.interpreter.import_cache_profiles(), 0)
                self.assertEqual(self.rows("preference_profiles"), [])
                self.assertEqual(self.rows("llm_attempts"), [])

    def test_offline_cache_rejects_incomplete_or_unknown_provenance(self):
        with self.sdk():
            original, _ = self.interpreter.get_profile_record("Crime dramas", "Horror")
        path = self.cache / f"{original['_provenance']['cache_key']}.json"
        for field, value in (
            ("origin", "invented"), ("profile_id", None), ("attempt_id", None),
            ("model", 77), ("response_id", {"unexpected": "object"}),
        ):
            with self.subTest(field=field):
                altered = copy.deepcopy(original)
                target = altered if field in ("model", "response_id") else altered["_provenance"]
                target[field] = value
                path.write_text(json.dumps(altered), encoding="utf-8")
                with patch("preferences.ProfileStore", side_effect=AssertionError("offline opened storage")):
                    with patch("preferences.OpenAI", side_effect=AssertionError("offline called SDK")):
                        with self.assertRaises(ColdStartError):
                            self.interpreter.get_profile_record("Crime dramas", "Horror", offline=True)

    def test_sdk_client_cleanup_failure_retains_the_received_paid_response(self):
        response = self.response()
        with self.sdk(response) as (factory, create, _):
            factory.return_value.__exit__.side_effect = RuntimeError("private-client-close-secret")
            with self.assertRaises(ColdStartError) as raised:
                self.interpreter.get_profile_record("Crime dramas", "Horror")
        self.assertEqual(create.call_count, 1)
        attempts = self.rows("llm_attempts")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["status"], "failed")
        self.assertEqual(attempts[0]["error_type"], "RuntimeError")
        self.assertEqual(attempts[0]["response"], response.envelope)
        self.assertEqual(attempts[0]["response_id"], response.id)
        self.assertEqual(attempts[0]["usage"], response.envelope["usage"])
        self.assertIsNotNone(attempts[0]["response_received_at"])
        self.assertEqual(self.rows("preference_profiles"), [])
        self.assertNotIn("private-client-close-secret", json.dumps(attempts))
        self.assertNotIn("private-client-close-secret", str(raised.exception))

    def test_changed_provider_summaries_fail_offline_and_regenerate_online_with_originals_retained(self):
        with self.sdk() as (_, create, _):
            original, _ = self.interpreter.get_profile_record("Crime dramas", "Horror")
            path = self.cache / f"{original['_provenance']['cache_key']}.json"
            with ProfileStore(self.database) as store:
                original_attempt = store.get_attempt(original["_provenance"]["attempt_id"])
                original_profile = store.get_profile(original["_provenance"]["profile_id"])
            cases = (
                ("model", "different-provider-model"),
                ("response_id", "different-provider-response"),
                ("usage", {"input_tokens": 9999, "output_tokens": 7777, "total_tokens": 17776}),
            )
            for index, (field, value) in enumerate(cases, 2):
                with self.subTest(field=field):
                    altered = copy.deepcopy(original)
                    altered[field] = value
                    path.write_text(json.dumps(altered), encoding="utf-8")
                    with patch("preferences.ProfileStore", side_effect=AssertionError("offline opened storage")):
                        with self.assertRaises(ColdStartError):
                            self.interpreter.get_profile_record("Crime dramas", "Horror", offline=True)
                    repaired, cached = self.interpreter.get_profile_record("Crime dramas", "Horror")
                    self.assertFalse(cached)
                    self.assertEqual(create.call_count, index)
                    self.assertNotEqual(repaired["_provenance"]["profile_id"], original["_provenance"]["profile_id"])
                    for summary in ("model", "response_id", "usage"):
                        self.assertEqual(repaired[summary], original[summary])
                    self.assertEqual(json.loads(path.read_text(encoding="utf-8")), repaired)
                    with ProfileStore(self.database) as store:
                        self.assertEqual(store.get_attempt(original_attempt["attempt_id"]), original_attempt)
                        self.assertEqual(store.get_profile(original_profile["profile_id"]), original_profile)
        self.assertEqual(len(self.rows("llm_attempts")), 4)
        self.assertEqual(len(self.rows("preference_profiles")), 4)

    def test_real_sdk_response_preserves_absent_fields_explicit_nulls_and_unknown_extensions(self):
        source = {
            "id": "real-sdk-response", "model": "returned-model-snapshot", "status": "completed",
            "error": None,
            "output": [{
                "id": "real-sdk-message", "type": "message", "role": "assistant", "status": "completed",
                "content": [{"type": "output_text", "text": json.dumps(self.profile), "annotations": []}],
            }],
            "usage": {
                "input_tokens": 17, "output_tokens": 23, "total_tokens": 40,
                "input_tokens_details": {"cached_tokens": 0, "provider_detail": None},
                "output_tokens_details": {"reasoning_tokens": 4},
                "provider_usage_extension": {"nullable": None, "observed": True},
            },
            "provider_extension": {"nullable": None, "nested": [{"unrecognized": [1, 2]}]},
        }
        response = Response.model_construct(**source)
        with self.sdk(response):
            saved, cached = self.interpreter.get_profile_record("Crime dramas", "Horror")
        self.assertFalse(cached)
        self.assertEqual(saved["usage"], source["usage"])
        with ProfileStore(self.database) as store:
            attempt = store.get_attempt(saved["_provenance"]["attempt_id"])
        self.assertEqual(attempt["response"], source)
        self.assertEqual(attempt["usage"], source["usage"])
        self.assertIn("error", attempt["response"])
        self.assertIsNone(attempt["response"]["error"])
        self.assertNotIn("incomplete_details", attempt["response"])
        self.assertNotIn("metadata", attempt["response"])
        self.assertNotIn("logprobs", attempt["response"]["output"][0]["content"][0])
        self.assertIsNone(attempt["response"]["provider_extension"]["nullable"])


if __name__ == "__main__":
    unittest.main()
