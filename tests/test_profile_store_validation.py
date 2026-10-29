"""Rejection paths of ProfileStore: the audit trail must refuse inconsistent records."""
import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from storage.database import StorageError
from storage.profiles import ProfileStore, content_version, utc_timestamp


# The store keeps timestamps in normalized UTC form and compares them exactly on update
START = utc_timestamp("2026-03-01T10:00:00+00:00")
LATER = utc_timestamp("2026-03-01T10:05:00+00:00")
EARLIER = utc_timestamp("2026-03-01T09:00:00+00:00")
PROFILE = {"liked_genres": ["Drama"], "excluded_genres": []}


class ProfileStoreValidationTests(unittest.TestCase):
    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._directory.cleanup)
        self.store = ProfileStore(Path(self._directory.name) / "profiles.sqlite3")
        self.addCleanup(self.store.close)

    def attempt(self, **changes):
        return {
            "attempt_id": "attempt-1", "cache_key": "key-1", "started_at": START,
            "request": {"likes": "dramas"}, "versions": {"prompt": "p1"}, **changes,
        }

    def profile_record(self, **changes):
        return {
            "profile_id": "profile-1", "cache_key": "key-1", "created_at": LATER,
            "attempt_id": "attempt-1", "origin": "llm", "profile": PROFILE,
            "content_version": content_version(PROFILE), "versions": {"prompt": "p1"}, **changes,
        }

    def finish(self, **changes):
        return self.attempt(status="success", finished_at=LATER, **changes)

    def successful_profile(self):
        self.store.start_attempt(self.attempt())
        self.store.update_attempt(self.finish(), profile=self.profile_record())

    # timestamps and identifiers

    def test_timestamps_require_a_timezone_and_are_normalized_to_utc(self):
        with self.assertRaisesRegex(ValueError, "require a timezone"):
            utc_timestamp("2026-03-01T10:00:00")
        self.assertEqual(
            utc_timestamp("2026-03-01T12:00:00+02:00"), "2026-03-01T10:00:00.000000+00:00"
        )

    def test_identifiers_must_be_nonempty_strings(self):
        for field in ["attempt_id", "cache_key"]:
            for bad in [None, "", "  ", 5]:
                with self.subTest(field=field, value=bad):
                    with self.assertRaisesRegex(ValueError, "nonempty strings"):
                        self.store.start_attempt(self.attempt(**{field: bad}))

    def test_attempts_must_reference_metadata_for_the_same_user(self):
        with self.assertRaisesRegex(ValueError, "same user"):
            self.store.start_attempt(self.attempt(user_id=7, source_snapshot_id="no-such-snapshot"))

    # attempts

    def test_restarting_an_attempt_is_idempotent_but_conflicts_are_rejected(self):
        first = self.store.start_attempt(self.attempt())
        self.assertEqual(self.store.start_attempt(self.attempt()), first)
        with self.assertRaisesRegex(StorageError, "different data"):
            self.store.start_attempt(self.attempt(request={"likes": "comedies"}))
        self.assertEqual(self.store.get_attempt("attempt-1")["status"], "pending")

    def test_update_requires_a_started_attempt_with_unchanged_inputs(self):
        with self.assertRaisesRegex(StorageError, "must be started"):
            self.store.update_attempt(self.finish())
        self.store.start_attempt(self.attempt())
        for field, value in [("cache_key", "other"), ("request", {}), ("versions", {}), ("started_at", EARLIER)]:
            with self.subTest(field=field):
                with self.assertRaisesRegex(StorageError, "inputs cannot change"):
                    self.store.update_attempt(self.finish(**{field: value}))

    def test_update_rejects_invalid_outcomes(self):
        self.store.start_attempt(self.attempt())
        with self.assertRaisesRegex(ValueError, "Invalid LLM processing outcome"):
            self.store.update_attempt(self.attempt(status="pending"))
        with self.assertRaisesRegex(ValueError, "requires a response"):
            self.store.update_attempt(self.attempt(status="responded"))
        with self.assertRaisesRegex(ValueError, "precedes its start"):
            self.store.update_attempt(self.attempt(status="failed", finished_at=EARLIER))

    def test_a_retained_response_cannot_change(self):
        self.store.start_attempt(self.attempt())
        self.store.update_attempt(self.attempt(status="responded", response={"id": "r1"}))
        self.assertEqual(self.store.get_attempt("attempt-1")["status"], "responded")
        with self.assertRaisesRegex(StorageError, "response cannot change"):
            self.store.update_attempt(self.attempt(status="responded", response={"id": "r2"}))

    def test_completed_attempts_are_immutable(self):
        self.store.start_attempt(self.attempt())
        failed = self.attempt(status="failed", finished_at=LATER)
        self.assertTrue(self.store.update_attempt(failed))
        self.assertFalse(self.store.update_attempt(failed))
        with self.assertRaisesRegex(StorageError, "cannot change"):
            self.store.update_attempt(self.finish())

    def test_profiles_only_attach_to_successful_matching_attempts(self):
        self.store.start_attempt(self.attempt())
        with self.assertRaisesRegex(ValueError, "successful LLM attempts"):
            self.store.update_attempt(
                self.attempt(status="failed", finished_at=LATER), profile=self.profile_record()
            )
        with self.assertRaisesRegex(ValueError, "successful LLM attempts"):
            self.store.update_attempt(self.finish(), profile=self.profile_record(attempt_id="other"))
        self.assertIsNone(self.store.get_profile("profile-1"))

    # profiles

    def test_profile_contents_must_match_their_version(self):
        self.store.start_attempt(self.attempt())
        self.store.update_attempt(self.finish())
        with self.assertRaisesRegex(ValueError, "must match their retained contents"):
            self.store.save_profile(self.profile_record(content_version="sha256:wrong"))
        with self.assertRaisesRegex(ValueError, "must be objects"):
            self.store.save_profile(self.profile_record(profile=["Drama"]))

    def test_profile_origin_must_agree_with_its_attempt(self):
        with self.assertRaisesRegex(ValueError, "identify generated or legacy"):
            self.store.save_profile(self.profile_record(origin="guess"))
        with self.assertRaisesRegex(ValueError, "successful matching attempt"):
            self.store.save_profile(self.profile_record())  # attempt never started
        self.store.start_attempt(self.attempt())
        with self.assertRaisesRegex(ValueError, "successful matching attempt"):
            self.store.save_profile(self.profile_record())  # attempt still pending
        with self.assertRaisesRegex(ValueError, "cannot claim an audited attempt"):
            self.store.save_profile(self.profile_record(origin="legacy_cache"))
        legacy = self.store.save_profile(self.profile_record(origin="legacy_cache", attempt_id=None))
        self.assertEqual(self.store.get_profile("profile-1"), legacy)

    def test_saved_profiles_are_idempotent_and_never_overwritten(self):
        self.successful_profile()
        saved = self.store.get_profile("profile-1")
        self.assertEqual(self.store.save_profile(self.profile_record()), saved)
        changed = {"liked_genres": ["Horror"], "excluded_genres": []}
        with self.assertRaisesRegex(StorageError, "different data"):
            self.store.save_profile(
                self.profile_record(profile=changed, content_version=content_version(changed))
            )
        self.assertEqual(self.store.latest_profile("key-1"), saved)

    # uses and listings

    def test_profile_use_must_follow_an_existing_profile(self):
        use = {"use_id": "use-1", "profile_id": "profile-1", "used_at": LATER}
        with self.assertRaisesRegex(ValueError, "must follow its availability"):
            self.store.record_use(use)
        self.successful_profile()
        with self.assertRaisesRegex(ValueError, "must follow its availability"):
            self.store.record_use({**use, "used_at": EARLIER})
        recorded = self.store.record_use(use)
        self.assertEqual(self.store.record_use(use), recorded)
        with self.assertRaisesRegex(StorageError, "different data"):
            self.store.record_use({**use, "used_at": utc_timestamp("2026-03-01T11:00:00+00:00")})

    def test_listing_limits_are_enforced(self):
        self.successful_profile()
        for limit in [0, 10001]:
            with self.subTest(limit=limit):
                with self.assertRaisesRegex(ValueError, "between 1 and 10000"):
                    self.store.list_attempts(limit)
                with self.assertRaisesRegex(ValueError, "between 1 and 10000"):
                    self.store.list_profiles("key-1", limit)
        self.assertEqual(len(self.store.list_attempts()), 1)
        self.assertEqual(len(self.store.list_profiles("key-1")), 1)
        self.assertEqual(self.store.list_profiles("other-key"), [])
        self.assertIsNone(self.store.latest_profile("other-key"))


if __name__ == "__main__":
    unittest.main()
