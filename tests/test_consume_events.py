import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import consume_events
from storage.events import EventStore
from test_event_consumer import FakeConsumer, FakeMessage


class ConsumeEventsCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "events.sqlite3"
        self.config = Path(self.temporary.name) / "mlip-kafka.conf"
        self.config.write_text("bootstrap.servers=localhost:9092\n", encoding="utf-8")
        self.arguments = [
            "consume_events.py", "--config", str(self.config),
            "--storage-path", str(self.path), "--group-id", "validation-group",
            "--source-id", "stable-cluster-id", "--max-messages", "1",
        ]

    def tearDown(self):
        self.temporary.cleanup()

    def test_cli_defaults_to_movielog2_persists_and_prints_counts(self):
        consumer = FakeConsumer([FakeMessage()])
        output = io.StringIO()
        previous_handler = object()
        with patch.object(sys, "argv", self.arguments):
            with patch("consume_events.Consumer", return_value=consumer) as factory:
                with patch("consume_events.signal.signal", return_value=previous_handler) as handlers:
                    with patch("consume_events.logging.basicConfig"), redirect_stdout(output):
                        self.assertEqual(consume_events.main(), 0)
        counts = json.loads(output.getvalue())
        self.assertEqual(counts["stored"], 1)
        self.assertEqual(counts["committed"], 1)
        self.assertEqual(consumer.subscriptions[0][0], ["movielog2"])
        self.assertEqual(factory.call_args.args[0]["group.id"], "validation-group")
        self.assertIsInstance(factory.call_args.kwargs["logger"], consume_events.SafeKafkaLogger)
        self.assertEqual(consumer.closed, 1)
        with EventStore(self.path) as store:
            self.assertIsNotNone(store.get_event("stable-cluster-id", "movielog2", 2, 100))
        restored = [call for call in handlers.call_args_list if call.args[1] is previous_handler]
        self.assertEqual(len(restored), 2)

    def test_bounded_run_without_any_records_returns_failure(self):
        output = io.StringIO()
        stats = {name: 0 for name in ("received", "stored", "duplicates", "parsed", "unrecognized", "failed", "committed")}
        with patch.object(sys, "argv", self.arguments):
            with patch("consume_events.Consumer", return_value=FakeConsumer()):
                with patch("consume_events.run_consumer", return_value=stats):
                    with patch("consume_events.signal.signal"), patch("consume_events.logging.basicConfig"):
                        with redirect_stdout(output):
                            self.assertEqual(consume_events.main(), 1)
        self.assertEqual(json.loads(output.getvalue())["received"], 0)

    def test_failure_logs_only_category_and_restores_signal_handlers(self):
        previous_handler = object()
        with patch.object(sys, "argv", self.arguments):
            with patch("consume_events.Consumer", side_effect=RuntimeError("credential-sentinel")):
                with patch("consume_events.signal.signal", return_value=previous_handler) as handlers:
                    with patch("consume_events.logging.basicConfig"):
                        with self.assertLogs("consume_events", level="ERROR") as captured:
                            self.assertEqual(consume_events.main(), 1)
        self.assertNotIn("credential-sentinel", " ".join(captured.output))
        self.assertIn("RuntimeError", " ".join(captured.output))
        restored = [call for call in handlers.call_args_list if call.args[1] is previous_handler]
        self.assertEqual(len(restored), 2)

    def test_cli_requires_source_group_and_positive_finite_limits(self):
        cases = (
            ["consume_events.py", "--source-id", "cluster"],
            ["consume_events.py", "--group-id", "group"],
            [*self.arguments, "--max-messages", "0"],
            [*self.arguments, "--idle-timeout", "nan"],
            [*self.arguments, "--idle-timeout", "inf"],
            [*self.arguments, "--busy-timeout", "0"],
            [*self.arguments, "--run-as-uid", "10001"],
            [*self.arguments, "--run-as-gid", "10001"],
            [*self.arguments, "--run-as-uid", "0", "--run-as-gid", "10001"],
            [*self.arguments, "--run-as-uid", "10001", "--run-as-gid", "-1"],
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                with patch.object(sys, "argv", arguments), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as raised:
                        consume_events.main()
                self.assertEqual(raised.exception.code, 2)


    def test_private_config_is_read_before_permissions_drop_and_storage_opens_afterward(self):
        order = []
        original_loader = consume_events.load_kafka_config
        def load(*arguments):
            order.append("config")
            return original_loader(*arguments)
        def drop(uid, gid):
            self.assertEqual((uid, gid), (10001, 10001))
            order.append("permissions")
        def store(*arguments):
            order.append("storage")
            return EventStore(*arguments)
        def consumer(*_arguments, **_keywords):
            order.append("client")
            return FakeConsumer([FakeMessage()])
        with patch.object(sys, "argv", [*self.arguments, "--run-as-uid", "10001", "--run-as-gid", "10001"]), \
                patch("consume_events.load_kafka_config", side_effect=load), \
                patch("consume_events.drop_permissions", side_effect=drop), \
                patch("consume_events.EventStore", side_effect=store), \
                patch("consume_events.Consumer", side_effect=consumer), \
                patch("consume_events.signal.signal"), patch("consume_events.logging.basicConfig"), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(consume_events.main(), 0)
        self.assertEqual(order, ["config", "permissions", "storage", "client"])

    def test_failed_permissions_drop_does_not_open_storage_or_create_client(self):
        with patch.object(sys, "argv", [*self.arguments, "--run-as-uid", "10001", "--run-as-gid", "10001"]), \
                patch("consume_events.drop_permissions", side_effect=PermissionError("secret-sentinel")), \
                patch("consume_events.EventStore") as store, patch("consume_events.Consumer") as consumer, \
                patch("consume_events.signal.signal"), patch("consume_events.logging.basicConfig"), \
                self.assertLogs("consume_events", level="ERROR") as captured:
            self.assertEqual(consume_events.main(), 1)
        store.assert_not_called()
        consumer.assert_not_called()
        self.assertIn("PermissionError", str(captured.output))
        self.assertNotIn("secret-sentinel", str(captured.output))


class ProcessIdentityTests(unittest.TestCase):
    def posix(self, uid, gid):
        return patch.multiple(consume_events.os, create=True, name="posix",
                              getuid=Mock(return_value=uid), geteuid=Mock(return_value=uid),
                              getgid=Mock(return_value=gid), getegid=Mock(return_value=gid),
                              setgroups=Mock(), setgid=Mock(), setuid=Mock())

    def test_without_identity_flags_existing_platform_behavior_is_preserved(self):
        with patch.object(consume_events.os, "name", "nt"):
            consume_events.drop_permissions(None, None)

    def test_root_drops_supplementary_groups_then_group_then_user(self):
        order = []
        with self.posix(0, 0):
            consume_events.os.setgroups.side_effect = lambda groups: order.append(("groups", groups))
            consume_events.os.setgid.side_effect = lambda gid: order.append(("gid", gid))
            consume_events.os.setuid.side_effect = lambda uid: order.append(("uid", uid))
            consume_events.drop_permissions(10001, 10001)
        self.assertEqual(order, [("groups", []), ("gid", 10001), ("uid", 10001)])

    def test_matching_nonroot_identity_requires_no_permission_changes(self):
        with self.posix(10001, 10001):
            consume_events.drop_permissions(10001, 10001)
            consume_events.os.setgroups.assert_not_called()
            consume_events.os.setgid.assert_not_called()
            consume_events.os.setuid.assert_not_called()

    def test_different_nonroot_identity_is_rejected_before_any_changes(self):
        with self.posix(10002, 10002):
            with self.assertRaises(PermissionError):
                consume_events.drop_permissions(10001, 10001)
            consume_events.os.setgroups.assert_not_called()
            consume_events.os.setgid.assert_not_called()
            consume_events.os.setuid.assert_not_called()

    def test_identity_flags_on_windows_are_rejected(self):
        with patch.object(consume_events.os, "name", "nt"), self.assertRaises(OSError):
            consume_events.drop_permissions(10001, 10001)


if __name__ == "__main__":
    unittest.main()
