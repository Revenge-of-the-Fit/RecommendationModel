import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch


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
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                with patch.object(sys, "argv", arguments), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as raised:
                        consume_events.main()
                self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
