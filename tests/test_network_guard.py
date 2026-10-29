import socket
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))

import network_guard
from network_guard import NetworkAccessError


def setUpModule():
    # Already installed by conftest.py under pytest; this keeps the module runnable under unittest
    network_guard.install()


class NetworkGuardTests(unittest.TestCase):
    def test_external_hosts_are_blocked_before_any_lookup_or_connection(self):
        with self.assertRaisesRegex(NetworkAccessError, "example.com"):
            socket.getaddrinfo("example.com", 80)
        with self.assertRaisesRegex(NetworkAccessError, "example.com"):
            socket.create_connection(("example.com", 80), timeout=1)
        with self.assertRaisesRegex(NetworkAccessError, "203.0.113.7"):
            socket.socket().connect(("203.0.113.7", 9092))
        with self.assertRaisesRegex(NetworkAccessError, "203.0.113.7"):
            socket.socket().connect_ex(("203.0.113.7", 9092))

    def test_the_error_cannot_be_mistaken_for_a_connection_failure(self):
        self.assertFalse(issubclass(NetworkAccessError, OSError))

    def test_loopback_is_allowed(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(204)
                self.end_headers()

            def log_message(self, *_):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.handle_request)
        thread.start()
        try:
            with socket.create_connection(("127.0.0.1", server.server_port), timeout=5) as client:
                client.sendall(b"GET / HTTP/1.0\r\n\r\n")
                self.assertTrue(client.recv(64).startswith(b"HTTP/1.0 204"))
        finally:
            thread.join()
            server.server_close()

    def test_unix_sockets_are_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "s")
            with socket.socket(socket.AF_UNIX) as server:
                server.bind(path)
                server.listen()
                with socket.socket(socket.AF_UNIX) as client:
                    client.connect(path)


if __name__ == "__main__":
    unittest.main()
