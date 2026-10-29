"""Fail any test that tries to reach a host outside this machine.

Tests must not touch production Kafka, the course APIs, or an LLM provider. Loopback
(for tests that start a local HTTP server) and unix sockets stay allowed. Name lookups
are blocked too, since they leave the machine.

This covers Python-level sockets, which is what HTTP clients and the LLM SDK use. The Kafka
client opens its own native sockets, which Python cannot intercept, so creating a real Kafka
Consumer or Producer is blocked instead. Tests inject fakes for Kafka.
"""
import socket


LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_original = {}


class NetworkAccessError(RuntimeError):
    """Raised instead of OSError so code under test cannot swallow it as a connection failure."""


def _check(host) -> None:
    if isinstance(host, bytes):
        host = host.decode(errors="replace")
    if host not in LOCAL_HOSTS and host not in (None, ""):
        raise NetworkAccessError(f"A test tried to reach {host!r}; tests may only use loopback")


def _guard_address(original):
    def guarded(self, address, *args, **kwargs):
        # A string or bytes address is a unix socket path
        if isinstance(address, tuple):
            _check(address[0])
        return original(self, address, *args, **kwargs)

    return guarded


def _block_kafka_clients() -> None:
    try:
        import confluent_kafka
    except ImportError:
        return

    def blocked(*args, **kwargs):
        raise NetworkAccessError("A test tried to create a real Kafka client; inject a fake instead")

    for name in ("Consumer", "Producer"):
        _original[name] = getattr(confluent_kafka, name)
        setattr(confluent_kafka, name, blocked)


def install() -> None:
    if _original:
        return
    _original["connect"] = socket.socket.connect
    _original["connect_ex"] = socket.socket.connect_ex
    _original["getaddrinfo"] = socket.getaddrinfo

    def getaddrinfo(host, *args, **kwargs):
        _check(host)
        return _original["getaddrinfo"](host, *args, **kwargs)

    socket.socket.connect = _guard_address(_original["connect"])
    socket.socket.connect_ex = _guard_address(_original["connect_ex"])
    socket.getaddrinfo = getaddrinfo
    _block_kafka_clients()
