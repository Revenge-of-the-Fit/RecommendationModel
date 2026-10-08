import pytest

from model_comparison.backoff import with_backoff


class Transient(Exception):
    pass


def test_retries_then_succeeds_with_exponential_delays():
    calls, delays = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise Transient()
        return "ok"

    wrapped = with_backoff(flaky, (Transient,), base_delay=2.0, sleep=delays.append)
    assert wrapped() == "ok"
    assert delays == [2.0, 4.0]


def test_gives_up_after_max_attempts():
    delays = []

    def always():
        raise Transient("429")

    wrapped = with_backoff(always, (Transient,), max_attempts=3, sleep=delays.append)
    with pytest.raises(Transient):
        wrapped()
    assert len(delays) == 2


def test_give_up_predicate_stops_retries_immediately():
    delays, calls = [], []

    def out_of_credit():
        calls.append(1)
        raise Transient("insufficient_quota")

    wrapped = with_backoff(
        out_of_credit, (Transient,), give_up=lambda error: "quota" in str(error), sleep=delays.append
    )
    with pytest.raises(Transient):
        wrapped()
    assert len(calls) == 1 and delays == []


def test_other_errors_are_not_retried():
    delays = []

    def broken():
        raise ValueError("bad input")

    wrapped = with_backoff(broken, (Transient,), sleep=delays.append)
    with pytest.raises(ValueError):
        wrapped()
    assert delays == []
