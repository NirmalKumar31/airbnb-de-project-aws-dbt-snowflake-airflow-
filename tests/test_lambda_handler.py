import importlib.util
import sys
import types
from pathlib import Path

import pytest


HANDLER_PATH = Path(__file__).parents[1] / "lambda" / "lambda_handler.py"
SPEC = importlib.util.spec_from_file_location("airbnb_lambda_handler", HANDLER_PATH)
handler_module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(handler_module)


def test_rejects_invalid_stream_type():
    with pytest.raises(ValueError, match="Invalid stream_type"):
        handler_module.handler({"stream_type": "unknown"}, None)


@pytest.mark.parametrize("count", [0, -1, 5001, "50", True])
def test_rejects_invalid_event_count(count):
    with pytest.raises(ValueError, match="count must be an integer"):
        handler_module.handler({"stream_type": "events", "count": count}, None)


def test_generator_failure_is_raised_for_lambda_retry(monkeypatch):
    fake_generator = types.ModuleType("generator")

    def fail(**_kwargs):
        raise RuntimeError("publish failed")

    fake_generator.run_events_batch = fail
    fake_generator.run_transactions_batch = lambda: None
    fake_generator.run_dimensions_batch = lambda: None
    monkeypatch.setitem(sys.modules, "generator", fake_generator)

    with pytest.raises(RuntimeError, match="publish failed"):
        handler_module.handler({"stream_type": "events", "count": 5}, None)
