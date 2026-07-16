"""
Airbnb Streaming DE — Lambda Handler
======================================
Wraps the data generator for AWS Lambda invocation.

EventBridge triggers this every 30 minutes with:
  {"stream_type": "all"}

You can also invoke it manually with specific stream types:
  {"stream_type": "events"}       → only listing_events
  {"stream_type": "transactions"} → only bookings + reviews
  {"stream_type": "dimensions"}   → only listings/hosts/guests/calendar

Or test a specific entity:
  {"stream_type": "events", "count": 5}
"""

import json
import logging
log = logging.getLogger()
log.setLevel(logging.INFO)

VALID_STREAM_TYPES = {"all", "events", "transactions", "dimensions"}
MAX_EVENT_COUNT = 5000


def handler(event: dict, context) -> dict:
    """
    Lambda entry point.
    'event' comes from EventBridge or direct invocation.
    'context' is the Lambda execution context (we don't use it).
    """
    stream_type = event.get("stream_type", "all")
    count = event.get("count", 50)  # only used for events stream

    if stream_type not in VALID_STREAM_TYPES:
        raise ValueError(f"Invalid stream_type: {stream_type}")
    if not isinstance(count, int) or isinstance(count, bool) or not 1 <= count <= MAX_EVENT_COUNT:
        raise ValueError(f"count must be an integer between 1 and {MAX_EVENT_COUNT}")

    log.info(f"Invoked with stream_type={stream_type}, count={count}")

    try:
        # Import here so Lambda cold-start errors are caught in the try/except
        from generator import run_events_batch, run_transactions_batch, run_dimensions_batch

        results = {}

        if stream_type in ("events", "all"):
            run_events_batch(count=count)
            results["events"] = f"{count} listing_events published"

        if stream_type in ("transactions", "all"):
            run_transactions_batch()
            results["transactions"] = "15 initial bookings + 10 updates + 30 linked reviews published"

        if stream_type in ("dimensions", "all"):
            run_dimensions_batch()
            results["dimensions"] = "10 listings + 5 hosts + 20 guests + 100 calendar entries published"

        log.info(f"Run complete: {results}")
        return {
            "statusCode": 200,
            "body": json.dumps({"status": "success", "results": results}),
        }

    except Exception:
        # Raising marks asynchronous EventBridge invocations as failed so Lambda
        # retries and configured dead-letter destinations can operate correctly.
        log.exception("Generator failed")
        raise
