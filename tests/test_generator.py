import importlib.util
from collections import defaultdict
from pathlib import Path


GENERATOR_PATH = Path(__file__).parents[1] / "lambda" / "generator.py"
SPEC = importlib.util.spec_from_file_location("airbnb_generator", GENERATOR_PATH)
generator = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(generator)


def normalize_uuid(value: str) -> str:
    return value.lower().replace("-", "")


def test_dirty_uid_preserves_identity():
    source = generator.HOST_IDS[0]
    for _ in range(25):
        assert normalize_uuid(generator.dirty_uid(source)) == normalize_uuid(source)


def test_listing_preserves_stable_host_relationship():
    listing_id = generator.ACTIVE_LISTING_IDS[0]
    listing = generator.generate_listing(listing_id)
    assert normalize_uuid(listing["listing_id"]) == normalize_uuid(listing_id)
    assert normalize_uuid(listing["host_id"]) == normalize_uuid(
        generator.LISTING_TO_HOST[listing_id]
    )


def test_booking_update_preserves_business_identity_and_immutable_values():
    booking_num = generator._BOOKING_BASE_NUMS[0]
    initial = generator.generate_booking(True, booking_num, "confirmed")
    updated = generator.generate_booking(False, booking_num, "completed")

    assert normalize_uuid(initial["listing_id"]) == normalize_uuid(updated["listing_id"])
    assert normalize_uuid(initial["guest_id"]) == normalize_uuid(updated["guest_id"])
    assert normalize_uuid(initial["host_id"]) == normalize_uuid(updated["host_id"])
    assert initial["base_price"] == updated["base_price"]
    assert initial["booked_at"] == updated["booked_at"]


def test_review_roles_and_parent_entities_match_booking():
    booking = generator.generate_booking(False, generator._BOOKING_BASE_NUMS[1], "completed")
    for _ in range(20):
        review = generator.generate_review(booking)
        assert review["booking_id"] == booking["booking_id"]
        assert normalize_uuid(review["listing_id"]) == normalize_uuid(booking["listing_id"])
        if review["review_type"] == "guest_to_host":
            assert normalize_uuid(review["reviewer_id"]) == normalize_uuid(booking["guest_id"])
            assert normalize_uuid(review["reviewee_id"]) == normalize_uuid(booking["host_id"])
        else:
            assert normalize_uuid(review["reviewer_id"]) == normalize_uuid(booking["host_id"])
            assert normalize_uuid(review["reviewee_id"]) == normalize_uuid(booking["guest_id"])


def test_event_batch_contains_ordered_coherent_sessions(monkeypatch):
    captured = []
    monkeypatch.setattr(generator, "publish", lambda _stream, records: captured.extend(records))
    generator.run_events_batch(count=50)
    assert len(captured) == 50

    sessions = defaultdict(list)
    for event in captured:
        sessions[event["session_id"]].append(event)

    allowed_order = {
        "search_impression": 1,
        "view": 2,
        "click": 3,
        "save": 4,
        "booking_start": 5,
        "booking_complete": 6,
    }
    for events in sessions.values():
        timestamps = [event["event_timestamp"] for event in events]
        assert timestamps == sorted(timestamps)
        stages = [
            next(name for name, variants in {
                "search_impression": ["SearchImpression", "search_impression", "impression"],
                "view": ["view", "View", "VIEW", "page_view"],
                "click": ["click", "Click", "listing_click"],
                "save": ["favourite", "favorite", "Favourite", "save", "wishlist_add"],
                "booking_start": ["booking_start", "BookingStart"],
                "booking_complete": ["booking_complete", "BookingComplete", "booking_confirmed"],
            }.items() if event["event_type"] in variants)
            for event in events
        ]
        assert [allowed_order[stage] for stage in stages] == sorted(allowed_order[stage] for stage in stages)


def test_publish_retries_only_failed_records(monkeypatch):
    class FakeKinesis:
        def __init__(self):
            self.calls = []

        def put_records(self, Records, StreamName):
            self.calls.append((Records, StreamName))
            if len(self.calls) == 1:
                return {"Records": [{}, {"ErrorCode": "ProvisionedThroughputExceededException"}]}
            return {"Records": [{} for _ in Records]}

    fake = FakeKinesis()
    monkeypatch.setattr(generator, "_kinesis", fake)
    monkeypatch.setattr(generator.time, "sleep", lambda _seconds: None)
    generator.publish("test-stream", [{"listing_id": "L1"}, {"listing_id": "L2"}])
    assert len(fake.calls) == 2
    assert len(fake.calls[0][0]) == 2
    assert len(fake.calls[1][0]) == 1


def test_transaction_batch_has_real_updates_and_linked_reviews(monkeypatch):
    captured = []
    monkeypatch.setattr(generator, "publish", lambda _stream, records: captured.extend(records))
    generator.run_transactions_batch()

    bookings = [record for record in captured if record["entity_type"] == "booking"]
    reviews = [record for record in captured if record["entity_type"] == "review"]
    normalized_booking_ids = [
        "BKG-" + "".join(character for character in booking["booking_id"] if character.isdigit())
        for booking in bookings
    ]
    review_booking_ids = {
        "BKG-" + "".join(character for character in review["booking_id"] if character.isdigit())
        for review in reviews
    }

    assert len(bookings) == 25
    assert len(reviews) == 30
    assert len(normalized_booking_ids) > len(set(normalized_booking_ids))
    assert review_booking_ids.issubset(set(normalized_booking_ids))
