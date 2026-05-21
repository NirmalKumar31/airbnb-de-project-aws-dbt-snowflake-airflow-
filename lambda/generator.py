#!/usr/bin/env python3
"""
Airbnb Streaming DE — Data Generator
======================================
Generates synthetic Airbnb data for all 7 entities with realistic dirty
data patterns, then publishes to the three Kinesis Data Streams.

Streams:
  airbnb-events-stream       → listing_events  (50 records / 15s)
  airbnb-transactions-stream → bookings + reviews
  airbnb-dimensions-stream   → listings, hosts, guests, availability_calendar

Usage:
  python generator.py --dry-run    # print sample records, no Kinesis publish
  python generator.py              # publish one full batch to Kinesis
"""

import json
import random
import uuid
import sys
import logging
import argparse
from datetime import datetime, timedelta, date
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Stream names ──────────────────────────────────────────────────────────────
STREAM_EVENTS       = "airbnb-events-stream"
STREAM_TRANSACTIONS = "airbnb-transactions-stream"
STREAM_DIMENSIONS   = "airbnb-dimensions-stream"
AWS_REGION          = "us-east-1"

# ── Lazy Kinesis client (only imported when actually publishing) ───────────────
_kinesis = None
def kinesis():
    global _kinesis
    if _kinesis is None:
        import boto3
        _kinesis = boto3.client("kinesis", region_name=AWS_REGION)
    return _kinesis


# ══════════════════════════════════════════════════════════════════════════════
#  DIRTY DATA HELPERS
#  Each helper intentionally produces the same logical value in multiple
#  string representations — exactly what the Silver layer has to clean.
# ══════════════════════════════════════════════════════════════════════════════

def dirty_bool() -> str:
    """Boolean in one of 10 string formats: t/f/True/False/1/0/Y/N/yes/no"""
    return random.choice(["t", "f", "True", "False", "1", "0", "Y", "N", "yes", "no"])

def dirty_price(min_v: float, max_v: float, nullable: bool = False) -> Optional[str]:
    """Price in one of 4 formats: '$120.00' / '$1,200' / '120.5' / '120'"""
    if nullable and random.random() < 0.10:
        return None
    val = round(random.uniform(min_v, max_v), 2)
    fmt = random.choice(["dollar_cents", "dollar_comma", "plain_float", "plain_int"])
    if fmt == "dollar_cents":
        return f"${val:.2f}"
    elif fmt == "dollar_comma":
        return f"${val:,.2f}"
    elif fmt == "plain_float":
        return str(val)
    else:
        return str(int(val))

def dirty_uid() -> str:
    """UUID in one of 3 formats: standard / no-hyphens / UPPERCASE"""
    u = uuid.uuid4()
    return random.choice([str(u), str(u).replace("-", ""), str(u).upper()])

def dirty_date(d: date) -> str:
    """Date in one of 3 formats: ISO / US / natural language"""
    fmt = random.choice(["iso", "us", "natural"])
    if fmt == "iso":
        return d.strftime("%Y-%m-%d")
    elif fmt == "us":
        return d.strftime("%m/%d/%Y")
    else:
        day = d.day
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(
            day % 10 if day not in (11, 12, 13) else 0, "th"
        )
        return d.strftime(f"%B {day}{suffix}, %Y")

def dirty_rating() -> str:
    """
    Rating with 5 dirty patterns:
    1. Correct 0–5 scale            e.g. "4.5"
    2. European decimal comma       e.g. "4,5"
    3. 0–100 scale                  e.g. "90"
    4. String with suffix           e.g. "4.5 stars"
    5. Zero (invalid / means null)  "0"
    """
    val = round(random.uniform(1.0, 5.0), 1)
    kind = random.choices(
        ["normal", "euro", "scale_100", "suffix", "invalid_zero"],
        weights=[50, 15, 20, 10, 5],
    )[0]
    if kind == "normal":      return str(val)
    elif kind == "euro":      return str(val).replace(".", ",")
    elif kind == "scale_100": return str(round(val * 20))
    elif kind == "suffix":    return f"{val} stars"
    else:                     return "0"

def dirty_response_rate() -> Optional[str]:
    """Host response/acceptance rate: '98%' / '0.98' / '98' / null"""
    if random.random() < 0.05:
        return None
    val = round(random.uniform(0.70, 1.00), 2)
    fmt = random.choice(["pct_str", "decimal", "integer", "float_100"])
    if fmt == "pct_str":   return f"{int(val * 100)}%"
    elif fmt == "decimal": return str(val)
    elif fmt == "integer": return str(int(val * 100))
    else:                  return str(round(val * 100, 1))

def dirty_amenities(items: list) -> str:
    """Amenity list: JSON array / pipe-delimited / comma-space delimited"""
    fmt = random.choice(["json", "pipe", "comma"])
    if fmt == "json":   return json.dumps(items)
    elif fmt == "pipe": return "|".join(items)
    else:               return ", ".join(items)

def dirty_verifications(items: list) -> str:
    """Verification list — same 3 formats as amenities"""
    fmt = random.choice(["json", "pipe", "comma"])
    if fmt == "json":   return json.dumps(items)
    elif fmt == "pipe": return "|".join(items)
    else:               return ", ".join(items)


# ══════════════════════════════════════════════════════════════════════════════
#  REFERENCE DATA  — pre-generated ID pools for FK consistency
# ══════════════════════════════════════════════════════════════════════════════

from faker import Faker
fake = Faker()

import random as _rng
_seeded = _rng.Random(42)   # fixed seed → same IDs on EVERY Lambda run

def _seeded_uuid():
    return str(uuid.UUID(int=_seeded.getrandbits(128)))

HOST_IDS    = [_seeded_uuid() for _ in range(50)]
LISTING_IDS = [_seeded_uuid() for _ in range(200)]
GUEST_IDS   = [_seeded_uuid() for _ in range(500)]

# Fixed pool of 300 booking base numbers for the status-update path.
# The same logical booking (e.g. base number 100042) can arrive across
# batches as "BKG-100042", "BKG100042", or "bkg_100042" — Silver must
# normalize all three to the same key before the MERGE matches them.
# New bookings use numbers ≥ 200000 so they never collide with this pool.
_BOOKING_BASE_NUMS = list(range(100000, 100300))

def _booking_id(num: int) -> str:
    """Apply a random format to a booking number — same number, different format each time."""
    fmt = random.choice(["dash", "no_sep", "lower_under"])
    if fmt == "dash":        return f"BKG-{num}"
    elif fmt == "no_sep":    return f"BKG{num}"
    else:                    return f"bkg_{num}"

BOSTON_COORDS = [
    (42.3601, -71.0589), (42.3522, -71.0552), (42.3677, -71.0688),
    (42.3451, -71.0624), (42.3723, -71.1200), (42.3370, -71.0482),
    (42.2900, -71.0590), (42.3801, -71.1396), (42.3150, -71.0532),
]

AMENITY_OPTIONS = [
    "WiFi", "Kitchen", "Washer", "Dryer", "Air conditioning", "Heating",
    "Dedicated workspace", "TV", "Hair dryer", "Iron", "Pool", "Gym",
    "Free parking", "Hot tub", "BBQ grill", "Breakfast", "Fireplace",
    "EV charger", "Smoke alarm", "Carbon monoxide alarm", "First aid kit",
]

NEIGHBORHOODS = [
    "Back Bay", "South End", "Beacon Hill", "Cambridge", "Fenway",
    "North End", "Charlestown", "Jamaica Plain", "Somerville", "Allston",
    "Brighton", "Roxbury", "Dorchester", "South Boston", "Downtown",
]

VERIFICATION_OPTIONS = ["email", "phone", "government_id", "work_email", "facebook", "linkedin"]


# ══════════════════════════════════════════════════════════════════════════════
#  ENTITY GENERATORS
# ══════════════════════════════════════════════════════════════════════════════

def generate_host() -> dict:
    host_id = random.choice(HOST_IDS)
    if random.random() < 0.30:
        host_id = dirty_uid()

    # Name casing: 60% proper, 20% ALL CAPS, 20% all lower
    name = fake.name()
    casing = random.choices(["proper", "upper", "lower"], weights=[60, 20, 20])[0]
    if casing == "upper": name = name.upper()
    elif casing == "lower": name = name.lower()

    host_since = fake.date_between(start_date="-8y", end_date="-6m")

    return {
        "entity_type": "host",
        "host_id": host_id,
        "host_name": name,
        "host_email": fake.email(),
        "host_since": dirty_date(host_since),
        "host_location": fake.city() if random.random() > 0.15 else None,
        "host_response_time": random.choice([
            "within an hour", "within a day", "a few days or more",
            "1 hour", "24 hours", "same day", None,
        ]),
        "host_response_rate": dirty_response_rate(),
        "host_acceptance_rate": dirty_response_rate(),
        "is_superhost": dirty_bool(),
        # Source bug: sometimes 0 even for active hosts
        "host_listings_count": 0 if random.random() < 0.08 else random.randint(1, 10),
        "host_total_listings_count": random.randint(1, 15),
        "host_verifications": dirty_verifications(
            random.sample(VERIFICATION_OPTIONS, random.randint(1, 4))
        ),
        "host_identity_verified": dirty_bool(),
        "_stream_timestamp": datetime.utcnow().isoformat() + "Z",
    }


def generate_listing() -> dict:
    listing_id = random.choice(LISTING_IDS)
    if random.random() < 0.30:
        listing_id = dirty_uid()

    host_id = random.choice(HOST_IDS)
    if random.random() < 0.30:
        host_id = dirty_uid()

    # Coordinate swap: 5% chance (latitude and longitude reversed)
    lat, lon = random.choice(BOSTON_COORDS)
    if random.random() < 0.05:
        lat, lon = lon, lat  # intentional dirty swap

    # Rating scale: 30% chance of 0–100 instead of 0–5
    def listing_score():
        if random.random() < 0.30:
            return round(random.uniform(60, 100), 1)   # 0–100 scale
        return round(random.uniform(3.0, 5.0), 1)      # 0–5 scale

    # minimum_nights: occasionally invalid (0 or negative)
    min_nights = random.randint(1, 30)
    if random.random() < 0.05:
        min_nights = random.choice([0, -1])

    # bathrooms: sometimes a string like "1.5 baths" or "shared"
    bath_val = round(random.choice([1, 1.5, 2, 2.5, 3]) , 1)
    bathrooms = random.choice([
        str(bath_val),
        f"{bath_val} baths",
        "shared" if random.random() < 0.04 else str(bath_val),
    ])

    amenity_count = random.randint(3, 15)
    amenities = dirty_amenities(
        random.sample(AMENITY_OPTIONS, min(amenity_count, len(AMENITY_OPTIONS)))
    )

    return {
        "entity_type": "listing",
        "listing_id": listing_id,
        "host_id": host_id,
        "listing_name": fake.sentence(nb_words=5).rstrip("."),
        "listing_description": fake.paragraph(nb_sentences=3),
        "property_type": random.choice([
            "Entire home/apt", "Private room", "Shared room", "Hotel room",
            "Boat", "Cabin", "Treehouse",
        ]),
        "room_type": random.choice([
            "Entire home/apt", "Private room", "Shared room", "Hotel room"
        ]),
        "accommodates": random.randint(1, 10),
        "bathrooms": bathrooms,
        "bedrooms": None if random.random() < 0.08 else random.randint(0, 5),
        "beds": random.randint(1, 8),
        "amenities": amenities,
        "price_per_night": dirty_price(30, 600),
        "cleaning_fee": dirty_price(15, 200, nullable=True),
        "minimum_nights": min_nights,
        "maximum_nights": random.randint(30, 365) if random.random() > 0.10 else None,
        "cancellation_policy": random.choice([
            "flexible", "moderate", "strict", "super_strict_30", "super_strict_60"
        ]),
        "instant_bookable": dirty_bool(),
        "neighborhood": random.choice(NEIGHBORHOODS),
        "city": random.choice(["Boston", "boston", "Cambridge", "Somerville", "Brookline"]),
        "country": random.choice(["US", "USA", "United States"]),
        "latitude": lat,
        "longitude": lon,
        "review_scores_rating": listing_score(),
        "review_scores_cleanliness": listing_score(),
        "review_scores_checkin": listing_score(),
        "review_scores_communication": listing_score(),
        "review_scores_location": listing_score(),
        "review_scores_value": listing_score(),
        "number_of_reviews": random.randint(0, 500),
        "last_review_date": (
            dirty_date(fake.date_between(start_date="-2y", end_date="today"))
            if random.random() > 0.10 else None
        ),
        "_stream_timestamp": datetime.utcnow().isoformat() + "Z",
    }


def generate_guest() -> dict:
    guest_id = random.choice(GUEST_IDS)
    if random.random() < 0.30:
        guest_id = dirty_uid()

    # Phone in 4 different formats
    digits = "".join(filter(str.isdigit, fake.phone_number()))[-10:]
    phone_fmt = random.choice(["parens", "dashes", "e164", "dots"])
    if phone_fmt == "parens":    phone = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif phone_fmt == "dashes":  phone = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif phone_fmt == "e164":    phone = f"+1{digits}"
    else:                        phone = f"{digits[:3]}.{digits[3:6]}.{digits[6:]}"

    # Gender with casing chaos
    gender_base = random.choice(["M", "F", "Other"])
    gender_variants = {
        "M": ["M", "Male", "male", "MALE"],
        "F": ["F", "Female", "female", "FEMALE"],
        "Other": ["Other", "other", "Non-binary"],
    }
    gender = random.choice(gender_variants[gender_base])

    # Nationality: 5 representations of same country
    nationality = random.choice(["US", "USA", "United States", "united states",
                                  "United States of America"])
    if random.random() < 0.25:
        nationality = fake.country_code()  # non-US users

    total_trips = random.randint(0, 50)
    # Occasionally negative (invalid — source bug)
    if random.random() < 0.03:
        total_trips = -random.randint(1, 5)

    return {
        "entity_type": "guest",
        "guest_id": guest_id,
        "guest_name": fake.name(),
        "guest_email": fake.email(),
        "guest_phone": phone if random.random() > 0.05 else None,
        "date_of_birth": dirty_date(fake.date_of_birth(minimum_age=18, maximum_age=75)),
        "guest_since": dirty_date(fake.date_between(start_date="-6y", end_date="-1m")),
        "nationality": nationality,
        "gender": gender,
        "preferred_language": random.choice(["en", "es", "fr", "zh", "de", "pt", "ja", "ko"]),
        "verified_id": dirty_bool(),
        "total_trips": total_trips,
        # 0.0 instead of null for zero-trip guests (Silver must fix this)
        "average_rating_as_guest": (
            0.0 if total_trips <= 0 else round(random.uniform(3.5, 5.0), 1)
        ),
        # Banned is very rare — weight heavily toward False variants
        "is_banned": (
            dirty_bool() if random.random() < 0.02
            else random.choice(["f", "False", "0", "no", "N"])
        ),
        "_stream_timestamp": datetime.utcnow().isoformat() + "Z",
    }


def generate_booking(is_new: bool = True) -> dict:
    if is_new:
        # Fresh booking — numbers ≥ 200000 never collide with the update pool
        booking_id = _booking_id(random.randint(200000, 999999))
        status = random.choice(["pending", "confirmed"])
    else:
        # Status update: reuse a booking from the fixed pool.
        # The same logical booking (e.g. base 100042) may arrive this time as
        # "BKG-100042", next time as "BKG100042", next as "bkg_100042".
        # Silver normalisation must reconcile all three before the MERGE runs.
        booking_id = _booking_id(random.choice(_BOOKING_BASE_NUMS))
        status = random.choice(["confirmed", "completed", "cancelled", "no_show"])

    check_in = fake.date_between(start_date="-30d", end_date="+90d")
    nights = random.randint(1, 14)
    check_out = check_in + timedelta(days=nights)

    # 5% chance of reversed dates (check_out < check_in)
    if random.random() < 0.05:
        check_in, check_out = check_out, check_in

    # nights_count: pre-computed but 15% chance it's wrong
    nights_count = nights + (random.choice([-1, 1, 2]) if random.random() < 0.15 else 0)

    # Price components
    nightly = random.uniform(50, 500)
    true_nights = max(abs((check_out - check_in).days), 1)
    base_price      = round(nightly * true_nights, 2)
    cleaning_fee    = round(random.uniform(20, 150), 2)
    service_fee     = round(base_price * 0.12, 2)
    taxes           = round(base_price * 0.08, 2)
    real_total      = base_price + cleaning_fee + service_fee + taxes

    # 20% chance of total_price mismatch (pre-computed wrong value)
    total_price = (
        round(real_total + random.uniform(-50, 50), 2)
        if random.random() < 0.20 else real_total
    )

    num_adults   = random.randint(1, 4)
    num_children = random.randint(0, 3)
    if random.random() < 0.03:
        num_children = -1  # invalid — occasionally produced by source

    # Status casing chaos (status already set above by is_new branch)
    status_str = random.choice([status, status.capitalize(), status.upper()])

    booked_at   = datetime.utcnow() - timedelta(days=random.randint(0, 60))
    updated_at  = booked_at + timedelta(hours=random.randint(0, 72))

    return {
        "entity_type": "booking",
        "booking_id": booking_id,
        "listing_id": random.choice(LISTING_IDS),
        "guest_id": random.choice(GUEST_IDS),
        "host_id": random.choice(HOST_IDS),
        "check_in_date": dirty_date(check_in),
        "check_out_date": dirty_date(check_out),
        "nights_count": nights_count,
        "num_guests": num_adults + max(0, num_children) + random.randint(0, 2),
        "num_adults": num_adults,
        "num_children": num_children,
        "num_infants": random.randint(0, 2),
        "booking_status": status_str,
        "payment_status": random.choice(["pending", "paid", "Paid", "PAID", "refunded", "failed"]),
        "total_price": total_price,
        "base_price": base_price,
        "cleaning_fee_charged": cleaning_fee,
        "service_fee": service_fee,
        "taxes": taxes,
        "special_requests": fake.sentence() if random.random() < 0.20 else None,
        "booked_at": booked_at.isoformat() + "Z",
        "updated_at": updated_at.isoformat() + "Z",
        "source_platform": random.choice([
            "web", "Web", "WEB", "mobile_ios", "Mobile iOS", "mobile_android", "api"
        ]),
        "promo_code": f"SAVE{random.randint(10, 30)}" if random.random() < 0.15 else None,
        "cancellation_reason": (
            random.choice(["change_of_plans", "found_better_option", "emergency"])
            if status == "cancelled" else None
        ),
        "_stream_timestamp": datetime.utcnow().isoformat() + "Z",
    }


def generate_review() -> dict:
    # Booking ID format matches generate_booking patterns
    num = random.randint(100000, 999999)
    id_fmt = random.choice(["dash", "no_sep", "lower_under"])
    booking_id = f"BKG-{num}" if id_fmt == "dash" else (
        f"BKG{num}" if id_fmt == "no_sep" else f"bkg_{num}"
    )

    # Review text dirty patterns: empty str / whitespace / punctuation / N/A / real text
    text_options = [
        fake.paragraph(nb_sentences=2),  # real text
        "",                              # empty string
        "   ",                           # whitespace only
        ".",                             # single punctuation
        "N/A",                           # literal N/A
        "None",                          # literal None string
    ]
    review_text = random.choices(text_options, weights=[70, 5, 5, 5, 10, 5])[0]

    return {
        "entity_type": "review",
        "review_id": dirty_uid(),
        "booking_id": booking_id,
        "listing_id": random.choice(LISTING_IDS),
        "reviewer_id": random.choice(GUEST_IDS + HOST_IDS),
        "reviewee_id": random.choice(HOST_IDS + GUEST_IDS),
        "review_type": random.choice(["guest_to_host", "host_to_guest"]),
        "overall_rating": dirty_rating(),
        "cleanliness_rating": dirty_rating(),
        "accuracy_rating": dirty_rating(),
        "communication_rating": dirty_rating(),
        "checkin_rating": dirty_rating(),
        "location_rating": dirty_rating(),
        "value_rating": dirty_rating(),
        "review_text": review_text,
        "response_text": fake.sentence() if random.random() < 0.20 else None,
        "is_public": dirty_bool(),
        "reviewed_at": (
            datetime.utcnow() - timedelta(days=random.randint(0, 30))
        ).isoformat() + "Z",
        "_stream_timestamp": datetime.utcnow().isoformat() + "Z",
    }


def generate_calendar_entry() -> dict:
    listing_id = random.choice(LISTING_IDS)
    if random.random() < 0.30:
        listing_id = dirty_uid()

    cal_date = fake.date_between(start_date="today", end_date="+365d")

    # calendar_id format chaos
    id_fmt = random.choice(["CAL_dash", "cal_underscore", "plain_uuid"])
    if id_fmt == "CAL_dash":
        cal_id = f"CAL-{listing_id[:8]}-{cal_date.strftime('%Y%m%d')}"
    elif id_fmt == "cal_underscore":
        cal_id = f"cal_{listing_id[:8]}_{cal_date.strftime('%Y%m%d')}"
    else:
        cal_id = str(uuid.uuid4())

    # is_available: 10 different representations
    is_available = random.choices(
        ["t", "f", "available", "blocked", "1", "0", "true", "false", "yes", "no"],
        weights=[25, 15, 15, 10, 10, 5, 8, 5, 5, 2],
    )[0]

    return {
        "entity_type": "availability_calendar",
        "calendar_id": cal_id,
        "listing_id": listing_id,
        "calendar_date": cal_date.strftime("%Y-%m-%d"),
        "is_available": is_available,
        "price_on_date": dirty_price(30, 600, nullable=True),
        "minimum_nights_override": random.randint(1, 7) if random.random() < 0.10 else None,
        "maximum_nights_override": random.randint(7, 30) if random.random() < 0.05 else None,
        "notes": fake.sentence() if random.random() < 0.05 else None,
        "_stream_timestamp": datetime.utcnow().isoformat() + "Z",
    }


def generate_listing_event() -> dict:
    # event_type: casing chaos + synonym chaos
    canonical_map = {
        "view":             ["view", "View", "VIEW", "page_view"],
        "click":            ["click", "Click", "listing_click"],
        "save":             ["favourite", "favorite", "Favourite", "save", "wishlist_add"],
        "booking_start":    ["booking_start", "BookingStart"],
        "booking_complete": ["booking_complete", "BookingComplete", "booking_confirmed"],
        "search_impression":["SearchImpression", "search_impression", "impression"],
    }
    canonical = random.choices(
        list(canonical_map.keys()),
        weights=[35, 20, 15, 10, 5, 15],
    )[0]
    event_type = random.choice(canonical_map[canonical])

    # device_type: casing chaos
    device_base = random.choice(["mobile", "desktop", "tablet"])
    device_type = random.choice([device_base, device_base.capitalize(), device_base.upper()])

    # os_version: compound field — Silver will split into os_name + os_version_clean
    os_version = random.choice([
        "iOS 16.2", "iOS 17.0", "iOS 17.1", "iOS 17.4",
        "Android 13", "Android 14",
        "macOS Ventura", "macOS Sonoma",
        "Windows 11", "Windows 10",
        "iPad OS 16.2", "iPad OS 17",
    ])

    # country_code: 10% chance of 3-letter ISO instead of 2-letter
    cc_map = {"US": "USA", "GB": "GBR", "CA": "CAN", "AU": "AUS",
              "DE": "DEU", "FR": "FRA", "JP": "JPN", "BR": "BRA"}
    country = random.choice(list(cc_map.keys()))
    if random.random() < 0.10:
        country = cc_map[country]

    # position_in_results: 0 is invalid (1-indexed field)
    position = None
    if canonical == "search_impression":
        position = random.randint(1, 20)
        if random.random() < 0.05:
            position = 0  # invalid — Silver will null this

    guest_id = random.choice(GUEST_IDS) if random.random() > 0.30 else None  # 30% anonymous

    return {
        "entity_type": "listing_event",
        "event_id": dirty_uid(),
        "event_type": event_type,
        "listing_id": random.choice(LISTING_IDS),
        "guest_id": guest_id,
        "session_id": str(uuid.uuid4()),
        "device_type": device_type,
        "os_version": os_version,
        "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge", None]),
        "country_code": country,
        "search_query": (
            " ".join(fake.words(nb=3)) if canonical == "search_impression" else None
        ),
        "price_shown": dirty_price(30, 600, nullable=True),
        "position_in_results": position,
        "event_timestamp": datetime.utcnow().isoformat() + "Z",
        "_stream_timestamp": datetime.utcnow().isoformat() + "Z",
    }


# ══════════════════════════════════════════════════════════════════════════════
#  KINESIS PUBLISHER
# ══════════════════════════════════════════════════════════════════════════════

def publish(stream_name: str, records: list) -> None:
    """
    Publish a batch of records to Kinesis using PutRecords.
    Handles the 500-record API limit automatically.
    """
    if not records:
        return

    entries = [
        {
            "Data": json.dumps(rec, default=str).encode("utf-8"),
            # Use listing_id as partition key to keep a listing's events on the same shard
            "PartitionKey": (
                rec.get("listing_id") or rec.get("host_id") or str(uuid.uuid4())
            ),
        }
        for rec in records
    ]

    for i in range(0, len(entries), 500):
        batch = entries[i : i + 500]
        response = kinesis().put_records(Records=batch, StreamName=stream_name)
        failed = response.get("FailedRecordCount", 0)
        if failed:
            log.warning(f"  ⚠  {failed}/{len(batch)} records failed on {stream_name}")
        else:
            log.info(f"  ✓  {len(batch):>4} records → {stream_name}")


# ══════════════════════════════════════════════════════════════════════════════
#  BATCH RUNNERS  — called by the Lambda handler
# ══════════════════════════════════════════════════════════════════════════════

def run_events_batch(count: int = 50) -> None:
    """High-frequency: 50 listing_events every 15 seconds ≈ 12,000 / hour."""
    records = [generate_listing_event() for _ in range(count)]
    publish(STREAM_EVENTS, records)


def run_transactions_batch() -> None:
    """
    Mid-frequency: 5 new bookings + 10 booking updates + 30 reviews per run.
    The 2:1 update-to-insert ratio stresses the Silver merge logic.
    """
    records = (
        [generate_booking(is_new=True)  for _ in range(5)]  +
        [generate_booking(is_new=False) for _ in range(10)] +
        [generate_review()              for _ in range(30)]
    )
    publish(STREAM_TRANSACTIONS, records)


def run_dimensions_batch() -> None:
    """
    Low-frequency: listings, hosts, guests, and availability_calendar updates.
    These are upserted in Silver. Listings and hosts trigger SCD-2 snapshots.
    """
    records = (
        [generate_listing()        for _ in range(10)]  +
        [generate_host()           for _ in range(5)]   +
        [generate_guest()          for _ in range(20)]  +
        [generate_calendar_entry() for _ in range(100)]
    )
    publish(STREAM_DIMENSIONS, records)


def run_all() -> None:
    log.info("▶  Starting generation run")
    run_events_batch()
    run_transactions_batch()
    run_dimensions_batch()
    log.info("✓  Run complete")


# ══════════════════════════════════════════════════════════════════════════════
#  CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Airbnb streaming data generator")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print sample records without publishing to Kinesis")
    parser.add_argument("--entity", choices=[
        "host", "listing", "guest", "booking", "review", "calendar", "event", "all"
    ], default="all", help="Which entity to generate (default: all)")
    args = parser.parse_args()

    if args.dry_run:
        generators = {
            "host":     generate_host,
            "listing":  generate_listing,
            "guest":    generate_guest,
            "booking":  generate_booking,
            "review":   generate_review,
            "calendar": generate_calendar_entry,
            "event":    generate_listing_event,
        }
        targets = generators if args.entity == "all" else {args.entity: generators[args.entity]}
        print("\n" + "="*60)
        print("DRY RUN — sample records (no Kinesis publish)")
        print("="*60)
        for name, gen in targets.items():
            print(f"\n── {name.upper()} ──")
            print(json.dumps(gen(), indent=2, default=str))
        print()
    else:
        run_all()


if __name__ == "__main__":
    main()
