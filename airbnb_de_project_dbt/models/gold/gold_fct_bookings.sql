{{ config(materialized='table') }}

WITH bookings AS (
    SELECT * FROM {{ ref('silver_bookings') }}
    WHERE is_date_valid = TRUE
),

-- Join to listing version that was active when booking was made
-- This is the SCD-2 range join — gets price/policy at booking time
listing_at_booking AS (
    SELECT
        listing_id,
        listing_key,
        listing_name,
        room_type,
        property_type,
        neighborhood,
        city,
        country,
        price_per_night      AS listed_price_at_booking,
        cancellation_policy  AS cancellation_policy_at_booking,
        minimum_nights       AS minimum_nights_at_booking,
        instant_bookable     AS instant_bookable_at_booking,
        accommodates,
        amenity_count,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('gold_dim_listings') }}
    WHERE is_current_record = TRUE

),

-- Join to host version that was active when booking was made
host_at_booking AS (
    SELECT
        host_id,
        host_key,
        host_name,
        is_superhost         AS is_superhost_at_booking,
        host_response_rate   AS response_rate_at_booking,
        host_listings_count  AS listings_count_at_booking,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('gold_dim_hosts') }}
    WHERE is_current_record = TRUE

),

-- dim_date for check_in date attributes
date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    -- ── Surrogate keys (FKs to dimensions) ───────────────────
    MD5(b.booking_id_clean)               AS booking_key,

    -- SCD-2 range join: listing version active at booking time
    l.listing_key,
    -- SCD-2 range join: host version active at booking time
    h.host_key,
    -- Current state join for guests (no SCD-2)
    g.guest_key,
    -- Date dimension FK
    d.date_key                            AS check_in_date_key,

    -- ── Booking identifiers ───────────────────────────────────
    b.booking_id_clean,
    b.booking_id_raw,

    -- ── Booking details ───────────────────────────────────────
    b.check_in_date,
    b.check_out_date,
    b.calculated_nights,
    b.booking_status,
    b.payment_status,
    b.source_platform,
    b.promo_code,
    b.cancellation_reason,

    -- ── Guest counts ──────────────────────────────────────────
    b.num_guests,
    b.num_adults,
    b.num_children,
    b.num_infants,

    -- ── Revenue metrics ───────────────────────────────────────
    b.base_price,
    b.cleaning_fee_charged,
    b.service_fee,
    b.taxes,
    b.calculated_total_price,

    -- Revenue per night — key analytical metric
    ROUND(
        b.calculated_total_price / NULLIF(b.calculated_nights, 0),
    2)                                    AS revenue_per_night,

    -- ── Data quality flags ────────────────────────────────────
    b.is_price_mismatch,
    b.nights_count_corrected,

    -- ── Listing context at booking time ──────────────────────
    -- These come from the SCD-2 join — historical values
    l.listing_name,
    l.room_type,
    l.property_type,
    l.neighborhood,
    l.city,
    l.country,
    l.listed_price_at_booking,
    l.cancellation_policy_at_booking,
    l.instant_bookable_at_booking,
    l.amenity_count,

    -- ── Host context at booking time ─────────────────────────
    -- These come from the SCD-2 join — historical values
    h.host_name,
    h.is_superhost_at_booking,
    h.response_rate_at_booking,
    h.listings_count_at_booking,

    -- ── Booking timestamps ────────────────────────────────────
    b.booked_at,
    b.updated_at,

    -- How far in advance was this booking made?
    DATEDIFF('day', b.booked_at, b.check_in_date) AS booking_lead_days,

    CASE
        WHEN DATEDIFF('day', b.booked_at, b.check_in_date) < 7
        THEN TRUE ELSE FALSE
    END                                   AS is_last_minute_booking,

    CASE
        WHEN b.calculated_nights >= 7
        THEN TRUE ELSE FALSE
    END                                   AS is_long_stay

FROM bookings b

-- SCD-2 range join to listing version active at booking time
LEFT JOIN listing_at_booking l
    ON  b.listing_id  = l.listing_id
    

-- SCD-2 range join to host version active at booking time
LEFT JOIN host_at_booking h
    ON  b.host_id    = h.host_id
    

-- Current state join for guests
LEFT JOIN {{ ref('gold_dim_guests') }} g
    ON b.guest_id = g.guest_id

-- Date dimension join on check_in date
LEFT JOIN date_dim d
    ON b.check_in_date = d.full_date