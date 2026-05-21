{{ config(materialized='table') }}

SELECT
    MD5(e.event_id)                       AS event_key,

    -- ── Dimension foreign keys ────────────────────────────────
    l.listing_key,
    g.guest_key,
    d.date_key                            AS event_date_key,

    -- ── Event identifiers ─────────────────────────────────────
    e.event_id,
    e.session_id,
    e.event_type_clean,
    e.is_event_type_mapped,

    -- ── Device & location ────────────────────────────────────
    e.device_type,
    e.os_name,
    e.os_version_clean,
    e.browser,
    e.country_code,

    -- ── Search context ────────────────────────────────────────
    e.search_query,
    e.price_shown,
    e.position_in_results,
    e.is_position_invalid,

    -- ── Behavioural flags ────────────────────────────────────
    e.is_anonymous,

    -- Funnel stage: maps event type to a numeric funnel step
    -- Used for conversion rate analysis
    CASE e.event_type_clean
        WHEN 'search_impression' THEN 1
        WHEN 'view'              THEN 2
        WHEN 'click'             THEN 3
        WHEN 'save'              THEN 4
        WHEN 'booking_start'     THEN 5
        WHEN 'booking_complete'  THEN 6
        ELSE NULL
    END                                   AS funnel_step,

    -- ── Listing context (current state) ──────────────────────
    l.listing_name,
    l.room_type,
    l.neighborhood,
    l.city,
    l.price_per_night                     AS current_listing_price,

    e.event_timestamp,
    e._loaded_at

FROM {{ ref('silver_listing_events') }} e

LEFT JOIN {{ ref('gold_dim_listings') }} l
    ON  e.listing_id = l.listing_id
    AND l.is_current_record = TRUE

LEFT JOIN {{ ref('gold_dim_guests') }} g
    ON e.guest_id = g.guest_id

LEFT JOIN {{ ref('dim_date') }} d
    ON e.event_timestamp::DATE = d.full_date