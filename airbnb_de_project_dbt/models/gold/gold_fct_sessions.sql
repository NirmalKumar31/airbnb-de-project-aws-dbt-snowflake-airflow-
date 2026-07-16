{{ config(materialized='table') }}

WITH events AS (
    SELECT
        session_id,
        guest_id,
        listing_id,
        event_type_clean,
        event_timestamp,
        CASE event_type_clean
            WHEN 'search_impression' THEN 1
            WHEN 'view' THEN 2
            WHEN 'click' THEN 3
            WHEN 'save' THEN 4
            WHEN 'booking_start' THEN 5
            WHEN 'booking_complete' THEN 6
        END AS funnel_step
    FROM {{ ref('silver_listing_events') }}
),

session_rollup AS (
    SELECT
        session_id,
        MIN(event_timestamp) AS session_started_at,
        MAX(event_timestamp) AS session_ended_at,
        MIN(guest_id) AS guest_id,
        MIN(listing_id) AS listing_id,
        COUNT(*) AS event_count,
        MAX(funnel_step) AS furthest_funnel_step,
        COUNT_IF(event_type_clean = 'search_impression') > 0 AS had_search_impression,
        COUNT_IF(event_type_clean = 'view') > 0 AS had_view,
        COUNT_IF(event_type_clean = 'click') > 0 AS had_click,
        COUNT_IF(event_type_clean = 'save') > 0 AS had_save,
        COUNT_IF(event_type_clean = 'booking_start') > 0 AS had_booking_start,
        COUNT_IF(event_type_clean = 'booking_complete') > 0 AS had_booking_complete
    FROM events
    GROUP BY session_id
)

SELECT
    MD5(s.session_id) AS session_key,
    s.session_id,
    l.listing_key,
    g.guest_key,
    d.date_key AS session_date_key,
    s.session_started_at,
    s.session_ended_at,
    DATEDIFF('second', s.session_started_at, s.session_ended_at) AS session_duration_seconds,
    s.event_count,
    s.furthest_funnel_step,
    s.had_search_impression,
    s.had_view,
    s.had_click,
    s.had_save,
    s.had_booking_start,
    s.had_booking_complete,
    s.had_booking_start AND NOT s.had_booking_complete AS abandoned_after_booking_start
FROM session_rollup s
LEFT JOIN {{ ref('gold_dim_listings') }} l
    ON s.listing_id = l.listing_id
   AND l.is_current_record = TRUE
LEFT JOIN {{ ref('gold_dim_guests') }} g
    ON s.guest_id = g.guest_id
LEFT JOIN {{ ref('dim_date') }} d
    ON s.session_started_at::DATE = d.full_date
