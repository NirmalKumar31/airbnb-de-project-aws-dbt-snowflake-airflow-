{{ config(materialized='table') }}

SELECT
    MD5(r.review_id)                      AS review_key,

    -- ── Dimension foreign keys ────────────────────────────────
    l.listing_key,
    h.host_key,
    g.guest_key,
    b.booking_key,
    d.date_key                            AS reviewed_date_key,

    -- ── Review identifiers ────────────────────────────────────
    r.review_id,
    r.booking_id_clean,
    r.review_type,

    -- ── Ratings ───────────────────────────────────────────────
    r.overall_rating,
    r.cleanliness_rating,
    r.accuracy_rating,
    r.communication_rating,
    r.checkin_rating,
    r.location_rating,
    r.value_rating,

    -- Average across all sub-ratings for quick scoring
    ROUND(
        (
            COALESCE(r.cleanliness_rating, 0) +
            COALESCE(r.accuracy_rating, 0) +
            COALESCE(r.communication_rating, 0) +
            COALESCE(r.checkin_rating, 0) +
            COALESCE(r.location_rating, 0) +
            COALESCE(r.value_rating, 0)
        ) / NULLIF(
            (CASE WHEN r.cleanliness_rating   IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN r.accuracy_rating      IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN r.communication_rating IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN r.checkin_rating       IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN r.location_rating      IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN r.value_rating         IS NOT NULL THEN 1 ELSE 0 END)
        , 0),
    2)                                    AS avg_sub_rating,

    -- ── Review content ────────────────────────────────────────
    r.review_text,
    r.response_text,
    CASE WHEN r.review_text IS NOT NULL
         THEN TRUE ELSE FALSE
    END                                   AS has_review_text,
    CASE WHEN r.response_text IS NOT NULL
         THEN TRUE ELSE FALSE
    END                                   AS has_response,

    r.is_public,
    r.reviewed_at,

    -- ── Listing context (current state for reviews) ───────────
    l.listing_name,
    l.room_type,
    l.neighborhood,
    l.city

FROM {{ ref('silver_reviews') }} r

LEFT JOIN {{ ref('gold_fct_bookings') }} b
    ON r.booking_id_clean = b.booking_id_clean

-- Reviews use current listing state (not SCD-2 range)
-- because what matters is the listing that exists today
LEFT JOIN {{ ref('gold_dim_listings') }} l
    ON  r.listing_id = l.listing_id
    AND l.is_current_record = TRUE

    -- Host participant; role depends on review direction.
    LEFT JOIN {{ ref('gold_dim_hosts') }} h
    ON  (
            (r.review_type = 'guest_to_host' AND r.reviewee_id = h.host_id)
         OR (r.review_type = 'host_to_guest' AND r.reviewer_id = h.host_id)
        )
    AND h.is_current_record = TRUE

    -- Guest participant; role depends on review direction.
    LEFT JOIN {{ ref('gold_dim_guests') }} g
    ON (
           (r.review_type = 'guest_to_host' AND r.reviewer_id = g.guest_id)
        OR (r.review_type = 'host_to_guest' AND r.reviewee_id = g.guest_id)
       )

-- Date dimension
LEFT JOIN {{ ref('dim_date') }} d
    ON r.reviewed_at::DATE = d.full_date

WHERE r.is_public = TRUE
