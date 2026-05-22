{{ config(
    materialized     = 'incremental',
    unique_key       = 'review_id',
    on_schema_change = 'sync_all_columns'
) }}

-- Reviews are append-only — once written they never change.
-- No merge strategy needed, just watermark-based append.

WITH source AS (

    SELECT * FROM {{ ref('bronze_reviews') }}

    {% if is_incremental() %}
    WHERE _loaded_at > (
        SELECT COALESCE(MAX(_loaded_at), '2000-01-01'::TIMESTAMP_TZ)
        FROM {{ this }}
    )
    {% endif %}

),

-- Pre-clean rating strings before applying scale normalisation.
-- Each rating column has up to 5 dirty patterns:
-- 1. Correct:           "4.5"
-- 2. European comma:    "4,5"   → replace comma with period
-- 3. String suffix:     "4.5 stars" → strip non-numeric
-- 4. Scale 0-100:       "92"    → divide by 20
-- 5. Invalid zero:      "0"     → treat as null
rating_prep AS (

    SELECT
        *,
        TRY_TO_DOUBLE(REGEXP_REPLACE(
            REPLACE(overall_rating,       ',', '.'), '[^0-9.]', ''))
                                          AS _overall,
        TRY_TO_DOUBLE(REGEXP_REPLACE(
            REPLACE(cleanliness_rating,   ',', '.'), '[^0-9.]', ''))
                                          AS _cleanliness,
        TRY_TO_DOUBLE(REGEXP_REPLACE(
            REPLACE(accuracy_rating,      ',', '.'), '[^0-9.]', ''))
                                          AS _accuracy,
        TRY_TO_DOUBLE(REGEXP_REPLACE(
            REPLACE(communication_rating, ',', '.'), '[^0-9.]', ''))
                                          AS _communication,
        TRY_TO_DOUBLE(REGEXP_REPLACE(
            REPLACE(checkin_rating,       ',', '.'), '[^0-9.]', ''))
                                          AS _checkin,
        TRY_TO_DOUBLE(REGEXP_REPLACE(
            REPLACE(location_rating,      ',', '.'), '[^0-9.]', ''))
                                          AS _location,
        TRY_TO_DOUBLE(REGEXP_REPLACE(
            REPLACE(value_rating,         ',', '.'), '[^0-9.]', ''))
                                          AS _value
    FROM source

),

cleaned AS (

    SELECT
        -- Identity 
        LOWER(REGEXP_REPLACE(review_id,  '[^a-zA-Z0-9]', '')) AS review_id,

        --  Foreign keys
        'BKG-' || REGEXP_REPLACE(UPPER(booking_id), '[^0-9]', '')
                                              AS booking_id_clean,
        LOWER(REGEXP_REPLACE(listing_id,  '[^a-zA-Z0-9]', '')) AS listing_id,
        LOWER(REGEXP_REPLACE(reviewer_id, '[^a-zA-Z0-9]', '')) AS reviewer_id,
        LOWER(REGEXP_REPLACE(reviewee_id, '[^a-zA-Z0-9]', '')) AS reviewee_id,

        TRIM(review_type)                     AS review_type,

        -- Ratings
        -- Apply: null if 0, scale if >5, round to 2 decimal places
        CASE WHEN _overall IS NULL OR _overall = 0        THEN NULL
             WHEN _overall > 5 THEN ROUND(_overall / 20.0, 2)
             ELSE ROUND(_overall, 2) END      AS overall_rating,

        CASE WHEN _cleanliness IS NULL OR _cleanliness = 0 THEN NULL
             WHEN _cleanliness > 5 THEN ROUND(_cleanliness / 20.0, 2)
             ELSE ROUND(_cleanliness, 2) END  AS cleanliness_rating,

        CASE WHEN _accuracy IS NULL OR _accuracy = 0     THEN NULL
             WHEN _accuracy > 5 THEN ROUND(_accuracy / 20.0, 2)
             ELSE ROUND(_accuracy, 2) END     AS accuracy_rating,

        CASE WHEN _communication IS NULL OR _communication = 0 THEN NULL
             WHEN _communication > 5 THEN ROUND(_communication / 20.0, 2)
             ELSE ROUND(_communication, 2) END AS communication_rating,

        CASE WHEN _checkin IS NULL OR _checkin = 0       THEN NULL
             WHEN _checkin > 5 THEN ROUND(_checkin / 20.0, 2)
             ELSE ROUND(_checkin, 2) END      AS checkin_rating,

        CASE WHEN _location IS NULL OR _location = 0     THEN NULL
             WHEN _location > 5 THEN ROUND(_location / 20.0, 2)
             ELSE ROUND(_location, 2) END     AS location_rating,

        CASE WHEN _value IS NULL OR _value = 0           THEN NULL
             WHEN _value > 5 THEN ROUND(_value / 20.0, 2)
             ELSE ROUND(_value, 2) END        AS value_rating,

        -- Review text
        -- Empty string / whitespace / "." / "N/A" / "None" → NULL
        CASE
            WHEN TRIM(review_text) IS NULL         THEN NULL
            WHEN TRIM(review_text) = ''            THEN NULL
            WHEN TRIM(review_text) = '.'           THEN NULL
            WHEN UPPER(TRIM(review_text)) = 'N/A'  THEN NULL
            WHEN UPPER(TRIM(review_text)) = 'NONE' THEN NULL
            WHEN LENGTH(TRIM(review_text)) < 3     THEN NULL
            ELSE TRIM(review_text)
        END                                   AS review_text,

        CASE
            WHEN TRIM(response_text) IS NULL        THEN NULL
            WHEN TRIM(response_text) = ''           THEN NULL
            WHEN UPPER(TRIM(response_text)) = 'N/A' THEN NULL
            ELSE TRIM(response_text)
        END                                   AS response_text,

        -- Boolean
        CASE
            WHEN LOWER(TRIM(is_public))
                IN ('t','true','1','y','yes') THEN TRUE
            WHEN LOWER(TRIM(is_public))
                IN ('f','false','0','n','no') THEN FALSE
            ELSE NULL
        END                                   AS is_public,

        TRY_TO_TIMESTAMP_TZ(reviewed_at)      AS reviewed_at,

        -- Metadata
        _loaded_at,
        _stream_timestamp,
        _source_file

    FROM rating_prep

),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY _loaded_at DESC
        ) AS _row_num
    FROM cleaned
)

SELECT * EXCLUDE _row_num
FROM deduped
WHERE _row_num = 1