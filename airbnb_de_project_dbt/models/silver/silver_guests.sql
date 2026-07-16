{{ config(
    materialized         = 'incremental',
    unique_key           = 'guest_id',
    incremental_strategy = 'merge',
    on_schema_change     = 'sync_all_columns'
) }}

WITH source AS (

    SELECT * FROM {{ ref('bronze_guests') }}

    {% if is_incremental() %}
    WHERE _loaded_at >= (
        SELECT COALESCE(MAX(_loaded_at), '2000-01-01'::TIMESTAMP_TZ)
        FROM {{ this }}
    )
    {% endif %}

),

cleaned AS (

    SELECT
        -- Identity 
        LOWER(REGEXP_REPLACE(guest_id, '[^a-zA-Z0-9]', '')) AS guest_id,

        --  Name & contact 
        INITCAP(TRIM(guest_name))             AS guest_name,
        LOWER(TRIM(guest_email))              AS guest_email,

        -- Phone normalisation
        -- 4 formats: (617) 555-1234 / 617-555-1234 /
        --            +16175551234 / 617.555.1234
        -- All normalised to E.164: +1XXXXXXXXXX
        CASE
            WHEN guest_phone IS NULL THEN NULL
            WHEN LENGTH(REGEXP_REPLACE(guest_phone, '[^0-9]', '')) = 10
                THEN '+1' || REGEXP_REPLACE(guest_phone, '[^0-9]', '')
            WHEN LENGTH(REGEXP_REPLACE(guest_phone, '[^0-9]', '')) = 11
             AND LEFT(REGEXP_REPLACE(guest_phone, '[^0-9]', ''), 1) = '1'
                THEN '+' || REGEXP_REPLACE(guest_phone, '[^0-9]', '')
            ELSE NULL
        END                                   AS guest_phone,

        --  Dates 
        TRY_TO_DATE(
            REGEXP_REPLACE(date_of_birth, '(st|nd|rd|th),?', ''),
            'AUTO'
        )                                     AS date_of_birth,

        TRY_TO_DATE(
            REGEXP_REPLACE(guest_since, '(st|nd|rd|th),?', ''),
            'AUTO'
        )                                     AS guest_since,

        -- Nationality
        -- US / USA / United States / united states → US
        CASE
            WHEN UPPER(TRIM(nationality)) IN (
                'US','USA','UNITED STATES','UNITED STATES OF AMERICA'
            ) THEN 'US'
            ELSE UPPER(TRIM(nationality))
        END                                   AS nationality_iso2,

        -- Gender 
        -- M/Male/male/MALE → M
        CASE
            WHEN UPPER(TRIM(gender)) IN ('M','MALE')           THEN 'M'
            WHEN UPPER(TRIM(gender)) IN ('F','FEMALE')         THEN 'F'
            WHEN UPPER(TRIM(gender)) IN ('OTHER','NON-BINARY') THEN 'Other'
            ELSE 'Unknown'
        END                                   AS gender,

        TRIM(preferred_language)              AS preferred_language,

        -- Booleans
        CASE
            WHEN LOWER(TRIM(verified_id))
                IN ('t','true','1','y','yes') THEN TRUE
            WHEN LOWER(TRIM(verified_id))
                IN ('f','false','0','n','no') THEN FALSE
            ELSE NULL
        END                                   AS verified_id,

        CASE
            WHEN LOWER(TRIM(is_banned))
                IN ('t','true','1','y','yes') THEN TRUE
            WHEN LOWER(TRIM(is_banned))
                IN ('f','false','0','n','no') THEN FALSE
            ELSE NULL
        END                                   AS is_banned,

        -- Trip stats
        -- total_trips occasionally arrives as negative (invalid)
        CASE
            WHEN TRY_TO_NUMBER(total_trips) < 0 THEN NULL
            ELSE TRY_TO_NUMBER(total_trips)
        END                                   AS total_trips,

        -- average_rating_as_guest arrives as 0.0 for zero-trip
        -- guests when it should be NULL
        -- Round to 2 decimal places
        CASE
            WHEN TRY_TO_NUMBER(total_trips) <= 0             THEN NULL
            WHEN TRY_TO_DOUBLE(average_rating_as_guest) = 0  THEN NULL
            ELSE ROUND(TRY_TO_DOUBLE(average_rating_as_guest), 2)
        END                                   AS average_rating_as_guest,

        -- Data quality flag
        CASE
            WHEN TRY_TO_NUMBER(total_trips) < 0 THEN TRUE
            ELSE FALSE
        END                                   AS is_data_quality_issue,

        -- Metadata
        _loaded_at,
        _stream_timestamp,
        _source_file

    FROM source

),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY guest_id
            ORDER BY TRY_TO_TIMESTAMP_TZ(_stream_timestamp) DESC, _loaded_at DESC
        ) AS _row_num
    FROM cleaned
)

SELECT * EXCLUDE _row_num
FROM deduped
WHERE _row_num = 1
