{{ config(
    materialized         = 'incremental',
    unique_key           = ['listing_id', 'calendar_date'],
    incremental_strategy = 'merge',
    on_schema_change     = 'sync_all_columns'
) }}

WITH source AS (

    SELECT * FROM {{ ref('bronze_availability_calendar') }}

    {% if is_incremental() %}
    WHERE _loaded_at > (
        SELECT COALESCE(MAX(_loaded_at), '2000-01-01'::TIMESTAMP_TZ)
        FROM {{ this }}
    )
    {% endif %}

),

cleaned AS (

    SELECT
        -- Identity 
        LOWER(REGEXP_REPLACE(listing_id, '[^a-zA-Z0-9]', '')) AS listing_id,

        TRY_TO_DATE(calendar_date, 'YYYY-MM-DD')              AS calendar_date,

        --  Availability
        -- 10 different representations of the same boolean:
        -- t/f/1/0/true/false/available/blocked/yes/no
        CASE
            WHEN LOWER(TRIM(is_available))
                IN ('t','true','1','y','yes','available') THEN TRUE
            WHEN LOWER(TRIM(is_available))
                IN ('f','false','0','n','no','blocked')   THEN FALSE
            ELSE NULL
        END                                   AS is_available,

        -- Price 
        -- Dynamic pricing: same price string mess as listings
        ROUND(
            TRY_TO_DECIMAL(
                REGEXP_REPLACE(TRIM(price_on_date), '[$,]', ''),
                10, 2
            ), 2
        )                                     AS price_on_date,

        --  Overrides
        TRY_TO_NUMBER(minimum_nights_override) AS minimum_nights_override,
        TRY_TO_NUMBER(maximum_nights_override) AS maximum_nights_override,

        TRIM(notes)                           AS notes,

        --  Metadata 
        _loaded_at,
        _stream_timestamp,
        _source_file

    FROM source

),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY listing_id, calendar_date
            ORDER BY _loaded_at DESC
        ) AS _row_num
    FROM cleaned
)

SELECT * EXCLUDE _row_num
FROM deduped
WHERE _row_num = 1