{{ config(
    materialized         = 'incremental',
    unique_key           = 'host_id',
    incremental_strategy = 'merge',
    on_schema_change     = 'sync_all_columns'
) }}

WITH source AS (

    SELECT * FROM {{ ref('bronze_hosts') }}

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
        LOWER(
            REGEXP_REPLACE(host_id, '[^a-zA-Z0-9]', '')
        )                                         AS host_id,

        -- Name & contact
        INITCAP(TRIM(host_name))                  AS host_name,
        LOWER(TRIM(host_email))                   AS host_email,

        --  Dates
        TRY_TO_DATE(
            REGEXP_REPLACE(host_since, '(st|nd|rd|th),?', ''),
            'AUTO'
        )                                         AS host_since,

        --  Response time 
        CASE
            WHEN LOWER(host_response_time) LIKE '%within an hour%'
              OR LOWER(host_response_time) = '1 hour'         THEN 'within_1_hour'
            WHEN LOWER(host_response_time) LIKE '%within a day%'
              OR LOWER(host_response_time) = '24 hours'
              OR LOWER(host_response_time) = 'same day'       THEN 'within_day'
            WHEN LOWER(host_response_time) LIKE '%few days%'  THEN 'few_days'
            ELSE NULL
        END                                       AS host_response_time,

        --  Response rate 
        -- Arrives as "98%", "0.98", "98", or NULL
        -- Normalise to 0-1 range, round to 2 decimal places
        CASE
            WHEN host_response_rate IS NULL THEN NULL
            ELSE
                ROUND(
                    LEAST(1.0,
                        TRY_TO_DECIMAL(
                            REPLACE(TRIM(host_response_rate), '%', ''),
                            10, 4
                        ) / CASE
                                WHEN TRY_TO_DECIMAL(
                                    REPLACE(TRIM(host_response_rate), '%', ''),
                                    10, 4
                                ) > 1 THEN 100
                                ELSE 1
                            END
                    ), 2
                )
        END                                       AS host_response_rate,

        --  Acceptance rate 
        CASE
            WHEN host_acceptance_rate IS NULL THEN NULL
            ELSE
                ROUND(
                    LEAST(1.0,
                        TRY_TO_DECIMAL(
                            REPLACE(TRIM(host_acceptance_rate), '%', ''),
                            10, 4
                        ) / CASE
                                WHEN TRY_TO_DECIMAL(
                                    REPLACE(TRIM(host_acceptance_rate), '%', ''),
                                    10, 4
                                ) > 1 THEN 100
                                ELSE 1
                            END
                    ), 2
                )
        END                                       AS host_acceptance_rate,

        --  Boolean normalisation
        -- Handles: t/f/True/False/1/0/Y/N/yes/no
        CASE
            WHEN LOWER(TRIM(is_superhost)) IN
                ('t','true','1','y','yes') THEN TRUE
            WHEN LOWER(TRIM(is_superhost)) IN
                ('f','false','0','n','no') THEN FALSE
            ELSE NULL
        END                                       AS is_superhost,

        CASE
            WHEN LOWER(TRIM(host_identity_verified)) IN
                ('t','true','1','y','yes') THEN TRUE
            WHEN LOWER(TRIM(host_identity_verified)) IN
                ('f','false','0','n','no') THEN FALSE
            ELSE NULL
        END                                       AS host_identity_verified,

        --  Listing counts 
        TRY_TO_NUMBER(host_listings_count)        AS host_listings_count,
        TRY_TO_NUMBER(host_total_listings_count)  AS host_total_listings_count,

        --  Data quality flag 
        -- Source bug: host_listings_count sometimes 0 when
        -- host_total_listings_count > 0. Flag but don't drop.
        CASE
            WHEN TRY_TO_NUMBER(host_listings_count) = 0
             AND TRY_TO_NUMBER(host_total_listings_count) > 0
            THEN TRUE
            ELSE FALSE
        END                                       AS has_listings_count_discrepancy,

        --  Verifications 
        -- Arrives as JSON array, pipe-delimited, or comma-space
        CASE
            WHEN TRY_PARSE_JSON(host_verifications) IS NOT NULL
                THEN TRY_PARSE_JSON(host_verifications)
            WHEN CONTAINS(host_verifications, '|')
                THEN TO_ARRAY(SPLIT(host_verifications, '|'))
            ELSE
                TO_ARRAY(SPLIT(host_verifications, ', '))
        END                                       AS host_verifications,

        --  Location 
        TRIM(host_location)                       AS host_location,

        --  Metadata
        _loaded_at,
        _stream_timestamp,
        _source_file

    FROM source

),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY host_id
            ORDER BY TRY_TO_TIMESTAMP_TZ(_stream_timestamp) DESC, _loaded_at DESC
        ) AS _row_num
    FROM cleaned
)

SELECT * EXCLUDE _row_num
FROM deduped
WHERE _row_num = 1
