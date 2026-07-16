{{ config(
    materialized         = 'incremental',
    unique_key           = 'booking_id_clean',
    incremental_strategy = 'merge',
    on_schema_change     = 'sync_all_columns'
) }}

WITH source AS (

    SELECT * FROM {{ ref('bronze_bookings') }}

    {% if is_incremental() %}
    WHERE _loaded_at >= (
        SELECT COALESCE(MAX(_loaded_at), '2000-01-01'::TIMESTAMP_TZ)
        FROM {{ this }}
    )
    {% endif %}

),

parsed AS (
    SELECT
        *,
        TRY_TO_DATE(
            REGEXP_REPLACE(check_in_date, '(st|nd|rd|th),?', ''),
            'AUTO'
        ) AS _check_in_date,
        TRY_TO_DATE(
            REGEXP_REPLACE(check_out_date, '(st|nd|rd|th),?', ''),
            'AUTO'
        ) AS _check_out_date
    FROM source
),

cleaned AS (

    SELECT
        -- Booking ID normalisation 
        -- Three formats: BKG-123456 / BKG123456 / bkg_123456
        -- All normalise to BKG-XXXXXX uppercase
        'BKG-' || REGEXP_REPLACE(UPPER(booking_id), '[^0-9]', '')
                                              AS booking_id_clean,
        booking_id                            AS booking_id_raw,

        --  Foreign keys
        LOWER(REGEXP_REPLACE(listing_id, '[^a-zA-Z0-9]', '')) AS listing_id,
        LOWER(REGEXP_REPLACE(guest_id,   '[^a-zA-Z0-9]', '')) AS guest_id,
        LOWER(REGEXP_REPLACE(host_id,    '[^a-zA-Z0-9]', '')) AS host_id,

        --  Dates 
        _check_in_date                        AS check_in_date,
        _check_out_date                       AS check_out_date,

        --  Date validation 
        -- check_out occasionally arrives BEFORE check_in (invalid)
        -- Flag but do not drop the row
        CASE
            WHEN _check_in_date IS NULL  THEN FALSE
            WHEN _check_out_date IS NULL THEN FALSE
            WHEN _check_out_date < _check_in_date THEN FALSE
            ELSE TRUE
        END                                   AS is_date_valid,

        --  Nights recalculation 
        -- nights_count is pre-computed by source but wrong ~15%
        TRY_TO_NUMBER(nights_count)           AS nights_count_raw,

        DATEDIFF('day',
            _check_in_date,
            _check_out_date
        )                                     AS calculated_nights,

        CASE
            WHEN TRY_TO_NUMBER(nights_count) !=
                 DATEDIFF('day',
                     _check_in_date,
                     _check_out_date
                 ) THEN TRUE
            ELSE FALSE
        END                                   AS nights_count_corrected,

        --  Guest counts
        TRY_TO_NUMBER(num_guests)             AS num_guests,
        TRY_TO_NUMBER(num_adults)             AS num_adults,
        TRY_TO_NUMBER(num_infants)            AS num_infants,

        -- num_children occasionally arrives as -1 (invalid)
        CASE
            WHEN TRY_TO_NUMBER(num_children) < 0 THEN NULL
            ELSE TRY_TO_NUMBER(num_children)
        END                                   AS num_children,

        --  Status fields 
        -- Casing chaos: confirmed / Confirmed / CONFIRMED
        CASE
           WHEN LOWER(TRIM(booking_status)) = 'no_show'  THEN 'no_show'
           ELSE LOWER(TRIM(booking_status))
        END   AS booking_status,
        LOWER(TRIM(payment_status))           AS payment_status,

        --  Platform 
        CASE
            WHEN LOWER(TRIM(source_platform)) IN ('web','desktop')
                THEN 'web'
            WHEN REGEXP_REPLACE(LOWER(TRIM(source_platform)), '[^a-z0-9]', '')
                = 'mobileios'
                THEN 'mobile_ios'
            WHEN REGEXP_REPLACE(LOWER(TRIM(source_platform)), '[^a-z0-9]', '')
                = 'mobileandroid'
                THEN 'mobile_android'
            WHEN LOWER(TRIM(source_platform)) = 'api' THEN 'api'
            ELSE LOWER(TRIM(source_platform))
        END                                   AS source_platform,

        TRIM(promo_code)                      AS promo_code,
        TRIM(cancellation_reason)             AS cancellation_reason,
        TRIM(special_requests)                AS special_requests,

        -- Prices 
        ROUND(TRY_TO_DECIMAL(total_price, 10, 2), 2)           AS total_price,
        ROUND(TRY_TO_DECIMAL(base_price, 10, 2), 2)            AS base_price,
        ROUND(TRY_TO_DECIMAL(cleaning_fee_charged, 10, 2), 2)  AS cleaning_fee_charged,
        ROUND(TRY_TO_DECIMAL(service_fee, 10, 2), 2)           AS service_fee,
        ROUND(TRY_TO_DECIMAL(taxes, 10, 2), 2)                 AS taxes,

        -- Price reconciliation 
        -- total_price doesn't always equal sum of components
        ROUND(
            COALESCE(TRY_TO_DECIMAL(base_price, 10, 2), 0) +
            COALESCE(TRY_TO_DECIMAL(cleaning_fee_charged, 10, 2), 0) +
            COALESCE(TRY_TO_DECIMAL(service_fee, 10, 2), 0) +
            COALESCE(TRY_TO_DECIMAL(taxes, 10, 2), 0),
        2)                                    AS calculated_total_price,

        CASE
            WHEN ABS(
                TRY_TO_DECIMAL(total_price, 10, 2) -
                (
                    COALESCE(TRY_TO_DECIMAL(base_price, 10, 2), 0) +
                    COALESCE(TRY_TO_DECIMAL(cleaning_fee_charged, 10, 2), 0) +
                    COALESCE(TRY_TO_DECIMAL(service_fee, 10, 2), 0) +
                    COALESCE(TRY_TO_DECIMAL(taxes, 10, 2), 0)
                )
            ) > 1.00 THEN TRUE
            ELSE FALSE
        END                                   AS is_price_mismatch,

        -- Timestamps 
        TRY_TO_TIMESTAMP_TZ(booked_at)        AS booked_at,
        TRY_TO_TIMESTAMP_TZ(updated_at)       AS updated_at,

        -- Data quality flag
        CASE
            WHEN TRY_TO_NUMBER(num_children) < 0 THEN TRUE
            WHEN _check_out_date < _check_in_date THEN TRUE
            ELSE FALSE
        END                                   AS is_data_quality_issue,

        -- Metadata
        _loaded_at,
        _stream_timestamp,
        _source_file

    FROM parsed

),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id_clean
            ORDER BY updated_at DESC,
                     TRY_TO_TIMESTAMP_TZ(_stream_timestamp) DESC,
                     _loaded_at DESC
        ) AS _row_num
    FROM cleaned
)

SELECT * EXCLUDE _row_num
FROM deduped
WHERE _row_num = 1
