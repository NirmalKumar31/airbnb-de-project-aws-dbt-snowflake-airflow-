{{ config(
    materialized     = 'incremental',
    unique_key       = 'event_id',
    on_schema_change = 'sync_all_columns'
) }}

-- listing_events is append-only and high volume (~12,000/hr)
-- No merge needed — events never change after they are created
-- The incremental filter is especially important here

WITH source AS (

    SELECT * FROM {{ ref('bronze_listing_events') }}

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
        LOWER(REGEXP_REPLACE(event_id, '[^a-zA-Z0-9]', '')) AS event_id,

        -- Event type normalisation
        -- Casing chaos + synonym chaos combined:
        -- view/View/VIEW/page_view      → view
        -- favourite/favorite/save       → save
        -- SearchImpression/impression   → search_impression
    

        CASE
            WHEN LOWER(event_type)
                IN ('view','page_view')                       THEN 'view'
             WHEN LOWER(event_type)
                IN ('click','listing_click')                  THEN 'click'
            WHEN LOWER(event_type)
                IN ('favourite','favorite','save','wishlist_add') THEN 'save'
            WHEN LOWER(event_type)
                IN ('booking_start','bookingstart')           THEN 'booking_start'
            WHEN LOWER(event_type)
                IN ('booking_complete','bookingcomplete',
            'booking_confirmed','bookingconfirmed')   THEN 'booking_complete'
            WHEN LOWER(event_type)
                IN ('search_impression','searchimpression',
            'impression','page_view')                 THEN 'search_impression'
            ELSE LOWER(event_type)
        END       AS event_type_clean,

        -- Flag events that could not be mapped to controlled vocabulary
        CASE
            WHEN LOWER(event_type) IN (
                'view','page_view','click','listing_click',
                'favourite','favorite','save','wishlist_add',
                'booking_start','booking_complete','booking_confirmed',
                'searchimpression','search_impression','impression',
                'bookingstart','bookingcomplete','searchimpression','bookingstart'
            ) THEN TRUE
            ELSE FALSE
        END                                   AS is_event_type_mapped,

        -- Foreign keys
        LOWER(REGEXP_REPLACE(listing_id, '[^a-zA-Z0-9]', '')) AS listing_id,
        -- guest_id is nullable (30% of events are anonymous)
        LOWER(REGEXP_REPLACE(guest_id, '[^a-zA-Z0-9]', ''))   AS guest_id,
        TRIM(session_id)                      AS session_id,

        -- Device
        -- mobile/Mobile/MOBILE → mobile
        CASE
            WHEN LOWER(TRIM(device_type)) = 'mobile'  THEN 'mobile'
            WHEN LOWER(TRIM(device_type)) = 'desktop' THEN 'desktop'
            WHEN LOWER(TRIM(device_type)) = 'tablet'  THEN 'tablet'
            ELSE LOWER(TRIM(device_type))
        END                                   AS device_type,

        -- OS version splitting
        -- "iOS 16.2"     → os_name="iOS",    os_version_clean="16.2"
        -- "macOS Ventura" → os_name="macOS", os_version_clean="Ventura"
        -- "Android 13"   → os_name="Android", os_version_clean="13"
        SPLIT_PART(TRIM(os_version), ' ', 1)  AS os_name,
        CASE
            WHEN TRIM(os_version) LIKE '% %'
            THEN SUBSTR(TRIM(os_version),
                        LENGTH(SPLIT_PART(TRIM(os_version), ' ', 1)) + 2)
            ELSE NULL
        END                                   AS os_version_clean,

        TRIM(browser)                         AS browser,

        -- Country code
        -- Occasionally arrives as 3-letter ISO (USA, GBR, etc.)
        -- Map to 2-letter standard
        CASE country_code
            WHEN 'USA' THEN 'US'  WHEN 'GBR' THEN 'GB'
            WHEN 'CAN' THEN 'CA'  WHEN 'AUS' THEN 'AU'
            WHEN 'DEU' THEN 'DE'  WHEN 'FRA' THEN 'FR'
            WHEN 'JPN' THEN 'JP'  WHEN 'BRA' THEN 'BR'
            ELSE UPPER(TRIM(country_code))
        END                                   AS country_code,

        TRIM(search_query)                    AS search_query,

        -- Price shown 
        ROUND(
            TRY_TO_DECIMAL(
                REGEXP_REPLACE(TRIM(price_shown), '[$,]', ''),
                10, 2
            ), 2
        )                                     AS price_shown,

        -- Position
        -- 1-indexed field: 0 is invalid → null + flag
        CASE
            WHEN TRY_TO_NUMBER(position_in_results) = 0 THEN NULL
            ELSE TRY_TO_NUMBER(position_in_results)
        END                                   AS position_in_results,

        CASE
            WHEN TRY_TO_NUMBER(position_in_results) = 0 THEN TRUE
            ELSE FALSE
        END                                   AS is_position_invalid,

        -- Anonymous flag
        CASE WHEN guest_id IS NULL THEN TRUE ELSE FALSE END
                                              AS is_anonymous,

        TRY_TO_TIMESTAMP_TZ(event_timestamp)  AS event_timestamp,

        -- Metadata
        _loaded_at,
        _stream_timestamp,
        _source_file

    FROM source

),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY _loaded_at DESC
        ) AS _row_num
    FROM cleaned
)

SELECT * EXCLUDE _row_num
FROM deduped
WHERE _row_num = 1