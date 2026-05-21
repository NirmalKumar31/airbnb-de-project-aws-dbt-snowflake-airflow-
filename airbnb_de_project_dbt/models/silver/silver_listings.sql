{{ config(
    materialized         = 'incremental',
    unique_key           = 'listing_id',
    incremental_strategy = 'merge',
    on_schema_change     = 'sync_all_columns'
) }}

WITH source AS (

    SELECT * FROM {{ ref('bronze_listings') }}

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
        LOWER(REGEXP_REPLACE(host_id,    '[^a-zA-Z0-9]', '')) AS host_id,

        -- Basic attributes
        TRIM(listing_name)                        AS listing_name,
        TRIM(listing_description)                 AS listing_description,
        LOWER(TRIM(room_type))                    AS room_type,
        LOWER(TRIM(property_type))                AS property_type,
        TRY_TO_NUMBER(accommodates)               AS accommodates,
        TRY_TO_NUMBER(beds)                       AS beds,
        TRY_TO_NUMBER(bedrooms)                   AS bedrooms,

        -- Bathrooms 
        -- Arrives as "1.5", "1.5 baths", "shared"
        CASE
            WHEN LOWER(TRIM(bathrooms)) = 'shared' THEN 0.5
            ELSE ROUND(
                TRY_TO_DECIMAL(
                    REGEXP_REPLACE(TRIM(bathrooms), '[^0-9.]', ''),
                    5, 1
                ), 1
            )
        END                                       AS bathrooms,

        -- Prices
        -- Arrives as "$120.00", "$1,200", "120.5", "120"
        ROUND(
            TRY_TO_DECIMAL(
                REGEXP_REPLACE(TRIM(price_per_night), '[$,]', ''),
                10, 2
            ), 2
        )                                         AS price_per_night,

        ROUND(
            TRY_TO_DECIMAL(
                REGEXP_REPLACE(TRIM(cleaning_fee), '[$,]', ''),
                10, 2
            ), 2
        )                                         AS cleaning_fee,

        -- Nights
        -- minimum_nights occasionally arrives as 0 or -1 (invalid)
        CASE
            WHEN TRY_TO_NUMBER(minimum_nights) <= 0 THEN NULL
            ELSE TRY_TO_NUMBER(minimum_nights)
        END                                       AS minimum_nights,

        TRY_TO_NUMBER(maximum_nights)             AS maximum_nights,

        -- Policies
        LOWER(TRIM(cancellation_policy))          AS cancellation_policy,

        CASE
            WHEN LOWER(TRIM(instant_bookable))
                IN ('t','true','1','y','yes') THEN TRUE
            WHEN LOWER(TRIM(instant_bookable))
                IN ('f','false','0','n','no') THEN FALSE
            ELSE NULL
        END                                       AS instant_bookable,

        -- Amenities
        -- Most complex column: JSON array / pipe / comma-space
        CASE
            WHEN TRY_PARSE_JSON(amenities) IS NOT NULL
                THEN TRY_PARSE_JSON(amenities)
            WHEN CONTAINS(amenities, '|')
                THEN TO_ARRAY(SPLIT(amenities, '|'))
            ELSE
                TO_ARRAY(SPLIT(amenities, ', '))
        END                                       AS amenities_array,

        ARRAY_SIZE(
            CASE
                WHEN TRY_PARSE_JSON(amenities) IS NOT NULL
                    THEN TRY_PARSE_JSON(amenities)
                WHEN CONTAINS(amenities, '|')
                    THEN TO_ARRAY(SPLIT(amenities, '|'))
                ELSE TO_ARRAY(SPLIT(amenities, ', '))
            END
        )                                         AS amenity_count,

        -- Location
        TRIM(neighborhood)                        AS neighborhood,
        INITCAP(TRIM(city))                       AS city,

        CASE
            WHEN UPPER(TRIM(country)) IN (
                'US','USA','UNITED STATES','UNITED STATES OF AMERICA'
            ) THEN 'US'
            ELSE UPPER(TRIM(country))
        END                                       AS country,

        -- Coordinates
        -- Boston lat ~42, lon ~-71
        -- If lat is negative and lon is positive → likely swapped
        TRY_TO_DOUBLE(latitude)                    AS latitude,
        TRY_TO_DOUBLE(longitude)                   AS longitude,

        CASE
            WHEN TRY_TO_DOUBLE(latitude) < 0
             AND TRY_TO_DOUBLE(longitude) > 0
            THEN TRUE
            ELSE FALSE
        END                                       AS is_coords_suspect,

        -- Review scores
        -- Scale mismatch: some 0-5, some 0-100
        -- If value > 5 divide by 20 to bring to 0-5 scale
        -- Round to 2 decimal places
        CASE
            WHEN TRY_TO_DOUBLE(review_scores_rating) IS NULL    THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_rating) = 0        THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_rating) > 5
                THEN ROUND(TRY_TO_DOUBLE(review_scores_rating) / 20.0, 2)
            ELSE ROUND(TRY_TO_DOUBLE(review_scores_rating), 2)
        END                                       AS review_scores_rating,

        CASE
            WHEN TRY_TO_DOUBLE(review_scores_cleanliness) IS NULL THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_cleanliness) = 0    THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_cleanliness) > 5
                THEN ROUND(TRY_TO_DOUBLE(review_scores_cleanliness) / 20.0, 2)
            ELSE ROUND(TRY_TO_DOUBLE(review_scores_cleanliness), 2)
        END                                       AS review_scores_cleanliness,

        CASE
            WHEN TRY_TO_DOUBLE(review_scores_checkin) IS NULL    THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_checkin) = 0        THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_checkin) > 5
                THEN ROUND(TRY_TO_DOUBLE(review_scores_checkin) / 20.0, 2)
            ELSE ROUND(TRY_TO_DOUBLE(review_scores_checkin), 2)
        END                                       AS review_scores_checkin,

        CASE
            WHEN TRY_TO_DOUBLE(review_scores_communication) IS NULL THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_communication) = 0    THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_communication) > 5
                THEN ROUND(TRY_TO_DOUBLE(review_scores_communication) / 20.0, 2)
            ELSE ROUND(TRY_TO_DOUBLE(review_scores_communication), 2)
        END                                       AS review_scores_communication,

        CASE
            WHEN TRY_TO_DOUBLE(review_scores_location) IS NULL   THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_location) = 0       THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_location) > 5
                THEN ROUND(TRY_TO_DOUBLE(review_scores_location) / 20.0, 2)
            ELSE ROUND(TRY_TO_DOUBLE(review_scores_location), 2)
        END                                       AS review_scores_location,

        CASE
            WHEN TRY_TO_DOUBLE(review_scores_value) IS NULL      THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_value) = 0          THEN NULL
            WHEN TRY_TO_DOUBLE(review_scores_value) > 5
                THEN ROUND(TRY_TO_DOUBLE(review_scores_value) / 20.0, 2)
            ELSE ROUND(TRY_TO_DOUBLE(review_scores_value), 2)
        END                                       AS review_scores_value,

        TRY_TO_NUMBER(number_of_reviews)          AS number_of_reviews,

        TRY_TO_DATE(
            REGEXP_REPLACE(last_review_date, '(st|nd|rd|th),?', ''),
            'AUTO'
        )                                         AS last_review_date,

        -- Data quality flag
        CASE
            WHEN TRY_TO_NUMBER(minimum_nights) <= 0 THEN TRUE
            ELSE FALSE
        END                                       AS is_data_quality_issue,

        -- Metadata
        _loaded_at,
        _stream_timestamp,
        _source_file

    FROM source

)

SELECT * FROM cleaned