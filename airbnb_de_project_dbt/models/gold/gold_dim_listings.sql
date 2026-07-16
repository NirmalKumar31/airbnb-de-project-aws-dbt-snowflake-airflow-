{{ config(materialized='table') }}

SELECT
    MD5(listing_id || '|' || CAST(dbt_valid_from AS VARCHAR)) AS listing_key,

    listing_id,
    host_id,
    listing_name,
    listing_description,
    room_type,
    property_type,
    accommodates,
    beds,
    bedrooms,
    bathrooms,
    price_per_night,
    cleaning_fee,
    minimum_nights,
    maximum_nights,
    cancellation_policy,
    instant_bookable,
    amenities_array,
    amenity_count,
    neighborhood,
    city,
    country,
    latitude,
    longitude,
    is_coords_suspect,
    review_scores_rating,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_location,
    review_scores_value,
    number_of_reviews,
    last_review_date,
    is_data_quality_issue,

    dbt_scd_id,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,

    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY dbt_valid_from) = 1
            THEN '1900-01-01'::TIMESTAMP_TZ
        ELSE dbt_valid_from
    END AS effective_valid_from,

    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current_record

FROM {{ ref('listings_snapshot') }}
