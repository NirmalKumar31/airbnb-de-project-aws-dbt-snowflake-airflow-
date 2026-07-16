{% snapshot listings_snapshot %}

{{
    config(
        target_schema  = 'snapshots',
        unique_key     = 'listing_id',
        strategy       = 'check',
        check_cols     = [
            'host_id', 'listing_name', 'listing_description', 'room_type',
            'property_type', 'accommodates', 'bathrooms', 'bedrooms', 'beds',
            'price_per_night', 'cleaning_fee', 'minimum_nights',
            'maximum_nights', 'cancellation_policy', 'instant_bookable',
            'amenities_array', 'amenity_count', 'neighborhood', 'city',
            'country', 'latitude', 'longitude', 'is_coords_suspect',
            'review_scores_rating', 'review_scores_cleanliness',
            'review_scores_checkin', 'review_scores_communication',
            'review_scores_location', 'review_scores_value',
            'number_of_reviews', 'last_review_date', 'is_data_quality_issue'
        ],
        invalidate_hard_deletes = true
    )
}}

SELECT * FROM {{ ref('silver_listings') }}

{% endsnapshot %}
