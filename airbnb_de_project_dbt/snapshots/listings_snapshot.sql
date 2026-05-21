{% snapshot listings_snapshot %}

{{
    config(
        target_schema  = 'snapshots',
        unique_key     = 'listing_id',
        strategy       = 'check',
        check_cols     = [
            'price_per_night',
            'cancellation_policy',
            'minimum_nights',
            'room_type',
            'instant_bookable'
        ],
        invalidate_hard_deletes = true
    )
}}

SELECT * FROM {{ ref('silver_listings') }}

{% endsnapshot %}