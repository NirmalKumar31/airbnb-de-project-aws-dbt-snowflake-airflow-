{% snapshot hosts_snapshot %}

{{
    config(
        target_schema  = 'snapshots',
        unique_key     = 'host_id',
        strategy       = 'check',
        check_cols     = [
            'is_superhost',
            'host_response_rate',
            'host_acceptance_rate',
            'host_listings_count'
        ],
        invalidate_hard_deletes = true
    )
}}

SELECT * FROM {{ ref('silver_hosts') }}

{% endsnapshot %}