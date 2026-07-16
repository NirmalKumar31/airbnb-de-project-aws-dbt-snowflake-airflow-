{% snapshot hosts_snapshot %}

{{
    config(
        target_schema  = 'snapshots',
        unique_key     = 'host_id',
        strategy       = 'check',
        check_cols     = [
            'host_name', 'host_email', 'host_since', 'host_response_time',
            'host_response_rate', 'host_acceptance_rate', 'is_superhost',
            'host_identity_verified', 'host_listings_count',
            'host_total_listings_count', 'has_listings_count_discrepancy',
            'host_verifications', 'host_location'
        ],
        invalidate_hard_deletes = true
    )
}}

SELECT * FROM {{ ref('silver_hosts') }}

{% endsnapshot %}
