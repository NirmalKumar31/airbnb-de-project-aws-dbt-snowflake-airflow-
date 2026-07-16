{{ config(materialized='table') }}

SELECT
    MD5(host_id || '|' || CAST(dbt_valid_from AS VARCHAR)) AS host_key,

    host_id,
    host_name,
    host_email,
    host_since,
    host_response_time,
    host_response_rate,
    host_acceptance_rate,
    is_superhost,
    host_identity_verified,
    host_listings_count,
    host_total_listings_count,
    has_listings_count_discrepancy,
    host_verifications,
    host_location,

    dbt_scd_id,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,

    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY host_id ORDER BY dbt_valid_from) = 1
            THEN '1900-01-01'::TIMESTAMP_TZ
        ELSE dbt_valid_from
    END AS effective_valid_from,

    -- Derived: current record has no end date
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current_record

FROM {{ ref('hosts_snapshot') }}
