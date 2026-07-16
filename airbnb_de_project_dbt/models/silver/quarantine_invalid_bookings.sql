{{ config(materialized='table') }}

SELECT
    *,
    CASE
        WHEN check_in_date IS NULL OR check_out_date IS NULL THEN 'unparseable_date'
        WHEN check_out_date < check_in_date THEN 'checkout_before_checkin'
        WHEN num_children IS NULL AND is_data_quality_issue THEN 'invalid_guest_count'
        ELSE 'other_quality_issue'
    END AS quarantine_reason
FROM {{ ref('silver_bookings') }}
WHERE NOT is_date_valid
   OR is_data_quality_issue
