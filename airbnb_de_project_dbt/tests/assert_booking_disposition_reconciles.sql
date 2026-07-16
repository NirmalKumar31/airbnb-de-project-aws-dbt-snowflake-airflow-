WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ ref('silver_bookings') }}) AS silver_count,
        (SELECT COUNT(*) FROM {{ ref('gold_fct_bookings') }}) AS valid_gold_count,
        (SELECT COUNT(*) FROM {{ ref('quarantine_invalid_bookings') }}
          WHERE NOT is_date_valid) AS invalid_quarantine_count
)
SELECT *
FROM counts
WHERE silver_count <> valid_gold_count + invalid_quarantine_count
