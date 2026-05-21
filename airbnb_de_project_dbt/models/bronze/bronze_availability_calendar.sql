{{ config(
    materialized     = 'incremental',
    unique_key       = ['listing_id', 'calendar_date'],
    on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            -- Dedup on the composite natural key
            PARTITION BY listing_id, calendar_date
            ORDER BY _loaded_at DESC
        ) AS _row_num
    FROM {{ source('raw', 'raw_availability_calendar') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (
    SELECT COALESCE(MAX(_loaded_at), '2000-01-01'::TIMESTAMP_TZ)
    FROM {{ this }}
)
    {% endif %}
)
SELECT * EXCLUDE _row_num
FROM source
WHERE _row_num = 1