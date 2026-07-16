{{ config(
    materialized     = 'incremental',
    unique_key       = 'event_id',
    on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY TRY_TO_TIMESTAMP_TZ(_stream_timestamp) DESC, _loaded_at DESC
        ) AS _row_num
    FROM {{ source('raw', 'raw_listing_events') }}
    {% if is_incremental() %}
    -- listing_events is append-only and can become the highest-volume source.
    -- The incremental filter is especially important here.
    WHERE _loaded_at >= (
    SELECT COALESCE(MAX(_loaded_at), '2000-01-01'::TIMESTAMP_TZ)
    FROM {{ this }}
)
    {% endif %}
)
SELECT * EXCLUDE _row_num
FROM source
WHERE _row_num = 1
