{{ config(
    materialized     = 'incremental',
    unique_key       = 'host_id',
    on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY host_id
            ORDER BY _loaded_at DESC
        ) AS row_num
    FROM {{ source('raw', 'raw_hosts') }}
{% if is_incremental() %}
    WHERE _loaded_at > (
    SELECT COALESCE(MAX(_loaded_at), '2000-01-01'::TIMESTAMP_TZ)
    FROM {{ this }}
)
{% endif %}
)

SELECT * EXCLUDE row_num
FROM source
WHERE row_num = 1
