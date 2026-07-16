SELECT 'listing' AS entity_type, a.listing_id AS entity_id,
       a.listing_key AS version_key, b.listing_key AS overlapping_version_key
FROM {{ ref('gold_dim_listings') }} a
JOIN {{ ref('gold_dim_listings') }} b
  ON a.listing_id = b.listing_id
 AND a.listing_key < b.listing_key
 AND a.effective_valid_from < COALESCE(b.dbt_valid_to, '9999-12-31'::TIMESTAMP_TZ)
 AND b.effective_valid_from < COALESCE(a.dbt_valid_to, '9999-12-31'::TIMESTAMP_TZ)

UNION ALL

SELECT 'host' AS entity_type, a.host_id AS entity_id,
       a.host_key AS version_key, b.host_key AS overlapping_version_key
FROM {{ ref('gold_dim_hosts') }} a
JOIN {{ ref('gold_dim_hosts') }} b
  ON a.host_id = b.host_id
 AND a.host_key < b.host_key
 AND a.effective_valid_from < COALESCE(b.dbt_valid_to, '9999-12-31'::TIMESTAMP_TZ)
 AND b.effective_valid_from < COALESCE(a.dbt_valid_to, '9999-12-31'::TIMESTAMP_TZ)
