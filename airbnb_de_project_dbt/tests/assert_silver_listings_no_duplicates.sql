-- Fails if silver_listings has duplicate listing_ids
-- Prevents snapshot from failing with error 100090

SELECT listing_id, COUNT(*) AS cnt
FROM {{ ref('silver_listings') }}
GROUP BY listing_id
HAVING cnt > 1