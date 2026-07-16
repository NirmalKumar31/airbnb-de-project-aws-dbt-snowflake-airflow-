SELECT b.booking_id_clean
FROM {{ ref('silver_bookings') }} b
JOIN {{ ref('silver_listings') }} l
  ON b.listing_id = l.listing_id
WHERE b.host_id <> l.host_id
