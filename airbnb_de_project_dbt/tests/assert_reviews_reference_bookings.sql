SELECT r.review_id
FROM {{ ref('silver_reviews') }} r
LEFT JOIN {{ ref('silver_bookings') }} b
  ON r.booking_id_clean = b.booking_id_clean
WHERE b.booking_id_clean IS NULL
