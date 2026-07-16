-- Fails if any completed booking has zero or negative revenue
-- A completed booking must have been paid for

SELECT booking_id_clean
FROM {{ ref('gold_fct_bookings') }}
WHERE booking_status = 'completed'
  AND (recognized_revenue IS NULL OR recognized_revenue <= 0)
