-- Fails if any booking has check_out before check_in
-- We flagged these in Silver but Gold fct_bookings filters
-- them out with WHERE is_date_valid = TRUE
-- This test confirms the filter is working

SELECT booking_id_clean
FROM {{ ref('gold_fct_bookings') }}
WHERE check_out_date < check_in_date