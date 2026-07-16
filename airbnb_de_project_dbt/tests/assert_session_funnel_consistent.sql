SELECT session_id
FROM {{ ref('gold_fct_sessions') }}
WHERE (had_booking_complete AND NOT had_booking_start)
   OR (had_booking_start AND NOT had_click)
   OR (had_click AND NOT had_view)
