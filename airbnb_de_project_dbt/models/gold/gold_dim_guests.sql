{{ config(materialized='table') }}

SELECT
    MD5(guest_id)                         AS guest_key,

    guest_id,
    guest_name,
    guest_email,
    guest_phone,
    date_of_birth,
    guest_since,
    nationality_iso2,
    gender,
    preferred_language,
    verified_id,
    is_banned,
    total_trips,
    average_rating_as_guest,

    _loaded_at

FROM {{ ref('silver_guests') }}
WHERE COALESCE(is_banned, FALSE) = FALSE