SELECT guest_id, guest_phone
FROM {{ ref('silver_guests') }}
WHERE guest_phone IS NOT NULL
  AND NOT REGEXP_LIKE(guest_phone, '^\\+1[0-9]{10}$')
