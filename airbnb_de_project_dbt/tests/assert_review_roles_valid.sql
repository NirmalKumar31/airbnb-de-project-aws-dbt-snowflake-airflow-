SELECT r.review_id
FROM {{ ref('silver_reviews') }} r
LEFT JOIN {{ ref('silver_guests') }} reviewer_guest
  ON r.reviewer_id = reviewer_guest.guest_id
LEFT JOIN {{ ref('silver_hosts') }} reviewer_host
  ON r.reviewer_id = reviewer_host.host_id
LEFT JOIN {{ ref('silver_guests') }} reviewee_guest
  ON r.reviewee_id = reviewee_guest.guest_id
LEFT JOIN {{ ref('silver_hosts') }} reviewee_host
  ON r.reviewee_id = reviewee_host.host_id
WHERE (r.review_type = 'guest_to_host'
       AND (reviewer_guest.guest_id IS NULL OR reviewee_host.host_id IS NULL))
   OR (r.review_type = 'host_to_guest'
       AND (reviewer_host.host_id IS NULL OR reviewee_guest.guest_id IS NULL))
