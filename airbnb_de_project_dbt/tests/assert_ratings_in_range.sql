-- Fails if any rating is outside 0-5 range after Silver cleaning
-- Proves the scale normalisation worked correctly

SELECT review_id
FROM {{ ref('silver_reviews') }}
WHERE overall_rating NOT BETWEEN 0 AND 5
   OR cleanliness_rating NOT BETWEEN 0 AND 5
   OR accuracy_rating NOT BETWEEN 0 AND 5
   OR communication_rating NOT BETWEEN 0 AND 5
   OR checkin_rating NOT BETWEEN 0 AND 5
   OR location_rating NOT BETWEEN 0 AND 5
   OR value_rating NOT BETWEEN 0 AND 5
