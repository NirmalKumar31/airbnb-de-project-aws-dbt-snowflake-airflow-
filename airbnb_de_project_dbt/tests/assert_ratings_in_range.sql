-- Fails if any rating is outside 0-5 range after Silver cleaning
-- Proves the scale normalisation worked correctly

SELECT review_id
FROM {{ ref('silver_reviews') }}
WHERE overall_rating > 5
   OR cleanliness_rating > 5
   OR accuracy_rating > 5
   OR communication_rating > 5
   OR checkin_rating > 5
   OR location_rating > 5
   OR value_rating > 5