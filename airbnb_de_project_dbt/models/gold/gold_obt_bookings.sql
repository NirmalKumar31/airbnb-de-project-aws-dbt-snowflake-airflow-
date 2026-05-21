{{ config(materialized='table') }}

-- ── Metadata-driven OBT ───────────────────────────────────────────────
-- Column definitions live in dbt_project.yml under vars.obt_booking_cols
-- To add/remove a column: edit the vars list only. No SQL changes needed.
-- Source aliases:
--   b = gold_fct_bookings (booking + listing + host context)
--   g = gold_dim_guests   (guest attributes)
--   d = dim_date          (check-in date attributes)
-- ─────────────────────────────────────────────────────────────────────

WITH bookings AS (
    SELECT * FROM {{ ref('gold_fct_bookings') }}
),

guests AS (
    SELECT * FROM {{ ref('gold_dim_guests') }}
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
),

joined AS (
    SELECT
        -- All booking + listing + host columns from fact table
        b.*,

        -- Guest dimension columns
        g.guest_name,
        g.nationality_iso2,
        g.gender,
        g.total_trips,
        g.average_rating_as_guest,
        g.verified_id,

        -- Date dimension columns
        d.year,
        d.month,
        d.month_name,
        d.quarter,
        d.day_name,
        d.is_weekend,
        d.is_us_federal_holiday,
        d.season

    FROM bookings b
    LEFT JOIN guests g    ON b.guest_key = g.guest_key
    LEFT JOIN date_dim d  ON b.check_in_date = d.full_date
)

-- Metadata-driven SELECT — reads column list from dbt_project.yml vars
SELECT
    {% for item in var('obt_booking_cols') %}
    {{ item.col }}{% if not loop.last %},{% endif %}
    {% endfor %}

FROM joined