-- ============================================================
--  Airbnb Streaming DE — Snowflake RAW Layer Setup
-- ============================================================
-- ════════════════════════════════════════════════════════════
--  SECTION 1: DATABASE AND SCHEMA SETUP
-- ════════════════════════════════════════════════════════════

-- Use a role that can create databases and integrations
USE ROLE SYSADMIN;

-- The main database for this project
CREATE DATABASE IF NOT EXISTS AIRBNB_DE
    DATA_RETENTION_TIME_IN_DAYS = 10
    COMMENT = 'Airbnb streaming data engineering portfolio project';

USE DATABASE AIRBNB_DE;

-- One schema per medallion layer
CREATE SCHEMA IF NOT EXISTS RAW        COMMENT = 'Raw data from Snowpipe auto-ingest. Never write here manually.';
CREATE SCHEMA IF NOT EXISTS BRONZE     COMMENT = 'dbt Bronze: deduped raw views, no transformation.';
CREATE SCHEMA IF NOT EXISTS SILVER     COMMENT = 'dbt Silver: cleaned, typed, merged.';
CREATE SCHEMA IF NOT EXISTS GOLD       COMMENT = 'dbt Gold: OBT and Star Schema models.';
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS  COMMENT = 'dbt SCD Type-2 snapshots for hosts and listings.';

-- Separate database for dbt metadata (optional but clean)
CREATE DATABASE IF NOT EXISTS DBT_METADATA;
CREATE SCHEMA IF NOT EXISTS DBT_METADATA.AIRBNB;

-- Warehouse for dbt transformations (XS for this project)
CREATE WAREHOUSE IF NOT EXISTS AIRBNB_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 240          -- suspend after 4 min idle
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for dbt + Snowpipe compute';

USE WAREHOUSE AIRBNB_WH;

SHOW SCHEMAS IN DATABASE AIRBNB_DE;

-- ════════════════════════════════════════════════════════════
--  SECTION 2: FILE FORMAT
--  Tell Snowpipe how to parse the files Firehose drops in S3.
--  JSON, newline-delimited, UTF-8.
-- ════════════════════════════════════════════════════════════

USE SCHEMA AIRBNB_DE.RAW;

CREATE OR REPLACE FILE FORMAT json_ndjson
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE          -- Firehose writes one JSON object per line
    IGNORE_UTF8_ERRORS = TRUE          -- Don't fail on encoding issues
    NULL_IF = ('NULL', 'null', '')     -- Treat these strings as NULL at ingest
    COMMENT = 'NDJSON format for Firehose-delivered files';

-- ════════════════════════════════════════════════════════════
--  SECTION 3: STORAGE INTEGRATION (IAM TRUST HANDSHAKE)
-- ════════════════════════════════════════════════════════════

-- Storage integrations require ACCOUNTADMIN privileges
USE ROLE ACCOUNTADMIN;

-- Replace YOUR_AWS_ACCOUNT_ID and S3_BUCKET_NAME with your actual values
CREATE OR REPLACE STORAGE INTEGRATION airbnb_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::000000000000:role/role-name-goes-here'  
    STORAGE_ALLOWED_LOCATIONS = ('s3 bucket name goes here')
    COMMENT = 'Allows Snowpipe to read Firehose-delivered files from S3';

-- Run this and save the two output values !!
DESC INTEGRATION airbnb_s3_integration;
-- You need:
--   STORAGE_AWS_IAM_USER_ARN  → looks like arn:aws:iam::000000000000:user/some-user
--    arn:aws:iam::000000000000:user/000000000
--   STORAGE_AWS_EXTERNAL_ID   → looks like ABCDEF1234567890

-- ════════════════════════════════════════════════════════════
--  SECTION 4: EXTERNAL STAGE
-- ════════════════════════════════════════════════════════════

CREATE OR REPLACE STAGE airbnb_raw_stage
    STORAGE_INTEGRATION = airbnb_s3_integration
    URL = 's3 location goes here'  -- s3://bucket/airbnb/
    FILE_FORMAT = AIRBNB_DE.RAW.json_ndjson
    COMMENT = 'Points to all entity prefixes under s3://bucket/airbnb/';

-- Verify the stage can see S3 (run after S3 has some files)
LIST @airbnb_raw_stage/;


-- ════════════════════════════════════════════════════════════
--  SECTION 5: RAW TABLES
-- ════════════════════════════════════════════════════════════
-- ── Table 01: raw_hosts ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_hosts (
    -- Natural key
    host_id                  VARCHAR,
    -- Attributes (all VARCHAR — type casting in Silver)
    host_name                VARCHAR,
    host_email               VARCHAR,
    host_since               VARCHAR,        -- dirty: ISO / US / natural language
    host_location            VARCHAR,
    host_response_time       VARCHAR,
    host_response_rate       VARCHAR,        -- dirty: '98%' / '0.98' / '98' / null
    host_acceptance_rate     VARCHAR,        -- same mess
    is_superhost             VARCHAR,        -- dirty: t/f/True/False/1/0/Y/N/yes/no
    host_listings_count      VARCHAR,        -- dirty: sometimes 0 when it shouldn't be
    host_total_listings_count VARCHAR,
    host_verifications       VARCHAR,        -- dirty: JSON / pipe / comma-space
    host_identity_verified   VARCHAR,        -- dirty: boolean chaos
    -- Metadata (set at ingest time)
    _loaded_at               TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _stream_timestamp        VARCHAR,
    _source_file             VARCHAR
);

-- ── Table 02: raw_listings ───────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_listings (
    listing_id                    VARCHAR,
    host_id                       VARCHAR,
    listing_name                  VARCHAR,
    listing_description           VARCHAR,
    property_type                 VARCHAR,
    room_type                     VARCHAR,
    accommodates                  VARCHAR,
    bathrooms                     VARCHAR,   -- dirty: '1.5' / '1.5 baths' / 'shared'
    bedrooms                      VARCHAR,   -- dirty: null for studios (valid null)
    beds                          VARCHAR,
    amenities                     VARCHAR,   -- dirty: JSON / pipe / comma-space
    price_per_night               VARCHAR,   -- dirty: '$120.00' / '$1,200' / '120.5' / '120'
    cleaning_fee                  VARCHAR,   -- dirty: same price formats + nullable
    minimum_nights                VARCHAR,   -- dirty: occasionally 0 or -1 (invalid)
    maximum_nights                VARCHAR,
    cancellation_policy           VARCHAR,
    instant_bookable              VARCHAR,   -- dirty: boolean chaos
    neighborhood                  VARCHAR,
    city                          VARCHAR,
    country                       VARCHAR,   -- dirty: US / USA / United States
    latitude                      VARCHAR,   -- dirty: occasionally swapped with longitude
    longitude                     VARCHAR,
    review_scores_rating          VARCHAR,   -- dirty: 0-5 scale OR 0-100 scale
    review_scores_cleanliness     VARCHAR,
    review_scores_checkin         VARCHAR,
    review_scores_communication   VARCHAR,
    review_scores_location        VARCHAR,
    review_scores_value           VARCHAR,
    number_of_reviews             VARCHAR,
    last_review_date              VARCHAR,   -- dirty: date format variety
    _loaded_at                    TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _stream_timestamp             VARCHAR,
    _source_file                  VARCHAR
);

-- ── Table 03: raw_guests ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_guests (
    guest_id                VARCHAR,
    guest_name              VARCHAR,
    guest_email             VARCHAR,
    guest_phone             VARCHAR,    -- dirty: 4 different phone formats
    date_of_birth           VARCHAR,
    guest_since             VARCHAR,
    nationality             VARCHAR,    -- dirty: US/USA/United States/etc
    gender                  VARCHAR,    -- dirty: M/Male/male/MALE/etc
    preferred_language      VARCHAR,
    verified_id             VARCHAR,    -- dirty: boolean chaos
    total_trips             VARCHAR,    -- dirty: occasionally negative (invalid)
    average_rating_as_guest VARCHAR,    -- dirty: 0.0 instead of null for zero-trip guests
    is_banned               VARCHAR,    -- dirty: boolean chaos
    _loaded_at              TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _stream_timestamp       VARCHAR,
    _source_file            VARCHAR
);

-- ── Table 04: raw_bookings ───────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_bookings (
    booking_id           VARCHAR,   -- dirty: BKG-XXXXXX / BKGXXXXXX / bkg_XXXXXX
    listing_id           VARCHAR,
    guest_id             VARCHAR,
    host_id              VARCHAR,
    check_in_date        VARCHAR,   -- dirty: date format variety
    check_out_date       VARCHAR,   -- dirty: sometimes BEFORE check_in (invalid)
    nights_count         VARCHAR,   -- dirty: pre-computed, 15% wrong
    num_guests           VARCHAR,
    num_adults           VARCHAR,
    num_children         VARCHAR,   -- dirty: occasionally -1 (invalid)
    num_infants          VARCHAR,
    booking_status       VARCHAR,   -- dirty: casing chaos (Confirmed/CONFIRMED/confirmed)
    payment_status       VARCHAR,
    total_price          VARCHAR,   -- dirty: 20% chance doesn't reconcile with components
    base_price           VARCHAR,
    cleaning_fee_charged VARCHAR,
    service_fee          VARCHAR,
    taxes                VARCHAR,
    special_requests     VARCHAR,
    booked_at            VARCHAR,   -- incremental watermark for Bronze (new rows)
    updated_at           VARCHAR,   -- incremental watermark for Silver (status updates)
    source_platform      VARCHAR,   -- dirty: web/Web/WEB/mobile_ios/Mobile iOS
    promo_code           VARCHAR,
    cancellation_reason  VARCHAR,
    _loaded_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _stream_timestamp    VARCHAR,
    _source_file         VARCHAR
);

-- ── Table 05: raw_reviews ────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_reviews (
    review_id              VARCHAR,
    booking_id             VARCHAR,
    listing_id             VARCHAR,
    reviewer_id            VARCHAR,   -- guest_id for guest_to_host; host_id for host_to_guest
    reviewee_id            VARCHAR,   -- the opposite direction
    review_type            VARCHAR,   -- guest_to_host | host_to_guest
    overall_rating         VARCHAR,   -- dirty: 5 issues (see data dictionary)
    cleanliness_rating     VARCHAR,
    accuracy_rating        VARCHAR,
    communication_rating   VARCHAR,
    checkin_rating         VARCHAR,
    location_rating        VARCHAR,
    value_rating           VARCHAR,
    review_text            VARCHAR,   -- dirty: '' / '   ' / '.' / 'N/A' / 'None' / real text
    response_text          VARCHAR,
    is_public              VARCHAR,   -- dirty: boolean chaos
    reviewed_at            VARCHAR,   -- incremental watermark for Bronze (append-only)
    _loaded_at             TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _stream_timestamp      VARCHAR,
    _source_file           VARCHAR
);

-- ── Table 06: raw_availability_calendar ─────────────────────
CREATE TABLE IF NOT EXISTS raw_availability_calendar (
    calendar_id              VARCHAR,  -- dirty: CAL-{id}-{date} / cal_{id}_{date} / UUID
    listing_id               VARCHAR,
    calendar_date            VARCHAR,
    is_available             VARCHAR,  -- dirty: 10 boolean formats
    price_on_date            VARCHAR,  -- dirty: price mess + nullable
    minimum_nights_override  VARCHAR,
    maximum_nights_override  VARCHAR,
    notes                    VARCHAR,
    _loaded_at               TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _stream_timestamp        VARCHAR,
    _source_file             VARCHAR
);

-- ── Table 07: raw_listing_events ─────────────────────────────
CREATE TABLE IF NOT EXISTS raw_listing_events (
    event_id             VARCHAR,
    event_type           VARCHAR,   -- dirty: view/View/VIEW/page_view/favourite/favorite/etc
    listing_id           VARCHAR,
    guest_id             VARCHAR,   -- dirty: nullable (30% anonymous)
    session_id           VARCHAR,
    device_type          VARCHAR,   -- dirty: mobile/Mobile/MOBILE/etc
    os_version           VARCHAR,   -- dirty: compound field (needs splitting in Silver)
    browser              VARCHAR,
    country_code         VARCHAR,   -- dirty: US (2-letter) or USA (3-letter)
    search_query         VARCHAR,
    price_shown          VARCHAR,   -- dirty: price mess + nullable
    position_in_results  VARCHAR,   -- dirty: 0 is invalid (1-indexed field)
    event_timestamp      VARCHAR,   -- incremental watermark for Bronze (append-only)
    _loaded_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _stream_timestamp    VARCHAR,
    _source_file         VARCHAR
);

-- Verify all tables exist
SHOW TABLES IN SCHEMA AIRBNB_DE.RAW;


-- ════════════════════════════════════════════════════════════
--  SECTION 6: SNOWPIPES
-- ════════════════════════════════════════════════════════════
SELECT COUNT(*) FROM AIRBNB_DE.RAW.raw_listing_events;
-- ── Pipe 01: hosts ───────────────────────────────────────────
CREATE OR REPLACE PIPE raw_hosts_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest host records from S3 → raw_hosts'
    AS
    COPY INTO AIRBNB_DE.RAW.raw_hosts (
        host_id, host_name, host_email, host_since, host_location,
        host_response_time, host_response_rate, host_acceptance_rate,
        is_superhost, host_listings_count, host_total_listings_count,
        host_verifications, host_identity_verified,
        _stream_timestamp, _source_file
    )
    FROM (
        SELECT
            $1:host_id::VARCHAR,
            $1:host_name::VARCHAR,
            $1:host_email::VARCHAR,
            $1:host_since::VARCHAR,
            $1:host_location::VARCHAR,
            $1:host_response_time::VARCHAR,
            $1:host_response_rate::VARCHAR,
            $1:host_acceptance_rate::VARCHAR,
            $1:is_superhost::VARCHAR,
            $1:host_listings_count::VARCHAR,
            $1:host_total_listings_count::VARCHAR,
            $1:host_verifications::VARCHAR,
            $1:host_identity_verified::VARCHAR,
            $1:_stream_timestamp::VARCHAR,
            METADATA$FILENAME
        FROM @AIRBNB_DE.RAW.airbnb_raw_stage/host/
    )
    FILE_FORMAT = (FORMAT_NAME = AIRBNB_DE.RAW.json_ndjson);

-- ── Pipe 02: listings ────────────────────────────────────────
CREATE OR REPLACE PIPE raw_listings_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest listing records from S3 → raw_listings'
    AS
    COPY INTO AIRBNB_DE.RAW.raw_listings (
        listing_id, host_id, listing_name, listing_description, property_type,
        room_type, accommodates, bathrooms, bedrooms, beds, amenities,
        price_per_night, cleaning_fee, minimum_nights, maximum_nights,
        cancellation_policy, instant_bookable, neighborhood, city, country,
        latitude, longitude,
        review_scores_rating, review_scores_cleanliness, review_scores_checkin,
        review_scores_communication, review_scores_location, review_scores_value,
        number_of_reviews, last_review_date,
        _stream_timestamp, _source_file
    )
    FROM (
        SELECT
            $1:listing_id::VARCHAR, $1:host_id::VARCHAR,
            $1:listing_name::VARCHAR, $1:listing_description::VARCHAR,
            $1:property_type::VARCHAR, $1:room_type::VARCHAR,
            $1:accommodates::VARCHAR, $1:bathrooms::VARCHAR,
            $1:bedrooms::VARCHAR, $1:beds::VARCHAR, $1:amenities::VARCHAR,
            $1:price_per_night::VARCHAR, $1:cleaning_fee::VARCHAR,
            $1:minimum_nights::VARCHAR, $1:maximum_nights::VARCHAR,
            $1:cancellation_policy::VARCHAR, $1:instant_bookable::VARCHAR,
            $1:neighborhood::VARCHAR, $1:city::VARCHAR, $1:country::VARCHAR,
            $1:latitude::VARCHAR, $1:longitude::VARCHAR,
            $1:review_scores_rating::VARCHAR, $1:review_scores_cleanliness::VARCHAR,
            $1:review_scores_checkin::VARCHAR, $1:review_scores_communication::VARCHAR,
            $1:review_scores_location::VARCHAR, $1:review_scores_value::VARCHAR,
            $1:number_of_reviews::VARCHAR, $1:last_review_date::VARCHAR,
            $1:_stream_timestamp::VARCHAR, METADATA$FILENAME
        FROM @AIRBNB_DE.RAW.airbnb_raw_stage/listing/
    )
    FILE_FORMAT = (FORMAT_NAME = AIRBNB_DE.RAW.json_ndjson);

-- ── Pipe 03: guests ──────────────────────────────────────────
CREATE OR REPLACE PIPE raw_guests_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest guest records from S3 → raw_guests'
    AS
    COPY INTO AIRBNB_DE.RAW.raw_guests (
        guest_id, guest_name, guest_email, guest_phone, date_of_birth,
        guest_since, nationality, gender, preferred_language, verified_id,
        total_trips, average_rating_as_guest, is_banned,
        _stream_timestamp, _source_file
    )
    FROM (
        SELECT
            $1:guest_id::VARCHAR, $1:guest_name::VARCHAR, $1:guest_email::VARCHAR,
            $1:guest_phone::VARCHAR, $1:date_of_birth::VARCHAR, $1:guest_since::VARCHAR,
            $1:nationality::VARCHAR, $1:gender::VARCHAR, $1:preferred_language::VARCHAR,
            $1:verified_id::VARCHAR, $1:total_trips::VARCHAR,
            $1:average_rating_as_guest::VARCHAR, $1:is_banned::VARCHAR,
            $1:_stream_timestamp::VARCHAR, METADATA$FILENAME
        FROM @AIRBNB_DE.RAW.airbnb_raw_stage/guest/
    )
    FILE_FORMAT = (FORMAT_NAME = AIRBNB_DE.RAW.json_ndjson);

-- ── Pipe 04: bookings ────────────────────────────────────────
CREATE OR REPLACE PIPE raw_bookings_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest booking records from S3 → raw_bookings'
    AS
    COPY INTO AIRBNB_DE.RAW.raw_bookings (
        booking_id, listing_id, guest_id, host_id,
        check_in_date, check_out_date, nights_count,
        num_guests, num_adults, num_children, num_infants,
        booking_status, payment_status, total_price,
        base_price, cleaning_fee_charged, service_fee, taxes,
        special_requests, booked_at, updated_at,
        source_platform, promo_code, cancellation_reason,
        _stream_timestamp, _source_file
    )
    FROM (
        SELECT
            $1:booking_id::VARCHAR, $1:listing_id::VARCHAR, $1:guest_id::VARCHAR,
            $1:host_id::VARCHAR, $1:check_in_date::VARCHAR, $1:check_out_date::VARCHAR,
            $1:nights_count::VARCHAR, $1:num_guests::VARCHAR, $1:num_adults::VARCHAR,
            $1:num_children::VARCHAR, $1:num_infants::VARCHAR,
            $1:booking_status::VARCHAR, $1:payment_status::VARCHAR,
            $1:total_price::VARCHAR, $1:base_price::VARCHAR,
            $1:cleaning_fee_charged::VARCHAR, $1:service_fee::VARCHAR, $1:taxes::VARCHAR,
            $1:special_requests::VARCHAR, $1:booked_at::VARCHAR, $1:updated_at::VARCHAR,
            $1:source_platform::VARCHAR, $1:promo_code::VARCHAR,
            $1:cancellation_reason::VARCHAR,
            $1:_stream_timestamp::VARCHAR, METADATA$FILENAME
        FROM @AIRBNB_DE.RAW.airbnb_raw_stage/booking/
    )
    FILE_FORMAT = (FORMAT_NAME = AIRBNB_DE.RAW.json_ndjson);

-- ── Pipe 05: reviews ─────────────────────────────────────────
CREATE OR REPLACE PIPE raw_reviews_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest review records from S3 → raw_reviews'
    AS
    COPY INTO AIRBNB_DE.RAW.raw_reviews (
        review_id, booking_id, listing_id, reviewer_id, reviewee_id,
        review_type, overall_rating, cleanliness_rating, accuracy_rating,
        communication_rating, checkin_rating, location_rating, value_rating,
        review_text, response_text, is_public, reviewed_at,
        _stream_timestamp, _source_file
    )
    FROM (
        SELECT
            $1:review_id::VARCHAR, $1:booking_id::VARCHAR, $1:listing_id::VARCHAR,
            $1:reviewer_id::VARCHAR, $1:reviewee_id::VARCHAR, $1:review_type::VARCHAR,
            $1:overall_rating::VARCHAR, $1:cleanliness_rating::VARCHAR,
            $1:accuracy_rating::VARCHAR, $1:communication_rating::VARCHAR,
            $1:checkin_rating::VARCHAR, $1:location_rating::VARCHAR,
            $1:value_rating::VARCHAR, $1:review_text::VARCHAR,
            $1:response_text::VARCHAR, $1:is_public::VARCHAR, $1:reviewed_at::VARCHAR,
            $1:_stream_timestamp::VARCHAR, METADATA$FILENAME
        FROM @AIRBNB_DE.RAW.airbnb_raw_stage/review/
    )
    FILE_FORMAT = (FORMAT_NAME = AIRBNB_DE.RAW.json_ndjson);

-- ── Pipe 06: availability_calendar ──────────────────────────
CREATE OR REPLACE PIPE raw_availability_calendar_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest availability_calendar records from S3'
    AS
    COPY INTO AIRBNB_DE.RAW.raw_availability_calendar (
        calendar_id, listing_id, calendar_date, is_available,
        price_on_date, minimum_nights_override, maximum_nights_override, notes,
        _stream_timestamp, _source_file
    )
    FROM (
        SELECT
            $1:calendar_id::VARCHAR, $1:listing_id::VARCHAR, $1:calendar_date::VARCHAR,
            $1:is_available::VARCHAR, $1:price_on_date::VARCHAR,
            $1:minimum_nights_override::VARCHAR, $1:maximum_nights_override::VARCHAR,
            $1:notes::VARCHAR,
            $1:_stream_timestamp::VARCHAR, METADATA$FILENAME
        FROM @AIRBNB_DE.RAW.airbnb_raw_stage/availability_calendar/
    )
    FILE_FORMAT = (FORMAT_NAME = AIRBNB_DE.RAW.json_ndjson);

-- ── Pipe 07: listing_events ──────────────────────────────────
CREATE OR REPLACE PIPE raw_listing_events_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest listing_event records from S3 → raw_listing_events'
    AS
    COPY INTO AIRBNB_DE.RAW.raw_listing_events (
        event_id, event_type, listing_id, guest_id, session_id,
        device_type, os_version, browser, country_code,
        search_query, price_shown, position_in_results, event_timestamp,
        _stream_timestamp, _source_file
    )
    FROM (
        SELECT
            $1:event_id::VARCHAR, $1:event_type::VARCHAR, $1:listing_id::VARCHAR,
            $1:guest_id::VARCHAR, $1:session_id::VARCHAR, $1:device_type::VARCHAR,
            $1:os_version::VARCHAR, $1:browser::VARCHAR, $1:country_code::VARCHAR,
            $1:search_query::VARCHAR, $1:price_shown::VARCHAR,
            $1:position_in_results::VARCHAR, $1:event_timestamp::VARCHAR,
            $1:_stream_timestamp::VARCHAR, METADATA$FILENAME
        FROM @AIRBNB_DE.RAW.airbnb_raw_stage/listing_events/
    )
    FILE_FORMAT = (FORMAT_NAME = AIRBNB_DE.RAW.json_ndjson);


-- ════════════════════════════════════════════════════════════
--  SECTION 7: GET SQS ARNs FOR S3 NOTIFICATIONS
-- ════════════════════════════════════════════════════════════

SHOW PIPES IN SCHEMA AIRBNB_DE.RAW;
-- Copy the notification_channel value for each pipe.
-- It looks like: arn:aws:sqs:us-east-1:000000000000:sf-snowpipe-XXXX_YYYYYYY

-- ════════════════════════════════════════════════════════════
--  SECTION 8: VERIFICATION QUERIES
-- ════════════════════════════════════════════════════════════

SELECT 'raw_hosts'                  AS table_name, COUNT(*) AS row_count FROM AIRBNB_DE.RAW.raw_hosts                UNION ALL
SELECT 'raw_listings'               AS table_name, COUNT(*) AS row_count FROM AIRBNB_DE.RAW.raw_listings              UNION ALL
SELECT 'raw_guests'                 AS table_name, COUNT(*) AS row_count FROM AIRBNB_DE.RAW.raw_guests                UNION ALL
SELECT 'raw_bookings'               AS table_name, COUNT(*) AS row_count FROM AIRBNB_DE.RAW.raw_bookings              UNION ALL
SELECT 'raw_reviews'                AS table_name, COUNT(*) AS row_count FROM AIRBNB_DE.RAW.raw_reviews               UNION ALL
SELECT 'raw_availability_calendar'  AS table_name, COUNT(*) AS row_count FROM AIRBNB_DE.RAW.raw_availability_calendar UNION ALL
SELECT 'raw_listing_events'         AS table_name, COUNT(*) AS row_count FROM AIRBNB_DE.RAW.raw_listing_events
ORDER BY table_name;

-- Check Snowpipe status (should show RUNNING, not PAUSED)
SELECT SYSTEM$PIPE_STATUS('AIRBNB_DE.RAW.raw_hosts_pipe');
SELECT SYSTEM$PIPE_STATUS('AIRBNB_DE.RAW.raw_listings_pipe');
SELECT SYSTEM$PIPE_STATUS('AIRBNB_DE.RAW.raw_bookings_pipe');
SELECT SYSTEM$PIPE_STATUS('AIRBNB_DE.RAW.raw_reviews_pipe');
SELECT SYSTEM$PIPE_STATUS('AIRBNB_DE.RAW.raw_availability_calendar_pipe');
SELECT SYSTEM$PIPE_STATUS('AIRBNB_DE.RAW.raw_listing_events_pipe');

-- Row counts (should be > 0 after first Lambda run)
SELECT 'raw_hosts'                 AS tbl, COUNT(*) AS rows FROM AIRBNB_DE.RAW.raw_hosts               UNION ALL
SELECT 'raw_listings'              AS tbl, COUNT(*) AS rows FROM AIRBNB_DE.RAW.raw_listings             UNION ALL
SELECT 'raw_guests'                AS tbl, COUNT(*) AS rows FROM AIRBNB_DE.RAW.raw_guests               UNION ALL
SELECT 'raw_bookings'              AS tbl, COUNT(*) AS rows FROM AIRBNB_DE.RAW.raw_bookings             UNION ALL
SELECT 'raw_reviews'               AS tbl, COUNT(*) AS rows FROM AIRBNB_DE.RAW.raw_reviews              UNION ALL
SELECT 'raw_availability_calendar' AS tbl, COUNT(*) AS rows FROM AIRBNB_DE.RAW.raw_availability_calendar UNION ALL
SELECT 'raw_listing_events'        AS tbl, COUNT(*) AS rows FROM AIRBNB_DE.RAW.raw_listing_events
ORDER BY tbl;

-- Spot-check a few dirty values to confirm generator is working
SELECT host_id, host_name, is_superhost, host_response_rate, host_verifications
FROM AIRBNB_DE.RAW.raw_hosts
LIMIT 10;

SELECT listing_id, price_per_night, amenities, minimum_nights, review_scores_rating
FROM AIRBNB_DE.RAW.raw_listings
LIMIT 10;

SELECT booking_id, check_in_date, check_out_date, nights_count, total_price,
       base_price + cleaning_fee_charged::FLOAT + service_fee::FLOAT + taxes::FLOAT AS calc_total
FROM AIRBNB_DE.RAW.raw_bookings
LIMIT 10;

-- Check Snowpipe ingestion history (last 24 hours)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START => DATEADD('hour', -12, CURRENT_TIMESTAMP()),
    DATE_RANGE_END   => CURRENT_TIMESTAMP()
))
ORDER BY pipe_name;

-- Check for any copy errors
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME    => 'AIRBNB_DE.RAW.RAW_HOSTS',
    START_TIME    => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    END_TIME      => CURRENT_TIMESTAMP()
))
WHERE STATUS != 'Loaded'
ORDER BY LAST_LOAD_TIME DESC;
