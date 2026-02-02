-- ============================================================================
-- Snowflake Setup Script for Raspberry Pi ADS-B Aircraft Streaming
-- ============================================================================
-- This script creates the necessary Snowflake objects for streaming 
-- ADS-B aircraft data from Raspberry Pi using Snowpipe Streaming v2 REST API.
--
-- Data source: http://localhost:8080/data/aircraft.json
-- Updates every ~3 seconds from dump1090/readsb ADS-B receiver
--
-- Based on:
-- - https://github.com/tspannhw/FLiP-Py-ADS-B
-- - https://github.com/tspannhw/AIM-ADS-B
-- ============================================================================

USE DATABASE DEMO;
USE SCHEMA DEMO;

-- Step 1: Create target table for ADS-B aircraft data
CREATE OR REPLACE TABLE ADSB_AIRCRAFT_DATA (
    -- Identifiers
    uuid VARCHAR(100),
    rowid VARCHAR(100),
    
    -- Timestamps
    datetimestamp TIMESTAMP_NTZ,
    ts BIGINT,
    
    -- Aircraft identification
    icao_hex VARCHAR(10),
    flight VARCHAR(20),
    registration VARCHAR(20),
    aircraft_type VARCHAR(10),
    description VARCHAR(200),
    
    -- Position and altitude
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    altitude_baro INTEGER,
    altitude_geom INTEGER,
    
    -- Velocity and direction
    ground_speed DECIMAL(10, 2),
    track DECIMAL(10, 2),
    true_heading DECIMAL(10, 2),
    mag_heading DECIMAL(10, 2),
    indicated_airspeed INTEGER,
    true_airspeed INTEGER,
    mach DECIMAL(5, 3),
    vertical_rate INTEGER,
    vertical_rate_geom INTEGER,
    
    -- Navigation
    nav_altitude INTEGER,
    nav_heading DECIMAL(10, 2),
    nav_qnh DECIMAL(10, 2),
    
    -- Transponder
    squawk VARCHAR(10),
    category VARCHAR(5),
    emergency VARCHAR(20),
    
    -- Signal quality
    rssi DECIMAL(10, 2),
    messages INTEGER,
    seen DECIMAL(10, 2),
    seen_pos DECIMAL(10, 2),
    
    -- Receiver metadata
    hostname VARCHAR(100),
    receiver_host VARCHAR(100),
    receiver_ip VARCHAR(50),
    receiver_time DECIMAL(20, 6),
    total_messages INTEGER,
    
    -- Ingestion metadata
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add comments
COMMENT ON TABLE ADSB_AIRCRAFT_DATA IS 
'Real-time ADS-B aircraft data from Raspberry Pi receiver, ingested via Snowpipe Streaming v2 REST API';

COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.icao_hex IS 'ICAO 24-bit address in hexadecimal';
COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.flight IS 'Flight number/callsign';
COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.altitude_baro IS 'Barometric altitude in feet';
COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.altitude_geom IS 'Geometric altitude in feet';
COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.ground_speed IS 'Ground speed in knots';
COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.track IS 'Track angle in degrees (0-360)';
COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.vertical_rate IS 'Vertical rate in feet per minute';
COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.squawk IS 'Transponder squawk code';
COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.category IS 'Aircraft category (A1-A5, B1-B7, etc.)';
COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.rssi IS 'Signal strength in dBFS';
COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.seen IS 'Seconds since last message';
COMMENT ON COLUMN ADSB_AIRCRAFT_DATA.seen_pos IS 'Seconds since last position update';

-- Step 2: Create PIPE object for streaming ingestion
CREATE OR REPLACE PIPE ADSB_AIRCRAFT_PIPE
COMMENT = 'Snowpipe Streaming v2 pipe for Raspberry Pi ADS-B aircraft data'
AS 
COPY INTO ADSB_AIRCRAFT_DATA (
    uuid, rowid, datetimestamp, ts,
    icao_hex, flight, registration, aircraft_type, description,
    latitude, longitude, altitude_baro, altitude_geom,
    ground_speed, track, true_heading, mag_heading, indicated_airspeed, true_airspeed, mach,
    vertical_rate, vertical_rate_geom,
    nav_altitude, nav_heading, nav_qnh,
    squawk, category, emergency,
    rssi, messages, seen, seen_pos,
    hostname, receiver_host, receiver_ip, receiver_time, total_messages
)
FROM (
    SELECT 
        $1:uuid::VARCHAR as uuid,
        $1:rowid::VARCHAR as rowid,
        TO_TIMESTAMP_NTZ($1:datetimestamp) as datetimestamp,
        $1:ts::BIGINT as ts,
        $1:icao_hex::VARCHAR as icao_hex,
        $1:flight::VARCHAR as flight,
        $1:registration::VARCHAR as registration,
        $1:aircraft_type::VARCHAR as aircraft_type,
        $1:description::VARCHAR as description,
        $1:latitude::DECIMAL(10,6) as latitude,
        $1:longitude::DECIMAL(10,6) as longitude,
        $1:altitude_baro::INTEGER as altitude_baro,
        $1:altitude_geom::INTEGER as altitude_geom,
        $1:ground_speed::DECIMAL(10,2) as ground_speed,
        $1:track::DECIMAL(10,2) as track,
        $1:true_heading::DECIMAL(10,2) as true_heading,
        $1:mag_heading::DECIMAL(10,2) as mag_heading,
        $1:indicated_airspeed::INTEGER as indicated_airspeed,
        $1:true_airspeed::INTEGER as true_airspeed,
        $1:mach::DECIMAL(5,3) as mach,
        $1:vertical_rate::INTEGER as vertical_rate,
        $1:vertical_rate_geom::INTEGER as vertical_rate_geom,
        $1:nav_altitude::INTEGER as nav_altitude,
        $1:nav_heading::DECIMAL(10,2) as nav_heading,
        $1:nav_qnh::DECIMAL(10,2) as nav_qnh,
        $1:squawk::VARCHAR as squawk,
        $1:category::VARCHAR as category,
        $1:emergency::VARCHAR as emergency,
        $1:rssi::DECIMAL(10,2) as rssi,
        $1:messages::INTEGER as messages,
        $1:seen::DECIMAL(10,2) as seen,
        $1:seen_pos::DECIMAL(10,2) as seen_pos,
        $1:hostname::VARCHAR as hostname,
        $1:receiver_host::VARCHAR as receiver_host,
        $1:receiver_ip::VARCHAR as receiver_ip,
        $1:receiver_time::DECIMAL(20,6) as receiver_time,
        $1:total_messages::INTEGER as total_messages
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);

-- Step 3: Grant necessary privileges (if using dedicated role)
-- GRANT USAGE ON DATABASE DEMO TO ROLE THERMAL_STREAMING_ROLE;
-- GRANT USAGE ON SCHEMA DEMO.DEMO TO ROLE THERMAL_STREAMING_ROLE;
-- GRANT SELECT, INSERT ON TABLE DEMO.DEMO.ADSB_AIRCRAFT_DATA TO ROLE THERMAL_STREAMING_ROLE;
-- GRANT OPERATE, MONITOR ON PIPE DEMO.DEMO.ADSB_AIRCRAFT_PIPE TO ROLE THERMAL_STREAMING_ROLE;

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Verify the pipe was created
SHOW PIPES LIKE 'ADSB_AIRCRAFT_PIPE' IN SCHEMA DEMO.DEMO;

-- Describe the pipe
DESC PIPE DEMO.DEMO.ADSB_AIRCRAFT_PIPE;

-- ============================================================================
-- Monitoring Queries (Run after data is flowing)
-- ============================================================================

-- Check row count
-- SELECT COUNT(*) FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA;

-- View recent aircraft
-- SELECT * FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA 
-- ORDER BY ingestion_timestamp DESC 
-- LIMIT 100;

-- Unique aircraft seen in last hour
-- SELECT 
--     icao_hex,
--     flight,
--     COUNT(*) as observations,
--     MIN(datetimestamp) as first_seen,
--     MAX(datetimestamp) as last_seen,
--     AVG(altitude_baro) as avg_altitude,
--     AVG(ground_speed) as avg_speed
-- FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
-- WHERE datetimestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
-- GROUP BY icao_hex, flight
-- ORDER BY observations DESC;

-- Aircraft currently visible (last 60 seconds)
-- SELECT 
--     icao_hex,
--     flight,
--     altitude_baro,
--     ground_speed,
--     latitude,
--     longitude,
--     track,
--     vertical_rate,
--     datetimestamp
-- FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
-- WHERE datetimestamp >= DATEADD('second', -60, CURRENT_TIMESTAMP())
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY icao_hex ORDER BY datetimestamp DESC) = 1
-- ORDER BY flight;

-- Flight paths (track positions over time)
-- SELECT 
--     icao_hex,
--     flight,
--     datetimestamp,
--     latitude,
--     longitude,
--     altitude_baro,
--     ground_speed,
--     track
-- FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
-- WHERE flight = 'UAL123'  -- Replace with actual flight
-- ORDER BY datetimestamp;

-- Hourly traffic statistics
-- SELECT 
--     DATE_TRUNC('hour', datetimestamp) as hour,
--     COUNT(*) as total_observations,
--     COUNT(DISTINCT icao_hex) as unique_aircraft,
--     AVG(altitude_baro) as avg_altitude_ft,
--     AVG(ground_speed) as avg_speed_kts
-- FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
-- GROUP BY 1
-- ORDER BY 1 DESC;

-- High altitude flights (above FL400)
-- SELECT 
--     icao_hex,
--     flight,
--     MAX(altitude_baro) as max_altitude,
--     AVG(ground_speed) as avg_speed,
--     MAX(datetimestamp) as last_seen
-- FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
-- WHERE altitude_baro > 40000
-- GROUP BY icao_hex, flight
-- ORDER BY max_altitude DESC;

-- Emergency squawks
-- SELECT *
-- FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
-- WHERE squawk IN ('7500', '7600', '7700')
-- ORDER BY datetimestamp DESC;

-- ============================================================================
-- Optional: Create views for easier querying
-- ============================================================================

-- CREATE OR REPLACE VIEW ADSB_CURRENT_AIRCRAFT AS
-- SELECT 
--     icao_hex,
--     flight,
--     aircraft_type,
--     latitude,
--     longitude,
--     altitude_baro as altitude_ft,
--     ground_speed as speed_kts,
--     track as heading,
--     vertical_rate as climb_rate_fpm,
--     squawk,
--     datetimestamp,
--     DATEDIFF('second', datetimestamp, CURRENT_TIMESTAMP()) as seconds_ago
-- FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY icao_hex ORDER BY datetimestamp DESC) = 1
-- ORDER BY flight;

-- CREATE OR REPLACE VIEW ADSB_HOURLY_STATS AS
-- SELECT 
--     DATE_TRUNC('hour', datetimestamp) as hour,
--     hostname as receiver,
--     COUNT(*) as observations,
--     COUNT(DISTINCT icao_hex) as unique_aircraft,
--     AVG(altitude_baro) as avg_altitude_ft,
--     MAX(altitude_baro) as max_altitude_ft,
--     AVG(ground_speed) as avg_speed_kts,
--     SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) as with_position
-- FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
-- GROUP BY 1, 2
-- ORDER BY 1 DESC;

-- ============================================================================
-- Cleanup (Use with caution!)
-- ============================================================================

-- DROP PIPE IF EXISTS DEMO.DEMO.ADSB_AIRCRAFT_PIPE;
-- DROP TABLE IF EXISTS DEMO.DEMO.ADSB_AIRCRAFT_DATA;
