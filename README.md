### SNACKAI-CoCo-ADSB

SNACK AI Snowflake Snowpipe Streaming High Speed v2 Python REST ADSB


````

2026-02-02 14:04:40,282 [INFO] __main__ - PRODUCTION MODE: Real ADS-B data + Snowpipe Streaming REST API only
2026-02-02 14:04:40,282 [INFO] __main__ - ======================================================================
2026-02-02 14:04:40,282 [INFO] __main__ - ADS-B Aircraft Streaming Application - PRODUCTION MODE
2026-02-02 14:04:40,283 [INFO] __main__ - Raspberry Pi ADS-B Receiver -> Snowflake via Snowpipe Streaming v2
2026-02-02 14:04:40,283 [INFO] __main__ - ======================================================================
2026-02-02 14:04:40,283 [INFO] __main__ - PRODUCTION CONFIGURATION:
2026-02-02 14:04:40,283 [INFO] __main__ -   - Real ADS-B aircraft data ONLY
2026-02-02 14:04:40,283 [INFO] __main__ -   - Snowpipe Streaming high-speed REST API ONLY
2026-02-02 14:04:40,284 [INFO] __main__ -   - Data source updates every ~3 seconds
2026-02-02 14:04:40,284 [INFO] __main__ - ======================================================================
2026-02-02 14:04:40,284 [INFO] __main__ - Initializing ADS-B receiver connection...
2026-02-02 14:04:40,285 [INFO] adsb_sensor - ADS-B Sensor initialized
2026-02-02 14:04:40,285 [INFO] adsb_sensor -   Data URL: http://localhost:8080/data/aircraft.json
2026-02-02 14:04:40,285 [INFO] adsb_sensor -   Hostname: thermal
2026-02-02 14:04:40,285 [INFO] adsb_sensor -   IP: 192.168.1.175
2026-02-02 14:04:40,302 [INFO] adsb_sensor - [OK] Connected to ADS-B receiver
2026-02-02 14:04:40,302 [INFO] adsb_sensor - [OK] Currently tracking 36 aircraft
2026-02-02 14:04:40,302 [INFO] __main__ - Initializing Snowpipe Streaming REST API client...
2026-02-02 14:04:40,303 [INFO] thermal_streaming_client - ======================================================================
2026-02-02 14:04:40,303 [INFO] thermal_streaming_client - SNOWPIPE STREAMING CLIENT - PRODUCTION MODE
2026-02-02 14:04:40,303 [INFO] thermal_streaming_client - Using ONLY Snowpipe Streaming v2 REST API
2026-02-02 14:04:40,303 [INFO] thermal_streaming_client - NO direct inserts - HIGH-PERFORMANCE STREAMING ONLY
2026-02-02 14:04:40,303 [INFO] thermal_streaming_client - ======================================================================
2026-02-02 14:04:40,304 [INFO] thermal_streaming_client - Loaded configuration from snowflake_config_adsb.json
2026-02-02 14:04:40,304 [INFO] snowflake_jwt_auth - PAT authentication initialized for user: THERMAL_STREAMING_USER
2026-02-02 14:04:40,304 [INFO] thermal_streaming_client - SnowpipeStreamingClient initialized
2026-02-02 14:04:40,304 [INFO] thermal_streaming_client - Database: DEMO
2026-02-02 14:04:40,304 [INFO] thermal_streaming_client - Schema: DEMO
2026-02-02 14:04:40,305 [INFO] thermal_streaming_client - Table: ADSB_AIRCRAFT_DATA
2026-02-02 14:04:40,305 [INFO] thermal_streaming_client - Channel: ADSB_CHNL_20260202_140440
2026-02-02 14:04:40,305 [INFO] __main__ - Initialization complete
2026-02-02 14:04:40,305 [INFO] __main__ - Batch size: 1 snapshot(s)
2026-02-02 14:04:40,305 [INFO] __main__ - Batch interval: 3.0 seconds
2026-02-02 14:04:40,305 [INFO] __main__ - ADS-B URL: http://localhost:8080/data/aircraft.json
2026-02-02 14:04:40,306 [INFO] __main__ - Local hostname: thermal
2026-02-02 14:04:40,306 [INFO] __main__ - Local IP: 192.168.1.175
2026-02-02 14:04:40,306 [INFO] __main__ - Setting up Snowpipe Streaming connection...
2026-02-02 14:04:40,306 [INFO] __main__ - Discovering ingest host...
2026-02-02 14:04:40,306 [INFO] thermal_streaming_client - Discovering ingest host...
2026-02-02 14:04:40,306 [INFO] thermal_streaming_client - Obtaining new scoped token...
2026-02-02 14:04:40,307 [INFO] snowflake_jwt_auth - Using Programmatic Access Token (PAT)
2026-02-02 14:04:40,307 [INFO] thermal_streaming_client - New scoped token obtained
2026-02-02 14:04:41,018 [INFO] thermal_streaming_client - Ingest host discovered: LXB29530.ingest.iadaax.snowflakecomputing.com
2026-02-02 14:04:41,018 [INFO] __main__ - [OK] Ingest host: LXB29530.ingest.iadaax.snowflakecomputing.com
2026-02-02 14:04:41,019 [INFO] __main__ - Opening streaming channel...
2026-02-02 14:04:41,019 [INFO] thermal_streaming_client - Opening channel: ADSB_CHNL_20260202_140440
2026-02-02 14:04:42,846 [INFO] thermal_streaming_client - Channel opened successfully
2026-02-02 14:04:42,846 [INFO] thermal_streaming_client - Continuation token: 0_1
2026-02-02 14:04:42,846 [INFO] thermal_streaming_client - Initial offset token: 0
2026-02-02 14:04:42,847 [INFO] __main__ - [OK] Channel opened successfully
2026-02-02 14:04:42,847 [INFO] __main__ - Snowpipe Streaming connection ready!
2026-02-02 14:04:42,847 [INFO] __main__ - ======================================================================
2026-02-02 14:04:42,847 [INFO] __main__ - Starting ADS-B data collection and streaming...
2026-02-02 14:04:42,847 [INFO] __main__ - Press Ctrl+C to stop
2026-02-02 14:04:42,847 [INFO] __main__ - ======================================================================
2026-02-02 14:04:42,848 [INFO] __main__ - 
--- Batch 1 ---
2026-02-02 14:04:42,855 [INFO] adsb_sensor - Read 36 aircraft records from 1 snapshots
2026-02-02 14:04:42,856 [INFO] __main__ - Captured 36 aircraft records
2026-02-02 14:04:42,856 [INFO] __main__ -   With callsign: 12
2026-02-02 14:04:42,856 [INFO] __main__ -   With position: 20
2026-02-02 14:04:42,856 [INFO] thermal_streaming_client - Appending 36 rows...
2026-02-02 14:04:43,591 [INFO] thermal_streaming_client - Successfully appended 36 rows
2026-02-02 14:04:43,592 [INFO] __main__ - [OK] Successfully sent 36 aircraft records to Snowpipe Streaming
2026-02-02 14:04:43,592 [INFO] __main__ - Waiting 2.3s until next batch...
2026-02-02 14:04:45,848 [INFO] __main__ - 
--- Batch 2 ---
2026-02-02 14:04:45,856 [INFO] adsb_sensor - Read 35 aircraft records from 1 snapshots
2026-02-02 14:04:45,856 [INFO] __main__ - Captured 35 aircraft records
2026-02-02 14:04:45,857 [INFO] __main__ -   With callsign: 12
2026-02-02 14:04:45,857 [INFO] __main__ -   With position: 20
2026-02-02 14:04:45,857 [INFO] thermal_streaming_client - Appending 35 rows...
2026-02-02 14:04:46,691 [INFO] thermal_streaming_client - Successfully appended 35 rows
2026-02-02 14:04:46,691 [INFO] __main__ - [OK] Successfully sent 35 aircraft records to Snowpipe Streaming
2026-02-02 14:04:46,691 [INFO] __main__ - Waiting 2.2s until next batch...
2026-02-02 14:04:48,848 [INFO] __main__ - 
--- Batch 3 ---
2026-02-02 14:04:48,857 [INFO] adsb_sensor - Read 36 aircraft records from 1 snapshots
2026-02-02 14:04:48,857 [INFO] __main__ - Captured 36 aircraft records
2026-02-02 14:04:48,857 [INFO] __main__ -   With callsign: 13
2026-02-02 14:04:48,858 [INFO] __main__ -   With position: 20
2026-02-02 14:04:48,858 [INFO] thermal_streaming_client - Appending 36 rows...
2026-02-02 14:04:49,021 [INFO] thermal_streaming_client - Successfully appended 36 rows
2026-02-02 14:04:49,022 [INFO] __main__ - [OK] Successfully sent 36 aircraft records to Snowpipe Streaming
2026-02-02 14:04:49,022 [INFO] __main__ - Waiting 2.8s until next batch...
^C2026-02-02 14:04:50,566 [INFO] __main__ - 
Received signal 2, shutting down gracefully...
2026-02-02 14:04:51,849 [INFO] __main__ - 


2026-02-02 14:04:51,849 [INFO] __main__ - 
======================================================================
2026-02-02 14:04:51,849 [INFO] __main__ - Shutting down...
2026-02-02 14:04:51,850 [INFO] __main__ - ======================================================================
2026-02-02 14:04:51,850 [INFO] thermal_streaming_client - ============================================================
2026-02-02 14:04:51,850 [INFO] thermal_streaming_client - INGESTION STATISTICS
2026-02-02 14:04:51,851 [INFO] thermal_streaming_client - ============================================================
2026-02-02 14:04:51,851 [INFO] thermal_streaming_client - Total rows sent: 107
2026-02-02 14:04:51,851 [INFO] thermal_streaming_client - Total batches: 3
2026-02-02 14:04:51,852 [INFO] thermal_streaming_client - Total bytes sent: 94,145
2026-02-02 14:04:51,852 [INFO] thermal_streaming_client - Errors: 0
2026-02-02 14:04:51,852 [INFO] thermal_streaming_client - Elapsed time: 11.55 seconds
2026-02-02 14:04:51,853 [INFO] thermal_streaming_client - Average throughput: 9.27 rows/sec
2026-02-02 14:04:51,853 [INFO] thermal_streaming_client - Current offset token: 3
2026-02-02 14:04:51,853 [INFO] thermal_streaming_client - ============================================================
2026-02-02 14:04:51,854 [INFO] __main__ - Closing streaming channel...
2026-02-02 14:04:51,854 [INFO] thermal_streaming_client - Closing channel: ADSB_CHNL_20260202_140440
2026-02-02 14:04:51,854 [INFO] thermal_streaming_client - Channel will auto-close after inactivity period
2026-02-02 14:04:51,855 [INFO] __main__ - [OK] Channel closed
2026-02-02 14:04:51,855 [INFO] __main__ - Cleaning up sensor...
2026-02-02 14:04:51,855 [INFO] adsb_sensor - ADS-B sensor cleaned up
2026-02-02 14:04:51,856 [INFO] __main__ - [OK] Sensor cleaned up
2026-02-02 14:04:51,856 [INFO] __main__ - Shutdown complete

````
