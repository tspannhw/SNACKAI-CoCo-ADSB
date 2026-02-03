### SNACKAI-CoCo-ADSB

SNACK AI Snowflake Snowpipe Streaming High Speed v2 Python REST ADSB

# ADS-B Aircraft Streaming to Snowflake

Stream real-time ADS-B (Automatic Dependent Surveillance-Broadcast) aircraft data from a Raspberry Pi receiver to Snowflake using Snowpipe Streaming v2 REST API.

## Overview

This pipeline captures aircraft transponder data from a local ADS-B receiver (dump1090, readsb, tar1090, etc.) and streams it to Snowflake in real-time. ADS-B data updates approximately every 3 seconds.

**Based on:**
- https://github.com/tspannhw/FLiP-Py-ADS-B
- https://github.com/tspannhw/AIM-ADS-B

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌───────────┐
│  Aircraft       │───>│ RTL-SDR      │───>│ dump1090/readsb │───>│ Snowflake │
│  Transponders   │    │ USB Dongle   │    │ (JSON API)      │    │           │
└─────────────────┘    └──────────────┘    └─────────────────┘    └───────────┘
     ADS-B 1090MHz        RF Reception      http://localhost:8080   Snowpipe
                                            /data/aircraft.json     Streaming v2
```

## Prerequisites

### Hardware
- Raspberry Pi (3B+ or newer recommended)
- RTL-SDR USB dongle (RTL2832U based)
- ADS-B antenna (1090 MHz)

### Software
- ADS-B decoder running and serving JSON:
  - [dump1090-fa](https://github.com/flightaware/dump1090)
  - [readsb](https://github.com/wiedehopf/readsb)
  - [tar1090](https://github.com/wiedehopf/tar1090)
- Python 3.8+
- `requests` library

### Snowflake
- Snowflake account with Snowpipe Streaming enabled
- PAT (Programmatic Access Token) or key-pair authentication
- Table and pipe created (see setup below)

## Quick Start

### 1. Setup Snowflake Objects

Run the SQL in Snowflake (or it's already created):
```sql
-- Table and pipe already exist in DEMO.DEMO:
-- - ADSB_AIRCRAFT_DATA (table)
-- - ADSB_AIRCRAFT_PIPE (pipe)
```

### 2. Configure Connection

Edit `snowflake_config_adsb.json`:
```json
{
  "user": "YOUR_USER",
  "account": "YOUR_ACCOUNT",
  "pat": "YOUR_PAT_TOKEN",
  "role": "YOUR_ROLE",
  "database": "DEMO",
  "schema": "DEMO",
  "table": "ADSB_AIRCRAFT_DATA",
  "pipe": "ADSB_AIRCRAFT_PIPE",
  "channel_name": "ADSB_CHNL"
}
```

### 3. Run the Pipeline

```bash
# Using the shell script (recommended)
./run_adsb.sh

# Or directly with Python
python3 adsb_main.py --config snowflake_config_adsb.json

# With custom ADS-B URL
python3 adsb_main.py --adsb-url http://192.168.1.100:8080/data/aircraft.json

# Fast mode for maximum throughput
./run_adsb.sh --fast
```

## Files

| File | Description |
|------|-------------|
| `adsb_main.py` | Main streaming application |
| `adsb_sensor.py` | ADS-B data fetcher module |
| `snowflake_config_adsb.json` | Snowflake connection config |
| `run_adsb.sh` | Bash script to run the pipeline |
| `SETUP_ADSB_SNOWFLAKE.sql` | SQL setup script (reference) |
| `ADSB_README.md` | This documentation |

## Data Fields

### Aircraft Identification
| Field | Description |
|-------|-------------|
| `icao_hex` | ICAO 24-bit address (hex) |
| `flight` | Flight number/callsign |
| `registration` | Aircraft registration |
| `aircraft_type` | Aircraft type code |

### Position & Altitude
| Field | Description |
|-------|-------------|
| `latitude` | Latitude in degrees |
| `longitude` | Longitude in degrees |
| `altitude_baro` | Barometric altitude (feet) |
| `altitude_geom` | Geometric/GPS altitude (feet) |

### Velocity & Direction
| Field | Description |
|-------|-------------|
| `ground_speed` | Ground speed (knots) |
| `track` | Track angle (0-360°) |
| `vertical_rate` | Climb/descent rate (fpm) |
| `mach` | Mach number |

### Transponder & Signal
| Field | Description |
|-------|-------------|
| `squawk` | Transponder code |
| `category` | Aircraft category (A1-A5, etc.) |
| `rssi` | Signal strength (dBFS) |
| `messages` | Message count |

## Example Queries

### Currently Visible Aircraft
```sql
SELECT 
    icao_hex,
    flight,
    altitude_baro as altitude_ft,
    ground_speed as speed_kts,
    latitude,
    longitude
FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
WHERE datetimestamp >= DATEADD('second', -60, CURRENT_TIMESTAMP())
QUALIFY ROW_NUMBER() OVER (PARTITION BY icao_hex ORDER BY datetimestamp DESC) = 1
ORDER BY flight;
```

### Unique Aircraft in Last Hour
```sql
SELECT 
    icao_hex,
    flight,
    COUNT(*) as observations,
    AVG(altitude_baro) as avg_altitude,
    AVG(ground_speed) as avg_speed
FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
WHERE datetimestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
GROUP BY icao_hex, flight
ORDER BY observations DESC;
```

### Flight Path Tracking
```sql
SELECT 
    datetimestamp,
    latitude,
    longitude,
    altitude_baro,
    ground_speed,
    track
FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
WHERE flight = 'UAL123'
ORDER BY datetimestamp;
```

### Emergency Squawks
```sql
SELECT *
FROM DEMO.DEMO.ADSB_AIRCRAFT_DATA
WHERE squawk IN ('7500', '7600', '7700')
ORDER BY datetimestamp DESC;
```

### Streamlit App

https://github.com/tspannhw/SNACKAI-CoCo-ADSB/blob/main/streamlit.app

![dashboard](https://github.com/tspannhw/SNACKAI-CoCo-ADSB/blob/main/dashboard1.png?raw=true)
![dashboard](https://github.com/tspannhw/SNACKAI-CoCo-ADSB/blob/main/dashboard2.png?raw=true)
![dashboard](https://github.com/tspannhw/SNACKAI-CoCo-ADSB/blob/main/dashboard3.png?raw=true)
![dashboard](https://github.com/tspannhw/SNACKAI-CoCo-ADSB/blob/main/dashboard4.png?raw=true)
![dashboard](https://github.com/tspannhw/SNACKAI-CoCo-ADSB/blob/main/dashboard5.png?raw=true)



### AI Semantic View

https://github.com/tspannhw/SNACKAI-CoCo-ADSB/blob/main/svadsbaircraft.yaml


### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIG_FILE` | `snowflake_config_adsb.json` | Config file path |
| `ADSB_URL` | `http://localhost:8080/data/aircraft.json` | ADS-B JSON endpoint |
| `INTERVAL` | `3` | Seconds between batches |
| `BATCH_SIZE` | `1` | Snapshots per batch |

## Troubleshooting

### Cannot connect to ADS-B receiver
```bash
# Check if dump1090/readsb is running
sudo systemctl status dump1090-fa
# or
sudo systemctl status readsb

# Test the endpoint
curl http://localhost:8080/data/aircraft.json | jq '.aircraft | length'
```

### No aircraft showing
- Check antenna connection
- Verify you're in an area with air traffic
- Check receiver gain settings
- Ensure clear line of sight to sky

### Snowflake authentication errors
- Verify PAT token is valid and not expired
- Check account identifier format
- Ensure user has proper permissions

## Running as a Service

Create `/etc/systemd/system/adsb-snowflake.service`:
```ini
[Unit]
Description=ADS-B to Snowflake Streaming
After=network.target dump1090-fa.service

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/RPIThermalStreaming
ExecStart=/usr/bin/python3 adsb_main.py --config snowflake_config_adsb.json
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable adsb-snowflake
sudo systemctl start adsb-snowflake
```

## License

Apache 2.0



### SQL SEtup

https://github.com/tspannhw/SNACKAI-CoCo-ADSB/blob/main/SETUP_ADSB_SNOWFLAKE.sql



* Initial Run *
  
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
