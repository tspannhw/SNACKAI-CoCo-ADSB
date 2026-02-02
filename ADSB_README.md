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

## Environment Variables

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
