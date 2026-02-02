#!/bin/bash
# ============================================================================
# ADS-B Aircraft Streaming to Snowflake
# ============================================================================
# Streams real-time ADS-B aircraft data from a local receiver (dump1090/readsb)
# to Snowflake using Snowpipe Streaming v2 REST API.
#
# Prerequisites:
#   - ADS-B receiver running (dump1090, readsb, tar1090, etc.)
#   - Data available at http://localhost:8080/data/aircraft.json
#   - Snowflake config file: snowflake_config_adsb.json
#   - Python 3.8+ with requests library
#
# Usage:
#   ./run_adsb.sh              # Run with defaults (3s interval)
#   ./run_adsb.sh --fast       # Fast mode for maximum throughput
#   ./run_adsb.sh --interval 5 # Custom interval
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONFIG_FILE="${CONFIG_FILE:-snowflake_config_adsb.json}"
ADSB_URL="${ADSB_URL:-http://localhost:8080/data/aircraft.json}"
INTERVAL="${INTERVAL:-3}"
BATCH_SIZE="${BATCH_SIZE:-1}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}  ADS-B Aircraft Streaming to Snowflake${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}[ERROR] Python 3 is not installed${NC}"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}[ERROR] Config file not found: $CONFIG_FILE${NC}"
    echo -e "${YELLOW}Create snowflake_config_adsb.json with your Snowflake credentials${NC}"
    exit 1
fi

echo -e "${YELLOW}Checking ADS-B receiver...${NC}"
if curl -s --connect-timeout 5 "$ADSB_URL" > /dev/null 2>&1; then
    AIRCRAFT_COUNT=$(curl -s "$ADSB_URL" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('aircraft',[])))" 2>/dev/null || echo "0")
    echo -e "${GREEN}[OK] ADS-B receiver is running - tracking $AIRCRAFT_COUNT aircraft${NC}"
else
    echo -e "${RED}[WARNING] Cannot reach ADS-B receiver at $ADSB_URL${NC}"
    echo -e "${YELLOW}Make sure dump1090/readsb is running${NC}"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""
echo -e "${GREEN}Configuration:${NC}"
echo -e "  Config file:  $CONFIG_FILE"
echo -e "  ADS-B URL:    $ADSB_URL"
echo -e "  Interval:     ${INTERVAL}s"
echo -e "  Batch size:   $BATCH_SIZE"
echo ""

echo -e "${GREEN}Starting ADS-B streaming...${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
echo ""

python3 adsb_main.py \
    --config "$CONFIG_FILE" \
    --adsb-url "$ADSB_URL" \
    --interval "$INTERVAL" \
    --batch-size "$BATCH_SIZE" \
    "$@"
