#!/usr/bin/env python3
"""
Main Application: Raspberry Pi ADS-B Receiver to Snowflake Streaming

Continuously reads ADS-B aircraft data from a local receiver (dump1090/readsb)
and streams it to Snowflake using Snowpipe Streaming v2 REST API.

Data source: http://localhost:8080/data/aircraft.json (updates every ~3 seconds)

Based on:
- https://github.com/tspannhw/FLiP-Py-ADS-B
- https://github.com/tspannhw/AIM-ADS-B

Usage:
    python adsb_main.py [--config CONFIG_FILE] [--batch-size SIZE] [--interval SECONDS] [--fast]
"""

import argparse
import logging
import time
import sys
import os
import signal
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from adsb_sensor import ADSBSensor
from thermal_streaming_client import SnowpipeStreamingClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('adsb_streaming.log')
    ]
)
logger = logging.getLogger(__name__)


class ADSBStreamingApp:
    """Main application for streaming ADS-B aircraft data to Snowflake."""
    
    def __init__(self, config_file: str = 'snowflake_config_adsb.json',
                 batch_size: int = 1, interval: float = 3.0, fast_mode: bool = False,
                 adsb_url: str = "http://localhost:8080/data/aircraft.json"):
        """
        Initialize the application.
        
        Args:
            config_file: Path to Snowflake configuration file
            batch_size: Number of snapshots per batch (each snapshot captures all visible aircraft)
            interval: Seconds between batch sends
            fast_mode: If True, maximize throughput
            adsb_url: URL to the ADS-B aircraft.json endpoint
        """
        self.config_file = config_file
        self.batch_size = batch_size
        self.interval = interval
        self.fast_mode = fast_mode
        self.running = False
        
        logger.info("=" * 70)
        logger.info("ADS-B Aircraft Streaming Application - PRODUCTION MODE")
        logger.info("Raspberry Pi ADS-B Receiver -> Snowflake via Snowpipe Streaming v2")
        logger.info("=" * 70)
        logger.info("PRODUCTION CONFIGURATION:")
        logger.info("  - Real ADS-B aircraft data ONLY")
        logger.info("  - Snowpipe Streaming high-speed REST API ONLY")
        logger.info("  - Data source updates every ~3 seconds")
        logger.info("=" * 70)
        
        logger.info("Initializing ADS-B receiver connection...")
        self.sensor = ADSBSensor(
            adsb_url=adsb_url,
            simulate=False,
            require_real_sensors=False
        )
        
        logger.info("Initializing Snowpipe Streaming REST API client...")
        self.client = SnowpipeStreamingClient(config_file)
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("Initialization complete")
        logger.info(f"Batch size: {batch_size} snapshot(s)")
        logger.info(f"Batch interval: {interval} seconds")
        logger.info(f"ADS-B URL: {adsb_url}")
        if fast_mode:
            logger.info("FAST MODE: Enabled for maximum throughput")
        
        logger.info(f"Local hostname: {self.sensor.hostname}")
        logger.info(f"Local IP: {self.sensor.ip_address}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"\nReceived signal {signum}, shutting down gracefully...")
        self.running = False
    
    def initialize(self):
        """Initialize the Snowpipe Streaming connection."""
        logger.info("Setting up Snowpipe Streaming connection...")
        
        try:
            logger.info("Discovering ingest host...")
            ingest_host = self.client.discover_ingest_host()
            logger.info(f"[OK] Ingest host: {ingest_host}")
            
            logger.info("Opening streaming channel...")
            self.client.open_channel()
            logger.info("[OK] Channel opened successfully")
            
            logger.info("Snowpipe Streaming connection ready!")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize streaming: {e}", exc_info=True)
            return False
    
    def run(self):
        """Main application loop."""
        if not self.initialize():
            logger.error("Initialization failed, exiting")
            return 1
        
        self.running = True
        batch_count = 0
        total_aircraft = 0
        
        logger.info("=" * 70)
        logger.info("Starting ADS-B data collection and streaming...")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 70)
        
        try:
            while self.running:
                batch_count += 1
                batch_start = time.time()
                
                logger.info(f"\n--- Batch {batch_count} ---")
                
                if self.fast_mode:
                    readings = self.sensor.read_batch(
                        count=self.batch_size,
                        interval=1.0,
                        fast_mode=True
                    )
                else:
                    readings = self.sensor.read_batch(
                        count=self.batch_size,
                        interval=max(3.0, self.interval / self.batch_size)
                    )
                
                if readings:
                    total_aircraft += len(readings)
                    sample = readings[0]
                    
                    flights_with_callsign = sum(1 for r in readings if r.get('flight'))
                    flights_with_position = sum(1 for r in readings if r.get('latitude') and r.get('longitude'))
                    
                    logger.info(f"Captured {len(readings)} aircraft records")
                    logger.info(f"  With callsign: {flights_with_callsign}")
                    logger.info(f"  With position: {flights_with_position}")
                    
                    if sample.get('flight'):
                        logger.info(f"Sample: {sample.get('flight')} "
                                   f"Alt={sample.get('altitude_baro')}ft "
                                   f"GS={sample.get('ground_speed'):.0f}kts "
                                   f"Lat={sample.get('latitude'):.4f}" if sample.get('latitude') else "")
                else:
                    logger.warning("No aircraft currently visible")
                    readings = []
                
                if readings:
                    try:
                        row_count = self.client.insert_rows(readings)
                        logger.info(f"[OK] Successfully sent {row_count} aircraft records to Snowpipe Streaming")
                        
                    except Exception as e:
                        logger.error(f"Failed to insert batch: {e}")
                
                if batch_count % 10 == 0:
                    self.client.print_statistics()
                    logger.info(f"Total aircraft records sent: {total_aircraft}")
                
                batch_elapsed = time.time() - batch_start
                sleep_time = max(0, self.interval - batch_elapsed)
                
                if sleep_time > 0 and self.running:
                    logger.info(f"Waiting {sleep_time:.1f}s until next batch...")
                    time.sleep(sleep_time)
        
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            return 1
        
        finally:
            self.shutdown()
        
        return 0
    
    def shutdown(self):
        """Graceful shutdown."""
        logger.info("\n" + "=" * 70)
        logger.info("Shutting down...")
        logger.info("=" * 70)
        
        try:
            self.client.print_statistics()
            
            logger.info("Closing streaming channel...")
            self.client.close_channel()
            logger.info("[OK] Channel closed")
            
            logger.info("Cleaning up sensor...")
            self.sensor.cleanup()
            logger.info("[OK] Sensor cleaned up")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        
        logger.info("Shutdown complete")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Stream Raspberry Pi ADS-B aircraft data to Snowflake'
    )
    parser.add_argument(
        '--config',
        default='snowflake_config_adsb.json',
        help='Path to Snowflake configuration file (default: snowflake_config_adsb.json)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1,
        help='Number of snapshots per batch (default: 1)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=3.0,
        help='Seconds between batches (default: 3.0, matches ADS-B update rate)'
    )
    parser.add_argument(
        '--adsb-url',
        default='http://localhost:8080/data/aircraft.json',
        help='URL to ADS-B aircraft.json endpoint'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--fast',
        action='store_true',
        help='Enable fast mode for maximum throughput'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("PRODUCTION MODE: Real ADS-B data + Snowpipe Streaming REST API only")
    
    app = ADSBStreamingApp(
        config_file=args.config,
        batch_size=args.batch_size,
        interval=args.interval,
        fast_mode=args.fast,
        adsb_url=args.adsb_url
    )
    
    exit_code = app.run()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
