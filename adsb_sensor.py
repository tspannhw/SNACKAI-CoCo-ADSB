#!/usr/bin/env python3
"""
ADS-B Aircraft Sensor Module

Fetches ADS-B (Automatic Dependent Surveillance-Broadcast) aircraft data from
a local dump1090 or similar ADS-B receiver running on the Raspberry Pi.

Data source: http://localhost:8080/data/aircraft.json
Updates every ~3 seconds

Based on:
- https://github.com/tspannhw/FLiP-Py-ADS-B
- https://github.com/tspannhw/AIM-ADS-B
"""

import json
import logging
import socket
import time
import uuid
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional
import threading

logger = logging.getLogger(__name__)


class ADSBSensor:
    """
    ADS-B Aircraft Data Sensor
    
    Fetches real-time aircraft data from a local ADS-B receiver.
    The receiver (dump1090, readsb, etc.) provides JSON data via HTTP.
    """
    
    def __init__(self, 
                 adsb_url: str = "http://localhost:8080/data/aircraft.json",
                 simulate: bool = False,
                 require_real_sensors: bool = False):
        """
        Initialize the ADS-B sensor.
        
        Args:
            adsb_url: URL to the aircraft.json endpoint
            simulate: If True, generate simulated data (for testing)
            require_real_sensors: If True, raise error if can't connect to real sensor
        """
        self.adsb_url = adsb_url
        self.simulate = simulate
        self.require_real_sensors = require_real_sensors
        
        self.hostname = socket.gethostname()
        self.ip_address = self._get_ip_address()
        self.mac_address = self._get_mac_address()
        
        self._running = True
        self._last_data = None
        self._last_fetch_time = 0
        self._fetch_interval = 1.0
        
        logger.info(f"ADS-B Sensor initialized")
        logger.info(f"  Data URL: {self.adsb_url}")
        logger.info(f"  Hostname: {self.hostname}")
        logger.info(f"  IP: {self.ip_address}")
        
        if not simulate:
            self._verify_connection()
    
    def _get_ip_address(self) -> str:
        """Get the local IP address."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"
    
    def _get_mac_address(self) -> str:
        """Get the MAC address."""
        try:
            import uuid as uuid_mod
            mac = uuid_mod.getnode()
            return ':'.join(('%012x' % mac)[i:i+2] for i in range(0, 12, 2))
        except Exception:
            return "00:00:00:00:00:00"
    
    def _get_cache_busted_url(self) -> str:
        """Get URL with random query string to prevent caching."""
        import random
        cache_buster = f"nocache={int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        separator = '&' if '?' in self.adsb_url else '?'
        return f"{self.adsb_url}{separator}{cache_buster}"
    
    def _verify_connection(self):
        """Verify connection to ADS-B receiver."""
        try:
            url = self._get_cache_busted_url()
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            aircraft_count = len(data.get('aircraft', []))
            logger.info(f"[OK] Connected to ADS-B receiver")
            logger.info(f"[OK] Currently tracking {aircraft_count} aircraft")
            
        except requests.exceptions.RequestException as e:
            msg = f"Cannot connect to ADS-B receiver at {self.adsb_url}: {e}"
            if self.require_real_sensors:
                logger.error(msg)
                raise RuntimeError(msg)
            else:
                logger.warning(msg)
                logger.warning("Will retry on each read attempt")
    
    def _fetch_aircraft_data(self) -> Dict:
        """Fetch raw aircraft data from the ADS-B receiver."""
        if self.simulate:
            return self._generate_simulated_data()
        
        try:
            url = self._get_cache_busted_url()
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            self._last_data = data
            self._last_fetch_time = time.time()
            return data
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch ADS-B data: {e}")
            if self._last_data and (time.time() - self._last_fetch_time) < 30:
                logger.warning("Using cached data")
                return self._last_data
            return {'aircraft': [], 'now': time.time(), 'messages': 0}
    
    def _generate_simulated_data(self) -> Dict:
        """Generate simulated aircraft data for testing."""
        import random
        
        num_aircraft = random.randint(5, 20)
        aircraft = []
        
        airlines = ['UAL', 'DAL', 'AAL', 'SWA', 'JBU', 'ASA', 'NKS', 'FFT']
        
        for i in range(num_aircraft):
            ac = {
                'hex': f'{random.randint(0xa00000, 0xafffff):06x}',
                'flight': f'{random.choice(airlines)}{random.randint(100, 9999)}',
                'alt_baro': random.randint(1000, 45000),
                'alt_geom': random.randint(1000, 45000),
                'gs': random.uniform(100, 550),
                'track': random.uniform(0, 360),
                'lat': random.uniform(25, 48),
                'lon': random.uniform(-125, -70),
                'baro_rate': random.randint(-2000, 2000),
                'squawk': f'{random.randint(1000, 7777):04d}',
                'category': random.choice(['A1', 'A2', 'A3', 'A4', 'A5']),
                'rssi': random.uniform(-30, -5),
                'messages': random.randint(100, 10000),
                'seen': random.uniform(0, 10),
                'seen_pos': random.uniform(0, 30)
            }
            aircraft.append(ac)
        
        return {
            'now': time.time(),
            'messages': random.randint(100000, 999999),
            'aircraft': aircraft
        }
    
    def read(self) -> List[Dict]:
        """
        Read current aircraft data and format for Snowflake ingestion.
        
        Returns:
            List of aircraft records ready for streaming
        """
        data = self._fetch_aircraft_data()
        
        now = datetime.now(timezone.utc)
        timestamp = now.isoformat()
        ts_epoch = int(now.timestamp())
        
        receiver_data = {
            'receiver_host': self.hostname,
            'receiver_ip': self.ip_address,
            'receiver_mac': self.mac_address,
            'receiver_time': data.get('now', time.time()),
            'total_messages': data.get('messages', 0)
        }
        
        records = []
        for ac in data.get('aircraft', []):
            record = {
                'uuid': f"adsb_{ac.get('hex', 'unknown')}_{ts_epoch}",
                'rowid': f"{ts_epoch}_{uuid.uuid4()}",
                'datetimestamp': timestamp,
                'ts': ts_epoch,
                
                'icao_hex': ac.get('hex'),
                'flight': ac.get('flight', '').strip() if ac.get('flight') else None,
                'registration': ac.get('r'),
                'aircraft_type': ac.get('t'),
                'description': ac.get('desc'),
                
                'altitude_baro': ac.get('alt_baro'),
                'altitude_geom': ac.get('alt_geom'),
                'ground_speed': ac.get('gs'),
                'track': ac.get('track'),
                'true_heading': ac.get('true_heading'),
                'mag_heading': ac.get('mag_heading'),
                'indicated_airspeed': ac.get('ias'),
                'true_airspeed': ac.get('tas'),
                'mach': ac.get('mach'),
                'vertical_rate': ac.get('baro_rate'),
                'vertical_rate_geom': ac.get('geom_rate'),
                
                'latitude': ac.get('lat'),
                'longitude': ac.get('lon'),
                'nav_altitude': ac.get('nav_altitude_mcp'),
                'nav_heading': ac.get('nav_heading'),
                'nav_qnh': ac.get('nav_qnh'),
                
                'squawk': ac.get('squawk'),
                'category': ac.get('category'),
                'emergency': ac.get('emergency'),
                
                'rssi': ac.get('rssi'),
                'messages': ac.get('messages'),
                'seen': ac.get('seen'),
                'seen_pos': ac.get('seen_pos'),
                
                'hostname': self.hostname,
                'receiver_host': receiver_data['receiver_host'],
                'receiver_ip': receiver_data['receiver_ip'],
                'receiver_time': receiver_data['receiver_time'],
                'total_messages': receiver_data['total_messages']
            }
            records.append(record)
        
        return records
    
    def read_batch(self, count: int = 1, interval: float = 3.0, fast_mode: bool = False) -> List[Dict]:
        """
        Read multiple batches of aircraft data.
        
        ADS-B data updates every ~3 seconds, so multiple reads capture temporal changes.
        
        Args:
            count: Number of snapshots to take
            interval: Seconds between snapshots (minimum 3s recommended)
            fast_mode: If True, use minimum interval
            
        Returns:
            List of all aircraft records from all snapshots
        """
        all_records = []
        actual_interval = 0.5 if fast_mode else max(3.0, interval)
        
        for i in range(count):
            records = self.read()
            all_records.extend(records)
            
            if i < count - 1:
                time.sleep(actual_interval)
        
        logger.info(f"Read {len(all_records)} aircraft records from {count} snapshots")
        return all_records
    
    def get_summary(self) -> Dict:
        """Get summary statistics of current aircraft."""
        data = self._fetch_aircraft_data()
        aircraft = data.get('aircraft', [])
        
        if not aircraft:
            return {
                'total_aircraft': 0,
                'with_position': 0,
                'with_altitude': 0,
                'avg_altitude': 0,
                'total_messages': data.get('messages', 0)
            }
        
        with_pos = sum(1 for ac in aircraft if ac.get('lat') and ac.get('lon'))
        with_alt = [ac.get('alt_baro', 0) for ac in aircraft if ac.get('alt_baro')]
        
        return {
            'total_aircraft': len(aircraft),
            'with_position': with_pos,
            'with_altitude': len(with_alt),
            'avg_altitude': sum(with_alt) / len(with_alt) if with_alt else 0,
            'total_messages': data.get('messages', 0)
        }
    
    def cleanup(self):
        """Cleanup resources."""
        self._running = False
        logger.info("ADS-B sensor cleaned up")


def main():
    """Test the ADS-B sensor."""
    logging.basicConfig(level=logging.INFO)
    
    sensor = ADSBSensor(simulate=True)
    
    print("\n=== ADS-B Sensor Test ===\n")
    
    summary = sensor.get_summary()
    print(f"Summary: {json.dumps(summary, indent=2)}")
    
    records = sensor.read()
    print(f"\nGot {len(records)} aircraft records")
    
    if records:
        print(f"\nSample record:")
        print(json.dumps(records[0], indent=2, default=str))
    
    sensor.cleanup()


if __name__ == '__main__':
    main()
