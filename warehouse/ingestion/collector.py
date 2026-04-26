"""
Module for collecting and storing raw F1 session data.
"""
import time
import logging
import requests
import fastf1
from warehouse.config import BRONZE_DIR, FF1_CACHE_DIR, MAX_RETRIES, RETRY_BACKOFF_SECONDS

logger = logging.getLogger(__name__)

fastf1.Cache.enable_cache(str(FF1_CACHE_DIR))

class DataCollector:
    """
    Engine for collecting telemetry and session data from FastF1 and OpenF1 APIs.
    
    Includes a built-in retry mechanism to handle transient network issues 
    and API rate limits during the ingestion process.
    """
    def __init__(self):
        self.max_retries = MAX_RETRIES
        self.backoff = RETRY_BACKOFF_SECONDS

    def _retry_wrapper(self, func, *args, **kwargs):
        """Wrapper to execute a network function with retry logic."""
        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except (requests.RequestException, ValueError, RuntimeError) as e:
                logger.warning("Attempt %d/%d failed: %s", attempt, self.max_retries, e)
                if attempt == self.max_retries:
                    logger.error("Max retries reached.")
                    raise
                time.sleep(self.backoff * attempt)
        return None

    def fetch_fastf1_data(self, year: int, round_num: int, session_name: str):
        """Fetches and loads core FastF1 data."""
        logger.info("Fetching FastF1 data for %s Round %s %s", year, round_num, session_name)
        session = fastf1.get_session(year, round_num, session_name)
        session.load(telemetry=True, laps=True, weather=True, messages=True)
        return session

    def fetch_openf1_data(self, session_key_openf1: int):
        """Fetches OpenF1 team radio data."""
        logger.info("Fetching OpenF1 data for session_key %s", session_key_openf1)
        url = f"https://api.openf1.org/v1/team_radio?session_key={session_key_openf1}"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()

    def _get_openf1_session_key(self, year: int, session_name: str, country: str):
        """Finds the corresponding OpenF1 session key using the year, session_name, and country."""
        logger.info("Looking up OpenF1 session key for %s %s (%s)", year, session_name, country)
        # OpenF1 uses standard names like 'Race', 'Qualifying', 'Practice 1', etc.
        url = f"https://api.openf1.org/v1/sessions?year={year}&session_name={session_name}"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        sessions = response.json()
        
        # Try to match country
        for s in sessions:
            if s.get('country_name', '').lower() == country.lower():
                return s.get('session_key')
                
        # Fallback if only one session matches year/name (e.g. some circuits have naming mismatches)
        if len(sessions) == 1:
            return sessions[0].get('session_key')
            
        return None

    def collect_session(self, session_meta: dict) -> bool:
        """
        Collects all data (FastF1 + OpenF1) for a given session.
        Returns True if successful, False if failed.
        """
        year = session_meta['year']
        round_num = session_meta['round']
        session_name = session_meta['session_name']
        session_key = session_meta['session_key']

        try:
            # 1. Fetch FastF1 Data
            ff1_session = self._retry_wrapper(self.fetch_fastf1_data, year, round_num, session_name)
            if not ff1_session:
                return False

            # Create directory before saving anything
            session_dir = BRONZE_DIR / session_key
            session_dir.mkdir(parents=True, exist_ok=True)

            # Save FastF1 data to Bronze layer (Parquet)
            if hasattr(ff1_session, 'laps') and not ff1_session.laps.empty:
                ff1_session.laps.to_parquet(session_dir / "laps.parquet")
            
            if hasattr(ff1_session, 'weather_data') and not ff1_session.weather_data.empty:
                ff1_session.weather_data.to_parquet(session_dir / "weather.parquet")

            # 2. Map and Fetch OpenF1 Data
            country = getattr(ff1_session.event, 'Country', '')
            openf1_key = self._retry_wrapper(self._get_openf1_session_key, year, session_name, country)
            
            if openf1_key:
                openf1_data = self._retry_wrapper(self.fetch_openf1_data, openf1_key)
                if openf1_data:
                    # Save OpenF1 Radio Data to Bronze layer (JSON)
                    import json
                    with open(session_dir / "team_radio.json", "w", encoding="utf-8") as f:
                        json.dump(openf1_data, f)
            else:
                logger.warning("Could not map OpenF1 session key for %s. Skipping radio data.", session_key)

            # Write a marker to signify successful raw ingestion
            (session_dir / "_SUCCESS").touch()

            return True

        except (requests.RequestException, ValueError, RuntimeError, OSError) as e:
            logger.error("Failed to collect session %s: %s", session_key, e)
            return False
