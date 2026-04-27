"""
Silver layer transformations for F1 data.
Cleans raw Parquet/JSON data from Bronze layer, normalizes timestamps, 
and stores optimized Parquet files for the Gold layer and APIs.
"""
import json
import logging
from pathlib import Path
import pandas as pd
from warehouse.config import BASE_DIR, BRONZE_DIR

SILVER_DIR = BASE_DIR / "silver"
SILVER_CATALOG_PATH = SILVER_DIR / "silver_catalog.json"
BRONZE_CATALOG_PATH = BRONZE_DIR / "catalog.json"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure Silver directory exists
SILVER_DIR.mkdir(parents=True, exist_ok=True)

class SilverTransformer:
    def __init__(self):
        self._ensure_catalog()

    def _ensure_catalog(self):
        if not SILVER_CATALOG_PATH.exists():
            with open(SILVER_CATALOG_PATH, 'w') as f:
                json.dump({}, f)

    def _read_catalog(self, path: Path) -> dict:
        if not path.exists():
            return {}
        with open(path, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}

    def _write_silver_catalog(self, data: dict):
        with open(SILVER_CATALOG_PATH, 'w') as f:
            json.dump(data, f, indent=4)

    def _timedelta_to_ms(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Converts a Pandas timedelta column to integer milliseconds."""
        if col in df.columns:
            # Handle NaN/NaT safely by filling with 0 or keeping as None
            # Here we fill with -1 or leave as Int64 (nullable int)
            ms_series = df[col].dt.total_seconds() * 1000
            df[col + '_ms'] = ms_series.round().astype('Int64')
            df.drop(columns=[col], inplace=True)
        return df

    def transform_laps(self, bronze_path: Path, silver_path: Path):
        """Cleans lap data and normalizes times."""
        laps_file = bronze_path / "laps.parquet"
        if not laps_file.exists():
            return False

        df = pd.read_parquet(laps_file)
        
        # Convert times
        time_cols = ['Time', 'LapTime', 'PitOutTime', 'PitInTime', 'Sector1Time', 
                     'Sector2Time', 'Sector3Time', 'Sector1SessionTime', 
                     'Sector2SessionTime', 'Sector3SessionTime', 'LapStartTime']
        
        for col in time_cols:
            df = self._timedelta_to_ms(df, col)

        # Drop truly impossible laps (e.g. 0ms) if they exist
        if 'LapTime_ms' in df.columns:
            df = df[(df['LapTime_ms'] > 10000) | (df['LapTime_ms'].isna())]

        df.to_parquet(silver_path / "clean_laps.parquet")
        return True

    def transform_weather(self, bronze_path: Path, silver_path: Path):
        """Normalizes weather data."""
        weather_file = bronze_path / "weather.parquet"
        if not weather_file.exists():
            return False

        df = pd.read_parquet(weather_file)
        df = self._timedelta_to_ms(df, 'Time')
        
        # Add a simple boolean flag
        if 'Rainfall' in df.columns:
            df['is_raining'] = df['Rainfall'] > 0
            
        df.to_parquet(silver_path / "unified_weather.parquet")
        return True

    def transform_telemetry(self, bronze_path: Path, silver_path: Path):
        """Cleans high-frequency telemetry data."""
        telem_file = bronze_path / "telemetry.parquet"
        if not telem_file.exists():
            return False

        df = pd.read_parquet(telem_file)
        
        df = self._timedelta_to_ms(df, 'Time')
        df = self._timedelta_to_ms(df, 'SessionTime')
        
        # Forward fill coordinates for very brief signal drops
        coord_cols = ['X', 'Y', 'Z']
        for col in coord_cols:
            if col in df.columns:
                df[col] = df[col].ffill()

        df.to_parquet(silver_path / "clean_telemetry.parquet")
        return True

    def process_all(self):
        """Scans Bronze catalog and processes new sessions."""
        logger.info("Starting Silver transformation run...")
        
        bronze_catalog = self._read_catalog(BRONZE_CATALOG_PATH)
        silver_catalog = self._read_catalog(SILVER_CATALOG_PATH)

        processed_count = 0

        for session_key, b_meta in bronze_catalog.items():
            if b_meta.get('status') == 'completed' and session_key not in silver_catalog:
                logger.info("Processing session to Silver: %s", session_key)
                
                bronze_session_dir = BRONZE_DIR / session_key
                silver_session_dir = SILVER_DIR / session_key
                silver_session_dir.mkdir(parents=True, exist_ok=True)

                laps_ok = self.transform_laps(bronze_session_dir, silver_session_dir)
                weat_ok = self.transform_weather(bronze_session_dir, silver_session_dir)
                tele_ok = self.transform_telemetry(bronze_session_dir, silver_session_dir)
                
                # Copy Track Corners directly (no heavy transform needed)
                import shutil
                corners_file = bronze_session_dir / "track_corners.parquet"
                if corners_file.exists():
                    shutil.copy(corners_file, silver_session_dir / "track_corners.parquet")

                # Copy Radio JSON directly
                radio_file = bronze_session_dir / "team_radio.json"
                if radio_file.exists():
                    shutil.copy(radio_file, silver_session_dir / "radio_metadata.json")

                # Register success
                silver_catalog[session_key] = {
                    "status": "completed",
                    "source": "bronze"
                }
                self._write_silver_catalog(silver_catalog)
                processed_count += 1
                logger.info("Successfully promoted %s to Silver layer.", session_key)

        logger.info("Silver transformation complete. Processed %d new sessions.", processed_count)

if __name__ == "__main__":
    transformer = SilverTransformer()
    transformer.process_all()
