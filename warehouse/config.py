from pathlib import Path
from datetime import datetime

# Base directories
BASE_DIR = Path(__file__).parent
BRONZE_DIR = BASE_DIR / "bronze"
FF1_CACHE_DIR = BASE_DIR / "ff1_cache"
CATALOG_PATH = BRONZE_DIR / "catalog.json"

# Ingestion Settings
START_SEASON = 2021
# Dynamically create a list of seasons from START_SEASON up to the current year
SUPPORTED_SEASONS = list(range(START_SEASON, datetime.now().year + 1))
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5

# Ensure core directories exist
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
FF1_CACHE_DIR.mkdir(parents=True, exist_ok=True)
