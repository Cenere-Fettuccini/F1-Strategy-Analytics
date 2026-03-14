from pathlib import Path

# Project Roots
ROOT_DIR = Path(__file__).resolve().parent
WAREHOUSE_DIR = ROOT_DIR / "warehouse"
BRONZE_DIR = WAREHOUSE_DIR / "bronze"
SILVER_DIR = WAREHOUSE_DIR / "silver"
GOLD_DIR = WAREHOUSE_DIR / "gold"

# FastF1 Settings
FASTF1_CACHE_DIR = ROOT_DIR / "ff1_cache"

# API Settings
API_HOST = "0.0.0.0"
API_PORT = 8000

# Ensure directories exist
for directory in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, FASTF1_CACHE_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
