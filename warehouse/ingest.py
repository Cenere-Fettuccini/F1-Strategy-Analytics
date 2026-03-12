import fastf1
import json
import os

# Bronze Layer: Raw Data Ingestion
BRONZE_PATH = "warehouse/data/bronze"

def ingest_session(year, round_num, session_type):
    """
    Fetches raw telemetry and session data and saves to Bronze layer.
    """
    os.makedirs(BRONZE_PATH, exist_ok=True)
    
    print(f"Fetching {year} Round {round_num} - {session_type}...")
    session = fastf1.get_session(year, round_num, session_type)
    session.load()
    
    # Save logic here...
    print("Ingestion complete.")

if __name__ == "__main__":
    # Example usage
    ingest_session(2024, 1, 'R')
