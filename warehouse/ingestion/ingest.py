from pathlib import Path
import fastf1
import pandas as pd
import json

BASE_DIR = Path(__file__).resolve().parent.parent
BASE_PATH = BASE_DIR / "bronze"

def write_parquet(df, path):
    if df is None or df.empty:
        return
    df.to_parquet(path)


def ingest_session(year, round_number, session_type):
    print(f"Ingesting {year} R{round_number} {session_type}")
    session = fastf1.get_session(year, round_number, session_type)
    session.load(laps=True, telemetry=True, weather=True, messages=True)
    
    session_dir = BASE_PATH / f"{year}" / f"{round_number}" / f"{session_type}"
    session_dir.mkdir(parents=True, exist_ok=True)

    # core tables
    write_parquet(session.results, session_dir / "results.parquet")
    write_parquet(session.laps, session_dir / "laps.parquet")
    write_parquet(session.weather_data, session_dir / "weather.parquet")
    write_parquet(session.track_status, session_dir / "track_status.parquet")
    write_parquet(session.race_control_messages, session_dir / "race_control.parquet")

    # telemetry folders
    car_dir = session_dir / "telemetry" / "car_data"
    pos_dir = session_dir / "telemetry" / "pos_data"

    car_dir.mkdir(parents=True, exist_ok=True)
    pos_dir.mkdir(parents=True, exist_ok=True)

    if session.car_data:
        for driver, df in session.car_data.items():
            write_parquet(df, car_dir / f"{driver}.parquet")

    if session.pos_data:
        for driver, df in session.pos_data.items():
            write_parquet(df, pos_dir / f"{driver}.parquet")

    # metadata
    with open(session_dir / "metadata_session.json", "w") as f:
        json.dump(dict(session.session_info), f, indent=2, default=str)

    with open(session_dir / "metadata_event.json", "w") as f:
        json.dump(dict(session.event), f, indent=2, default=str)

    print("Done.")


def ingest_season(year):

    schedule = fastf1.get_event_schedule(year)

    for _, event in schedule.iterrows():

        round_number = event["RoundNumber"]

        ingest_session(year, round_number, "R")

        if event["EventFormat"] in (
            "sprint",
            "sprint_shootout",
            "sprint_qualifying"
        ):
            ingest_session(year, round_number, "S")


def build_warehouse(start_year=2018):

    fastf1.Cache.enable_cache("ff1_cache")

    current_year = pd.Timestamp.today().year

    for year in range(start_year, current_year + 1):
        print(f"\nProcessing {year}")
        ingest_season(year)
