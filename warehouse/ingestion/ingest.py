import fastf1
import pandas as pd
import os
import json

BASE_PATH = "../warehouse/bronze"

def write_parquet(df, path):
    if df is None or df.empty:
        return
    df.to_parquet(path)


def ingest_session(year, round_number, session_type):
    print(f"Ingesting {year} R{round_number} {session_type}")
    session = fastf1.get_session(year, round_number, session_type)
    session.load(laps=True, telemetry=True, weather=True, messages=True)
    session_dir = os.path.join(BASE_PATH,f"{year}",f"{round_number}",f"{session_type}")
    os.makedirs(session_dir, exist_ok=True)

    # core tables
    write_parquet(session.results, f"{session_dir}/results.parquet")
    write_parquet(session.laps, f"{session_dir}/laps.parquet")
    write_parquet(session.weather_data, f"{session_dir}/weather.parquet")
    write_parquet(session.track_status, f"{session_dir}/track_status.parquet")
    write_parquet(session.race_control_messages, f"{session_dir}/race_control.parquet")

    # telemetry folders
    car_dir = os.path.join(session_dir, "telemetry/car_data")
    pos_dir = os.path.join(session_dir, "telemetry/pos_data")

    os.makedirs(car_dir, exist_ok=True)
    os.makedirs(pos_dir, exist_ok=True)

    if session.car_data:
        for driver, df in session.car_data.items():
            write_parquet(df, f"{car_dir}/{driver}.parquet")

    if session.pos_data:
        for driver, df in session.pos_data.items():
            write_parquet(df, f"{pos_dir}/{driver}.parquet")

    # metadata
    with open(f"{session_dir}/metadata_session.json", "w") as f:
        json.dump(dict(session.session_info), f, indent=2, default=str)

    with open(f"{session_dir}/metadata_event.json", "w") as f:
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