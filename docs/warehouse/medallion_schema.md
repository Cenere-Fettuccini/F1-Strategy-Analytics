# Medallion Architecture Schema Documentation

This document outlines the purpose, structural design, and target schemas for the three data layers within the F1 Data Warehouse.

---

## 1. Bronze Layer (Raw Data)
**Purpose**: The immutable source of truth. Data is ingested directly from FastF1 and OpenF1 APIs with zero transformations or aggregations applied.
**Location**: `warehouse/bronze/<session_key>/`
**Formats**: `.parquet`, `.json`

### Contents per Session Directory:
*   **`laps.parquet` (Source: FastF1)**
    *   **Columns**: `Time`, `DriverNumber`, `LapTime`, `LapNumber`, `Stint`, `PitOutTime`, `PitInTime`, `Sector1Time`, `Sector2Time`, `Sector3Time`, `Sector1SessionTime`, `Sector2SessionTime`, `Sector3SessionTime`, `SpeedI1`, `SpeedI2`, `SpeedFL`, `SpeedST`, `IsPersonalBest`, `Compound`, `TyreLife`, `FreshTyre`, `LapStartTime`, `Team`, `Driver`, `TrackStatus`, `IsAccurate`.
    *   **Note**: Times are provided as Pandas `timedelta` objects relative to the session start.
*   **`weather.parquet` (Source: FastF1)**
    *   **Columns**: `Time`, `AirTemp`, `Humidity`, `Pressure`, `Rainfall`, `TrackTemp`, `WindDirection`, `WindSpeed`.
*   **`team_radio.json` (Source: OpenF1)**
    *   **Structure**: Array of JSON objects.
    *   **Keys**: `{"session_key": int, "meeting_key": int, "driver_number": int, "date": str, "recording_url": str}`.

---

## 2. Silver Layer (Normalized Data)
**Purpose**: Cleaned, filtered, and unified data. Timestamps are standardized to absolute UTC. Nulls and anomalies are handled. Different data sources are aligned for cross-referencing.
**Location**: `warehouse/silver/<table_name>/`
**Format**: Partitioned `.parquet` (Partitioned by Year/Session)

### Target Tables & Schemas:
*   **`clean_laps`**
    *   **Schema**: `session_key` (String), `driver_id` (String), `lap_number` (Int), `lap_time_ms` (Int), `sector_1_ms` (Int), `sector_2_ms` (Int), `sector_3_ms` (Int), `tyre_compound` (String), `tyre_life` (Int), `is_pit_in_lap` (Boolean), `is_pit_out_lap` (Boolean).
    *   **Transformations**: Converted `timedelta` objects to absolute milliseconds for easier database querying. Dropped rows with physically impossible lap times.
*   **`unified_weather`**
    *   **Schema**: `session_key` (String), `timestamp_utc` (Datetime), `track_temp` (Float), `air_temp` (Float), `is_raining` (Boolean).
    *   **Transformations**: Aligned FastF1 session relative time to a global UTC timestamp using the session start time.
*   **`radio_metadata`**
    *   **Schema**: `session_key` (String), `driver_id` (String), `timestamp_utc` (Datetime), `audio_url` (String).
    *   **Transformations**: Mapped `driver_number` to unified `driver_id`.

---

## 3. Gold Layer (Business & Analysis Engine Data)
**Purpose**: Highly aggregated, feature-engineered tables designed explicitly to feed the Backend APIs and the **Decision Analysis Engine**.
**Location**: `warehouse/gold/<table_name>/`
**Format**: `.parquet` (Designed to be loaded directly into the FastAPI in-memory DB or PostgreSQL)

### Target Tables & Schemas:
*   **`pit_decision_metrics`**
    *   **Description**: Evaluates the efficiency of a pit stop strategy relative to the grid and tyre degradation.
    *   **Schema**: 
        *   `session_key` (String)
        *   `driver_id` (String)
        *   `lap_number` (Int)
        *   `time_lost_in_pit_ms` (Int)
        *   `undercut_success` (Boolean)
        *   `expected_tyre_delta_ms` (Int)
        *   `actual_tyre_delta_ms` (Int)
*   **`tyre_degradation_curves`**
    *   **Description**: Aggregated drop-off in lap times per tyre compound per circuit, used to predict optimal pit windows.
    *   **Schema**: 
        *   `circuit_id` (String)
        *   `year` (Int)
        *   `compound` (String)
        *   `average_lap_time_drop_per_lap_ms` (Float)
*   **`weather_response_times`**
    *   **Description**: Measures how many laps a driver/team took to pit after a registered rain event compared to the optimal crossover point.
    *   **Schema**: 
        *   `session_key` (String)
        *   `driver_id` (String)
        *   `rain_start_lap` (Int)
        *   `pitted_lap` (Int)
        *   `laps_lost_to_crossover` (Int)
