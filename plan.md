# F1 Strategy & Telemetry Analyzer: Comprehensive Project Plan

This document serves as the master blueprint for building an advanced Formula 1 Race Strategy and Telemetry Prediction application. The architecture relies on the **Medallion Data Lakehouse structure (Bronze, Silver, Gold)**, utilizing **Apache Spark (PySpark)** for massive-scale ETL, and a **Progressive Web App (PWA)** dashboard for the frontend to provide a seamless, app-like experience.

---

## Phase 1: Data Ingestion & Backend Setup (Bronze Layer)
**Objective:** Establish the backend python engine to fetch and store raw data.

*   **1.1 Environment Setup:** Set up Python virtual environment (`fastf1`, `pyspark`, `fastapi`). 
*   **1.2 Telemetry & Radio Clients:** Write Python scripts to fetch high-frequency X/Y coordinates, speed, braking %, and raw Team Radio text.
*   **1.3 Bronze Storage:** Dump raw JSON/CSV data directly into local Bronze data lake folders.
*   **1.4 API Backend:** Initialize a FastAPI server to serve data to our frontend.

---

## Phase 2: Data Cleansing (Silver Layer - The ETL Pipeline)
**Objective:** Transform disorganized API responses into optimized datasets using PySpark.

*   **2.1 Schema Mapping:** Define PySpark `StructType` and `ArrayType` schemas for the nested JSON.
*   **2.2 Telemetry Interpolation:** Group by Driver ID and use Window functions to interpolate missing X/Y coordinates for smooth visual movement on track.
*   **2.3 Radio Transcript Formatting:** Parse radio texts and associate them strictly with Driver IDs.
*   **2.4 Silver Storage:** Write DataFrames as Parquet files for extremely fast reads by the backend API.

---

## Phase 3: The PWA Dashboard UI (Gold Layer Foundation)
**Objective:** Build a sleek, dark-mode Progressive Web Application (PWA) with tabs and sections acting as a fully-fledged "Race Engineer Wall".

*   **3.1 Frontend Initialization (Vite + React/Vanilla):** Create a modern web app, configure `manifest.json` and Service Workers to make it an installable PWA.
*   **3.2 Dashboard Layout & Tabs:** Implement a tabbed navigation system UI:
    *   *Tab 1: Live Track Map & Radio:* A 2D plot of the cars moving, alongside the scrolling radio text.
    *   *Tab 2: Individual Telemetry:* Delta comparisons, speed, gear, and braking traces for a selected driver.
    *   *Tab 3: Strategy & Predictions:* Probability curves and tire degradation modeling.
*   **3.3 Real-Time Connection:** Connect the PWA to the FastAPI backend using WebSockets or fast polling to stream the Silver/Gold data live to the dashboard tabs.

---

## Phase 4: Analytics & Advanced Predictive AI (Gold Layer)
**Objective:** Add the advanced ML and mathematical models to the dashboard's "Strategy" tab.

*   **4.1 Dynamic Tire Degradation:** Calculate stint-specific tire drop-off rates and display them.
*   **4.2 Teammate Delta Lap:** Align telemetry arrays by Track Distance (X/Y) to highlight braking/throttle differences.
*   **4.3 Driver/Chassis Profiling & Monte Carlo:** Extract individual car variables (Top Speed, Dirty Air Penalty) and run 10,000+ probabilistic simulations of the remaining race laps. Feed these probability curves to the frontend Strategy Tab.
