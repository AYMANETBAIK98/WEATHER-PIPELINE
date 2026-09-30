"""Central configuration: cities, API settings and database connection."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class City:
    code: str
    name: str
    country: str
    latitude: float
    longitude: float
    timezone: str


# Markets I work with day to day (UK / DE / ES) plus a few other hubs.
CITIES: list[City] = [
    City("LON", "London",     "United Kingdom",       51.5074,  -0.1278, "Europe/London"),
    City("BER", "Berlin",     "Germany",              52.5200,  13.4050, "Europe/Berlin"),
    City("MAD", "Madrid",     "Spain",                40.4168,  -3.7038, "Europe/Madrid"),
    City("PAR", "Paris",      "France",               48.8566,   2.3522, "Europe/Paris"),
    City("AMS", "Amsterdam",  "Netherlands",          52.3676,   4.9041, "Europe/Amsterdam"),
    City("CAS", "Casablanca", "Morocco",              33.5731,  -7.5898, "Africa/Casablanca"),
    City("DXB", "Dubai",      "United Arab Emirates", 25.2048,  55.2708, "Asia/Dubai"),
    City("YYZ", "Toronto",    "Canada",               43.6532, -79.3832, "America/Toronto"),
]

# Open-Meteo: free, no API key. Forecast API covers recent days,
# the archive API covers older history (used for backfills).
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
RECENT_DAYS_LIMIT = 60  # older than this -> archive API

HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]

HTTP_TIMEOUT_SECONDS = 30
HTTP_MAX_RETRIES = 3


def get_db_dsn() -> str:
    """Build the warehouse DSN from environment variables."""
    return (
        f"host={os.getenv('WAREHOUSE_HOST', 'localhost')} "
        f"port={os.getenv('WAREHOUSE_PORT', '5432')} "
        f"dbname={os.getenv('WAREHOUSE_DB', 'weather')} "
        f"user={os.getenv('WAREHOUSE_USER', 'etl')} "
        f"password={os.getenv('WAREHOUSE_PASSWORD', 'etl')}"
    )
