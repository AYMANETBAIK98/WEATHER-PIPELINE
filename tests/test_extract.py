from datetime import date

import pytest
import requests
from conftest import TEST_DATE, make_payload

from etl.config import ARCHIVE_URL, CITIES, FORECAST_URL
from etl.extract import ExtractError, build_params, choose_endpoint, fetch_city_day


class FakeResponse:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload or {}
        self.text = str(payload)
        self.url = "https://api.open-meteo.com/v1/forecast?fake"

    def json(self):
        return self._payload


class FakeSession:
    """Returns queued responses (or raises queued exceptions) in order."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_endpoint_switches_to_archive_for_old_dates():
    today = date(2026, 9, 30)
    assert choose_endpoint(date(2026, 9, 29), today) == FORECAST_URL
    assert choose_endpoint(date(2026, 1, 1), today) == ARCHIVE_URL


def test_params_request_single_utc_day():
    p = build_params(CITIES[0], TEST_DATE)
    assert p["start_date"] == p["end_date"] == "2026-09-28"
    assert p["timezone"] == "UTC"
    assert "temperature_2m" in p["hourly"]


def test_success_first_try():
    s = FakeSession([FakeResponse(200, make_payload())])
    url, payload = fetch_city_day(CITIES[0], TEST_DATE, session=s, backoff_seconds=0)
    assert "hourly" in payload and s.calls == 1


def test_retries_on_5xx_and_network_errors_then_succeeds():
    s = FakeSession([
        FakeResponse(503, {"error": True}),
        requests.ConnectionError("reset"),
        FakeResponse(200, make_payload()),
    ])
    _, payload = fetch_city_day(CITIES[0], TEST_DATE, session=s, backoff_seconds=0)
    assert s.calls == 3


def test_does_not_retry_on_400():
    s = FakeSession([FakeResponse(400, {"reason": "bad"}), FakeResponse(200, make_payload())])
    with pytest.raises(ExtractError, match="HTTP 400"):
        fetch_city_day(CITIES[0], TEST_DATE, session=s, backoff_seconds=0)
    assert s.calls == 1


def test_gives_up_after_max_retries():
    s = FakeSession([FakeResponse(500)] * 3)
    with pytest.raises(ExtractError, match="Giving up"):
        fetch_city_day(CITIES[0], TEST_DATE, session=s, backoff_seconds=0)
    assert s.calls == 3


def test_rejects_payload_without_hourly_block():
    s = FakeSession([FakeResponse(200, {"latitude": 1})])
    with pytest.raises(ExtractError, match="no 'hourly'"):
        fetch_city_day(CITIES[0], TEST_DATE, session=s, backoff_seconds=0)
