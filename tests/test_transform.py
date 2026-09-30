from datetime import UTC

from conftest import TEST_DATE, make_payload

from etl.transform import transform_payload


def test_returns_24_rows_for_requested_day_only(payload):
    rows = transform_payload("LON", payload, TEST_DATE)
    assert len(rows) == 24  # 25th hour belongs to the next day
    assert all(r.observed_date_utc == TEST_DATE for r in rows)
    assert rows[0].observed_at_utc.tzinfo == UTC


def test_rows_are_sorted_and_typed(payload):
    rows = transform_payload("LON", payload, TEST_DATE)
    assert [r.observed_at_utc for r in rows] == sorted(r.observed_at_utc for r in rows)
    assert isinstance(rows[0].relative_humidity, int)
    assert isinstance(rows[0].temperature_c, float)


def test_out_of_range_values_are_nulled():
    p = make_payload()
    p["hourly"]["temperature_2m"][3] = 999.0
    p["hourly"]["relative_humidity_2m"][4] = -5
    rows = transform_payload("LON", p, TEST_DATE)
    assert rows[3].temperature_c is None
    assert rows[4].relative_humidity is None


def test_missing_and_garbage_values_become_null():
    p = make_payload()
    p["hourly"]["wind_speed_10m"][0] = None
    p["hourly"]["precipitation"][1] = "n/a"
    del p["hourly"]["weather_code"]
    rows = transform_payload("LON", p, TEST_DATE)
    assert rows[0].wind_speed_kmh is None
    assert rows[1].precipitation_mm is None
    assert all(r.weather_code is None for r in rows)


def test_duplicate_timestamps_are_deduplicated():
    p = make_payload()
    h = p["hourly"]
    for key in h:
        h[key].append(h[key][5])  # repeat hour 05:00
    rows = transform_payload("LON", p, TEST_DATE)
    assert len(rows) == 24
