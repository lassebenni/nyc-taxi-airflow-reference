# tests/test_taxi_pipeline.py
from dags.taxi_pipeline import parquet_url_for


def test_parquet_url_january():
    url = parquet_url_for("2024-01-01")
    assert url == (
        "https://d37ci6vzurychx.cloudfront.net/"
        "trip-data/green_tripdata_2024-01.parquet"
    )


def test_parquet_url_end_of_month():
    # The logical date for a @monthly run is the first of the month,
    # but double-check that other days in the month also slice to the
    # right year-month prefix.
    url = parquet_url_for("2024-01-31")
    assert "2024-01.parquet" in url


def test_parquet_url_december_rollover():
    url = parquet_url_for("2023-12-01")
    assert "2023-12.parquet" in url
