"""DAG integrity tests - skipped automatically if Airflow is not installed."""
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS_FOLDER = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_task_order(dagbag):
    dag = dagbag.get_dag("weather_etl")
    assert dag is not None
    order = ["setup", "extract", "transform_load", "data_quality"]
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert downstream in dag.get_task(upstream).downstream_task_ids


def test_retries_configured(dagbag):
    dag = dagbag.get_dag("weather_etl")
    assert dag.get_task("extract").retries == 3
    assert dag.get_task("data_quality").retries == 0  # DQ failures need a human
