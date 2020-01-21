from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from orion.core.airflow_utils import misctools
from orion.core.operators.mag_geocode_task import GeocodingOperator

default_args = {
    "owner": "Kostas St",
    "start_date": datetime(2019, 11, 22),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DAG_ID = "geocoding_places"
DB_CONFIG = misctools.get_config("orion_config.config", "postgresdb")["database_uri"]

google_key = misctools.get_config("orion_config.config", "google")["google_key"]

with DAG(dag_id=DAG_ID, default_args=default_args, schedule_interval=timedelta(days=365)) as dag:
    dummy_task = DummyOperator(task_id='dummy_task')

    geocode_places = GeocodingOperator(task_id='geocoding_places', db_config=DB_CONFIG, subscription_key=google_key)

    dummy_task >> geocode_places
