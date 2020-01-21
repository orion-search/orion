from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from orion.core.airflow_utils import misctools
from orion.core.operators.mag_collect_task import MagFosCollectionOperator

default_args = {
    "owner": "Kostas St",
    "start_date": datetime(2019, 11, 22),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DAG_ID = "fos_collection"
DB_CONFIG = misctools.get_config("orion_config.config", "postgresdb")["database_uri"]
MAG_API_KEY = misctools.get_config("orion_config.config", "mag")["mag_api_key"]

with DAG(dag_id=DAG_ID, default_args=default_args, schedule_interval=timedelta(days=365)) as dag:
    dummy_task = DummyOperator(task_id='start')

    collect_fos = MagFosCollectionOperator(task_id='collect_fos_metadata', db_config=DB_CONFIG, subscription_key=MAG_API_KEY)

    dummy_task >> collect_fos
