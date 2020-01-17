from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from orion.core.airflow_utils import misctools
from orion.core.operators.text2vec_task import Text2VectorOperator

default_args = {
    "owner": "Kostas St",
    "start_date": datetime(2019, 11, 22),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DAG_ID = "search_engine"
DB_CONFIG = misctools.get_config("orion_config.config", "postgresdb")["database_uri"]
bucket = "document-vectors"
prefix = "doc_vectors"


with DAG(
    dag_id=DAG_ID, default_args=default_args, schedule_interval=timedelta(days=365)
) as dag:

    dummy_task = DummyOperator(task_id="start")

    text2vector = Text2VectorOperator(
        task_id="text2vector",
        db_config=DB_CONFIG,
        bucket="document-vectors",
        prefix="doc_vectors",
    )

    dummy_task >> text2vector
