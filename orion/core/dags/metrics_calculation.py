from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from orion.core.airflow_utils import misctools
from orion.core.operators.calculate_metrics_task import RCAOperator

default_args = {
    "owner": "Kostas St",
    "start_date": datetime(2019, 11, 22),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DAG_ID = "mag_metrics"
DB_CONFIG = misctools.get_config("orion_config.config", "postgresdb")["database_uri"]
# rds_config = misctools.get_config("orion_config.config", "postgresdb")["rds"]

# task 10
topic_bucket = "mag-topics"
topic_prefix = "filtered_topics"

with DAG(
    dag_id=DAG_ID, default_args=default_args, schedule_interval=timedelta(days=365)
) as dag:

    dummy_task = DummyOperator(task_id="start")

    rca = RCAOperator(task_id="rca_measurement", db_config=DB_CONFIG, s3_bucket=topic_bucket, prefix=topic_prefix,)

    dummy_task >> rca
