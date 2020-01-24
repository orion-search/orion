from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from orion.core.airflow_utils import misctools
from orion.packages.utils.batches import split_batches
from orion.core.operators.infer_gender_task import (
    NamesBatchesOperator,
    GenderInferenceOperator,
)
from orion.packages.utils.s3_utils import load_from_s3, s3_bucket_obj

default_args = {
    "owner": "Kostas St",
    "start_date": datetime(2019, 11, 22),
    "depends_on_past": False,
    "retries": 0,
}

DAG_ID = "infer_gender"
DB_CONFIG = misctools.get_config("orion_config.config", "postgresdb")["rds"]

# task 1
BATCH_SIZE = 85000
S3_BUCKET = "names-batches"
PREFIX = "batched_names"

# task 2
parallel_tasks = 3
auth_token = misctools.get_config("orion_config.config", "genderapi")["auth"]

with DAG(
    dag_id=DAG_ID, default_args=default_args, schedule_interval=timedelta(days=365)
) as dag:

    dummy_task = DummyOperator(task_id="start")

    batch_names = NamesBatchesOperator(
        task_id="batch_names",
        db_config=DB_CONFIG,
        s3_bucket=S3_BUCKET,
        prefix=PREFIX,
        batch_size=BATCH_SIZE,
    )

    batch_task_gender = []
    for parallel_task in range(parallel_tasks):
        task_id = f"name_inference_{parallel_task}"
        batch_task_gender.append(
            GenderInferenceOperator(
                task_id=task_id,
                db_config=DB_CONFIG,
                s3_bucket=S3_BUCKET,
                prefix=f"{PREFIX}_{parallel_task}",
                auth_token=auth_token,
            )
        )

    dummy_task >> batch_names >> batch_task_gender
