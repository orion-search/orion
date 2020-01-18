from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from orion.core.airflow_utils import misctools
from orion.packages.utils.batches import split_batches
from orion.core.operators.mag_parse_task import MagParserOperator
from orion.core.operators.mag_process_titles_task import ProcessTitlesOperator
from orion.core.operators.mag_collect_task import MagCollectionOperator
from orion.core.operators.mag_geocode_task import GeocodingOperator
from orion.packages.utils.s3_utils import load_from_s3, s3_bucket_obj

default_args = {
    "owner": "Kostas St",
    "start_date": datetime(2019, 11, 22),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DAG_ID = "mag_collection"
DB_CONFIG = misctools.get_config("orion_config.config", "postgresdb")["database_uri"]
MAG_API_KEY = misctools.get_config("orion_config.config", "mag")["mag_api_key"]

# task 1
BATCH_SIZE = 100
S3_BUCKET = "processed-mag-expressions-batches"
PREFIX = "processed-titles"

# task 2
MAG_OUTPUT_BUCKET = "collected-mag-data"
parallel_tasks = 4

# task 3
google_key = misctools.get_config("orion_config.config", "google")["google_key"]

with DAG(
    dag_id=DAG_ID, default_args=default_args, schedule_interval=timedelta(days=365)
) as dag:

    dummy_task = DummyOperator(task_id="dummy_task")

    process_titles = ProcessTitlesOperator(
        task_id="process_titles",
        db_config=DB_CONFIG,
        entity_name="Ti",
        max_length=16000,
        s3_bucket=S3_BUCKET,
        prefix=PREFIX,
        batch_size=BATCH_SIZE,
    )

    batch_task = []
    for parallel_task in range(parallel_tasks):
        task_id = f"mag_collection_batch_{parallel_task}"
        batch_task.append(
            MagCollectionOperator(
                task_id=task_id,
                db_config=DB_CONFIG,
                input_bucket=S3_BUCKET,
                output_bucket=MAG_OUTPUT_BUCKET,
                prefix=f"{PREFIX}-{parallel_task}",
                batch_process=f"batch-process-{parallel_task}",
                subscription_key=MAG_API_KEY,
            )
        )

    parse_mag = MagParserOperator(
        task_id="parse_mag", s3_bucket=MAG_OUTPUT_BUCKET, db_config=DB_CONFIG
    )

    geocode_places = GeocodingOperator(task_id='geocode_places', db_config=DB_CONFIG, subscription_key=google_key)
    
    dummy_task >> process_titles >> batch_task >> parse_mag >> geocode_places
