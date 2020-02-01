from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from orion.core.airflow_utils import misctools
from orion.core.operators.mag_parse_task import MagParserOperator

# from orion.core.operators.mag_process_titles_task import ProcessTitlesOperator
# from orion.core.operators.mag_collect_task import MagCollectionOperator, MagFosCollectionOperator
# from orion.core.operators.mag_geocode_task import GeocodingOperator
from orion.core.operators.mag_collect_task_v2 import MagCollectionOperator
import orion

default_args = {
    "owner": "Kostas St",
    "start_date": datetime(2019, 11, 22),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DAG_ID = "mag_collection_v2"
DB_CONFIG = misctools.get_config("orion_config.config", "postgresdb")["orion"]
MAG_API_KEY = misctools.get_config("orion_config.config", "mag")["mag_api_key"]

mag_config = orion.config["data"]["mag"]

# # task 1
# BATCH_SIZE = 100
# S3_BUCKET = "processed-mag-expressions-batches"
# PREFIX = "processed-titles"

# task 2
MAG_OUTPUT_BUCKET = "mag-data-dump"
query_values = mag_config["query_values"]
entity_name = mag_config["entity_name"]
metadata = mag_config["metadata"]

# task 3
# google_key = misctools.get_config("orion_config.config", "google")["google_key"]

with DAG(
    dag_id=DAG_ID, default_args=default_args, schedule_interval=timedelta(days=365)
) as dag:

    dummy_task = DummyOperator(task_id="start")

    query_mag = MagCollectionOperator(
        task_id="query_mag",
        output_bucket=MAG_OUTPUT_BUCKET,
        subscription_key=MAG_API_KEY,
        query_values=query_values,
        entity_name=entity_name,
        metadata=metadata,
    )

    parse_mag = MagParserOperator(
        task_id="parse_mag", s3_bucket=MAG_OUTPUT_BUCKET, db_config=DB_CONFIG
    )

    # geocode_places = GeocodingOperator(task_id='geocode_places', db_config=DB_CONFIG, subscription_key=google_key)

    # collect_fos = MagFosCollectionOperator(task_id='collect_fos_metadata', db_config=DB_CONFIG, subscription_key=MAG_API_KEY)

    dummy_task >> query_mag >> parse_mag
    # >> geocode_places
    # parse_mag >> collect_fos
