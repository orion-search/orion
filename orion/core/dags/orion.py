from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from orion.core.airflow_utils import misctools
from orion.core.operators.mag_parse_task import MagParserOperator, FosFrequencyOperator
from orion.core.operators.draw_collaboration_graph_task import (
    CountryCollaborationOperator,
)
from orion.core.operators.mag_geocode_task import GeocodingOperator
from orion.core.operators.mag_collect_task import (
    MagCollectionOperator,
    MagFosCollectionOperator,
)
import orion
from orion.core.operators.infer_gender_task import (
    NamesBatchesOperator,
    GenderInferenceOperator,
)
from orion.core.operators.calculate_metrics_task import RCAOperator
from orion.core.operators.text2vec_task import Text2TfidfOperator
from orion.core.operators.dim_reduction_task import DimReductionOperator

default_args = {
    "owner": "Kostas St",
    "start_date": datetime(2020, 2, 2),
    "depends_on_past": False,
    "retries": 0,
}

DAG_ID = "orion"
DB_CONFIG = misctools.get_config("orion_config.config", "postgresdb")["orion_prod"]
MAG_API_KEY = misctools.get_config("orion_config.config", "mag")["mag_api_key"]

# task 1:
MAG_OUTPUT_BUCKET = "mag-data-dump"
mag_config = orion.config["data"]["mag"]
query_values = mag_config["query_values"]
entity_name = mag_config["entity_name"]
metadata = mag_config["metadata"]
# prod = orion.config["data"]["prod"]
prod = True

# task 3: geocode places
google_key = misctools.get_config("orion_config.config", "google")["google_key"]

# task 6: batch names
BATCH_SIZE = 80000
S3_BUCKET = "names-batches"
PREFIX = "batched_names"

# task 7: infer gender
parallel_tasks = 4
auth_token = misctools.get_config("orion_config.config", "genderapi")["auth"]

# task 8: text embeddings
text_vectors_prefix = "doc_vectors"
text_vectors_bucket = "document-vectors"

# task 9: dimensionality reduction
config = orion.config["umap"]
# umap hyperparameters
n_neighbors = config["n_neighbors"]
n_components = config["n_components"]
metric = config["metric"]
min_dist = config["min_dist"]


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
        prod=prod,
    )

    parse_mag = MagParserOperator(
        task_id="parse_mag", s3_bucket=MAG_OUTPUT_BUCKET, db_config=DB_CONFIG
    )

    geocode_places = GeocodingOperator(
        task_id="geocode_places", db_config=DB_CONFIG, subscription_key=google_key
    )

    collect_fos = MagFosCollectionOperator(
        task_id="collect_fos_metadata",
        db_config=DB_CONFIG,
        subscription_key=MAG_API_KEY,
    )

    fos_frequency = FosFrequencyOperator(task_id="fos_frequency", db_config=DB_CONFIG)

    batch_names = NamesBatchesOperator(
        task_id="batch_names",
        db_config=DB_CONFIG,
        s3_bucket=S3_BUCKET,
        prefix=PREFIX,
        batch_size=BATCH_SIZE,
    )

    batch_task_gender = []
    for parallel_task in range(parallel_tasks):
        task_id = f"gender_inference_{parallel_task}"
        batch_task_gender.append(
            GenderInferenceOperator(
                task_id=task_id,
                db_config=DB_CONFIG,
                s3_bucket=S3_BUCKET,
                prefix=f"{PREFIX}_{parallel_task}",
                auth_token=auth_token,
            )
        )

    rca = RCAOperator(task_id="rca_measurement", db_config=DB_CONFIG)

    text2vector = Text2TfidfOperator(
        task_id="text2vector",
        db_config=DB_CONFIG,
        bucket=text_vectors_bucket,
        prefix=text_vectors_prefix,
    )

    dim_reduction = DimReductionOperator(
        task_id="dim_reduction",
        db_config=DB_CONFIG,
        bucket=text_vectors_bucket,
        prefix=text_vectors_prefix,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        n_components=n_components,
        metric=metric,
    )

    country_collaboration_graph = CountryCollaborationOperator(
        task_id="country_collaboration", db_config=DB_CONFIG
    )

    dummy_task >> query_mag >> parse_mag
    parse_mag >> geocode_places >> rca
    parse_mag >> geocode_places >> country_collaboration_graph
    parse_mag >> collect_fos >> fos_frequency
    parse_mag >> batch_names >> batch_task_gender
    parse_mag >> text2vector >> dim_reduction
