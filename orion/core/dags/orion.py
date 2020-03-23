from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from orion.packages.utils.s3_utils import create_s3_bucket
from orion.core.operators.mag_parse_task import MagParserOperator, FosFrequencyOperator
from orion.core.operators.draw_collaboration_graph_task import (
    CountryCollaborationOperator,
    CountrySimilarityOperator,
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
from orion.core.operators.calculate_metrics_task import (
    RCAOperator,
    ResearchDiversityOperator,
    GenderDiversityOperator,
)
from orion.core.operators.text2vec_task import Text2TfidfOperator
from orion.core.operators.dim_reduction_task import DimReductionOperator
from orion.core.operators.topic_filtering_task import (
    FilterTopicsByDistributionOperator,
    FilteredTopicsMetadataOperator,
)
from orion.core.operators.faiss_index_task import FaissIndexOperator
from orion.core.operators.create_viz_tables_task import CreateVizTables
from orion.core.operators.affiliation_type_task import AffiliationTypeOperator
from orion.core.operators.collect_wb_indicators_task import WBIndicatorOperator
from orion.core.operators.country_details_task import (
    HomogeniseCountryNamesOperator,
    CountryDetailsOperator,
)
from orion.packages.mag.create_tables import create_db_and_tables
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

default_args = {
    "owner": "Kostas St",
    "start_date": datetime(2020, 2, 2),
    "depends_on_past": False,
    "retries": 0,
}

DAG_ID = "orion"
db_name = orion.config["data"]["db_name"]
DB_CONFIG = os.getenv(db_name)
MAG_API_KEY = os.getenv("mag_api_key")

# query_mag
MAG_OUTPUT_BUCKET = orion.config["s3_buckets"]["mag"]
mag_config = orion.config["data"]["mag"]
query_values = mag_config["query_values"]
entity_name = mag_config["entity_name"]
metadata = mag_config["metadata"]
prod = orion.config["data"]["prod"]

# geocode_places
google_key = os.getenv("google_api_key")

# country_collaboration
collab_year = orion.config["country_collaboration"]["year"]

# batch_names
BATCH_SIZE = orion.config["batch_size"]
S3_BUCKET = orion.config["s3_buckets"]["gender"]
PREFIX = orion.config["prefix"]["gender"]

# gender_inference_N
parallel_tasks = orion.config["parallel_tasks"]
auth_token = os.getenv("gender_api")

# text2vector
text_vectors_prefix = orion.config["prefix"]["text_vectors"]
text_vectors_bucket = orion.config["s3_buckets"]["text_vectors"]

# dim_reduction
umap_config = orion.config["umap"]
# umap hyperparameters
n_neighbors = umap_config["n_neighbors"]
n_components = umap_config["n_components"]
metric = umap_config["metric"]
min_dist = umap_config["min_dist"]

# topic_filtering
topic_prefix = orion.config["prefix"]["topic"]
topic_bucket = orion.config["s3_buckets"]["topic"]
topic_config = orion.config["topic_filter"]
levels = topic_config["levels"]
percentiles = topic_config["percentiles"]

# metrics
thresh = orion.config["gender_diversity"]["threshold"]
paper_thresh_low = orion.config["metrics"]["paper_count_low"]
paper_thresh_high = orion.config["metrics"]["paper_count_high"]
year_thresh = orion.config["metrics"]["year"]
fos_thresh = orion.config["metrics"]["fos_count"]

# wb indicators
wb_country = orion.config["data"]["wb"]["country"]
wb_end_year = orion.config["data"]["wb"]["end_year"]
wb_indicators = orion.config["data"]["wb"]["indicators"]
wb_table_names = orion.config["data"]["wb"]["table_names"]

with DAG(
    dag_id=DAG_ID, default_args=default_args, schedule_interval=timedelta(days=365)
) as dag:

    dummy_task = DummyOperator(task_id="start")

    dummy_task_2 = DummyOperator(task_id="gender_agg")

    dummy_task_3 = DummyOperator(task_id="world_bank_indicators")

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_db_and_tables,
        op_kwargs={"db": db_name},
    )

    create_buckets = [
        PythonOperator(
            task_id=bucket,
            python_callable=create_s3_bucket,
            op_kwargs={"bucket": bucket},
        )
        for bucket in [MAG_OUTPUT_BUCKET, S3_BUCKET, topic_bucket, text_vectors_bucket]
    ]

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

    rca = RCAOperator(
        task_id="rca_measurement",
        db_config=DB_CONFIG,
        year_thresh=year_thresh,
        paper_thresh=paper_thresh_high,
    )

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
        task_id="country_collaboration", db_config=DB_CONFIG, year=collab_year
    )

    country_similarity = CountrySimilarityOperator(
        task_id="country_similarity",
        db_config=DB_CONFIG,
        year=collab_year,
        bucket=text_vectors_bucket,
        prefix=text_vectors_prefix,
    )

    topic_filtering = FilterTopicsByDistributionOperator(
        task_id="filter_topics",
        db_config=DB_CONFIG,
        s3_bucket=topic_bucket,
        prefix=topic_prefix,
        levels=levels,
        percentiles=percentiles,
    )

    filtered_topic_metadata = FilteredTopicsMetadataOperator(
        task_id="topic_metadata",
        db_config=DB_CONFIG,
        s3_bucket=topic_bucket,
        prefix=topic_prefix,
    )

    research_diversity = ResearchDiversityOperator(
        task_id="research_diversity",
        db_config=DB_CONFIG,
        fos_thresh=fos_thresh,
        year_thresh=year_thresh,
    )

    gender_diversity = GenderDiversityOperator(
        task_id="gender_diversity",
        db_config=DB_CONFIG,
        paper_thresh=paper_thresh_low,
        thresh=thresh,
    )

    faiss_index = FaissIndexOperator(
        task_id="faiss_index", bucket=text_vectors_bucket, prefix=text_vectors_prefix
    )

    viz_tables = CreateVizTables(task_id="viz_tables", db_config=DB_CONFIG)

    aff_types = AffiliationTypeOperator(task_id="affiliation_type", db_config=DB_CONFIG)

    batch_task_wb = []
    for wb_indicator, wb_table_name in zip(wb_indicators, wb_table_names):
        task_id = f"{wb_table_name}"
        batch_task_wb.append(
            WBIndicatorOperator(
                task_id=task_id,
                db_config=DB_CONFIG,
                indicator=wb_indicator,
                start_year=year_thresh,
                end_year=wb_end_year,
                country=wb_country,
                table_name=wb_table_name,
            )
        )

    country_association = HomogeniseCountryNamesOperator(
        task_id="homogenise_countries", db_config=DB_CONFIG
    )

    country_details = CountryDetailsOperator(
        task_id="country_details", db_config=DB_CONFIG
    )

    dummy_task >> create_tables >> query_mag >> parse_mag
    dummy_task >> create_buckets
    parse_mag >> geocode_places >> rca
    parse_mag >> geocode_places >> country_collaboration_graph
    parse_mag >> collect_fos >> fos_frequency >> topic_filtering >> filtered_topic_metadata >> viz_tables
    filtered_topic_metadata >> rca >> viz_tables
    filtered_topic_metadata >> research_diversity
    filtered_topic_metadata >> gender_diversity
    geocode_places >> research_diversity >> viz_tables
    geocode_places >> gender_diversity >> viz_tables
    geocode_places >> country_similarity
    geocode_places >> viz_tables
    text2vector >> country_similarity
    filtered_topic_metadata >> country_similarity
    parse_mag >> batch_names >> batch_task_gender >> dummy_task_2 >> gender_diversity
    parse_mag >> text2vector >> dim_reduction
    text2vector >> faiss_index
    parse_mag >> aff_types
    dummy_task >> create_tables >> dummy_task_3 >> batch_task_wb >> country_association
    geocode_places >> country_association >> country_details
