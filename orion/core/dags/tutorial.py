import torch
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from orion.packages.utils.s3_utils import create_s3_bucket
from orion.core.operators.mag_parse_task import MagParserOperator, FosFrequencyOperator
from orion.core.operators.mag_collect_task import (
    MagCollectionOperator,
    MagFosCollectionOperator,
)
import orion
from orion.core.operators.topic_filtering_task import (
    FilterTopicsByDistributionOperator,
    FilteredTopicsMetadataOperator,
)
from orion.core.operators.affiliation_type_task import AffiliationTypeOperator
from orion.core.operators.collect_wb_indicators_task import WBIndicatorOperator
from orion.packages.mag.create_tables import create_db_and_tables
from orion.core.operators.open_access_journals_task import OpenAccessJournalOperator
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

default_args = {
    "owner": "Orion",
    "start_date": datetime(2020, 2, 2),
    "depends_on_past": False,
    "retries": 0,
}

DAG_ID = "tutorial"
db_name = orion.config["data"]["db_name"]
DB_CONFIG = os.getenv(db_name)
MAG_API_KEY = os.getenv("mag_api_key")
MAG_OUTPUT_BUCKET = orion.config["s3_buckets"]["mag"]
mag_config = orion.config["data"]["mag"]
query_values = mag_config["query_values"]
entity_name = mag_config["entity_name"]
metadata = mag_config["metadata"]
with_doi = mag_config["with_doi"]
mag_start_date = mag_config["mag_start_date"]
mag_end_date = mag_config["mag_end_date"]
intervals_in_a_year = mag_config["intervals_in_a_year"]

# topic_filtering
topic_prefix = orion.config["prefix"]["topic"]
topic_bucket = orion.config["s3_buckets"]["topic"]
topic_config = orion.config["topic_filter"]
levels = topic_config["levels"]
percentiles = topic_config["percentiles"]

# wb indicators
wb_country = orion.config["data"]["wb"]["country"]
wb_end_year = orion.config["data"]["wb"]["end_year"]
wb_indicators = orion.config["data"]["wb"]["indicators"]
wb_table_names = orion.config["data"]["wb"]["table_names"]
year_thresh = orion.config["metrics"]["year"]

with DAG(
    dag_id=DAG_ID, default_args=default_args, schedule_interval=timedelta(days=365)
) as dag:

    dummy_task = DummyOperator(task_id="start")

    dummy_task_3 = DummyOperator(task_id="world_bank_indicators")

    dummy_task_4 = DummyOperator(task_id="create_s3_buckets")

    dummy_task_5 = DummyOperator(task_id="s3_buckets")

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
        for bucket in [MAG_OUTPUT_BUCKET, topic_bucket]
    ]

    query_mag = MagCollectionOperator(
        task_id="query_mag",
        output_bucket=MAG_OUTPUT_BUCKET,
        subscription_key=MAG_API_KEY,
        query_values=query_values,
        entity_name=entity_name,
        metadata=metadata,
        with_doi=with_doi,
        mag_start_date=mag_start_date,
        mag_end_date=mag_end_date,
        intervals_in_a_year=intervals_in_a_year,
    )

    parse_mag = MagParserOperator(
        task_id="parse_mag", s3_bucket=MAG_OUTPUT_BUCKET, db_config=DB_CONFIG
    )

    collect_fos = MagFosCollectionOperator(
        task_id="collect_fos_metadata",
        db_config=DB_CONFIG,
        subscription_key=MAG_API_KEY,
    )

    fos_frequency = FosFrequencyOperator(task_id="fos_frequency", db_config=DB_CONFIG)

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

    open_access = OpenAccessJournalOperator(task_id="open_access", db_config=DB_CONFIG)

    dummy_task >> create_tables >> query_mag >> parse_mag
    dummy_task >> dummy_task_4 >> create_buckets >> dummy_task_5 >> query_mag
    parse_mag >> collect_fos >> fos_frequency >> topic_filtering >> filtered_topic_metadata
    parse_mag >> aff_types
    parse_mag >> open_access
    dummy_task >> create_tables >> dummy_task_3 >> batch_task_wb
