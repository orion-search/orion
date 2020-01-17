from datetime import datetime, timedelta
from airflow import DAG
import orion
from airflow.operators.dummy_operator import DummyOperator
from orion.core.airflow_utils import misctools
from orion.core.operators.text2vec_task import Text2VectorOperator
from orion.core.operators.dim_reduction_task import DimReductionOperator

default_args = {
    "owner": "Kostas St",
    "start_date": datetime(2019, 11, 22),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DAG_ID = "text_embeddings"
DB_CONFIG = misctools.get_config("orion_config.config", "postgresdb")["database_uri"]
# rds_config = misctools.get_config("orion_config.config", "postgresdb")["rds"]
bucket = "document-vectors"
prefix = "doc_vectors"

# task 2
config = orion.config['umap']
# umap hyperparameters
n_neighbors = config['n_neighbors']
n_components = config['n_components']
metric = config['metric']
min_dist = config['min_dist']

with DAG(
    dag_id=DAG_ID, default_args=default_args, schedule_interval=timedelta(days=365)
) as dag:

    dummy_task = DummyOperator(task_id="start")

    text2vector = Text2VectorOperator(
        task_id="text2vector",
        db_config=DB_CONFIG,
        bucket=bucket,
        prefix=prefix,
    )

    dim_reduction = DimReductionOperator(task_id='dim_reduction', db_config=db_config, bucket=bucket, prefix=prefix,n_neighbors=n_neighbors, min_dist=min_dist, n_components=n_components, metric=metric)

    dummy_task >> text2vector >> dim_reduction
