"""
Get expressions (ie processed paper titles) from S3 and query MAG API. The API will return matches to these titles which will be stored in a PostgreSQL DB.
"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import Paper
from orion.core.orms.bioarxiv_orm import Article
from orion.packages.mag.query_mag_api import query_mag_api
from orion.packages.utils.s3_utils import store_on_s3, load_from_s3

metadata = [
    "Id",
    "Ti",
    "AA.AfId",
    "AA.AfN",
    "AA.AuId",
    "AA.DAuN",
    "AA.S",
    "CC",
    "D",
    "F.DFN",
    "F.FId",
    "J.JId",
    "J.JN",
    "Pt",
    "RId",
    "Y",
    "DOI",
    "PB",
    "BT",
]


class MagCollectionOperator(BaseOperator):
    """Queries MAG API."""

    # template_fields = ['']

    @apply_defaults
    def __init__(
        self,
        db_config,
        input_bucket,
        output_bucket,
        prefix,
        batch_process,
        subscription_key,
        metadata=metadata,
        *args,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.metadata = metadata
        self.subscription_key = subscription_key
        self.batch_process = batch_process
        self.input_bucket = input_bucket
        self.output_bucket = output_bucket
        self.prefix = prefix

    def execute(self, context):
        # Connect to PostgreSQL DB
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        # Load processed queries from S3
        queries = load_from_s3(self.input_bucket, self.prefix)
        logging.info(f"Total number of queries: {len(queries)}")

        for i, query in enumerate(queries):
            logging.info(f"Query length: {len(query)}")
            data = query_mag_api(query, self.metadata, self.subscription_key)
            logging.info(f'Results: {len(data["entities"])}')

            # Keep only results with a DOI
            results = [ents for ents in data["entities"] if "DOI" in ents.keys()]
            logging.info(f"Results with DOI: {len(results)}")
            filename = "-".join(
                [
                    self.output_bucket,
                    "process",
                    self.prefix.split("-")[-1],
                    "batch",
                    str(i),
                ]
            )
            logging.info(f"File on s3: {filename}")

            store_on_s3(results, self.output_bucket, filename)
