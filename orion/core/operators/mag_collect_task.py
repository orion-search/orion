"""
MagCollectionOperator: Get expressions (ie processed paper titles) from S3 and query MAG API. The API will return matches to these titles which will be stored in a PostgreSQL DB.

MagFosCollectionOperator: Get the IDs from the FieldOfStudy table and collect their level in the hierarchy, child and parent nodes (only if they're in tje FieldOfStudy table).
"""
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import exists
from itertools import repeat
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import Paper, FieldOfStudy, PaperFieldsOfStudy, FosHierarchy, FosMetadata
from orion.core.orms.bioarxiv_orm import Article
from orion.packages.mag.query_mag_api import query_mag_api, query_fields_of_study
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


class MagFosCollectionOperator(BaseOperator):
    """Queries MAG API with Fields of Study to collect their level 
    in hierarchy, child and parent nodes."""

    @apply_defaults
    def __init__(self, db_config, subscription_key, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.subscription_key = subscription_key

    def execute(self, context):
        # Connect to PostgreSQL DB
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        # Fetch FoS IDs
        all_fos_ids = set([id_[0] for id_ in s.query(FieldOfStudy.id)])
        # Keep the FoS IDs that haven't been collected yet
        fields_of_study_ids = [
            id_[0]
            for id_ in s.query(FieldOfStudy.id).filter(
                ~exists().where(FieldOfStudy.id == FosMetadata.id)
            )
        ]
        logging.info(f"Fields of study left: {len(fields_of_study_ids)}")

        # Collect FoS metadata
        fos = query_fields_of_study(self.subscription_key, ids=fields_of_study_ids)

        # Parse api response
        for response in fos:
            s.add(FosMetadata(id=response["id"], level=response["level"], frequency=None))

            # Keep only the child and parent IDs that exist in our DB
            if "child_ids" in response.keys():
                unique_child_ids = list(set(response["child_ids"]) & all_fos_ids)
            else:
                unique_child_ids = None

            if "parent_ids" in response.keys():
                unique_parent_ids = list(set(response["parent_ids"]) & all_fos_ids)
            else:
                unique_parent_ids = None

            s.add(
                FosHierarchy(
                    id=response["id"],
                    child_id=unique_child_ids,
                    parent_id=unique_parent_ids,
                )
            )

            # Commit all additions
            s.commit()
