"""
NamesBatchesOperator: Fetches full names from PostgreSQL, removes those with just an initial
and stores them as batches on S3. 

GenderInferenceOperator: Infers the gender of a person by using their name. I am using GenderAPI for this,
which is supposed to be one of the most reliable name to gender inference services. 

"""
import logging
import pandas as pd
from sqlalchemy.sql import exists
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.packages.utils.batches import split_batches, put_s3_batch
from orion.packages.utils.s3_utils import load_from_s3
from orion.packages.gender.query_gender_api import query_gender_api, parse_response
from orion.core.orms.mag_orm import Author, AuthorGender
from orion.packages.utils.nlp_utils import clean_name
from botocore.exceptions import ClientError
import toolz


class NamesBatchesOperator(BaseOperator):
    """Creates batches for parallel processing."""

    @apply_defaults
    def __init__(self, db_config, s3_bucket, prefix, batch_size, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.s3_bucket = s3_bucket
        self.prefix = prefix
        self.batch_size = batch_size

    def execute(self, context):
        # Connect to PostgreSQL DB
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        # Get author names if they haven't had their name inferred yet.
        authors = pd.read_sql(
            s.query(Author.id, Author.name)
            .filter(~exists().where(Author.id == AuthorGender.id))
            .statement,
            s.bind,
        )

        # Process their name and drop missing values
        authors["proc_name"] = authors.name.apply(clean_name)
        authors = authors.dropna()

        # Group author IDs by full name
        grouped_ids = authors.groupby("proc_name")["id"].apply(list)
        logging.info(f"Authors passed to GenderAPI: {grouped_ids.shape[0]}")

        # Store (full names, IDs[]) batches on S3
        for i, batch in enumerate(
            split_batches(grouped_ids.to_dict().items(), self.batch_size)
        ):
            put_s3_batch(batch, self.s3_bucket, "_".join([self.prefix, str(i)]))


class GenderInferenceOperator(BaseOperator):
    """Infers gender by name using GenderAPI."""

    @apply_defaults
    def __init__(self, db_config, s3_bucket, prefix, auth_token, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.s3_bucket = s3_bucket
        self.prefix = prefix
        self.auth_token = auth_token

    def execute(self, context):
        # Connect to PostgreSQL DB
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        # # Fetch all collected author IDs
        collected_full_names = set(
            [full_name[0] for full_name in s.query(AuthorGender.full_name)]
        )
        try:
            # Load queries from S3
            queries = load_from_s3(self.s3_bucket, self.prefix)

            # Convert queries to dict
            queries = {tup[0]: tup[1] for tup in queries}

            # Filter authors that already exist in the DB
            # This is mainly to catch existing keys after task failures or re-runs
            queries = {
                k: v for k, v in queries.items() if k not in collected_full_names
            }
            logging.info(f"Total number of queries: {len(queries.keys())}")

            i = 1
            # Bulk query GenderAPI
            for chunk in toolz.partition_all(100, queries.keys()):
                logging.info(f"Chunk: {i}, count: {len(chunk)}")
                results = query_gender_api(chunk, self.auth_token)

                # Parse response
                parsed_responses = []
                for result in [result for result in results if result]:
                    if result["result_found"]:
                        parsed_response = parse_response(result)
                        for id_ in queries[parsed_response["full_name"]]:
                            # Add author id in the response object
                            parsed_response_copy = parsed_response.copy()
                            parsed_response_copy.update({"id": id_})
                            parsed_responses.append(parsed_response_copy)

                # Insert bulk
                s.bulk_insert_mappings(AuthorGender, parsed_responses)
                s.commit()
                i += 1
            logging.info("Done! :)")
        except ClientError as err:
            logging.info(err)
            pass
