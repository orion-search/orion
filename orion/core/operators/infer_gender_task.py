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

        # Fetch all collected author IDs
        ids = set([id[0] for id in s.query(AuthorGender.id)])

        try:
            # Load queries from S3
            queries = load_from_s3(self.s3_bucket, self.prefix)

            # Filter authors that already exist in the DB
            # This is mainly to catch existing keys after task failures
            queries = [tup for tup in queries if tup[0] not in ids]
            logging.info(f"Total number of queries: {len(queries)}")

            for i, (name, ids) in enumerate(queries, start=1):
                logging.info(f"Query {name}: {i} / {len(queries)}")
                data = query_gender_api(name, self.auth_token)
                if data is not None:
                    for id_ in ids:
                        data = parse_response(id_, name, data)

                        # Inserting to DB
                        s.add(AuthorGender(**data))
                        s.commit()
                else:
                    continue
            logging.info("Done! :)")
        except ClientError as err:
            logging.info(err)
            pass
