"""
Infer the gender of a person by using their name. I am using GenderAPI for this, which is supposed to be one of the most reliable name to gender inference services. 

Disclaimer: Gender is not binary and the gender that has been assigned to a person by using this system might not be the one that person identifies with. Please treat the results with caution.
"""
import logging
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
        authors = s.query(Author.id, Author.name).filter(
            ~exists().where(Author.id == AuthorGender.id)
        )
        logging.info(f"Total authors: {authors.count()}")

        # Remove authors without a first name
        authors = [tup for tup in authors if clean_name(tup[1])]
        logging.info(f"Authors passed to GenderAPI: {len(authors)}")

        for i, batch in enumerate(split_batches(authors, self.batch_size)):
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

        # Load queries from S3
        queries = load_from_s3(self.s3_bucket, self.prefix)

        # Filter authors that already exist in the DB
        # This is mainly to catch existing keys after task failures
        queries = [tup for tup in queries if tup[0] not in ids]
        logging.info(f"Total number of queries: {len(queries)}")

        for i, (id_, name) in enumerate(queries, start=1):
            logging.info(f"Query {name}: {i} / {len(queries)}")
            data = query_gender_api(name, self.auth_token)
            if data is not None:
                data = parse_response(id_, name, data)

                # Inserting to DB
                s.add(AuthorGender(**data))
                s.commit()
            else:
                continue
        logging.info("Done! :)")
