"""
Get paper titles from a PostgreSQL DB and process them to be used as queries to Microsoft Academic Graph API. Queries are stored in S3.
"""
import logging
from sqlalchemy.sql import exists
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import Paper
from orion.core.orms.bioarxiv_orm import Article
from orion.packages.utils.batches import split_batches, put_s3_batch
from orion.packages.mag.query_mag_api import prepare_title, build_expr


class ProcessTitlesOperator(BaseOperator):
    """Preprocesses paper titles and transforms them to expressions which will be used to query Microsoft Academic Graph API (MAG)."""

    # template_fields = ['']

    @apply_defaults
    def __init__(
        self,
        db_config,
        entity_name,
        max_length,
        s3_bucket,
        prefix,
        batch_size,
        *args,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.entity_name = entity_name
        self.max_length = max_length
        self.s3_bucket = s3_bucket
        self.prefix = prefix
        self.batch_size = batch_size

    def execute(self, context):
        # Connect to PostgreSQL DB
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        # Get the titles of bioarXiv papers the DOI of which hasn't been collected from MAG yet.
        papers = s.query(Article.title).filter(
            ~exists().where(Paper.doi == Article.doi)
        )

        # Normalise paper titles.
        processed_titles = [prepare_title(paper[0]) for paper in papers]
        logging.info(f"Paper titles to be queried: {len(processed_titles)}")

        # Add multiple titles in a single expression to query MAG with.
        expressions = list(
            build_expr(processed_titles, self.entity_name, self.max_length)
        )
        logging.info(f"Built {len(expressions)}")

        for i, batch in enumerate(split_batches(expressions, self.batch_size)):
            put_s3_batch(batch, self.s3_bucket, "-".join([self.prefix, str(i)]))
