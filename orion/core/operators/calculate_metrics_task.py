"""
Calculate all the metrics (RCA, research diversity, gender diversity).
"""
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import exists
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.packages.metrics.metrics import calculate_rca_by_sum, calculate_rca_by_count
from orion.core.orms.mag_orm import (
    Paper,
    PaperAuthor,
    Affiliation,
    AuthorAffiliation,
    AffiliationLocation,
    PaperFieldsOfStudy,
    MetricAffiliationRCA,
    MetricCountryRCA,
)
from orion.packages.utils.utils import flatten_lists, dict2psql_format


class RCAOperator(BaseOperator):
    """Calculate RCA for institutions and countries."""

    @apply_defaults
    def __init__(self, db_config, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config

    def execute(self, context):
        # Connect to postgresql db
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        # Load all the tables needed for the metrics
        papers = pd.read_sql(s.query(Paper).statement, s.bind)
        aff_location = pd.read_sql(s.query(AffiliationLocation).statement, s.bind)
        author_aff = pd.read_sql(s.query(AuthorAffiliation).statement, s.bind)
        paper_author = pd.read_sql(s.query(PaperAuthor).statement, s.bind)
        paper_fos = pd.read_sql(s.query(PaperFieldsOfStudy).statement, s.bind)

        # Merge tables
        df = (
            aff_location[["affiliation_id", "country"]]
            .merge(author_aff, left_on="affiliation_id", right_on="affiliation_id")
            .merge(
                paper_author[["paper_id", "author_id"]],
                left_on="author_id",
                right_on="author_id",
            )
            .merge(
                papers[["id", "year", "citations"]], left_on="paper_id", right_on="id"
            )
            .merge(paper_fos, left_on="paper_id", right_on="paper_id")[
                [
                    "affiliation_id",
                    "field_of_study_id",
                    "country",
                    "paper_id",
                    "citations",
                    "year",
                ]
            ]
        )
        logging.info(f"Overall DF shape: {df.shape}")

        # RCA by summing citations - country level
        country_level = df.drop_duplicates(
            subset=["field_of_study_id", "country", "paper_id"]
        )
        logging.info(f"DF country_level shape: {country_level.shape}")

        d = {}
        for fos in country_level.field_of_study_id.unique()[:10]:
            d[fos] = calculate_rca_by_sum(
                country_level, entity_column="country", commodity=fos, value="citations"
            )

        rca_country_level_sum = dict2psql_format(d)

        s.bulk_insert_mappings(MetricCountryRCA, rca_country_level_sum)

        # RCA by summing citations - affiliation level
        affiliation_level = df.drop_duplicates(
            subset=["field_of_study_id", "affiliation_id", "paper_id"]
        )
        logging.info(f"DF affiliation_level shape: {affiliation_level.shape}")

        d = {}
        for fos in affiliation_level.field_of_study_id.unique()[:10]:
            d[fos] = calculate_rca_by_sum(
                affiliation_level,
                entity_column="affiliation_id",
                commodity=fos,
                value="citations",
            )

        rca_affiliation_level_sum = dict2psql_format(d)
        s.bulk_insert_mappings(MetricAffiliationRCA, rca_affiliation_level_sum)
        s.commit()
