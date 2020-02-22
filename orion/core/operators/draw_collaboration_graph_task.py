"""
Draw a collaboration graph between countries and between institutions.
"""
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.packages.utils.utils import cooccurrence_graph
from orion.core.orms.mag_orm import (
    AuthorAffiliation,
    AffiliationLocation,
    CountryCollaboration,
    Paper,
)


class CountryCollaborationOperator(BaseOperator):
    """Create a cooccurrence graph of country-level collaboration."""

    @apply_defaults
    def __init__(self, db_config, year, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.year = year

    def execute(self, context):
        # Connect to postgresql db
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        # Load all the tables needed for the collaboration graph.
        aff_location = pd.read_sql(s.query(AffiliationLocation).statement, s.bind)
        author_aff = pd.read_sql(s.query(AuthorAffiliation).statement, s.bind)
        papers = pd.read_sql(s.query(Paper).statement, s.bind)

        # Merge tables
        df = (
            aff_location[["affiliation_id", "country"]]
            .merge(author_aff, left_on="affiliation_id", right_on="affiliation_id")
            .merge(papers[["id", "year"]], left_on="paper_id", right_on="id")
        )

        # Group countries by paper, remove duplicates and missing entries.
        for year in [year for year in sorted(df.year.unique()) if year > self.year]:
            logging.info(f"Collaboration network for year: {year}")
            grouped_df = (
                df[(df.country != "") & (df.year == year)][["country", "paper_id"]]
                .dropna()
                .groupby("paper_id")["country"]
                .apply(set)
            )
            logging.info(f"Grouped DF shape: {grouped_df.shape}")
            graph = cooccurrence_graph(grouped_df)
            for k, v in graph.items():
                s.add(
                    CountryCollaboration(
                        **{
                            "country_a": k[0],
                            "country_b": k[1],
                            "weight": v,
                            "year": year,
                        }
                    )
                )
                s.commit()

        logging.info("Done :)")
