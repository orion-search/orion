"""
CountryCollaborationOperator: Draws a collaboration graph between countries based on
the author affiliations. Papers are filtered by their publication year.

CountrySimilarityOperator: Finds the similarity between countries based on their abstracts.
It averages the abstract vectors of a country to create a country vector.
Uses the text vectors that were calculated from the text2vector task. It filters papers 
by publication year and users can choose the number of similar countries to return.

"""
import logging
import pandas as pd
import numpy as np
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
    FilteredFos,
    CountrySimilarity,
    PaperFieldsOfStudy,
    HighDimDocVector,
)
from orion.packages.projection.faiss_index import faiss_index


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


class CountrySimilarityOperator(BaseOperator):
    """Find the semantic similarity between abstracts."""

    @apply_defaults
    def __init__(self, db_config, year, k=5, thresh=2, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.year = year
        self.k = k
        self.thresh = thresh

    def execute(self, context):
        # Connect to postgresql
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        # Drop and recreate the country similarity table to update the metric
        CountrySimilarity.__table__.drop(engine, checkfirst=True)
        CountrySimilarity.__table__.create(engine, checkfirst=True)

        # Load all the tables needed for the metrics
        papers = pd.read_sql(s.query(Paper).statement, s.bind)
        aff_location = pd.read_sql(s.query(AffiliationLocation).statement, s.bind)
        author_aff = pd.read_sql(s.query(AuthorAffiliation).statement, s.bind)
        paper_fos = pd.read_sql(s.query(PaperFieldsOfStudy).statement, s.bind)
        filtered_fos = pd.read_sql(s.query(FilteredFos).statement, s.bind)
        vectors = pd.read_sql(
            s.query(HighDimDocVector.vector, HighDimDocVector.id).statement, s.bind
        )
        vectors["vector"] = vectors.vector.apply(np.array)

        # dict(topic id, all children)
        d = {}
        for _, row in filtered_fos.drop_duplicates("field_of_study_id").iterrows():
            d[row["field_of_study_id"]] = row["all_children"]

        for parent, children in d.items():
            logging.info(f"Parent ID: {parent} - Number of children: {len(children)}")
            # Merge tables for a particular "discipline" (level 1 FoS and its children)
            df = (
                aff_location[aff_location.country != ""][["affiliation_id", "country"]]
                .merge(author_aff, left_on="affiliation_id", right_on="affiliation_id")
                .merge(
                    papers[["id", "year", "citations"]],
                    left_on="paper_id",
                    right_on="id",
                )
                .merge(vectors, left_on="paper_id", right_on="id")
                .merge(
                    paper_fos[paper_fos["field_of_study_id"].isin(children)],
                    left_on="paper_id",
                    right_on="paper_id",
                )[
                    [
                        "affiliation_id",
                        "field_of_study_id",
                        "country",
                        "paper_id",
                        "citations",
                        "year",
                        "vector",
                    ]
                ]
            )
            # Filter country/year pairs based on paper frequency
            filter_ = (
                df.drop_duplicates(["country", "paper_id", "year"])
                .groupby(["year", "country"])["paper_id"]
                .count()
            )
            logging.info(f"Remaining country/year pairs: {filter_.shape}")

            # Group and drop countries with less than N papers
            grouped = (
                df.drop_duplicates(["country", "paper_id", "year"])
                .groupby(["year", "country"])["vector"]
                .apply(lambda x: np.mean(x, axis=0))
                .loc[filter_.where(filter_ > self.thresh).dropna().index]
            )

            # Find similar countries on annual basis
            for year in set([tup[0] for tup in grouped.index if tup[0] > self.year]):
                v = np.array([v for v in grouped.loc[year]]).astype("float32")
                ids = range(len(grouped.loc[year].index))
                logging.info(f"Vectors shape: {v.shape}")
                # Check that we have at least more than 5 countries in that year and topic
                if v.shape[0] > self.k:
                    # Create FAISS index
                    index = faiss_index(v, ids)
                    # Find similar countries for each country
                    for vector, country in zip(v, grouped.loc[year].index):
                        D, I = index.search(np.array([vector]), self.k + 1)
                        for i, (idx, similarity) in enumerate(zip(I[0][1:], D[0][1:])):

                            s.add(
                                CountrySimilarity(
                                    country_a=country,
                                    country_b=grouped.loc[year].index[idx],
                                    closeness=float(similarity),
                                    year=year,
                                    field_of_study_id=parent,
                                )
                            )
                            s.commit()
                        # logging.info(f"Stored in DB for {country} - {year}")
                else:
                    continue
