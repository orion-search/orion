"""
Creates the following tables that are used in the front-end:
- CountryTopicOutput: Shows a country's total citations and paper volume by year and topic.
- AllMetrics: Combines all the metrics (gender diversity, research diversity, RCA) we've derived by year and topic.
- PaperCountry: Shows the paper IDs of a country. Used in the particle visualisation. 
- PaperTopics:  Shows the paper IDs of a topic. Used in the particle visualisation.
- PaperYear:  Shows the paper IDs of a year. Used in the particle visualisation.

Note: Topics are fetched from the FilteredFos table.
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, func, distinct
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import (
    FilteredFos,
    Paper,
    PaperFieldsOfStudy,
    AffiliationLocation,
    AuthorAffiliation,
    FieldOfStudy,
    AllMetrics,
    GenderDiversityCountry,
    ResearchDiversityCountry,
    MetricCountryRCA,
    PaperCountry,
    PaperTopics,
    PaperYear,
    CountryTopicOutput,
)


class CreateVizTables(BaseOperator):
    """Creates tables used in visualisation."""

    @apply_defaults
    def __init__(self, db_config, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config

    def execute(self, context):
        # Connect to postgresql db
        engine = create_engine(self.db_config)
        # Drop and recreate the tables to update the front-end tables
        AllMetrics.__table__.drop(engine, checkfirst=True)
        AllMetrics.__table__.create(engine, checkfirst=True)
        CountryTopicOutput.__table__.drop(engine, checkfirst=True)
        CountryTopicOutput.__table__.create(engine, checkfirst=True)
        PaperCountry.__table__.drop(engine, checkfirst=True)
        PaperCountry.__table__.create(engine, checkfirst=True)
        PaperYear.__table__.drop(engine, checkfirst=True)
        PaperYear.__table__.create(engine, checkfirst=True)
        PaperTopics.__table__.drop(engine, checkfirst=True)
        PaperTopics.__table__.create(engine, checkfirst=True)
        Session = sessionmaker(engine)
        s = Session()

        # Load all the tables needed
        papers = pd.read_sql(s.query(Paper).statement, s.bind)
        paper_fos = pd.read_sql(s.query(PaperFieldsOfStudy).statement, s.bind)
        aff_location = pd.read_sql(s.query(AffiliationLocation).statement, s.bind)
        author_aff = pd.read_sql(s.query(AuthorAffiliation).statement, s.bind)
        filtered_fos = pd.read_sql(s.query(FilteredFos).statement, s.bind)
        fos = pd.read_sql(s.query(FieldOfStudy).statement, s.bind)

        # CountryTopicOutput table
        df = (
            papers[["id", "citations", "year"]]
            .merge(paper_fos, left_on="id", right_on="paper_id")
            .drop("id", axis=1)
            .merge(
                author_aff[["affiliation_id", "author_id", "paper_id"]],
                left_on="paper_id",
                right_on="paper_id",
            )
            .merge(
                aff_location[["country", "affiliation_id"]],
                left_on="affiliation_id",
                right_on="affiliation_id",
            )
        ).drop_duplicates(["paper_id", "affiliation_id", "field_of_study_id"])

        # Aggregate on topic level
        for _, row in (
            filtered_fos.merge(fos, left_on="field_of_study_id", right_on="id")
            .drop_duplicates("field_of_study_id")
            .iterrows()
        ):
            # logging.info(f"fos id: {row['field_of_study_id']}")
            g = (
                df[df.field_of_study_id.isin(row["all_children"])]
                .drop_duplicates("paper_id")
                .groupby(["country", "year"])
            )
            for (country, year), paper_count, total_citations in zip(
                g.groups.keys(), g["paper_id"].count(), g["citations"].sum()
            ):
                s.add(
                    CountryTopicOutput(
                        field_of_study_id=int(row["field_of_study_id"]),
                        country=country,
                        year=year,
                        paper_count=int(paper_count),
                        total_citations=int(total_citations),
                        name=row["name"],
                    )
                )
                s.commit()
        logging.info("Stored CountryTopicOutput table!")

        # AllMetrics table
        res = (
            s.query(
                ResearchDiversityCountry.year,
                ResearchDiversityCountry.entity,
                ResearchDiversityCountry.shannon_diversity,
                ResearchDiversityCountry.field_of_study_id,
                MetricCountryRCA.rca_sum,
                GenderDiversityCountry.female_share,
                FieldOfStudy.name,
            )
            .join(
                MetricCountryRCA,
                (ResearchDiversityCountry.year == MetricCountryRCA.year)
                & (
                    ResearchDiversityCountry.field_of_study_id
                    == MetricCountryRCA.field_of_study_id
                )
                & (ResearchDiversityCountry.entity == MetricCountryRCA.entity),
            )
            .join(
                GenderDiversityCountry,
                (MetricCountryRCA.year == GenderDiversityCountry.year)
                & (
                    MetricCountryRCA.field_of_study_id
                    == GenderDiversityCountry.field_of_study_id
                )
                & (MetricCountryRCA.entity == GenderDiversityCountry.entity),
            )
            .join(FieldOfStudy, (MetricCountryRCA.field_of_study_id == FieldOfStudy.id))
        )

        # Store results in a new table
        for r in res:
            s.add(
                AllMetrics(
                    year=r.year,
                    country=r.entity,
                    shannon_diversity=r.shannon_diversity,
                    field_of_study_id=r.field_of_study_id,
                    name=r.name,
                    rca_sum=r.rca_sum,
                    female_share=r.female_share,
                )
            )
            s.commit()
        logging.info("Stored AllMetrics table!")

        # PaperCountry table
        res = (
            s.query(
                AffiliationLocation.country,
                func.count(distinct(AuthorAffiliation.paper_id)).label("count"),
                func.array_agg(distinct(AuthorAffiliation.paper_id)).label("paper_ids"),
            )
            .join(
                AuthorAffiliation,
                (
                    AffiliationLocation.affiliation_id
                    == AuthorAffiliation.affiliation_id
                ),
            )
            .group_by(AffiliationLocation.country)
        )

        # Store results in a new table
        for r in res:
            if r.country is not None:
                s.add(
                    PaperCountry(
                        country=r.country, count=r.count, paper_ids=r.paper_ids
                    )
                )
                s.commit()
        logging.info("Stored PaperCountry table!")

        # PaperTopics table
        for _, row in (
            filtered_fos.merge(fos, left_on="field_of_study_id", right_on="id")
            .drop_duplicates("field_of_study_id")
            .iterrows()
        ):
            # logging.info(f"fos id: {row['field_of_study_id']}")
            s.add(
                PaperTopics(
                    field_of_study_id=int(row["field_of_study_id"]),
                    name=row["name"],
                    paper_ids=set(
                        paper_fos[
                            paper_fos.field_of_study_id.isin(row["all_children"])
                        ]["paper_id"]
                    ),
                )
            )
            s.commit()
        logging.info("Stored PaperTopics table!")

        # PaperYear table
        res = s.query(
            Paper.year,
            func.count(Paper.id).label("count"),
            func.array_agg(Paper.id).label("paper_ids"),
        ).group_by(Paper.year)

        for r in res:
            s.add(PaperYear(year=r.year, count=r.count, paper_ids=r.paper_ids))
            s.commit()
        logging.info("Stored PaperYear table!")
