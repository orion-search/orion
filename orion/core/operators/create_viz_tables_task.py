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
from sqlalchemy import create_engine, func, distinct, and_
from sqlalchemy.orm import sessionmaker
from orion.packages.utils.s3_utils import store_on_s3
from sqlalchemy.orm.exc import NoResultFound
from collections import defaultdict
import pyarrow as pa
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
    PaperTopicsGrouped,
    CountryTopicOutputsMetrics,
    BlobArrow,
    DocVector,
)


class CreateVizTables(BaseOperator):
    """Creates tables used in visualisation."""

    @apply_defaults
    def __init__(self, db_config, thresh_year="2012", *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.thresh_year = thresh_year

    def _filter_query(self, res, year, country, name):
        return res.filter(
            and_(
                ResearchDiversityCountry.year == year,
                ResearchDiversityCountry.entity == country,
                FieldOfStudy.name == name,
            )
        ).one()

    def execute(self, context):
        # Connect to postgresql db
        engine = create_engine(self.db_config, pool_pre_ping=True)
        # Drop and recreate the tables to update the front-end tables
        AllMetrics.__table__.drop(engine, checkfirst=True)
        AllMetrics.__table__.create(engine, checkfirst=True)
        CountryTopicOutputsMetrics.__table__.drop(engine, checkfirst=True)
        CountryTopicOutputsMetrics.__table__.create(engine, checkfirst=True)
        PaperCountry.__table__.drop(engine, checkfirst=True)
        PaperCountry.__table__.create(engine, checkfirst=True)
        PaperYear.__table__.drop(engine, checkfirst=True)
        PaperYear.__table__.create(engine, checkfirst=True)
        PaperTopics.__table__.drop(engine, checkfirst=True)
        PaperTopics.__table__.create(engine, checkfirst=True)
        PaperTopicsGrouped.__table__.drop(engine, checkfirst=True)
        PaperTopicsGrouped.__table__.create(engine, checkfirst=True)
        Session = sessionmaker(engine)
        s = Session()

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
            .filter(ResearchDiversityCountry.entity != "NaN")
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
                aff_location[aff_location.country != ""].dropna(subset=["country"])[
                    ["country", "affiliation_id"]
                ],
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
                try:
                    # logging.info(country)
                    s.add(
                        CountryTopicOutputsMetrics(
                            field_of_study_id=int(row["field_of_study_id"]),
                            country=country,
                            year=year,
                            paper_count=int(paper_count),
                            total_citations=int(total_citations),
                            name=row["name"],
                            shannon_diversity=self._filter_query(
                                res, year, country, row["name"]
                            ).shannon_diversity,
                            female_share=self._filter_query(
                                res, year, country, row["name"]
                            ).female_share,
                            rca_sum=self._filter_query(
                                res, year, country, row["name"]
                            ).rca_sum,
                        )
                    )
                    s.commit()
                except NoResultFound as e:
                    # logging.info(f'No result for : {year, country, row["name"]}')
                    continue
        logging.info("Stored CountryTopicOutput table!")

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
            if r.country is not None and r.country is not "":
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
                    count=len(
                        set(
                            paper_fos[
                                paper_fos.field_of_study_id.isin(row["all_children"])
                            ]["paper_id"]
                        )
                    ),
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
            if r.year > self.thresh_year:
                s.add(PaperYear(year=r.year, count=r.count, paper_ids=r.paper_ids))
                s.commit()

        logging.info("Stored PaperYear table!")

        # PaperTopicsGrouped table
        g = paper_fos.groupby("paper_id")["field_of_study_id"].apply(list)
        d = defaultdict(list)
        filtered_fos = filtered_fos.merge(
            fos, left_on="field_of_study_id", right_on="id"
        ).drop_duplicates("field_of_study_id")

        for paper_id, fos_lst in g.iteritems():
            for _, row in filtered_fos.iterrows():
                if len(set(fos_lst).intersection(set(row["all_children"]))) > 0:
                    d[paper_id].append(row["name"])

        store_on_s3(d, "document-vectors", "paper_topics")

        for k, v in d.items():
            s.add(PaperTopicsGrouped(id=k, field_of_study=v))
            s.commit()

        logging.info("Stored PaperTopicsGrouped table!")


class Pandas2Arrow(BaseOperator):
    @apply_defaults
    def __init__(self, db_config, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config

    def unroll_df(self, df, fields):
        """Transform a pandas dataframe to long format."""
        lst = []
        for _, row in df.iterrows():
            for elem in row[fields[1]]:
                lst.append(tuple((row[fields[0]], elem)))
        return pd.DataFrame(lst, columns=[fields[0], fields[1]])

    def execute(self, context):
        # Connect to postgresql
        engine = create_engine(self.db_config, pool_pre_ping=True)
        Session = sessionmaker(engine)
        s = Session()

        # Delete rows in table
        s.query(BlobArrow).delete()
        s.commit()

        # Read tables from db
        paper_topics_grouped = pd.read_sql(
            s.query(PaperTopicsGrouped).statement, s.bind
        )
        paper_country = pd.read_sql(s.query(PaperCountry).statement, s.bind)
        vecs = pd.read_sql(s.query(DocVector).statement, s.bind)
        papers = pd.read_sql(s.query(Paper).statement, s.bind)

        # Unroll tables to long format
        unrolled_paper_topics_grouped = self.unroll_df(
            paper_topics_grouped, ["id", "field_of_study"]
        )
        unrolled_paper_country = self.unroll_df(paper_country, ["country", "paper_ids"])

        # Merge all tables and keep relevant columns
        data = (
            vecs.merge(papers[["id", "year"]], left_on="id", right_on="id")
            .merge(unrolled_paper_topics_grouped, left_on="id", right_on="id")
            .merge(unrolled_paper_country, left_on="id", right_on="paper_ids")[
                ["id", "vector_3d", "citations", "year", "field_of_study", "country"]
            ]
        )

        # Pandas to arrow
        table = pa.Table.from_pandas(data, preserve_index=False)
        logging.info(f"Table schema:{table.schema}")

        # Serialise and compress the arrow table
        serialised_table = pa.serialize(table).to_buffer()
        compressed_table = pa.compress(serialised_table, codec="gzip", asbytes=True)
        size = serialised_table.size
        logging.info(f"Compressed table size: {len(compressed_table)}")

        # Commit to DB
        s.add(BlobArrow(blob=compressed_table, size=size))
        s.commit()
