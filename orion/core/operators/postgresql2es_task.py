"""
Migrates data from PostgreSQL to Elasticsearch.

Postgreqsl2ElasticSearchOperator: Creates an index that contains 
the following data for every paper:
- original_title
- abstract
- citations
- publication date
- publication year
- field_of_study_id
- field_of_study name
- author name
- author affiliation

Users have the option to delete the index before uploading documents. The task also
checks if the index exists before creating it.
"""
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import (
    Paper,
    PaperFieldsOfStudy,
    FieldOfStudy,
    AuthorAffiliation,
    Author,
    Affiliation,
)
from elasticsearch_dsl import Index, connections
from orion.core.orms.es_mapping import PaperES
from elasticsearch import helpers, Elasticsearch
from orion.packages.utils.utils import aws_es_client


class Postgreqsl2ElasticSearchOperator(BaseOperator):
    """Migrate data from PostgreSQL to Elastic Search."""

    @apply_defaults
    def __init__(
        self,
        db_config,
        es_index,
        es_host,
        es_port,
        region,
        erase_es_index,
        *args,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.es_index = es_index
        self.es_host = es_host
        self.es_port = es_port
        self.region = region
        self.erase_es_index = erase_es_index

    def execute(self, context):
        # Connect to postgresql db
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        # Read MAG data
        mag = pd.read_sql(s.query(Paper).statement, s.bind)

        # Read Fields of study and merge them with papers
        fos = pd.read_sql(s.query(FieldOfStudy).statement, s.bind)
        pfos = pd.read_sql(s.query(PaperFieldsOfStudy).statement, s.bind)
        pfos = pfos.merge(fos, left_on="field_of_study_id", right_on="id")
        mag = mag.merge(
            pfos[["paper_id", "field_of_study_id", "name"]],
            left_on="id",
            right_on="paper_id",
        )

        author_aff = pd.read_sql(s.query(AuthorAffiliation).statement, s.bind)
        author = pd.read_sql(s.query(Author).statement, s.bind)
        affiliation = pd.read_sql(s.query(Affiliation).statement, s.bind)
        author_aff = (
            author_aff.merge(author, left_on="author_id", right_on="id")
            .merge(affiliation, how="left", left_on="affiliation_id", right_on="id")[
                ["affiliation", "name", "paper_id"]
            ]
            .fillna("")
        )
        author_aff = pd.DataFrame(
            author_aff.groupby("paper_id")["name"].apply(list)
        ).merge(
            pd.DataFrame(author_aff.groupby("paper_id")["affiliation"].apply(list)),
            left_index=True,
            right_index=True,
        )

        # Groupby fos name and fos id and merge them in a table
        fos_names = pd.DataFrame(
            mag.groupby(
                ["paper_id", "year", "date", "original_title", "abstract", "citations"]
            )["name"].apply(list)
        )
        fos_ids = pd.DataFrame(
            mag.groupby(
                ["paper_id", "year", "date", "original_title", "abstract", "citations"]
            )["field_of_study_id"].apply(list)
        )

        table = (
            fos_names.merge(fos_ids, left_index=True, right_index=True)
            .merge(author_aff, how="left", left_index=True, right_index=True)
            .rename(
                index=str, columns={"name_x": "field_of_study", "name_y": "author_name"}
            )
        )

        # Setup ES connection
        es = aws_es_client(self.es_host, self.es_port, self.region)

        # Delete index if needed (usually not)
        if self.erase_es_index:
            Index(self.es_index, using=es).delete()
            logging.info(f"Deleted ES index: {self.es_index}")

        # Create the index if it does not exist
        if not Index(self.es_index, using=es).exists():
            PaperES.init(using=es)
            logging.info(f"Created ES index: {self.es_index}")

        def _docs_for_load(table):
            """Indexes documents in bulk."""
            for (
                (paper_id, year, date, title, abstract, citations),
                row,
            ) in table.iterrows():
                yield PaperES(
                    meta={"id": paper_id},
                    year=datetime.strptime(date, "%Y-%m-%d").date().year,
                    publication_date=datetime.strptime(date, "%Y-%m-%d").date(),
                    original_title=title,
                    abstract=abstract,
                    citations=citations,
                    fields_of_study=[
                        {"name": name, "id": id_}
                        for name, id_ in zip(
                            row["field_of_study"], row["field_of_study_id"]
                        )
                    ],
                    author=[
                        {"name": name, "affiliation": aff}
                        for name, aff in zip(row["author_name"], row["affiliation"])
                    ],
                ).to_dict(include_meta=True)
        # Increase timeout from 10 to 180 
        helpers.bulk(es, _docs_for_load(table), request_timeout=180)
