"""
Migrates data from PostgreSQL to Elasticsearch.

Postgreqsl2ElasticSearchOperator: Creates an "mag_papers" index that contains 
the following data for every paper:
- original_title
- abstract
- publication date
- publication year
- field_of_study_id
- field_of_study name

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
from orion.core.orms.mag_orm import Paper, PaperFieldsOfStudy, FieldOfStudy
from elasticsearch_dsl import Index, connections
from orion.core.orms.es_mapping import PaperES
from elasticsearch import helpers, Elasticsearch


class Postgreqsl2ElasticSearchOperator(BaseOperator):
    """Migrate data from PostgreSQL to Elastic Search."""

    @apply_defaults
    def __init__(self, db_config, es_index, es_host, erase_es_index, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.es_index = es_index
        self.es_host = es_host
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

        # Groupby fos name and fos id and merge them in a table
        fos_names = pd.DataFrame(
            mag.groupby(["paper_id", "year", "date", "original_title", "abstract"])[
                "name"
            ].apply(list)
        )
        fos_ids = pd.DataFrame(
            mag.groupby(["paper_id", "year", "date", "original_title", "abstract"])[
                "field_of_study_id"
            ].apply(list)
        )
        table = fos_names.merge(fos_ids, left_index=True, right_index=True)

        # Setup ES connection
        connections.create_connection(hosts=[self.es_host])

        # Delete index if needed (usually not)
        if self.erase_es_index:
            Index(self.es_index).delete()

        # Create the index if it does not exist
        if not Index(self.es_index).exists():
            PaperES.init()

        def _docs_for_load(table):
            for (paper_id, year, date, title, abstract), row in table.iterrows():
                yield PaperES(
                    meta={"id": paper_id},
                    year=datetime.strptime(date, "%Y-%m-%d").date().year,
                    publication_date=datetime.strptime(date, "%Y-%m-%d").date(),
                    original_title=title,
                    abstract=abstract,
                    fields_of_study=row["name"],
                    field_of_study_id=row["field_of_study_id"],
                ).to_dict(include_meta=True)

        helpers.bulk(Elasticsearch(), _docs_for_load(table))
