"""
Filter topics so that they can be used in downstream tasks.
"""
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import numpy as np
from orion.core.orms.mag_orm import (
    FosMetadata,
    FilteredFos,
    Paper,
    PaperFieldsOfStudy,
    FosHierarchy,
)
from orion.packages.utils.s3_utils import store_on_s3, load_from_s3
from orion.packages.utils.utils import flatten_lists, get_all_children


class FilterTopicsByDistributionOperator(BaseOperator):
    """Filter topics by level and frequency."""

    @apply_defaults
    def __init__(
        self, db_config, s3_bucket, prefix, levels, percentiles, *args, **kwargs
    ):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.s3_bucket = s3_bucket
        self.prefix = prefix
        self.levels = levels
        self.percentiles = percentiles

    def execute(self, context):
        # Connect to postgresql db
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        # Fetch tables
        metadata = pd.read_sql(s.query(FosMetadata).statement, s.bind)
        logging.info(f"Number of FoS: {metadata.id.shape[0]}")

        d = {}
        for lvl, perc in zip(self.levels, self.percentiles):
            # Filter by level
            frame = metadata[metadata.level == lvl]
            # Find the percentile
            num = int(np.percentile(frame.frequency, perc))
            d[lvl] = list(frame[frame.frequency > num]["id"].values)

        # Store pickle on s3
        store_on_s3(d, self.s3_bucket, self.prefix)
        logging.info("Done :)")


class FilteredTopicsMetadataOperator(BaseOperator):
    """Creates a table with the filtered Fields of Study, their children, 
        annual citation sum and paper count."""

    @apply_defaults
    def __init__(self, db_config, s3_bucket, prefix, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.s3_bucket = s3_bucket
        self.prefix = prefix

    def execute(self, context):
        # Load topics
        topics = flatten_lists(list(load_from_s3(self.s3_bucket, self.prefix).values()))
        logging.info(f"Number of topics: {len(topics)}")

        # Connect to postgresql db
        engine = create_engine(self.db_config)
        FilteredFos.__table__.drop(engine, checkfirst=True)
        FilteredFos.__table__.create(engine, checkfirst=True)
        Session = sessionmaker(engine)
        s = Session()

        # Load all the tables needed for the metrics
        papers = pd.read_sql(s.query(Paper).statement, s.bind)
        paper_fos = pd.read_sql(s.query(PaperFieldsOfStudy).statement, s.bind)
        hierarchy = pd.read_sql(s.query(FosHierarchy).statement, s.bind)

        # Merge papers with fields of study, citations and publication year.
        papers = (
            papers[["id", "citations", "year"]]
            .merge(paper_fos, left_on="id", right_on="paper_id")
            .drop("id", axis=1)
        )
        logging.info("Merged tables.")

        # Traverse the FoS hierarchy tree and get all children
        d = {topic: get_all_children(hierarchy, topic) for topic in topics}
        logging.info(f"Got children of {len(d)} topics.")

        for fos_ids in d.values():
            logging.info(fos_ids[:10])
            logging.info(f"fos id: {fos_ids[0]}")
            g = (
                papers[papers.field_of_study_id.isin(fos_ids)]
                .drop_duplicates("paper_id")
                .groupby("year")
            )
            for year, paper_count, total_citations in zip(
                g.groups.keys(), g["paper_id"].count(), g["citations"].sum()
            ):
                s.add(
                    FilteredFos(
                        field_of_study_id=int(fos_ids[0]),
                        all_children=[int(f) for f in fos_ids],
                        year=year,
                        paper_count=int(paper_count),
                        total_citations=int(total_citations),
                    )
                )
                s.commit()
