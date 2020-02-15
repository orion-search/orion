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
from orion.core.orms.mag_orm import FosMetadata
from orion.packages.utils.s3_utils import store_on_s3


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
        logging.info(f'Number of FoS: {metadata.id.shape[0]}')

        d = {}
        for lvl, perc in zip(self.levels, self.percentiles):
            # Filter by level
            frame = metadata[metadata.level == lvl]
            # Find the percentile
            num = int(np.percentile(frame.frequency, perc))
            d[lvl] = list(frame[frame.frequency > num]["id"].values)

        # Store pickle on s3
        store_on_s3(d, self.s3_bucket, self.prefix)
        logging.info('Done :)')
