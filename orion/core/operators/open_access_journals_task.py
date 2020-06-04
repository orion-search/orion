"""
OpenAccessJournalOperator: Splits the journals to non-open access (= 0) and open access (= 1) 
using a seed list of tokens. The seed list can be found in `model_config.yaml`.

"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import Journal, OpenAccess
import orion

seed_list = orion.config["open_access"]


class OpenAccessJournalOperator(BaseOperator):
    """Flag a journal as open access or not."""

    @apply_defaults
    def __init__(self, db_config, seed_list=seed_list, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.seed_list = seed_list

    def _is_open_access(self, name):
        if name in set(seed_list):
            return 1
        else:
            return 0

    def execute(self, context):
        # Connect to postgresql db
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        s.query(OpenAccess).delete()
        s.commit()

        # Get journal names and IDs
        journal_access = [
            {"id": id, "open_access": self._is_open_access(journal_name)}
            for (id, journal_name) in s.query(Journal.id, Journal.journal_name)
            .distinct()
            .all()
        ]

        logging.info(f"{len(journal_access)}")

        # Store journal types
        s.bulk_insert_mappings(OpenAccess, journal_access)
        s.commit()
