"""
Split the affiliations to industry and non-industry by using a seed list of tokens. 
The seed list can be found in `model_config.yaml`
"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import Affiliation, AffiliationType
import orion

seed_list = orion.config["affiliations"]["non_profit"]


class AffiliationTypeOperator(BaseOperator):
    """Find the type (industry, non-industry) of an affiliation."""

    @apply_defaults
    def __init__(self, db_config, seed_list=seed_list, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.seed_list = seed_list

    def _find_academic_affiliations(self, name):
        if any(val in name for val in self.seed_list):
            return 1
        else:
            return 0

    def execute(self, context):
        # Connect to postgresql db
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        # Get affiliation names and IDs
        aff_types = [
            {"id": aff.id, "type": self._find_academic_affiliations(aff.affiliation)}
            for aff in s.query(Affiliation).all()
        ]
        logging.info(f"Mapped {len(aff_types)} affiliations.")

        # Store affiliation types
        s.bulk_insert_mappings(AffiliationType, aff_types)
        s.commit()
