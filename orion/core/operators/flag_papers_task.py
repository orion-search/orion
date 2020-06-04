"""
FlagPapersOperator: Splits papers to AI, CI, AI+CI based on their Fields of Study.

"""
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import PaperFieldsOfStudy, FieldOfStudy, PaperFlag
from orion.packages.utils.utils import allocate_in_group
import orion

AI = orion.config['paper_flags']['ai_fos']
CI = orion.config['paper_flags']['ci_fos']

class FlagPapersOperator(BaseOperator):
    """Flag a paper as AI, CI or AI+CI."""

    @apply_defaults
    def __init__(self, db_config, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config

    def execute(self, context):
        # Connect to postgresql db
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        # Fetch postgres tables
        fos = pd.read_sql(s.query(FieldOfStudy).statement, s.bind)
        pfos = pd.read_sql(s.query(PaperFieldsOfStudy).statement, s.bind)

        # Merge and groupby so that FoS are in a list
        pfos = pfos.merge(fos, left_on="field_of_study_id", right_on="id")
        pfos = pd.DataFrame(pfos.groupby("paper_id")["name"].apply(list))

        # Match ci, ai, ai_ci FoS in the list
        pfos["type"] = pfos.name.apply(allocate_in_group, args=(CI, AI))

        logging.info(f"AI papers: {pfos[pfos['type']=='ai'].shape[0]}")
        logging.info(f"CI papers: {pfos[pfos['type']=='ci'].shape[0]}")
        logging.info(f"AI/CI papers: {pfos[pfos['type']=='ai_ci'].shape[0]}")

        for idx, row in pfos.iterrows():
            s.add(PaperFlag(id=idx, type=row["type"]))
            s.commit()
