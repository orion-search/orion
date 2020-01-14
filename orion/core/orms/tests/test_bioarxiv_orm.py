import pytest
import unittest
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from orion.core.orms.bioarxiv_orm import Base
from orion.core.airflow_utils import misctools


class TestMag(unittest.TestCase):
    """Check that the bioarXiv ORM works as expected"""

    db_config = misctools.get_config("orion_config.config", "postgresdb")["test_uri"]
    engine = create_engine(db_config)
    Session = sessionmaker(engine)

    def setUp(self):
        """Create the temporary table"""
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        """Drop the temporary table"""
        Base.metadata.drop_all(self.engine)

    def test_build(self):
        pass


if __name__ == "__main__":
    unittest.main()
