import pytest
import unittest
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from orion.core.orms.mag_orm import Base
from dotenv import load_dotenv, find_dotenv
import os
load_dotenv(find_dotenv())

class TestMag(unittest.TestCase):
    """Check that the MAG ORM works as expected"""

    db_config = os.getenv('orion_test')
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
