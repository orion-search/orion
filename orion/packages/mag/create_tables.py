import logging
import psycopg2
from orion.core.airflow_utils import misctools
from sqlalchemy import create_engine, exc
from orion.core.orms.mag_orm import Base


def create_db_and_tables(db):
    # Try to create the database if it doesn't already exist.
    try:
        db_config = misctools.get_config("orion_config.config", "postgresdb")[
            "orion_test"
        ]
        engine = create_engine(db_config)
        conn = engine.connect()
        conn.execute("commit")
        conn.execute(f"create database {db}")
        conn.close()
    except exc.DBAPIError as e:
        if isinstance(e.orig, psycopg2.errors.DuplicateDatabase):
            logging.info(e)
        else:
            logging.error(e)
            raise

    db_config = misctools.get_config("orion_config.config", "postgresdb")[db]
    engine = create_engine(db_config)
    Base.metadata.create_all(engine)
