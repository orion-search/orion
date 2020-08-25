import logging
import psycopg2
from sqlalchemy import create_engine, exc
from orion.core.orms.mag_orm import Base
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())


def create_db_and_tables(db):
    # Try to create the database if it doesn't already exist.
    try:
        db_config = os.getenv("postgres")
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

    db_config = os.getenv(db)
    engine = create_engine(db_config)
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    create_db_and_tables("test_db")
