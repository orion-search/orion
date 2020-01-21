"""
Read university and company names from a database, geocode them and collect additional details using Google Places API, parse the response and store it in PostgreSQL.
"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.sql import exists
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.packages.geo.geocode import place_by_id, place_by_name, parse_response
from orion.core.orms.mag_orm import Affiliation, AffiliationLocation


class GeocodingOperator(BaseOperator):
    """Find a place's details given its name."""

    # template_fields = ['']
    @apply_defaults
    def __init__(self, db_config, subscription_key, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.subscription_key = subscription_key

    def execute(self, context):
        # Connect to postgresql db
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        # Fetch affiliations that have not been geocoded yet.
        queries = s.query(Affiliation.id, Affiliation.affiliation).filter(
            ~exists().where(Affiliation.id == AffiliationLocation.affiliation_id)
        )
        logging.info(f"Number of queries: {queries.count()}")

        for id, name in queries:
            logging.info(name)
            try:
                r = place_by_name(name, self.subscription_key)
            except IndexError as e:
                logging.info(e)
                continue
            response = place_by_id(r, self.subscription_key)
            place_details = parse_response(response)
            place_details.update({"affiliation_id": id})
            s.add(AffiliationLocation(**place_details))
            s.commit()
