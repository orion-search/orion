"""
HomogeniseCountryNamesOperator: Homogenises country names from Google Places API 
and the World Bank. It uses a country mapping dictionary from `model_config.yaml`.

CountryDetailsOperator: Fetches additional country details from the restcountries API.
This includes things such as population, capital, region (continent), sub-region and 
country codes.

"""
import logging
from sqlalchemy import create_engine, distinct
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import (
    WorldBankFemaleLaborForce,
    WorldBankGovEducation,
    WorldBankResearchDevelopment,
    WorldBankGDP,
    CountryAssociation,
    AffiliationLocation,
    CountryDetails,
)
import orion
import requests
from orion.packages.geo.enrich_countries import (
    parse_country_details,
    get_country_details,
)

google2wb = orion.config["country_name_mapping"]["google2wb"]
google2restcountries = orion.config["country_name_mapping"]["google2restcountries"]


class HomogeniseCountryNamesOperator(BaseOperator):
    """Homogenises country names from Google Places API and the World Bank."""

    @apply_defaults
    def __init__(self, db_config, country_name_mapping=google2wb, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.country_name_mapping = google2wb

    def execute(self, context):
        # Connect to postgresdb
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        s.query(CountryAssociation).delete()
        s.commit()

        # Country names from Google Places API
        country_names = [
            country_name[0]
            for country_name in s.query(distinct(AffiliationLocation.country)).filter(
                (AffiliationLocation.country != None),
                (AffiliationLocation.country != ""),
            )
        ]
        logging.info(f"Countries from Google: {len(country_names)}")

        # Country names from the World Bank
        wb_countries = set()
        for c1, c2, c3, c4 in zip(
            s.query(WorldBankGDP.country),
            s.query(WorldBankGovEducation.country),
            s.query(WorldBankFemaleLaborForce.country),
            s.query(WorldBankResearchDevelopment.country),
        ):
            wb_countries.update(c1, c2, c3, c4)

        # Match country names
        for country_name in country_names:
            if country_name in self.country_name_mapping.keys():
                s.add(
                    CountryAssociation(
                        google_country=country_name,
                        wb_country=self.country_name_mapping[country_name],
                    )
                )
            else:
                s.add(
                    CountryAssociation(
                        google_country=country_name, wb_country=country_name
                    )
                )
            s.commit()


class CountryDetailsOperator(BaseOperator):
    """Fetch country information from restcountries."""

    @apply_defaults
    def __init__(
        self, db_config, country_name_mapping=google2restcountries, *args, **kwargs
    ):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.country_name_mapping = country_name_mapping

    def execute(self, context):
        # Connect to postgresdb
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        s.query(CountryDetails).delete()
        s.commit()

        # Query restcountries API with Google Places country names.
        d = {}
        for country_name in [
            country_name[0]
            for country_name in s.query(CountryAssociation.google_country)
        ]:
            try:
                d[country_name] = get_country_details(country_name)
            except requests.exceptions.HTTPError as h:
                logging.info(f"{country_name} - {h}: Trying with country_mapping")
                try:
                    d[country_name] = get_country_details(
                        self.country_name_mapping[country_name]
                    )
                except requests.exceptions.HTTPError as h:
                    logging.info(f"Failed: {country_name}")
                    continue
                except KeyError as e:
                    logging.info(f"{country_name} not in mapping.")
                    continue
        # Parse country info
        country_info = []
        for k, v in d.items():
            # These countries are not the first match so we choose `pos=1`
            if k in ["India", "United States", "Sudan"]:
                parsed_response = parse_country_details(v, pos=1)
            else:
                parsed_response = parse_country_details(v)

            parsed_response.update({"google_name": k})
            parsed_response.update(
                {
                    "wb_name": s.query(CountryAssociation.wb_country)
                    .filter(CountryAssociation.google_country == k)
                    .first()[0]
                }
            )
            country_info.append(parsed_response)
        logging.info(f"Parsed countries: {len(country_info)}")

        s.bulk_insert_mappings(CountryDetails, country_info)
        s.commit()
