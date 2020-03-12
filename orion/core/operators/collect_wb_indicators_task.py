"""
Collects indicators from the World Bank. Currently, we collect indicators from the following URLs:
- http://datatopics.worldbank.org/world-development-indicators/themes/economy.html#featured-indicators_1
- http://datatopics.worldbank.org/world-development-indicators/themes/states-and-markets.html#featured-indicators_1
- http://datatopics.worldbank.org/world-development-indicators/themes/global-links.html#featured-indicators_1
- http://datatopics.worldbank.org/world-development-indicators/themes/people.html#featured-indicators_1
"""
from pandas_datareader import wb
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import (
    WorldBankFemaleLaborForce,
    WorldBankGovEducation,
    WorldBankResearchDevelopment,
    WorldBankGDP,
)


class WBIndicatorOperator(BaseOperator):
    """Fetches indicators from the World Bank."""

    @apply_defaults
    def __init__(
        self,
        db_config,
        table_name,
        indicator,
        start_year,
        end_year,
        country,
        *args,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.indicator = indicator
        self.start_year = start_year
        self.end_year = end_year
        self.country = country
        self.table_name = table_name
        self.tables = {
            "wb_gdp": WorldBankGDP,
            "wb_edu_expenditure": WorldBankGovEducation,
            "wb_rnd_expenditure": WorldBankResearchDevelopment,
            "wb_female_workforce": WorldBankFemaleLaborForce,
        }

    def execute(self, context):
        # Connect to postgresql db
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        # Fetch WB Indicator
        ind = wb.download(
            indicator=self.indicator,
            country=self.country,
            start=self.start_year,
            end=self.end_year,
        )

        # Store in DB
        for (area, year), row in ind.iterrows():
            s.add(
                self.tables[self.table_name](
                    country=area, year=year, indicator=row[self.indicator]
                )
            )
            s.commit()
