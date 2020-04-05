"""
MagCollectionOperator: Queries Microsoft Academic Knowledge API and stores the responses on S3. 
The pickle file is a list of JSONs where every JSON object is the API response corresponsing to a paper. 
Every  pickle contains a maximum of 1,000 objects (that's the maximum number of 
papers we can retrieve from MAG with a query).

Example API response:
{'logprob': -24.006,
 'prob': 3.75255e-11,
 'Id': 2904236373,
 'Ti': 'conspiracy ideation and fake science news',
 'Pt': '0',
 'Y': 2018,
 'D': '2018-01-18',
 'CC': 0,
 'RId': [2067319876, 2130121899],
 'PB': 'OSF',
 'BT': 'a',
 'AA': [{'DAuN': 'Asheley R. Landrum',
   'AuId': 2226866834,
   'AfId': None,
   'S': 1},
  {'DAuN': 'Alex Olshansky', 'AuId': 2883323127, 'AfId': None, 'S': 2}],
 'F': [{'DFN': 'Science communication',
   'FN': 'science communication',
   'FId': 472806},
  {'DFN': 'Public relations', 'FN': 'public relations', 'FId': 39549134},
  {'DFN': 'Public awareness of science',
   'FN': 'public awareness of science',
   'FId': 176049440},
  {'DFN': 'Political science', 'FN': 'political science', 'FId': 17744445},
  {'DFN': 'Misinformation', 'FN': 'misinformation', 'FId': 2776990098},
  {'DFN': 'Ideation', 'FN': 'ideation', 'FId': 170477896},
  {'DFN': 'Fake news', 'FN': 'fake news', 'FId': 2779756789}]}

MagFosCollectionOperator: Fetches Fields of Study IDs from PostgreSQL and collects their level in the hierarchy, 
child and parent nodes from Microsoft Academic Graph.

"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.sql import exists
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import FieldOfStudy, FosHierarchy, FosMetadata
from orion.packages.mag.query_mag_api import (
    query_mag_api,
    query_fields_of_study,
    build_composite_expr,
)
from orion.packages.utils.s3_utils import store_on_s3


class MagCollectionOperator(BaseOperator):
    """Queries MAG API."""

    # template_fields = ['']

    @apply_defaults
    def __init__(
        self,
        subscription_key,
        output_bucket,
        query_values,
        entity_name,
        metadata,
        prod,
        with_doi,
        *args,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.metadata = metadata
        self.query_values = query_values
        self.entity_name = entity_name
        self.subscription_key = subscription_key
        self.output_bucket = output_bucket
        self.prod = prod
        self.with_doi = with_doi

    def execute(self, context):
        expression = build_composite_expr(self.query_values, self.entity_name)
        logging.info(f"{expression}")

        has_content = True
        i = 1
        offset = 0
        query_count = 1000
        # Request the API as long as we receive non-empty responses
        while has_content:
            logging.info(f"Query {i} - Offset {offset}...")

            data = query_mag_api(
                expression,
                self.metadata,
                self.subscription_key,
                query_count=query_count,
                offset=offset,
            )

            if self.with_doi:
                # Keep only papers with a DOI
                results = [ents for ents in data["entities"] if "DOI" in ents.keys()]
            else:
                results = [ents for ents in data["entities"]]

            filename = "-".join([self.output_bucket, str(i),])
            logging.info(f"File on s3: {filename}")

            store_on_s3(results, self.output_bucket, filename)
            logging.info(f"Number of stored results from query {i}: {len(results)}")

            i += 1
            offset += query_count

            if len(results) == 0:
                has_content = False

            if not self.prod:
                logging.info(f"Working in dev mode. {i}: {len(results[:10])}")
                store_on_s3(results[:10], self.output_bucket, filename)
                has_content = False


class MagFosCollectionOperator(BaseOperator):
    """Queries MAG API with Fields of Study to collect their level 
    in hierarchy, child and parent nodes."""

    @apply_defaults
    def __init__(self, db_config, subscription_key, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.subscription_key = subscription_key

    def execute(self, context):
        # Connect to PostgreSQL DB
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        # Fetch FoS IDs
        all_fos_ids = set([id_[0] for id_ in s.query(FieldOfStudy.id)])
        # Keep the FoS IDs that haven't been collected yet
        fields_of_study_ids = [
            id_[0]
            for id_ in s.query(FieldOfStudy.id).filter(
                ~exists().where(FieldOfStudy.id == FosMetadata.id)
            )
        ]
        logging.info(f"Fields of study left: {len(fields_of_study_ids)}")

        # Collect FoS metadata
        fos = query_fields_of_study(self.subscription_key, ids=fields_of_study_ids)

        # Parse api response
        for response in fos:
            s.add(
                FosMetadata(id=response["id"], level=response["level"], frequency=None)
            )

            # Keep only the child and parent IDs that exist in our DB
            if "child_ids" in response.keys():
                unique_child_ids = list(set(response["child_ids"]) & all_fos_ids)
            else:
                unique_child_ids = None

            if "parent_ids" in response.keys():
                unique_parent_ids = list(set(response["parent_ids"]) & all_fos_ids)
            else:
                unique_parent_ids = None

            s.add(
                FosHierarchy(
                    id=response["id"],
                    child_id=unique_child_ids,
                    parent_id=unique_parent_ids,
                )
            )

            # Commit all additions
            s.commit()
