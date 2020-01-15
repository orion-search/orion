"""
Transform a variable length text to a fixed-length vector. We use pretrained models from
the transformers library to create word vectors which are then averaged to produce a 
document vector.
"""
import logging
from sqlalchemy import create_engine, and_
from sqlalchemy.sql import exists
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.packages.nlp.text2vec import encode_text, feature_extraction, average_vectors
from orion.core.orms.bioarxiv_orm import Article
from orion.core.orms.mag_orm import Paper, DocVector


class Text2VectorOperator(BaseOperator):
    """Transforms text to document embeddings.
    
    TODO: 
    * Collect and use MAG's `inverted abstract` field to make the package more generalisable.
    
    """

    # template_fields = ['']
    def __init__(self, db_config, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config

    def execute(self, context):
        # Connect to postgresql
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        # Get the abstracts of bioRxiv papers the DOI of which has been matched with MAG.
        abstracts = s.query(Article.doi, Article.abstract).filter(
            and_(
                exists().where(Paper.doi == Article.doi),
                ~exists().where(Paper.doi == DocVector.doi),
                Article.doi.isnot(None),
                Article.abstract.isnot(None),
            )
        )
        logging.info(f"Number of documents to be vectorised: {abstracts.count()}")

        abstracts = {tup[0]: tup[1] for tup in abstracts}

        vectors = []
        for doi, abstract in abstracts.items():
            vec = average_vectors(feature_extraction(encode_text(abstract)))
            vectors.append({"doi": doi, "vector": vec})
        logging.info("Embedding documents - Done!")

        s.bulk_insert_mapping(DocVector, vectors)
        logging.info("Inserted to DB!")
