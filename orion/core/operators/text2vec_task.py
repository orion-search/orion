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
from orion.packages.nlp.text2vec import Text2Vector
from orion.core.orms.mag_orm import Paper, DocVector
from orion.packages.utils.s3_utils import store_on_s3
from orion.packages.utils.utils import inverted2abstract


class Text2VectorOperator(BaseOperator):
    """Transforms text to document embeddings."""

    # template_fields = ['']
    @apply_defaults
    def __init__(self, db_config, bucket, prefix, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.bucket = bucket
        self.prefix = prefix

    def execute(self, context):
        # Connect to postgresql
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        # Get the abstracts of bioRxiv papers.
        papers = (
            s.query(Paper.inverted_abstract, Paper.id, Paper.doi)
            .filter(
                and_(
                    ~exists().where(Paper.id == DocVector.id),
                    Paper.doi.isnot(None),
                    Paper.inverted_abstract != 'NaN',
                )
            )
        )
        logging.info(f"Number of documents to be vectorised: {papers.count()}")
        papers = papers[:10]
        # TRANSFORM INVERTED_ABSTRACT TO ABSTRACT
        logging.info(f"Number of documents to be vectorised: {len(papers)}")
        tv = Text2Vector()
        vectors = []
        for i, (inverted_abstract, id_, doi) in enumerate(papers):
            logging.info(f'{i}: {doi}')
            abstract = inverted2abstract(inverted_abstract)
            vec = tv.average_vectors(tv.feature_extraction(tv.encode_text(abstract)))
            vectors.append([doi, vec, id_])
        logging.info("Embedding documents - Done!")

        store_on_s3(vectors, self.bucket, self.prefix)
        # s.bulk_insert_mappings(DocVector, vectors)
        logging.info("Stored to S3!")
