"""
FaissIndexOperator: Creates a FAISS index. It fetches vectors, DOIs and paper IDs from S3.
It serialises and stores the index as a pickle in S3.

"""
import logging
import numpy as np
from airflow.models import BaseOperator
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from orion.core.orms.mag_orm import HighDimDocVector
from airflow.utils.decorators import apply_defaults
from orion.packages.utils.s3_utils import store_on_s3
import faiss
from orion.packages.projection.faiss_index import faiss_index


class FaissIndexOperator(BaseOperator):
    @apply_defaults
    def __init__(self, db_config, bucket, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.bucket = bucket

    def execute(self, context):
        # Connect to postgresql
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        vectors = s.query(HighDimDocVector.vector, HighDimDocVector.id)

        # Load vectors
        vectors, ids = zip(*vectors)
        logging.info("Loaded document vectors")

        # Store vectors in an array
        vectors = np.array([vector for vector in vectors]).astype("float32")

        # Build the FAISS index with custom IDs
        index = faiss_index(vectors, ids)
        logging.info(f"Created index with {index.ntotal} elements.")

        # Serialise index and store it on S3
        store_on_s3(faiss.serialize_index(index), self.bucket, "faiss_index")
