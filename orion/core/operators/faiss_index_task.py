"""
Create a FAISS index.
"""
import logging
import numpy as np
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.packages.utils.s3_utils import store_on_s3, load_from_s3
import faiss
from orion.packages.projection.faiss_index import faiss_index
import boto3

class FaissIndexOperator(BaseOperator):
    @apply_defaults
    def __init__(self, bucket, prefix, *args, **kwargs):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix

    def execute(self, context):
        doi, vectors, ids = zip(*load_from_s3(self.bucket, self.prefix))
        vectors = np.array([vector for vector in vectors])
        logging.info("Loaded document vectors")

        # Build the FAISS index with custom IDs
        index = faiss_index(vectors, ids)
        logging.info(f"Created index with {index.ntotal} elements.")

        # Serialise index and store it on S3
        store_on_s3(faiss.serialize_index(index), self.bucket, "faiss_index")
