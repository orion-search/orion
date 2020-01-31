import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import DocVector
from orion.packages.projection.dim_reduction import umap_embeddings
from orion.packages.utils.s3_utils import load_from_s3


class DimReductionOperator(BaseOperator):
    """Transforms a high dimensional array to 2D or 3D."""
    @apply_defaults
    def __init__(self, db_config, bucket, prefix, n_neighbors, min_dist, n_components, metric, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.bucket = bucket
        self.prefix = prefix
        self.n_neighbors = n_neighbors
        self.min_dist = min_dist
        self.n_components = n_components
        self.metric = metric

    def execute(self, context):        
        # Load vectors from S3
        doi, vectors, ids = zip(*load_from_s3(self.bucket, self.prefix))
        
        # Reduce dimensionality to 2D with umap
        embeddings = umap_embeddings(vectors, self.n_neighbors, self.min_dist, self.n_components, self.metric)
        
        logging.info(f'UMAP embeddings: {embeddings.shape}')

        doc_vectors = [{"id": id_, "doi": doi_, "vector":embedding.tolist()} for doi_, embedding, id_ in zip(doi, embeddings, ids)]
        logging.info(f'Constructed document vector triplets.')

        # Store document vectors in PostgreSQL
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        s.bulk_insert_mappings(DocVector, doc_vectors)
        s.commit()
        logging.info('Commited to DB!')
