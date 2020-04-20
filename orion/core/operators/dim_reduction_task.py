"""
DimReductionOperator: Transforms high dimensional arrays to 2D and 3D using UMAP.
Fetches vectors and paper IDs from PostgreSQL and stores the low dimensional 
representation in PostgreSQL.

"""
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.mag_orm import DocVector, HighDimDocVector
from orion.packages.projection.dim_reduction import umap_embeddings
from orion.packages.utils.s3_utils import load_from_s3


class DimReductionOperator(BaseOperator):
    """Transforms a high dimensional array to 2D or 3D."""

    @apply_defaults
    def __init__(
        self, db_config, n_neighbors, min_dist, n_components, metric, *args, **kwargs,
    ):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.n_neighbors = n_neighbors
        self.min_dist = min_dist
        self.n_components = n_components
        self.metric = metric

    def execute(self, context):

        # Connect to postgresql
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        vectors = s.query(HighDimDocVector.vector, HighDimDocVector.id)

        # Load vectors
        vectors, ids = zip(*vectors)

        logging.info(
            f"UMAP hyperparameters: n_neighbors:{self.n_neighbors}, min_dist:{self.min_dist}, metric:{self.metric}"
        )

        # Reduce dimensionality to 3D with umap
        embeddings_3d = umap_embeddings(
            vectors, self.n_neighbors, self.min_dist, self.n_components + 1, self.metric
        )

        logging.info(f"UMAP embeddings: {embeddings_3d.shape}")

        # Construct DB insertions
        doc_vectors = [
            {"id": id_, "vector_3d": embed_3d.tolist(),}
            for embed_3d, id_ in zip(embeddings_3d, ids)
        ]
        logging.info(f"Constructed DocVector input")

        # Store document vectors in PostgreSQL
        s.bulk_insert_mappings(DocVector, doc_vectors)
        s.commit()
        logging.info("Commited to DB!")
