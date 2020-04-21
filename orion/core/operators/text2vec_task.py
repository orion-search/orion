"""
Transforms a variable length text to a fixed-length vector.

Text2VectorOperator: Uses a pretrained model (ALBERT) from the transformers library 
to create word vectors which are then averaged to produce a document vector. It fetches 
abstracts from PostgreSQL which decodes to text. The output vectors are 
stored on S3.

Text2TfidfOperator: Transforms text to vectors using TF-IDF and SVD. TF-IDF from scikit-learn 
preprocesses the data and SVD reduces the dimensionality of the document vectors. It fetches
abstracts from PostgreSQL which decodes to text. The output vectors are 
stored on S3.

"""
import logging
from sqlalchemy import create_engine, and_
from sqlalchemy.sql import exists
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.packages.nlp.text2vec import Text2Vector
from orion.core.orms.mag_orm import Paper, HighDimDocVector
from orion.packages.utils.s3_utils import store_on_s3
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
import orion

svd_components = orion.config["svd"]["n_components"]
seed = orion.config["seed"]
max_features = orion.config["tfidf"]["max_features"]


class Text2VectorOperator(BaseOperator):
    """Transforms text to document embeddings."""

    # template_fields = ['']
    @apply_defaults
    def __init__(self, db_config, bucket, *args, **kwargs):
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
        papers = s.query(Paper.abstract, Paper.id).filter(
            and_(
                ~exists().where(Paper.id == HighDimDocVector.id),
                Paper.abstract != "NaN",
            )
        )
        logging.info(f"Number of documents to be vectorised: {papers.count()}")

        # Convert text to vectors
        tv = Text2Vector()
        vectors = []
        for i, (abstract, id_) in enumerate(papers, start=1):
            logging.info(f"{i}: {id_}")
            vec = tv.average_vectors(tv.feature_extraction(tv.encode_text(abstract)))
            vectors.append({"vector": vec, "id": id_})

            if i % 100:
                s.bulk_insert_mappings(HighDimDocVector)
                s.commit()
                vectors.clear()
                logging.info("Stored {i} vectors to DB!")


class Text2TfidfOperator(BaseOperator):
    """Transforms text to document embeddings."""

    # template_fields = ['']
    @apply_defaults
    def __init__(self, db_config, bucket, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.bucket = bucket
        # self.prefix = prefix

    def execute(self, context):
        # Connect to postgresql
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        # Get the paper abstracts.
        papers = s.query(Paper.abstract, Paper.id).filter(
            and_(
                ~exists().where(Paper.id == HighDimDocVector.id),
                Paper.abstract != "NaN",
            )
        )
        logging.info(f"Number of documents to be vectorised: {papers.count()}")

        # Unroll abstracts and IDs
        abstracts, ids = zip(*papers)

        # Get tfidf vectors
        vectorizer = TfidfVectorizer(
            stop_words="english", analyzer="word", max_features=max_features
        )
        X = vectorizer.fit_transform(abstracts)
        logging.info("Embedding documents - Done!")

        # Reduce dimensionality with SVD to speed up UMAP computation
        svd = TruncatedSVD(n_components=svd_components, random_state=seed)
        features = svd.fit_transform(X)
        logging.info(f"SVD dimensionality reduction shape: {features.shape}")

        vectors = [{"vector": vec, "id": id_} for vec, id_ in zip(features, ids)]
        logging.info(f"Embeddings: {len(vectors)}")

        # Store models to S3
        store_on_s3(vectorizer, self.bucket, "tfidf_model")
        store_on_s3(svd, self.bucket, "svd_model")
        logging.info("Stored models to S3!")

        # Store vectors to DB
        s.bulk_insert_mappings(HighDimDocVector, vectors)
        s.commit()
        logging.info("Stored vectors to DB!")
