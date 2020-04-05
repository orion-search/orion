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


Text2USEOperator: Uses a pretrained model (Universal Sentence Encoder) from TensorHub to
transform text to vectors. It fetches abstracts from PostgreSQL which decodes to 
text. The output vectors are stored on S3.

"""
import logging
from sqlalchemy import create_engine, and_
from sqlalchemy.sql import exists
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.packages.nlp.text2vec import Text2Vector, use_vectors
from orion.core.orms.mag_orm import Paper, DocVector
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
        papers = s.query(Paper.abstract, Paper.id, Paper.doi).filter(
            and_(
                ~exists().where(Paper.id == DocVector.id),
                Paper.doi.isnot(None),
                Paper.abstract != "NaN",
            )
        )
        logging.info(f"Number of documents to be vectorised: {papers.count()}")

        # Convert text to vectors
        tv = Text2Vector()
        vectors = []
        for i, (abstract, id_, doi) in enumerate(papers):
            logging.info(f"{i}: {doi}")
            vec = tv.average_vectors(tv.feature_extraction(tv.encode_text(abstract)))
            vectors.append([doi, vec, id_])
        logging.info("Embedding documents - Done!")

        store_on_s3(vectors, self.bucket, self.prefix)
        logging.info("Stored to S3!")


class Text2TfidfOperator(BaseOperator):
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

        # Get the paper abstracts.
        papers = s.query(Paper.abstract, Paper.id, Paper.doi).filter(
            and_(
                ~exists().where(Paper.id == DocVector.id),
                Paper.doi.isnot(None),
                Paper.abstract != "NaN",
            )
        )
        logging.info(f"Number of documents to be vectorised: {papers.count()}")

        # Reconstruct abstracts
        abstracts, ids, doi = zip(*papers)

        # Get tfidf vectors
        vectorizer = TfidfVectorizer(
            stop_words="english", analyzer="word", max_features=max_features
        )
        X = vectorizer.fit_transform(abstracts)
        logging.info("Embedding documents - Done!")

        # Reduce dimensionality with SVD to speed up UMAP computation
        svd = TruncatedSVD(n_components=svd_components, random_state=seed)
        features = svd.fit_transform(X).astype("float32")
        logging.info(f"SVD dimensionality reduction shape: {features.shape}")

        vectors = [[doi, vec, id_] for doi, vec, id_ in zip(doi, features, ids)]
        logging.info(f"Embedding triplets: {len(vectors)}")

        store_on_s3(vectors, self.bucket, self.prefix)
        store_on_s3(vectorizer, self.bucket, "tfidf_model")
        store_on_s3(svd, self.bucket, "svd_model")
        logging.info("Stored to S3!")


class Text2USEOperator(BaseOperator):
    """Transform text to vectors with the Universal Sentence Encoder model."""

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
        papers = s.query(Paper.abstract, Paper.id, Paper.doi).filter(
            and_(
                ~exists().where(Paper.id == DocVector.id),
                Paper.doi.isnot(None),
                Paper.abstract != "NaN",
            )
        )
        logging.info(f"Number of documents to be vectorised: {papers.count()}")

        # Reconstruct abstracts
        abstracts, ids, doi = zip(*papers)
        assert len(abstracts) == len(ids)
        logging.info(f"Number of abstracts to vectorise: {len(abstracts)}")

        # Vectorisation with USE
        vectors = use_vectors(abstracts)
        logging.info("Finished vectorisation :)")
        vectors = [[doi, vec, id_] for doi, vec, id_ in zip(doi, vectors, ids)]
        store_on_s3(vectors, self.bucket, "use_vectors")
        logging.info("Stored to S3!")
