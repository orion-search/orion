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
from orion.packages.nlp.text2vec import Text2Vector, use_vectors
from orion.core.orms.mag_orm import Paper, DocVector
from orion.packages.utils.s3_utils import store_on_s3
from orion.packages.utils.utils import inverted2abstract
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD


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
        papers = s.query(Paper.inverted_abstract, Paper.id, Paper.doi).filter(
            and_(
                ~exists().where(Paper.id == DocVector.id),
                Paper.doi.isnot(None),
                Paper.inverted_abstract != "NaN",
            )
        )
        logging.info(f"Number of documents to be vectorised: {papers.count()}")
        # papers = papers[:10]
        # logging.info(f"Number of documents to be vectorised: {len(papers)}")
        tv = Text2Vector()
        vectors = []
        for i, (inverted_abstract, id_, doi) in enumerate(papers):
            logging.info(f"{i}: {doi}")
            abstract = inverted2abstract(inverted_abstract)
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
        papers = s.query(Paper.inverted_abstract, Paper.id, Paper.doi).filter(
            and_(
                ~exists().where(Paper.id == DocVector.id),
                Paper.doi.isnot(None),
                Paper.inverted_abstract != "NaN",
            )
        )
        logging.info(f"Number of documents to be vectorised: {papers.count()}")

        # Reconstruct abstracts
        inverted_abstracts, ids, doi = zip(*papers)
        abstracts = [inverted2abstract(abstract) for abstract in inverted_abstracts]

        # Get tfidf vectors
        vectorizer = TfidfVectorizer(
            stop_words="english", analyzer="word", max_features=120000
        )
        X = vectorizer.fit_transform(abstracts)
        logging.info("Embedding documents - Done!")

        # Reduce dimensionality with SVD to speed up UMAP computation
        svd = TruncatedSVD(n_components=500, random_state=42)
        features = svd.fit_transform(X).astype("float32")
        logging.info(f"SVD dimensionality reduction shape: {features.shape}")

        vectors = [[doi, vec, id_] for doi, vec, id_ in zip(doi, features, ids)]
        logging.info(f"Embedding triplets: {len(vectors)}")

        store_on_s3(vectors, self.bucket, self.prefix)
        store_on_s3(vectorizer, self.bucket, "tfidf_model")
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
        papers = s.query(Paper.inverted_abstract, Paper.id, Paper.doi).filter(
            and_(
                ~exists().where(Paper.id == DocVector.id),
                Paper.doi.isnot(None),
                Paper.inverted_abstract != "NaN",
            )
        )
        logging.info(f"Number of documents to be vectorised: {papers.count()}")

        # Reconstruct abstracts
        inverted_abstracts, ids, doi = zip(*papers)
        abstracts = [inverted2abstract(abstract) for abstract in inverted_abstracts]
        assert len(abstracts) == len(ids)
        logging.info(f'Number of abstracts to vectorise: {len(abstracts)}')
        
        # Vectorisation with USE
        vectors = use_vectors(abstracts)
        logging.info('Finished vectorisation :)')
        vectors = [[doi, vec, id_] for doi, vec, id_ in zip(doi, vectors, ids)]
        store_on_s3(vectors, self.bucket, 'test')
