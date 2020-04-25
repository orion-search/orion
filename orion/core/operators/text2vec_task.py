"""
Transforms a variable length text to a fixed-length vector.

Text2VectorOperator: Uses a pretrained model (DistilBERT) from the transformers library 
to create word vectors which are then averaged to produce a document vector. It fetches 
abstracts from PostgreSQL which decodes to text. The output vectors are 
stored on PostgreSQL.

Text2TfidfOperator: Transforms text to vectors using TF-IDF and SVD. TF-IDF from scikit-learn 
preprocesses the data and SVD reduces the dimensionality of the document vectors. It fetches
abstracts from PostgreSQL which decodes to text. The output vectors are 
stored on PostgreSQL.

Text2SentenceBertOperator: Uses a pretrained model (DistilBERT) to create sentence-level embeddings
which are max-pooled to produce a document vector. It fetches abstracts from PostgreSQL and
decodes to text. The output vectors are stored on PostgreSQL.

Note: Running Text2VectorOperator and Text2SentenceBertOperator on a GPU will massively speed up
the computation.

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
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import TruncatedSVD
import orion
import toolz

svd_components = orion.config["svd"]["n_components"]
seed = orion.config["seed"]
max_features = orion.config["tfidf"]["max_features"]


class Text2VectorOperator(BaseOperator):
    """Transforms text to document embeddings."""

    # template_fields = ['']
    @apply_defaults
    def __init__(self, db_config, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config

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
        for i, (abstract, id_) in enumerate(papers, start=1):
            vec = tv.average_vectors(tv.feature_extraction(tv.encode_text(abstract)))

            # Commit to db
            s.add(HighDimDocVector(**{"vector": vec.astype(float), "id": id_}))
            s.commit()
        logging.info("Committed to DB!")


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


class Text2SentenceBertOperator(BaseOperator):
    """Transforms text to document embeddings."""

    # template_fields = ['']
    @apply_defaults
    def __init__(self, db_config, batch_size, bert_model, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.batch_size = batch_size
        self.bert_model = bert_model

    def execute(self, context):
        # Instantiate SentenceTransformer
        model = SentenceTransformer(self.bert_model)

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

        # Unroll abstracts and paper IDs
        abstracts, ids = zip(*papers)
        for i, (id_chunk, abstracts_chunk) in enumerate(
            zip(
                list(toolz.partition_all(self.batch_size, ids)),
                list(toolz.partition_all(self.batch_size, abstracts)),
            ),
            start=1,
        ):

            # Convert text to vectors
            embeddings = model.encode(abstracts_chunk)

            # Group IDs with embeddings
            batch = [
                {"id": id_, "vector": vector.astype(float)}
                for id_, vector in zip(id_chunk, embeddings)
            ]

            # Commit to db
            s.bulk_insert_mappings(HighDimDocVector, batch)
            s.commit()
            logging.info("Committed batch {i}")
