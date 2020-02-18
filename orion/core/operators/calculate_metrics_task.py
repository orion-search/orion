"""
Calculate all the metrics (RCA, research diversity, gender diversity).
"""
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.packages.metrics.metrics import calculate_rca_by_sum
from orion.core.orms.mag_orm import (
    Paper,
    PaperAuthor,
    AuthorAffiliation,
    AffiliationLocation,
    PaperFieldsOfStudy,
    MetricAffiliationRCA,
    MetricCountryRCA,
    FosHierarchy,
    ResearchDiversityCountry,
    GenderDiversityCountry,
    AuthorGender,
)
from orion.packages.utils.utils import dict2psql_format, flatten_lists, get_all_children
from orion.packages.utils.s3_utils import load_from_s3
from orion.packages.utils.nlp_utils import identity_tokenizer
from skbio.diversity.alpha import simpson, simpson_e, shannon
from sklearn.feature_extraction.text import CountVectorizer


class RCAOperator(BaseOperator):
    """Calculate RCA for institutions and countries."""

    @apply_defaults
    def __init__(self, db_config, s3_bucket, prefix, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.s3_bucket = s3_bucket
        self.prefix = prefix

    def execute(self, context):
        # Load topics
        topics = flatten_lists(list(load_from_s3(self.s3_bucket, self.prefix).values()))
        logging.info(f"Number of topics: {len(topics)}")

        # Connect to postgresql db
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        # Load all the tables needed for the metrics
        papers = pd.read_sql(s.query(Paper).statement, s.bind)
        aff_location = pd.read_sql(s.query(AffiliationLocation).statement, s.bind)
        author_aff = pd.read_sql(s.query(AuthorAffiliation).statement, s.bind)
        paper_author = pd.read_sql(s.query(PaperAuthor).statement, s.bind)
        paper_fos = pd.read_sql(s.query(PaperFieldsOfStudy).statement, s.bind)

        # Merge tables
        df = (
            aff_location[aff_location.country != ""][["affiliation_id", "country"]]
            .merge(author_aff, left_on="affiliation_id", right_on="affiliation_id")
            .merge(
                paper_author[["paper_id", "author_id"]],
                left_on="author_id",
                right_on="author_id",
            )
            .merge(
                papers[["id", "year", "citations"]], left_on="paper_id", right_on="id"
            )
            .merge(
                paper_fos[paper_fos["field_of_study_id"].isin(topics)],
                left_on="paper_id",
                right_on="paper_id",
            )[
                [
                    "affiliation_id",
                    "field_of_study_id",
                    "country",
                    "paper_id",
                    "citations",
                    "year",
                ]
            ]
        )
        logging.info(f"Overall DF shape: {df.shape}")

        # RCA by summing citations - country level
        country_level = df.drop_duplicates(
            subset=["field_of_study_id", "country", "paper_id"]
        )
        logging.info(f"DF country_level shape: {country_level.shape}")

        d = {}
        for fos in country_level.field_of_study_id.unique():
            d[fos] = calculate_rca_by_sum(
                country_level, entity_column="country", commodity=fos, value="citations"
            )

        rca_country_level_sum = dict2psql_format(d)

        s.bulk_insert_mappings(MetricCountryRCA, rca_country_level_sum)

        # RCA by summing citations - affiliation level
        affiliation_level = df.drop_duplicates(
            subset=["field_of_study_id", "affiliation_id", "paper_id"]
        )
        logging.info(f"DF affiliation_level shape: {affiliation_level.shape}")

        d = {}
        for fos in affiliation_level.field_of_study_id.unique()[:10]:
            d[fos] = calculate_rca_by_sum(
                affiliation_level,
                entity_column="affiliation_id",
                commodity=fos,
                value="citations",
            )

        rca_affiliation_level_sum = dict2psql_format(d)
        s.bulk_insert_mappings(MetricAffiliationRCA, rca_affiliation_level_sum)
        s.commit()


class ResearchDiversityOperator(BaseOperator):
    """Calculates diversity metrics for each country, year and level 1 topic."""

    @apply_defaults
    def __init__(self, db_config, s3_bucket, prefix, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.s3_bucket = s3_bucket
        self.prefix = prefix

    def execute(self, context):
        # Load topics
        topics = flatten_lists(list(load_from_s3(self.s3_bucket, self.prefix).values()))
        logging.info(f"Number of topics: {len(topics)}")

        # Connect to postgresql db
        engine = create_engine(self.db_config)
        Session = sessionmaker(engine)
        s = Session()

        # Load all the tables needed for the metrics
        papers = pd.read_sql(s.query(Paper).statement, s.bind)
        aff_location = pd.read_sql(s.query(AffiliationLocation).statement, s.bind)
        author_aff = pd.read_sql(s.query(AuthorAffiliation).statement, s.bind)
        paper_author = pd.read_sql(s.query(PaperAuthor).statement, s.bind)
        paper_fos = pd.read_sql(s.query(PaperFieldsOfStudy).statement, s.bind)
        hierarchy = pd.read_sql(s.query(FosHierarchy).statement, s.bind)

        # Traverse the FoS hierarchy tree and get all children
        d = {topic: get_all_children(hierarchy, topic) for topic in topics}

        for parent, children in d.items():
            logging.info(f"Parent ID: {parent} - Number of children: {len(children)}")
            # Merge tables for a particular "discipline" (level 1 FoS and its children)
            df = (
                aff_location[aff_location.country != ""][["affiliation_id", "country"]]
                .merge(author_aff, left_on="affiliation_id", right_on="affiliation_id")
                .merge(
                    paper_author[["paper_id", "author_id"]],
                    left_on="author_id",
                    right_on="author_id",
                )
                .merge(
                    papers[["id", "year", "citations"]],
                    left_on="paper_id",
                    right_on="id",
                )
                .merge(
                    paper_fos[paper_fos["field_of_study_id"].isin(children)],
                    left_on="paper_id",
                    right_on="paper_id",
                )[
                    [
                        "affiliation_id",
                        "field_of_study_id",
                        "country",
                        "paper_id",
                        "citations",
                        "year",
                    ]
                ]
            )

            # Keep unique pairs and group FoS by year and country
            country_level = (
                df.drop_duplicates(
                    subset=["field_of_study_id", "country", "paper_id", "year"]
                )
                .groupby(["year", "country"])["field_of_study_id"]
                .apply(list)
            )
            logging.info(f"Country level frame: {country_level.shape}")

            # Slice country_level by year
            for year in country_level.index.levels[0]:
                frame = country_level.loc[[year], :]
                logging.info(f"{year} - Countries: {frame.shape[0]}")

                # Create a Bag-of-FoS
                vectorizer = CountVectorizer(
                    tokenizer=identity_tokenizer, lowercase=False
                )
                X = vectorizer.fit_transform(list(frame))
                X = X.toarray()
                logging.info(
                    f"Number of vectorised FoS: {len(vectorizer.get_feature_names())}"
                )

                # Calculate diversity metrics
                shannon_div = [shannon(arr) for arr in X]
                simpson_e_div = [simpson_e(arr) for arr in X]
                simpson_div = [simpson(arr) for arr in X]
                logging.info("Calculated diversity metrics.")

                for idx, i in zip(frame.index, range(X.shape[0])):
                    s.add(
                        ResearchDiversityCountry(
                            shannon_diversity=shannon_div[i],
                            simpson_e_diversity=simpson_e_div[i],
                            simpson_diversity=simpson_div[i],
                            year=year,
                            entity=idx[1],
                            field_of_study_id=int(parent),
                        )
                    )
                    s.commit()
                    logging.info("Added to DB!")


class GenderDiversityOperator(BaseOperator):
    """Measures the gender diversity for a country, topic and year."""

    @apply_defaults
    def __init__(self, db_config, s3_bucket, prefix, thresh, *args, **kwargs):
        super().__init__(**kwargs)
        self.db_config = db_config
        self.s3_bucket = s3_bucket
        self.prefix = prefix
        self.thresh = thresh

    def execute(self, context):
        # Load topics
        topics = flatten_lists(list(load_from_s3(self.s3_bucket, self.prefix).values()))
        logging.info(f"Number of topics: {len(topics)}")

        # Connect to postgresql db
        engine = create_engine(self.db_config, pool_pre_ping=True)
        Session = sessionmaker(engine)
        s = Session()

        # Load all the tables needed for the metrics
        papers = pd.read_sql(s.query(Paper).statement, s.bind)
        aff_location = pd.read_sql(s.query(AffiliationLocation).statement, s.bind)
        author_aff = pd.read_sql(s.query(AuthorAffiliation).statement, s.bind)
        paper_author = pd.read_sql(s.query(PaperAuthor).statement, s.bind)
        paper_fos = pd.read_sql(s.query(PaperFieldsOfStudy).statement, s.bind)
        hierarchy = pd.read_sql(s.query(FosHierarchy).statement, s.bind)
        gender = pd.read_sql(s.query(AuthorGender).statement, s.bind)
        # Keep only inferred gender with a probability higher than .75
        gender = gender[gender.probability >= self.thresh]
        logging.info("Read all tables")

        # Merge papers IDs with authors and their gender
        paper_author_gender = paper_author[["paper_id", "author_id"]].merge(
            gender[["id", "gender"]], left_on="author_id", right_on="id"
        )
        paper_author_gender = pd.DataFrame(
            paper_author_gender.groupby(["paper_id", "gender"])["author_id"].count()
            / paper_author_gender.groupby(["paper_id"])["author_id"].count()
        ).reset_index()

        # Add female share = 0 when a paper has only male authors
        female_share_zero = [
            pd.DataFrame(
                {"paper_id": row["paper_id"], "gender": ["female"], "author_id": [0.0]}
            )
            for idx, row in paper_author_gender.iterrows()
            if row["gender"] == "male" and row["author_id"] == 1.0
        ]
        female_share = pd.concat(
            [
                paper_author_gender[paper_author_gender.gender == "female"],
                pd.concat(female_share_zero),
            ]
        )
        female_share = female_share.rename(columns={"author_id": "female_share"}).drop(
            "gender", axis=1
        )
        logging.info("Prepared table with female share")

        # Traverse the FoS hierarchy tree and get all children
        d = {topic: get_all_children(hierarchy, topic) for topic in topics}

        for parent, children in d.items():
            logging.info(f"Parent ID: {parent} - Number of children: {len(children)}")
            df = (
                aff_location[aff_location.country != ""][["affiliation_id", "country"]]
                .merge(author_aff, left_on="affiliation_id", right_on="affiliation_id")
                .merge(
                    paper_author[["paper_id", "author_id"]],
                    left_on="author_id",
                    right_on="author_id",
                )
                .merge(
                    papers[
                        papers.id.isin(
                            paper_fos[paper_fos.field_of_study_id.isin(children)][
                                "paper_id"
                            ]
                        )
                    ][["id", "year", "citations"]],
                    left_on="paper_id",
                    right_on="id",
                )
                .merge(female_share, left_on="paper_id", right_on="paper_id")[
                    [
                        "affiliation_id",
                        "country",
                        "paper_id",
                        "citations",
                        "year",
                        "female_share",
                    ]
                ]
            )

            country_level = (
                df.drop_duplicates(subset=["country", "paper_id", "year"])
                .groupby(["year", "country"])["female_share"]
                .mean()
            )
            logging.info(f"Country level frame: {country_level.shape}")

            for idx, val in country_level.iteritems():
                s.add(
                    GenderDiversityCountry(
                        female_share=val,
                        year=idx[0],
                        entity=idx[1],
                        field_of_study_id=int(parent),
                    )
                )
                s.commit()
