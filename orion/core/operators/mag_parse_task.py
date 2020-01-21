import glob
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from orion.core.orms.bioarxiv_orm import Article
from orion.packages.utils.s3_utils import load_from_s3, s3_bucket_obj
from orion.packages.utils.utils import (
    unique_dicts,
    unique_dicts_by_value,
    flatten_lists,
)
from orion.packages.mag.parsing_mag_data import (
    parse_affiliations,
    parse_authors,
    parse_fos,
    parse_journal,
    parse_papers,
)
from orion.core.orms.mag_orm import (
    Paper,
    Journal,
    Author,
    AuthorAffiliation,
    Affiliation,
    PaperAuthor,
    PaperFieldsOfStudy,
    FieldOfStudy,
)


class MagParserOperator(BaseOperator):
    """Parses files from S3 that contain MAG paper information."""

    # template_fields = ['']
    def __init__(self, s3_bucket, db_config, *args, **kwargs):
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.db_config = db_config

    def execute(self, context):
        # Connect to postgresql
        engine = create_engine(self.db_config)
        Session = sessionmaker(bind=engine)
        s = Session()

        # Collect IDs from tables to ensure we're not inserting duplicates
        paper_ids = {id_[0] for id_ in s.query(Paper.doi)}
        author_ids = {id_[0] for id_ in s.query(Author.id)}
        fos_ids = {id_[0] for id_ in s.query(FieldOfStudy.id)}
        aff_ids = {id_[0] for id_ in s.query(Affiliation.id)}

        # Read data from S3
        data = []
        for obj in s3_bucket_obj(self.s3_bucket):
            data.extend(load_from_s3(self.s3_bucket, obj.key.split(".")[0]))
        logging.info(f"Number of collected papers: {len(data)}")

        # Remove duplicates and keep only papers that are not already in the mag_papers table.
        data = [
            d for d in unique_dicts_by_value(data, "Id") if d["DOI"] not in paper_ids
        ]
        logging.info(f"Number of unique  papers not existing in DB: {len(data)}")

        papers = [parse_papers(response) for response in data]
        # logging.info(f'Completed parsing papers: {len(papers)}')

        journals = [
            parse_journal(response, response["Id"])
            for response in data
            if "J" in response.keys()
        ]
        # logging.info(f'Completed parsing journals: {len(journals)}')
        # Parse author information
        items = [parse_authors(response, response["Id"]) for response in data]
        authors = [
            d
            for d in unique_dicts_by_value(
                flatten_lists([item[0] for item in items]), "id"
            )
            if d["id"] not in author_ids
        ]
        paper_with_authors = unique_dicts(flatten_lists([item[1] for item in items]))

        # Parse Fields of Study
        items = [
            parse_fos(response, response["Id"])
            for response in data
            if "F" in response.keys()
        ]
        paper_with_fos = unique_dicts(flatten_lists([item[0] for item in items]))
        fields_of_study = [
            d
            for d in unique_dicts(flatten_lists([item[1] for item in items]))
            if d["id"] not in fos_ids
        ]

        # Parse affiliations
        items = [parse_affiliations(response) for response in data]
        affiliations = [
            d
            for d in unique_dicts(flatten_lists([item[0] for item in items]))
            if d["id"] not in aff_ids
        ]
        author_with_aff = [
            d
            for d in unique_dicts(flatten_lists([item[1] for item in items]))
            if d["author_id"] not in author_ids
        ]
        logging.info(f"Parsing completed!")

        # Insert dicts into postgresql
        s.bulk_insert_mappings(Paper, papers)
        s.bulk_insert_mappings(Journal, journals)
        s.bulk_insert_mappings(Author, authors)
        s.bulk_insert_mappings(PaperAuthor, paper_with_authors)
        s.bulk_insert_mappings(FieldOfStudy, fields_of_study)
        s.bulk_insert_mappings(PaperFieldsOfStudy, paper_with_fos)
        s.bulk_insert_mappings(Affiliation, affiliations)
        s.bulk_insert_mappings(AuthorAffiliation, author_with_aff)

        s.commit()
        logging.info("Committed to DB")
