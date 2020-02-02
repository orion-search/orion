from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import TEXT, VARCHAR, ARRAY, FLOAT
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.types import Integer, Float, BIGINT

Base = declarative_base()


class Paper(Base):
    """MAG paper. Collected by matching its title with a title from BioRxiv."""

    __tablename__ = "mag_papers"

    id = Column(BIGINT, primary_key=True, autoincrement=False)
    prob = Column(Float)
    title = Column(TEXT)
    publication_type = Column(TEXT)
    year = Column(TEXT)
    date = Column(TEXT)
    citations = Column(Integer)
    references = Column(
        TEXT
    )  # This is transformed from list to string using json.dumps().
    doi = Column(VARCHAR(200))
    publisher = Column(TEXT)
    bibtex_doc_type = Column(TEXT)
    inverted_abstract = Column(TEXT)
    journals = relationship("Journal", back_populates="paper")
    fields_of_study = relationship("PaperFieldsOfStudy", back_populates="paper")
    authors = relationship("PaperAuthor", back_populates="paper")


class Journal(Base):
    """Journal where a paper was published."""

    __tablename__ = "mag_paper_journal"

    id = Column(BIGINT)
    journal_name = Column(TEXT)
    paper_id = Column(
        BIGINT, ForeignKey("mag_papers.id"), primary_key=True, autoincrement=False
    )
    paper = relationship("Paper")

class Conference(Base):
    """Conference where a paper was published."""

    __tablename__ = "mag_paper_conferences"

    id = Column(BIGINT)
    conference_name = Column(TEXT)
    paper_id = Column(
        BIGINT, ForeignKey("mag_papers.id"), primary_key=True, autoincrement=False
    )
    paper = relationship("Paper")

class PaperAuthor(Base):
    """Authors of a paper."""

    __tablename__ = "mag_paper_authors"

    paper_id = Column(
        BIGINT, ForeignKey("mag_papers.id"), primary_key=True, autoincrement=False
    )
    author_id = Column(
        BIGINT, ForeignKey("mag_authors.id"), primary_key=True, autoincrement=False
    )
    order = Column(Integer)
    paper = relationship("Paper", back_populates="authors")
    author = relationship("Author", back_populates="papers")


class Author(Base):
    """Details of an author."""

    __tablename__ = "mag_authors"

    id = Column(BIGINT, primary_key=True, autoincrement=False)
    name = Column(VARCHAR(100))
    papers = relationship("PaperAuthor", back_populates="author")
    affiliation = relationship("AuthorAffiliation")


class Affiliation(Base):
    """Details of an author affiliation."""

    __tablename__ = "mag_affiliation"

    id = Column(BIGINT, primary_key=True)
    affiliation = Column(TEXT)
    author_affiliation = relationship("AuthorAffiliation")
    aff_location = relationship("AffiliationLocation")


class AuthorAffiliation(Base):
    """Linking author with their affiliation."""

    __tablename__ = "mag_author_affiliation"

    affiliation_id = Column(
        BIGINT, ForeignKey("mag_affiliation.id"), primary_key=True, autoincrement=False
    )
    author_id = Column(
        BIGINT, ForeignKey("mag_authors.id"), primary_key=True, autoincrement=False
    )
    affiliations = relationship("Affiliation")
    authors = relationship("Author")


class FieldOfStudy(Base):
    """Fields of study."""

    __tablename__ = "mag_fields_of_study"

    id = Column(BIGINT, primary_key=True, autoincrement=False)
    name = Column(VARCHAR(250))


class PaperFieldsOfStudy(Base):
    """Linking papers with their fields of study."""

    __tablename__ = "mag_paper_fields_of_study"

    paper_id = Column(
        BIGINT, ForeignKey("mag_papers.id"), primary_key=True, autoincrement=False
    )
    field_of_study_id = Column(
        BIGINT,
        ForeignKey("mag_fields_of_study.id"),
        primary_key=True,
        autoincrement=False,
    )
    paper = relationship("Paper", back_populates="fields_of_study")
    field_of_study = relationship("FieldOfStudy")


class AffiliationLocation(Base):
    """Geographic information of an affiliation."""

    __tablename__ = "geocoded_places"

    id = Column(TEXT, primary_key=True, autoincrement=False)
    affiliation_id = Column(
        BIGINT, ForeignKey("mag_affiliation.id"), primary_key=True, autoincrement=False
    )
    lat = Column(Float)
    lng = Column(Float)
    address = Column(TEXT)
    name = Column(TEXT)
    types = Column(TEXT)
    website = Column(TEXT)
    postal_town = Column(TEXT)
    administrative_area_level_2 = Column(TEXT)
    administrative_area_level_1 = Column(TEXT)
    country = Column(TEXT)
    geocoded_affiliation = relationship("Affiliation", back_populates="aff_location")


class DocVector(Base):
    """Abstract vector of a paper."""

    __tablename__ = "doc_vectors"

    id = Column(
        BIGINT, ForeignKey("mag_papers.id"), primary_key=True, autoincrement=False
    )
    doi = Column(VARCHAR(200))
    vector = Column(ARRAY(FLOAT))


class FosHierarchy(Base):
    """Parent and child nodes of a FoS."""

    __tablename__ = "mag_field_of_study_hierarchy"

    id = Column(
        BIGINT,
        ForeignKey("mag_fields_of_study.id"),
        primary_key=True,
        autoincrement=False,
    )

    parent_id = Column(ARRAY(BIGINT))
    child_id = Column(ARRAY(BIGINT))


class FosMetadata(Base):
    """Level in the hierarchy and the frequency of a Field of Study."""

    __tablename__ = "mag_field_of_study_metadata"
    id = Column(
        BIGINT,
        ForeignKey("mag_fields_of_study.id"),
        primary_key=True,
        autoincrement=False,
    )
    level = Column(Integer)
    frequency = Column(Integer)


class MetricCountryRCA(Base):
    """Revealed comparative advantage of a country."""

    __tablename__ = "rca_country"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rca_sum = Column(Float)
    year = Column(TEXT)
    entity = Column(TEXT)
    field_of_study_id = Column(BIGINT, ForeignKey("mag_fields_of_study.id"))


class MetricAffiliationRCA(Base):
    """Revealed comparative advantage of an institution."""

    __tablename__ = "rca_affiliation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rca_sum = Column(Float)
    year = Column(TEXT)
    entity = Column(BIGINT)
    field_of_study_id = Column(BIGINT, ForeignKey("mag_fields_of_study.id"))


class AuthorGender(Base):
    """Gender of an author."""

    __tablename__ = "author_gender"

    id = Column(
        BIGINT, ForeignKey("mag_authors.id"), primary_key=True, autoincrement=False
    )
    full_name = Column(VARCHAR(100))
    first_name = Column(VARCHAR(100))
    gender = Column(TEXT)
    samples = Column(Integer)
    probability = Column(Float)


if __name__ == "__main__":
    import logging
    import psycopg2
    from orion.core.airflow_utils import misctools
    from sqlalchemy import create_engine, exc



    # Try to create the database if it doesn't already exist.
    try:
        db_config = misctools.get_config("orion_config.config", "postgresdb")["test_uri"]
        engine = create_engine(db_config) 
        conn = engine.connect()
        conn.execute("commit") 
        conn.execute("create database orion") 
        conn.close()
    except exc.DBAPIError as e:
        if isinstance(e.orig, psycopg2.errors.DuplicateDatabase):
            logging.info(e)
        else:
            logging.error(e)
            raise

    db_config = misctools.get_config("orion_config.config", "postgresdb")["orion"]
    engine = create_engine(db_config)
    Base.metadata.create_all(engine)
