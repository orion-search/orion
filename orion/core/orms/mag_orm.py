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
    """Linking papers with authors and their affiliation."""

    __tablename__ = "mag_author_affiliation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    affiliation_id = Column(BIGINT, ForeignKey("mag_affiliation.id"))
    author_id = Column(BIGINT, ForeignKey("mag_authors.id"))
    paper_id = Column(BIGINT, ForeignKey("mag_papers.id"))
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
    vector_2d = Column(ARRAY(FLOAT))
    vector_3d = Column(ARRAY(FLOAT))


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


class CountryCollaboration(Base):
    """Collaborators of a country and their number of shared papers."""

    __tablename__ = "country_collaboration"

    id = Column(Integer, primary_key=True, autoincrement=True)
    country_a = Column(TEXT)
    country_b = Column(TEXT)
    weight = Column(Integer)
    year = Column(TEXT)


class ResearchDiversityCountry(Base):
    """Research diversity metrics for a country."""

    __tablename__ = "research_diversity_country"

    id = Column(Integer, primary_key=True, autoincrement=True)
    shannon_diversity = Column(Float)
    simpson_e_diversity = Column(Float)
    simpson_diversity = Column(Float)
    year = Column(TEXT)
    entity = Column(TEXT)
    field_of_study_id = Column(BIGINT, ForeignKey("mag_fields_of_study.id"))


class GenderDiversityCountry(Base):
    """Average number of female co-authors for a country."""

    __tablename__ = "gender_diversity_country"

    id = Column(Integer, primary_key=True, autoincrement=True)
    female_share = Column(Float)
    year = Column(TEXT)
    entity = Column(TEXT)
    field_of_study_id = Column(BIGINT, ForeignKey("mag_fields_of_study.id"))


class FilteredFos(Base):
    """Paper count and citation sum for a field of study"""

    __tablename__ = "mag_filtered_field_of_study"

    id = Column(Integer, primary_key=True, autoincrement=True)
    field_of_study_id = Column(BIGINT, ForeignKey("mag_fields_of_study.id"))
    year = Column(TEXT)
    all_children = Column(ARRAY(BIGINT))
    paper_count = Column(Integer)
    total_citations = Column(Integer)


class CountrySimilarity(Base):
    """Country similarity for each topic and year."""

    __tablename__ = "country_similarity"

    id = Column(Integer, primary_key=True, autoincrement=True)
    country_a = Column(TEXT)
    country_b = Column(TEXT)
    closeness = Column(Float)
    year = Column(TEXT)
    field_of_study_id = Column(BIGINT, ForeignKey("mag_fields_of_study.id"))


class CountryTopicOutput(Base):
    """Output for each country, topic and year. Used in front-end."""

    __tablename__ = "viz_output_by_research_area_country"

    id = Column(Integer, primary_key=True, autoincrement=True)
    country = Column(TEXT)
    field_of_study_id = Column(BIGINT, ForeignKey("mag_fields_of_study.id"))
    year = Column(TEXT)
    paper_count = Column(Integer)
    total_citations = Column(Integer)
    name = Column(TEXT)


class AllMetrics(Base):
    """Consolidates metrics from other tables. Used in front-end."""

    __tablename__ = "viz_metrics_by_country"

    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(TEXT)
    country = Column(TEXT)
    shannon_diversity = Column(Float)
    field_of_study_id = Column(BIGINT, ForeignKey("mag_fields_of_study.id"))
    rca_sum = Column(Float)
    female_share = Column(Float)
    name = Column(TEXT)


class PaperCountry(Base):
    """Paper IDs and Paper ID count for every country. Used in front-end."""

    __tablename__ = "viz_paper_country"

    country = Column(TEXT, primary_key=True, autoincrement=False)
    count = Column(Integer)
    paper_ids = Column(ARRAY(BIGINT))


class PaperTopics(Base):
    """Paper IDs for every topic. Used in front-end."""

    __tablename__ = "viz_paper_topics"

    field_of_study_id = Column(
        BIGINT,
        ForeignKey("mag_fields_of_study.id"),
        primary_key=True,
        autoincrement=False,
    )
    name = Column(TEXT)
    count = Column(Integer)
    paper_ids = Column(ARRAY(BIGINT))


class PaperYear(Base):
    """Paper IDs and paper ID count for every publication year. Used in front-end."""

    __tablename__ = "viz_paper_year"

    year = Column(TEXT, primary_key=True, autoincrement=False)
    count = Column(Integer)
    paper_ids = Column(ARRAY(BIGINT))


class AffiliationType(Base):
    """Type (1: non-industry, 0: industry) of an affiliation."""

    __tablename__ = "affiliation_type"

    id = Column(
        BIGINT, ForeignKey("mag_affiliation.id"), primary_key=True, autoincrement=False
    )
    type = Column(Integer)


class WorldBankGDP(Base):
    """World Bank GDP indicator."""

    __tablename__ = "wb_gdp"

    id = Column(Integer, primary_key=True, autoincrement=True)
    country = Column(TEXT)
    indicator = Column(Float)
    year = Column(TEXT)


class WorldBankResearchDevelopment(Base):
    """World Bank Research and development expenditure (% of GDP) indicator."""

    __tablename__ = "wb_rnd_expenditure"

    id = Column(Integer, primary_key=True, autoincrement=True)
    country = Column(TEXT)
    indicator = Column(Float)
    year = Column(TEXT)


class WorldBankGovEducation(Base):
    """World Bank Government expenditure on education, total (% of GDP) indicator."""

    __tablename__ = "wb_edu_expenditure"

    id = Column(Integer, primary_key=True, autoincrement=True)
    country = Column(TEXT)
    indicator = Column(Float)
    year = Column(TEXT)


class WorldBankFemaleLaborForce(Base):
    """World Bank Ratio of female to male labor force participation rate (%) indicator."""

    __tablename__ = "wb_female_workforce"

    id = Column(Integer, primary_key=True, autoincrement=True)
    country = Column(TEXT)
    indicator = Column(Float)
    year = Column(TEXT)


class CountryAssociation(Base):
    """Associates a country name from the World Bank with Google Places."""

    __tablename__ = "country_association"

    wb_country = Column(TEXT, primary_key=True, autoincrement=False)
    google_country = Column(TEXT, primary_key=True, autoincrement=False)


class CountryDetails(Base):
    """Country details."""

    __tablename__ = "country_details"

    alpha2Code = Column(TEXT, primary_key=True, autoincrement=False)
    alpha3Code = Column(TEXT, primary_key=True, autoincrement=False)
    name = Column(TEXT)
    google_name = Column(TEXT, primary_key=True)
    wb_name = Column(TEXT, primary_key=True)
    # google_name = Column(TEXT, ForeignKey('country_association.country'))
    # wb_name = Column(TEXT, ForeignKey('country_association.country'))
    region = Column(TEXT)
    subregion = Column(TEXT)
    population = Column(BIGINT)
    capital = Column(TEXT)


if __name__ == "__main__":
    import logging
    import psycopg2
    from orion.core.airflow_utils import misctools
    from sqlalchemy import create_engine, exc

    # Try to create the database if it doesn't already exist.
    try:
        db_config = misctools.get_config("orion_config.config", "postgresdb")[
            "orion_test"
        ]
        engine = create_engine(db_config)
        conn = engine.connect()
        conn.execute("commit")
        conn.execute("create database orion_prod")
        conn.close()
    except exc.DBAPIError as e:
        if isinstance(e.orig, psycopg2.errors.DuplicateDatabase):
            logging.info(e)
        else:
            logging.error(e)
            raise

    db_config = misctools.get_config("orion_config.config", "postgresdb")["orion_prod"]
    engine = create_engine(db_config)
    Base.metadata.create_all(engine)
