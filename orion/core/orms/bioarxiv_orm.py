from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import TEXT, VARCHAR, TSVECTOR
from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import Integer, Date, Boolean
from sqlalchemy.orm import relationship

Base = declarative_base()


class Article(Base):
    """BioarXiv article."""

    __tablename__ = "articles"
    id = Column(Integer, primary_key=True)
    url = Column(TEXT)
    title = Column(TEXT)
    abstract = Column(TEXT)
    doi = Column(VARCHAR(200))
    collection = Column(TEXT)
    title_vector = Column(TSVECTOR)
    abstract_vector = Column(TSVECTOR)
    last_crawled = Column(Date)
    posted = Column(Date)
    author_vector = Column(TSVECTOR)
    article_author = relationship("ArticleAuthor", back_populates="article_author")
    article_doi = relationship("ArticlePublication", back_populates="article_doi")
    article_traffic = relationship("ArticleTraffic", back_populates="traffic")
    # crossref_paper = relationship("CrossRef", back_populates="article")
    pub_date = relationship("PublicationDate", back_populates="articles")


class ArticleAuthor(Base):
    """Article and its authors."""

    __tablename__ = "article_authors"
    id = Column(Integer, primary_key=True)
    article = Column(Integer, ForeignKey("articles.id"))
    author = Column(Integer, ForeignKey("authors.id"))
    institution = Column(TEXT)
    article_author = relationship("Article")
    authors = relationship("Author")


class ArticlePublication(Base):
    """Article with DOI that has been published after peer review."""

    __tablename__ = "article_publications"
    article = Column(Integer, ForeignKey("articles.id"), primary_key=True)
    doi = Column(VARCHAR(200))
    publication = Column(TEXT)
    article_doi = relationship("Article")
    # crossref_paper = relationship("CrossRef", back_populates="publication")


class ArticleTraffic(Base):
    """Article traffic on bioarXiv."""

    __tablename__ = "article_traffic"
    id = Column(Integer, primary_key=True)
    article = Column(Integer, ForeignKey("articles.id"))
    month = Column(Integer)
    year = Column(Integer)
    abstract = Column(Integer)
    pdf = Column(Integer)
    traffic = relationship("Article")


class AuthorRank(Base):
    """Author ranking (in terms of downloads) per category."""

    __tablename__ = "author_ranks_category"
    id = Column(Integer, primary_key=True)
    author = Column(Integer, ForeignKey("authors.id"))
    category = Column(TEXT)
    rank = Column(Integer)
    tie = Column(Boolean)
    downloads = Column(Integer)
    ranking = relationship("Author")


class Author(Base):
    """Author information."""

    __tablename__ = "authors"
    id = Column(Integer, primary_key=True)
    name = Column(TEXT)
    institution = Column(TEXT)
    orcid = Column(TEXT)
    noperiodname = Column(TEXT)
    author_ranking = relationship("AuthorRank", back_populates="ranking")
    article_author = relationship("ArticleAuthor", back_populates="authors")


# class CrossRef(Base):
#     """CrossRef information for article with DOI."""

#     __tablename__ = "crossref_daily"
#     id = Column(Integer, primary_key=True)
#     source_date = Column(Date)
#     doi = Column(TEXT)
#     count = Column(Integer)
#     crawled = Column(Date)
#     article = relationship("Article")
#     publication = relationship("ArticlePublication")


class PublicationDate(Base):
    """Publication date of an article."""

    __tablename__ = "publication_dates"
    article = Column(Integer, ForeignKey("articles.id"), primary_key=True)
    date = Column(Date)
    articles = relationship("Article")


if __name__ == "__main__":
    from orion.core.airflow_utils import misctools
    from sqlalchemy import create_engine

    db_config = misctools.get_config("orion_config.config", "postgresdb")["database_uri"]
    engine = create_engine(db_config)

    # Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
