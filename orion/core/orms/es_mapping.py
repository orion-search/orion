from datetime import datetime
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text, Nested, Long


class PaperES(Document):
    # ES mappings
    original_title = Text(analyzer="standard", fields={"raw": Keyword()})
    abstract = Text(analyzer="standard")
    year = Keyword()
    publication_date = Date()
    citations = Integer()
    fields_of_study = Nested(
        properties={"name": Text(fields={"raw": Keyword()}), "id": Long()},
        include_in_parent=True,
    )
    authors = Nested(
        properties={
            "name": Text(fields={"raw": Keyword()}),
            "affiliation": Text(fields={"raw": Keyword()}),
        }
    )

    class Index:
        # Index name
        name = "mag_papers"

        settings = {"number_of_shards": 2, "number_of_replicas": 2}

    def save(self, **kwargs):
        return super(PaperES, self).save(**kwargs)
