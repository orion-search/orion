from datetime import datetime
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text


class PaperES(Document):
    # ES mappings
    original_title = Text(analyzer="standard", fields={"raw": Keyword()})
    abstract = Text(analyzer="standard")
    year = Keyword()
    publication_date = Date()
    field_of_study = Text(fields={"raw": Keyword()})
    field_of_study_id = Text(multi=True)

    class Index:
        # Index name
        name = "mag_papers"

        settings = {
            "number_of_shards": 2,
        }

    def save(self, **kwargs):
        return super(PaperES, self).save(**kwargs)
