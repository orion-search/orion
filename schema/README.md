# Schema #
There is a separate `yaml` file with the schema for each PostgreSQL table.

## Entity relation diagram ##

![ER Diagram](schema.png?raw=true)

## Notes ##
* Do not use `crossref_daily` table. The API is not operational according to the [Rxivist documentation](https://zenodo.org/record/3558312#.XgXzlxczZQI).
* All the tables starting with the `mag` prefix were collected from [Microsoft Academic](https://docs.microsoft.com/en-us/azure/cognitive-services/academic-knowledge/home). They should be the primary source for most of the metadata used in analysis and data visualisation in order to ensure our work can be used in a different context (ie without using the bioarXiv database). 
* Apart from the tables with the `mag` prefix and `geocoded_places` which originates from Google Places API, the rest of the tables are from the [Rxivist](https://docs.microsoft.com/en-us/azure/cognitive-services/academic-knowledge/home).
* The link between `articles` and `mag_papers` tables is **virtual** and does not actually happen in the codebase in order to be as modular as possible (bioarXiv might be dropped in another future but the Microsoft Academic tables will be used). For the later stages of the analysis, these two tables will be joined on `doi`.
