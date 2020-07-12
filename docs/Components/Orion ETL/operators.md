# Airflow Operators in Orion #
Every operator fetches data from a **PostgreSQL** database, **AWS S3** or an **API**, applies a set of transformations and stores them in a PostgreSQL database or AWS S3. We list Orion's operators below:

## `AffiliationTypeOperator` ##

Splits the affiliations to industry (= 0) and non-industry (= 1) using a seed list of tokens. The seed list can be found in the `model_config.yaml`.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `RCAOperator` ##

Measures the country-level and affiliation-level thematic topic specialisation on an annual basis using the Revealed Comparative Advantage (RCA). It creates this indicator by summing the number of citations of a country (or affiliation) on a particular topic. An entity with an RCA > 1 is more specialised on a topic than the rest of the entities. Before measuring RCA, it filters publications by year and the countries by their number of papers. You can change the thresholds in the `model_config.yaml`.

Note that instead of summing citations to measure the RCA, you can use the [number of papers.](https://github.com/orion-search/orion/blob/dev/orion/packages/metrics/metrics.py#L59) 

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `ResearchDiversityOperator` ##

Measures the country-level research diversity for each topic on an annual basis. For each topic, it recursively collects its children topics and creates a country-level count vector. It measures diversity with the Shannon-Wiener and Simpson indexes. You can filter countries by the number of Fields of Study they have used and the papers by their publication year. You can modify the thresholds in the `model_config.yaml`.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `GenderDiversityOperator` ##

Measures the country-level gender diversity for each topic on an annual basis. For each topic and year, it first finds the share of women in each publication and then averages it to create the indicator. You can filter countries by their number of publications and author names by the accuracy of the name-gender-inference service. You can modify the thresholds in the `model_config.yaml`.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `WBIndicatorOperator` ##

Collects country-level World Bank indicators using [pandas-datareader](https://github.com/pydata/pandas-datareader), a Python package that gives access to [economic databases](https://pandas-datareader.readthedocs.io/en/latest/readers/index.html). You can choose indicators and filter them by timeframe and country in the `model_config.yaml`. 

>If you plan to collect indicators that are not listed here, you have to create an ORM for them.

**Source**: World Bank API  
**Target**: PostgreSQL

## `HomogeniseCountryNamesOperator` ##

Homogenises country names from Google Places API and the World Bank. It uses a country mapping dictionary from `model_config.yaml`.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `CountryDetailsOperator` ##

Fetches additional country details such as continent, country code and population from the [restcountries API](https://restcountries.eu/). 

**Source**: PostgreSQL, restcountries API  
**Target**: PostgreSQL

## `CreateVizTables` ##

Creates the following tables that are used in the front-end:

- `CountryTopicOutput`: Shows a country's total citations and paper volume by year and topic.
- `AllMetrics`: Combines all the metrics (gender diversity, research diversity, RCA) we've derived by year and topic.
- `PaperCountry`: Shows the paper IDs of a country. Used in the particle visualisation.
- `PaperTopics`: Shows the paper IDs of a topic. Used in the particle visualisation.
- `PaperYear`: Shows the paper IDs of a year. Used in the particle visualisation.

>Topics are fetched from the `FilteredFos` table.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `DimReductionOperator` ##

Transforms high dimensional arrays to 3D using UMAP. The output table is used in the front-end. You can filter documents by their character length or exclude them using their MAG ID. You can also tune UMAP's hyperparameters. This configuration is done in the `model_config.yaml`.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `CountryCollaborationOperator` ##

Draws a collaboration graph between countries based on the author affiliations. Papers are filtered by their publication year. You can modify this in the `model_config.yaml`.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `CountrySimilarityOperator` ##

Finds the similarity between countries based on their abstracts. It averages the abstract vectors of a country to create a country vector. Uses the text vectors that were calculated from the `text2vector` task. You can filter papers by publication year and choose the number of similar countries to return by modifying the `model_config.yaml`.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `FaissIndexOperator` ##

Creates a FAISS index using the text vectors that were calculated from the `text2vector` task.

**Source**: PostgreSQL  
**Target**: AWS S3

## `NamesBatchesOperator` ##

Fetches authors' full names and removes those with just an initial. It stores the rest of the author names in batches. You can specify the number of batches in the `model_config.yaml`. 

**Source**: PostgreSQL  
**Target**: AWS S3

## `GenderInferenceOperator` ##

Infers an authors' gender using their full name. This is a batch task and you have to define the number of batches in the `NamesBatchesOperator`.

**Source**: AWS S3  
**Target**: PostgreSQL

## `MagCollectionOperator` ##

Queries Microsoft Academic Knowledge API with a conference, journal or field of study. You can modify the task by tweaking the following parameters in the `model_config.yaml`:

- `with_doi`: If `true`, it collects only documents with a DOI.
- `mag_start_date`: Starting date for the data collection. This corresponds to the paper publication date.
- `mag_end_date`: Ending date for the data collection. This corresponds to the paper publication date.
- `intervals_in_a_year`: The number of time periods in a year. MAG throttles computationally costly queries. The higher the number of the `intervals_in_a_year`, the lower the chance for your request to be throttled.
- `metadata`: The fields from MAG to be collected.
- `query_values`: Name of your query (for example, *biorxiv*),
- `entity_name`: Specify if you are collecting a journal, conference or field of study.

>Possible values in the `entity_name`:
>- Field of study: `F.FN`
>- Conference: `C.CN` 
>- Journal: `J.JN`

**Source**: Microsoft Academic Knowledge API  
**Target**: AWS S3

## `MagFosCollectionOperator` ##

Fetches Fields of Study IDs and collects their level in the hierarchy, child and parent Fields of Study from Microsoft Academic Graph.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `GeocodingOperator`

Geocodes affiliations names.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `MagParserOperator` ##

Fetches and parses MAG responses and populates Orion's database.

**Source**: AWS S3  
**Target**: PostgreSQL

## `FosFrequencyOperator` ##

Calculates the frequency of Fields of Study.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `OpenAccessJournalOperator` ##

Splits the journals to non-open access (= 0) and open access (= 1) using a seed list of tokens. You can modify the seed list in the `model_config.yaml`.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `Postgreqsl2ElasticSearchOperator` ##

Migrates some data from PostgreSQL to Elasticsearch and creates an index with the following metadata for every paper:

- Title
- Abstract
- Citations
- Publication date
- Publication year
- Field of study ID
- Field of study name
- Author name
- Author affiliation

By default, it deletes the index (if it exists) before indexing documents. You can modify this in the `model_config.yaml`.

**Source**: PostgreSQL  
**Target**: AWS Elasticsearch service

## `Text2VectorOperator` ##

Uses a pretrained model (DistilBERT) from the [transformers](https://github.com/huggingface/transformers) library to create word vectors which are then averaged to produce a document vector. 

>Using a GPU for this task will decrease the vector inference time.

**Source**: PostgreSQL  
**Target**: PostgreSQL

## `Text2TfidfOperator` ##

Transforms text to vectors using TF-IDF and SVD. TF-IDF from scikit-learn preprocesses the data and SVD reduces the dimensionality of the document vectors. 

>This is the fastest text vectorisation method in Orion and should be your first choice.

**Source**: PostgreSQL  
**Target:** PostgreSQL

## `Text2SentenceBertOperator` ##

Creates sentence-level embeddings using a pretrained [sentence DistilBERT model](https://github.com/UKPLab/sentence-transformers). It batches documents to speed up the inference. You can modify the batch size and choose another BERT model in the `model_config.yaml`.

>Using a GPU for this task will decrease the vector inference time.

**Source**: PostgreSQL  
**Target:** PostgreSQL

## `FilterTopicsByDistributionOperator` ##

Filter topics by level in the MAG hierarchy and frequency. These topics are used to calculate the Orion's metrics and are shown in the front-end. You can modify the level and frequency thresholds in the `model_config.yaml`.

**Source**: PostgreSQL  
**Target**: AWS S3

## `FilteredTopicsMetadataOperator` ##

Creates a table with the filtered Fields of Study, their children, annual citation sum and paper count.

**Source**: AWS S3  
**Target**: PostgreSQL

## `PythonOperator("S3_BUCKET_NAME")` ##

Creates Orion's S3 buckets. The Airflow task is named after the S3 bucket it creates.

## `PythonOperator("create_tables")` ##

Creates Orion's PostgreSQL tables and instantiates a database if it does not exist.

## `DummyOperators` ##

Orion uses a number of `DummyOperators` to ease the visual navigation of the DAG. These tasks do not transform or move data around. We list them below:

```python
dummy_task = DummyOperator(task_id="start")
dummy_task_2 = DummyOperator(task_id="gender_agg")
dummy_task_3 = DummyOperator(task_id="world_bank_indicators")
dummy_task_4 = DummyOperator(task_id="create_s3_buckets")
dummy_task_5 = DummyOperator(task_id="s3_buckets")
```
