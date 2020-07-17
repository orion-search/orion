# The `model_config.yaml` file #
`model_config.yaml` contains Orion's configuration parameters. Its purpose is to increase Orion's transparency and give you an easy way to modify its data pipeline. `model_config.yaml` **does not** contain API keys, database configurations and other personal information. These are kept in a `.env` file and are loaded as environmental variables.

## Example of a `model_config.yaml` ##

We will explain the content of the `model_config.yaml` by using the example below.

### Naming the database ###

```yaml
data:
    db_name: 'misinformation_deployment'
```

Creates a database named `misinformation_deployment` and instantiates the required tables.

### Querying Microsoft Academic Knowledge API ###

```yaml
data:
    mag:
        query_values: ['biorxiv', 'medrxiv']
        entity_name: 'J.JN'
        metadata: ['Id', 'Ti', 'S']
        with_doi: True
        mag_start_date: '2000-01-01'
        mag_end_date: 'today'
        intervals_in_a_year: 2
```

Orion creates a composite query that will retrieve papers published (`entity_name`) in bioRxiv or medRxiv (`query_values`). It will only collect papers with a DOI (`with_doi`) that were published between 01-01-2000 (`mag_start_date`) and today, whenever that is (`mag_end_date`). Orion will query each year separately, split it in two periods (`intervals_in_a_year`) and for every paper, it will collect its id, title and source (`metadata`).

**How to query MAG with a conference name?**

Change the `query_values` and `entity_name`. For example, the snippet below will collect papers presented at the ACL.

```yaml
mag:
    query_values: ['acl']
    entity_name: 'C.CN'
```

**How to query MAG with a field of study?**

Change the `query_values` and `entity_name`. For example, the snippet below will collect papers with machine learning or deep learning as a field of study.

```yaml
mag:
    query_values: ['machine learning', 'deep learning']
    entity_name: 'F.FN'
```

The journal, conference and field of study must match the Microsoft Academic Graph format. To find the right format, start typing your query to the [Microsoft Academic](https://academic.microsoft.com/home) and it will recommend you the appropriate format.

`query_values` can take multiple inputs, as we did in the first example. Note that this works as an OR query; Orion will retrieve papers published either in bioRxiv or medRxiv, or in both.

### Collecting World bank indicators ###

```yaml
data:	
    wb:
        end_year: "2019"
        country: "all"
        table_names: ["wb_gdp"]
        indicators: ["NY.GDP.MKTP.CD"]
```

Collects data on the [NY.GDP.MKTP.CD](https://data.worldbank.org/indicator/NY.GDP.MKTP.CD) World Bank indicator (`indicators`) for every country (`country`) and year till 2019 (`end_year`). It stores the data in a table named `wb_gdp` (`table_names`). You can collect multiple indicators by extending the extending the `indicators` and `table_names` lists.

You have to create an ORM and create the table for each new indicator.

By default, Orion fetches the following indicators and creates their corresponding tables:

```yaml
data:
	wb:
        end_year: "2019"
        country: "all"
        table_names: ["wb_gdp", "wb_rnd_expenditure", "wb_edu_expenditure", "wb_female_workforce"]
        indicators: ["NY.GDP.MKTP.CD", "GB.XPD.RSDV.GD.ZS", "SE.XPD.TOTL.GD.ZS", "SL.TLF.CACT.FM.ZS"]
```

### Creating and populating the Elasticsearch index ###

```yaml
elasticsearch:
    erase_index: True
```

Erases the existing Elasticsearch index when the `erase_index` is set to `True`. Set to `False` if you want to add documents to the existing index.

### Transforming text to vectors with Sentence Transformers ###

```yaml
sentence_bert:
    bert_model: 'distilbert-base-nli-stsb-mean-tokens'
    batch_size: 1000
```

Uses a pretrained sentence-DistilBERT (`bert_model`) and to encode batches of 1,000 documents (`batch_size`). You can choose any transformer model from this [list](https://docs.google.com/spreadsheets/d/14QplCdTCDwEmTqrn1LH4yrbKvdogK4oQvYO1K1aPR5M/edit?usp=gmail#gid=0).

### Fitting UMAP ###

```yaml
umap:
    n_neighbors: 10
    n_components: 2
    metric: 'cosine'
    min_dist: .01
    exclude: [2767711857, 2783409909]
```

Reduces the dimensionality of the text embeddings with UMAP. You can provide a list of paper IDs to exclude before the model fitting. Leave the list empty if you want to fit a UMAP with all the documents (recommended for the first run).

```yaml
umap:
    n_neighbors: 10
    n_components: 2
    metric: 'cosine'
    min_dist: .01
    exclude: []
```

>You may need to experiment with UMAP and run it multiple times to get the desired low dimensional projection. Read the [UMAP documentation](https://umap-learn.readthedocs.io/en/latest/parameters.html) and this [blog](https://pair-code.github.io/understanding-umap/) on how to tune UMAP effectively.  

### Transforming text to vectors with TFIDF and SVD ###

```yaml
tfidf:
    max_features: 120000

svd:
    n_components: 500
```

Vectorises documents with TFIDF and reduces their dimensionality with Singular Value Decomposition. You can change the length of the TFIDF and SVD vector by modifying the `max_features` and `n_components` parameters respectively. 

### Creating a country collaboration network ###

```yaml
country_collaboration:
    year: '2013'
```

Considers academic documents published after 2013 (`year`). 

### Selecting topics for the research indicators and the visualisations ###

```yaml
topic_filter:
    levels: [1]
    percentiles: [75]
```

Orion leverages [MAG's Field of Study taxonomy](https://arxiv.org/abs/1805.12216) to create a set of topics that are granular enough to make meaningful comparisons and broad enough to capture the diversity of the research topics in the data. In the snippet above, it considers only the `level` one topics and the Fields of Study with a frequency in the 75th `percentile`. 

You can also select topics from multiple levels and percentiles. For example, this snippet would consider the levels one and three and the Fields of Study with a frequency in the 75th and the 50th percentiles respectively. 

```yaml
topic_filter:
    levels: [1,3]
    percentiles: [75, 50]
```

### Removing low accuracy name-to-gender matches ###

```yaml
gender_diversity:
    threshold: .75
```

Consider name-to-gender matches with an accuracy higher than .75% (`threshold`). We advise keeping this threshold relatively high and monitor how it affects the metrics. 

### Selecting research indicators parameters ###

```yaml
metrics:
    year: '2013'
    paper_count_low: 10
    fos_count: 15
```

Considers academic documents published after 2013 (`year`) and filters countries prior to calculating the research indicators:

- **Research specialisation**: Filters countries with less than 10 publications in a year (`paper_count_low`)
- **Gender diversity**: Filters countries with less than 10 publications in a year (`paper_count_low`)
- **Research diversity:** Filters countries with less than 15 academic documents containing the field of study that the indicator is measured for (`fos_count`).

Use thresholds to remove any countries with a low number of academic documents to avoid procuding misleading indicators.

### Creating S3 buckets ###

```yaml
s3_buckets:
    mag: "mag-data-dump"
    gender: "names-batches"
    text_vectors: "document-vectors"
    topic: "mag-topics"
```

Creates four S3 buckets that are used in Orion's pipeline: 

- `mag-data-bucket` contains the raw response from MAG API.
- `names-batches` contains batched author names that Orion queries GenderAPI with.
- `document-vectors` contains the Faiss index that is used in the search engine. When using the TFIDF+SVD approach to vectorise documents, Orion stores the TFIDF vectors in this bucket too.
- `mag-topics` contains the filtered topics that are used to produce the research indicators and the visualisations.

You can rename these buckets. 

### Creating prefixes for output files ###

```yaml
prefix:
    gender: "batched_names"
    text_vectors: "doc_vectors"
    topic: "filtered_topics"
```

Adds a prefix to files before storing them on S3. These are intermediate outputs used in downstream tasks. You don't need to change them.

### Inferring authors' gender ###

```yaml
batch_size: 20000
parallel_tasks: 4
```

Orion creates four batches (`parallel_tasks`) with 20,000 author names each (`batch_size`) before querying them to GenderAPI, a name-to-gender inference service. If you are running Orion for a new data collection, you are advised to set the `batch_size` high enough to cover all author names. If you do not do so, some of them will not be passed to the GenderAPI. No need to worry though. Orion's ETL label the rest of the names once you rerun that part of the pipeline.

>Consider how many cores your machine has before choosing the number of `parallel_tasks`.

### Mapping country names ###

```yaml
country_name_mapping:
    google2wb : {'Czechia':'Czech Republic', "Côte d'Ivoire":"Cote d'Ivoire"}
    google2restcountries : {'United States':'United States of America'}
```

Homogenises country names between the Google Places API and the World Bank (`google2wb`) and the Google Places API and restcountries API (`google2restcountries`). Orion's `[model_config.yaml](https://github.com/orion-search/orion/blob/dev/model_config.yaml)` contains the full mapping and you do not have to make any changes.

### Identifying non-industry affiliations ###

```yaml
affiliations:
    non_profit: ['university', 'foundation', 'institution', 'universidade']
```

Uses a hand-crafted list of terms or names to tag an author affiliation as non-industry or industry. You can find and modify the full list in Orion's `[model_config.yaml](https://github.com/orion-search/orion/blob/dev/model_config.yaml)`.

### Identifying open access publications ###

```yaml
open_access: ['arxiv computation and language']
```

Uses a hand-crafted list of terms or journal names to tag an academic publication as open access. You can find and modify the full list in Orion's `[model_config.yaml](https://github.com/orion-search/orion/blob/dev/model_config.yaml)`.
