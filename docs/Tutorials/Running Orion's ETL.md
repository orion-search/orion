This tutorial walks you through Orion's ETL. It shows you how to modify Orion's DAG, collect, enrich and analyse data from Microsoft Academic Graph.

## Get Orion ##

Clone the repository:

```bash
git clone https://github.com/orion-search/orion
```

## Modify `model_config.yaml` ##

`model_config.yaml` contains Orion's configuration parameters. Change the following: 

1. **Database name**

```yaml
data:
    db_name: 'arxiv_cl'
```

This is where Orion stores the MAG data.

**2. MAG query parameters**

```yaml
mag:
    query_values: ['arxiv computation and language']
    entity_name: 'J.JN'
    metadata: ["Id", "Ti", "AA.AfId", "AA.AfN", "AA.AuId", "AA.DAuN", "AA.S", "CC", "D", "F.DFN", "F.FId", "J.JId", "J.JN", "Pt", "RId", "Y", "DOI", "PB", "BT", "IA", "C.CN", "C.CId", "DN", "S"]
    with_doi: False
    mag_start_date: '2020-05-01'
    mag_end_date: '2020-05-30'
    intervals_in_a_year: 1
```

We will query MAG with `arxiv computation and language` and specify that is a journal using the `J.JN` in the `entity_name` field. `metadata` contains all the [MAG fields](https://docs.microsoft.com/en-us/academic-services/project-academic-knowledge/reference-paper-entity-attributes) that Orion collects. We set the `mag_start_date` and `mag_end_date` to 2020-05-01 and 2020-05-30 respectively to collect papers published in May 2020. We will collect documents even if they do not have a DOI. We can specify this by setting the `with_doi` parameter to `False`.

**3. S3 buckets**

```yaml
s3_buckets:
    mag: "arxiv-mag-data-dump"
    gender: "arxiv-names-batches"
    text_vectors: "arxiv-document-vectors"
    topic: "arxiv-mag-topics"
```

Set the S3 buckets that Orion will create and store intermediate data.

## Modify **the environmental variables in `.env`** ##

```
# postgresdb
arxiv_cl=postgres+psycopg2://postgres:admin@postgres:5432/arxiv_cl
postgres=postgres+psycopg2://postgres:admin@postgres:5432/postgres

# mag
mag_api_key=<MAG_API_KEY>

# NOT USED IN THE TUTORIAL
# google_api_key=<GOOGLE_API_KEY>

# NOT USED IN THE TUTORIAL
# gender_api=<GENDERAPI_AUTH>

# NOT USED IN THE TUTORIAL
# es_host=<AWS_ES_ENDPOINT>
# es_port=<AWS_ES_PORT>
# es_index=<AWS_ES_INDEX_NAME>
# region=<AWS_ES_REGION>

# docker-compose.yml ENV variables
DB_HOST=postgres
DB_PORT=5432
DB_USER=postgres
DB_PASS=admin
MAIN_DB=arxiv_cl
DB_NAME=airflow

AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
AWS_DEFAULT_REGION=<AWS_DEFAULT_REGION>
```

Create a `.env` file in the root directory and add the above. Alternatively, you can modify the `.env.example` file and rename it to `.env`. `.env` contains the environmental variables used in Orion. For this tutorial, we define the PostgreSQL databases, the MAG API key, the AWS credentials and the environmental variables used in Docker.

To run the tutorial, you need to create an **AWS account** and get an **API key for the Microsoft Academic Graph**. See External Dependencies for details.

## Build the docker container ##

Orion's ETL runs in a docker container. The `docker-compose.yml` has the instructions to create two containers; one for the PostgreSQL database and another one for Orion's data pipeline. Use the following command to **build** and **run** the containers:

```bash
docker-compose up
```

This should take some time to run. After the setup, you will receive a stream of messages about the PostgreSQL database and Airflow like the one below. 

![docker-compose](/docs/images/airflow-log.png)

If you need to make changes in a file after running `docker-compose up`, you should do the following to update the docker container:

```bash
docker-compose up --build
```

## Run the DAG with Airflow's UI ##

Airflow is running locally and you can access it at:

```
http://localhost:8080/admin/
```

Here, you can see all the DAGs in the `orion/core/dags` directory. Turn on the `tutorial` DAG and click on it.

![airflow-ui](/docs/images/airflow-ui-on.png)

You can inspect the DAG in different ways, let's choose the **Graph View.** You can see all the tasks and their dependencies in the DAG. Let's list the tasks in the `tutorial` DAG:

- Create Orion's PostgreSQL database and tables.
- Create AWS S3 buckets.
- Collect four indicators from the World Bank.
- Query Microsoft Academic Graph with the configuration parameters we set in step 2.
- Parse the Microsoft Academic Graph response and store it in the PostgreSQL database.
- Tag open access publications.
- Tag non-industry author affiliations.
- Collect metadata and calculate statistics on the Fields of Study used in the publications we retrieved.
- Assign a set of thematic topics to the academic publications based on their Fields of Study.

![airflow-dag](/docs/images/airflow-dag.png)

Click on **Trigger DAG.** It will run all tasks in the pipeline. The status of the DAG will change to **running** and the border of the tasks will turn **green** as they are completed successfully.

![airflow-successful-tasks](/docs/images/airflow-successful-tasks.png)

The run will be completed once the status of the DAG changes to **success.**

## Access the database ##

There are three ways to access the database.

### Using `psql` ###

If you have `psql` installed, type in your terminal:

```bash
psql arxiv_cl -h localhost postgres
```

You will be prompted to type your password which is `admin` (we set it in the `.env` file). 

### Using the docker container ###

If you do not have `psql`, do the following to open the the `postgres` container in interactive mode:

```bash
docker exec -it postgres psql -U postgres arxiv_cl
```

### Using SQLAlchemy ###

This is the most useful way if you plan to use the data in your analysis. You can either access the data from the docker container or export the database to your local PostgreSQL distribution. Let's do the latter.

While the docker container is running, type the following in a separate shell to store the `arxiv_cl` database in a `sql` file:

```bash
docker exec -t postgres pg_dump --no-owner -U postgres arxiv_cl > arxiv_cl_dump.sql
```

Shut down the container, open your local PostgreSQL distribution and type

```bash
psql -d arxiv_cl < arxiv_cl_dump.sql
```

to load the database dump in a local database named `arxiv_cl`. Note that you have to create the local database before loading the dump. You can then use Python's `SQLAlchemy` to read the data.

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

db_config = 'postgres+psycopg2://postgres@localhost:5432/arxiv_cl'
engine = create_engine(db_config)
Session = sessionmaker(engine)
s = Session()
```
