# Core #
Orion runs on [Apache Airflow](https://github.com/apache/airflow). Core contains the DAG components and the ORMs used to connect to PostgreSQL DB.

## Config file ##
All of the credentials are stored in a config file that is not synced on GitHub. You should create a file in the following path:

```
orion/orion/core/config/orion_config.config
```

and with the following format:

```
[postgresdb]
DATABASE_URI=postgres+psycopg2://USER:PASSWORD@localhost:5432/bioarxiv
TEST_URI=postgres+psycopg2://USER:PASSWORD@localhost:5432/postgres

[mag]
MAG_API_KEY=MY_MAG_API_KEY

[google]
GOOGLE_KEY=MY_GOOGLE_API_KEY

[genderapi]
GENDER_API_KEY=MY_GENDER_API_KEY
```

### Notes ###
* **Important**: [`misctools.py`](https://github.com/kstathou/orion/blob/dev/orion/core/airflow_utils/misctools.py) is currently being used to pick up the required parts of the config file. This will probably be replaced in the future by `dotenv`. Thus, the format of the congif file will change too. 
* The database is actually stored on Amazon RDS. This means that you need an AWS account to access the data and the URI differs from the one above.

## How to setup PostgreSQL ##
Install PostgreSQL:
* For macOS users, the fastest way is to download the [Postgres.app](https://postgresapp.com/) and follow the installation instructions. To connect to a database, make sure that the app is running.
* For all other users, you should be able to find a suitable distribution [here](https://www.postgresql.org/download/).
* Set `listen_addresses = '*'` in the `postgresql.conf`.

## How to connect to the AWS RDS instance from the Command Line ##
You can connect to the PostgreSQL DB on AWS using IAM Authentication ([official instructions here](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.AWSCLI.PostgreSQL.html)):
1. Install the command line for AWS (preferably in a virtual environment).

``` python
pip install awscli
```

2. Generate an IAM Authentication Token. This will generate a very long string that will be stored as an environmental variable.

``` bash
export PGPASSWORD="$(aws rds generate-db-auth-token --hostname MY-DB-ENDPOINT --port 5432 --region MY-REGION --username MY-USER-NAME )"
```

3. Connect to the DB instance.

``` bash
psql "host=MY-DB-ENDPOINT port=5432 dbname=MY-DB-NAME user=MY-USER-NAME"
```

**Note**: You can find the required details on the AWS RDS console.

## How to connect to the AWS RDS instance using SQLAlchemy ##
Assuming you have already installed the AWS CLI, you can run the following to start a session:

``` python
import boto3
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

client = boto3.client('rds', region_name=MY-REGION)
token = client.generate_db_auth_token(MY-DB-ENDPOINT, 5432, MY-USER-NAME)
engine = create_engine(sqlalchemy.engine.url.URL('postgres+psycopg2', username=MY-USER-NAME, password=token, host=MY-DB-ENDPOINT, port=5432, database=DBNAME))
session = sessionmaker(engine)
s = session()
```

You can then use the session to query the data.

## How to connect to a local PostgreSQL DB ##

TODO: how to complete setup and use postgresdb.  

<!-- Then, run `python mag_orm.py` to create the project's database (`orion`) and its tables. -->

<!-- Note that the `.env` file contains two connections to PostgreSQL in the following format: -->

<!-- ``` python -->
<!-- postgresdb = postgres+psycopg2://postgres@localhost/orion -->
<!-- test_postgresdb = postgres+psycopg2://postgres@localhost/postgres -->
<!-- ``` -->

<!-- `orion`: the project's database.   -->
<!-- `postgres`: default database that is shipped with PostgreSQL and used here for testing the ORMs. -->

## Working with Apache Airflow ##
[Apache Airflow](https://airflow.apache.org/) is an open-source tool for programmatically author, schedule and monitor workflows. The workflows are defined directed acyclic graphs (DAGs) of tasks. 

We use Airflow to _glue_ our work together. Developers can rerun parts of the project (or all of it) using the command line or Airflow's neat UI. We also get a helpful overview of what we have already done by looking at the generated DAGs. Lastly, it is fairly straightforward (albeit costly) to schedule the dag to run in regular intervals. 

### How to install and configure Apache Airflow ###

1. Install [Anaconda](https://www.anaconda.com/) and create a [virtual environment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html).
2. Use `conda activate MYENV` to activate the environment you created and install Airflow with:

``` bash
conda install -c conda-forge airflow
```
3. Create a directory where the Airflow stuff (DAGs, operators, config files etc) will be stored. In this project, the directory is named `core`. Skip this step if you have cloned the repo.
4. Use the following command to create an environmental variable with the path to the directory:

``` bash
export AIRFLOW_HOME=`pwd`/core
```
5. Run the `airflow version` command to create Airflow's default configuration (don't worry if you receive an error message). Another file, called `airflow.db` might also be created - please delete it. By default, Airflow creates a SQLite Database which we are not using.
6. You have to make a few changes in `airflow.cfg`
   * Set `executor = LocalExecutor`
   * Many variables, such as the `sql_alchemy_conn`, point to the SQLite DB we deleted before. Change them to the PostgreSQL DB we use.

**Note**: The DAG has been tested and runs locally, with a local PostgreSQL DB. It still hasn't been tested with a remote DB.

### How to run the DAG ###
Assuming that everything has worked out well so far, running a DAG is straightforward. In two separate of the command line, do:

``` bash
$ airflow webserver
```
to activate the webserver (Airflow's UI) and 

``` bash
$ airflow scheduler
```
to turn on the scheduler. You should now be able to click on the DAG and **Trigger** its execution.

### What's in the DAG ###
There is a set of tasks and the DAG shows how they depend on each other. In Orion, every task fetches data from a source, usually a PostgreSQL DB or AWS S3, applies a set of transformations and stores them in a PostgreSQL DB or AWS S3. Here I will briefly explain what every operator does:

#### `start` ####
Dummy task to initiate the DAG. Used only for clarity.

#### `query_mag` ####
* Source: Microsoft Academic Graph API.
* Action: For a given query (conference, journal or Field of Study name), retrieve all of the papers.
* Target: AWS S3

#### `parse_mag` ####
* Source: AWS S3
* Action: Parse the response from MAG.
* Target: PostgreSQL DB

#### `geocode_places` ####
* Source: PostgreSQL DB, Google Places API
* Action: Geocode affiliations using Google Places API.
* Target: PostgreSQL DB

#### `collect_fos_metadata` ####
* Source: Microsoft Academic Graph API, PostgreSQL DB
* Action: Retrieve metadata (child nodes, parent nodes, level in hierarchy) for every queried Field of Study.
* Target: PostgreSQL DB

#### `text2vector` ####
* Source: PostgreSQL DB
* Action: Transform text to vector.
* Target: AWS S3

#### `batch_names` ####
* Source: PostgreSQL DB
* Action: Fetch author names and split them into batches.
* Target: AWS S3

#### `gender_inference_NUM` ####
* Source: AWS S3, GenderAPI
* Action: Fetch author names and query them on GenderAPI to infer their gender.
* Target: PostgreSQL DB

#### `fos_frequency` ####
* Source: PostgreSQL DB
* Action: Find the frequency of Fields of Study.
* Target: PostgreSQL DB

#### `dim_reduction` ####
* Source: AWS S3 
* Action: Transform a set of high dimensional vectors to 2D and 3D.
* Target: PostgreSQL DB

#### `country_collaboration` ####
* Source: PostgreSQL DB
* Action: Create a cooccurrence network of countries based on paper co-authorship.
* Target: PostgreSQL DB

#### `filter_topics` ####
* Source: PostgreSQL DB
* Action: Filter Fields of Study based on their level and frequency.
* Target: AWS S3

#### `gender_agg` ####
Dummy task used only for clarity (reduces the edges towards downstream tasks).

#### `topic_metadata` ####
* Source: AWS S3
* Action: Calculate the citation sum and paper count for every topic (including its children).
* Target: AWS S3

#### `rca_measurement` ####
* Source: PostgreSQL DB
* Action: Calculate the comparative advantage of every topic for each country and year.
* Target: PostgreSQL DB

#### `gender_diversity` ####
* Source: PostgreSQL DB
* Action: Calculate the gender diversity of every topic for each country and year.
* Target: PostgreSQL DB

#### `research_diversity` ####
* Source: PostgreSQL DB
* Action: Calculate the a variety of within-discipline diversity metrics for every topic, each country and year.
* Target: PostgreSQL DB

#### `faiss_index` ####
* Source: AWS S3
* Action: Create a FAISS index using the document vectors and the paper IDs from MAG.
* Target: AWS S3
