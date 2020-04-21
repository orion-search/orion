# orion
[![Build Status](https://travis-ci.org/orion-search/orion.svg?branch=dev)](https://travis-ci.org/kstathou/orion)

Knowledge and meta-knowledge are fragmented across databases, making it difficult to spot gaps and opportunities in research, track emerging topics and find collaborators. Orion depends on a flexible data collection, enrichment, and analysis system that enables users to create and explore research databases.

Learn more about Orion by reading the following non-technical blogs:
* [Orion: An open-source tool for the science of science](https://medium.com/@kstathou/orion-an-open-source-tool-for-the-science-of-science-4259935f91d4)
* [A walkthrough of Orion's backend, data and design decisions](https://medium.com/@kstathou/a-walkthrough-of-orions-backend-data-and-design-decisions-f60c01b507aa)

Orion is an open-source tool. If you want to use our work or parts of it, be a good citizen of the Internet and drop us an acknowledgement. We would also love to know what you are developing so get in touch! 

## Data ##
We are currently collecting data from the following sources:
* [Microsoft Academic](https://docs.microsoft.com/en-us/azure/cognitive-services/academic-knowledge/home): Microsoft’s knowledge base of academic publications. It’s used to enrich the bioRxiv papers.
* [Google Places](https://developers.google.com/places/web-service/intro): Google’s API that returns information about places, in this case, author affiliations.
* [Gender API](https://gender-api.com/en/): A name to gender inference system used to find the gender of authors.
* [pandas-datareader](https://github.com/pydata/pandas-datareader): Extracts data from a wide range of Internet sources into a pandas DataFrame. Here, we collect indicators from the World Bank.
* [restcountries API](https://restcountries.eu/): Fetches information about countries.

Find out how these data sources are linked [here](/schema).

## Installation ##
1. Clone the repo
`$ git clone https://github.com/orion-search/orion`

2. `cd` in the repo and install the requirements and the orion package.

``` bash
$ pip install -r requirements.txt
$ pip install -e .
```

3. Setup Airflow. 
   - Export path to Airflow HOME: `export AIRFLOW_HOME=/Users/USERNAME/Desktop/orion/orion/core`. For convenience, add this in the `~/.bash_profile`.
   - Create a database named `airflow` to store Airflow's metadata: `create database airflow;`
   - Run `airflow version` or `airflow initdb`. This might return an error but will generate some needed files in `orion/core` such as `airflow.cfg` and `unittests.cfg`.
   - In both `airflow.cfg` and `unittests.cfg`, change the following:
   
   ```
   sql_alchemy_conn = postgres+psycopg2://USER:PASSWORD@localhost:5432/airflow
   executor = LocalExecutor
   ```
   
   - Delete the `airflow.db` file `$ rm orion/core/airflow.db`
4. Create a `.env` file with these [variables](.env.example).

It is recommended to use Orion in a virtual environment, preferably Anaconda. Orion's backend uses a PostgreSQL database. Download a PostgreSQL distribution if you plan to use Orion locally, otherwise, setup a PostgreSQL database on AWS RDS. 

This repo has been tested for Python 3.6 and 3.7.

## How to use Orion ##
After installing Orion, you can customise the data collection by changing the `query_values` and `entity_name` in the `model_config.yaml` file. 

You also need to create accounts on Google Cloud, Microsoft Academic Graph and GenderAPI to get the required API keys. See instructions [here](/orion/packages/README.md).

You can then access Airflow's UI by doing the following:
1. Start Airflow's webserver with `$ airflow webserver`
2. Open a new tab on your terminal and start Airflow's scheduler with `$ airflow scheduler`
3. Go to http://localhost:8080/, toggle orion to ON and click on it. You should be able to see the DAG now.
4. Click on **Trigger** to execute the whole DAG
