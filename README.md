# Orion
[![Build Status](https://travis-ci.org/orion-search/orion.svg?branch=dev)](https://travis-ci.org/kstathou/orion)

Orion is a research measurement and knowledge discovery tool that enables you to monitor progress in science, visually explore the scientific landscape and search for relevant publications.

This repo contains Orion's data collection, enrichment and analysis pipeline for scientific documents from Microsoft Academic Graph. You can find the rest of our work in the following repositories:
- [Search engine](https://github.com/orion-search/search-engine)
- [Web-interface](https://github.com/orion-search/orion-search.org)
- [Talks, demos, papers and tutorials on Orion](https://github.com/orion-search/tutorials). Most of the content was made for presentations at venues such as the SciNLP, NetSci, IC2S2 and WOOC.
- [Micro-service deployment [WIP]](https://github.com/orion-search/universe)

To learn more about Orion, check out the **[documentation website](https://docs.orion-search.org/)**.  

Orion is open-source. If you want to use our work or parts of it, be a good citizen of the Internet and drop us an acknowledgement. We would also love to know what you are developing so get in touch! 

## Installation ##
1. Clone Orion's ETL 

``` bash
git clone https://github.com/orion-search/orion
```

2. Modify Orion using the `model_config.yaml` and the `.env` files as shown in [this tutorial](https://docs.orion-search.org/docs/running_etl). 
3. Run Orion's ETL in docker

``` bash
docker-compose up
```
4. Access and run Orion's DAG at

``` bash
http://localhost:8080/admin/
```

## TODO ##
- Update data schema.
- Change Airflow operators to kubernetes.
