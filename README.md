# orion
[![Build Status](https://travis-ci.org/orion-search/orion.svg?branch=dev)](https://travis-ci.org/kstathou/orion)

Orion is a research measurement and knowledge discovery tool that enables you to monitor progress in science, visually explore the scientific landscape and search for relevant publications. This repository contains Orion's data collection, enrichment and analysis pipeline for scientific documents from Microsoft Academic Graph. 

Learn more about Orion:
* [Orion: An open-source tool for the science of science](https://medium.com/@kstathou/orion-an-open-source-tool-for-the-science-of-science-4259935f91d4)
* [A walkthrough of Orion's backend, data and design decisions](https://medium.com/@kstathou/a-walkthrough-of-orions-backend-data-and-design-decisions-f60c01b507aa)
* [SciNLP | Orion: An interactive information retrieval system for scientific knowledge discovery](https://youtu.be/m0s5sjlpfAY)

Orion is an open-source tool. If you want to use our work or parts of it, be a good citizen of the Internet and drop us an acknowledgement. We would also love to know what you are developing so get in touch! 

## Installation ##
1. Clone Orion's ETL 

``` bash
git clone https://github.com/orion-search/orion
```

2. Orion using the `model_config.yaml` and the `.env` files as shown in this tutorial. 
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
- Create `docs/` website.
