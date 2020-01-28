# orion
[![Build Status](https://travis-ci.org/orion-search/orion.svg?branch=dev)](https://travis-ci.org/kstathou/orion) [![Total alerts](https://img.shields.io/lgtm/alerts/g/orion-search/orion.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/orion-search/orion/alerts/) [![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/orion-search/orion.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/orion-search/orion/context:python)

Public information is fragmented across databases, making it difficult to spot gaps and opportunities in the life sciences research, track emerging topics and find collaborators. The goal of this project is to develop an open-source data collection, enrichment and analysis system that will enable users to identify relevant publications and track progress in the life sciences.

## Data ##
We are currently collecting data from the following sources:
* [bioRxiv](https://www.biorxiv.org/) (collected by the [Rxivist](https://rxivist.org/)): The largest, open repository of preprints in the life sciences.
* [Microsoft Academic](https://docs.microsoft.com/en-us/azure/cognitive-services/academic-knowledge/home): Microsoft’s knowledge base of academic publications. It’s used to enrich the bioarXiv papers.
* [Google Places](https://developers.google.com/places/web-service/intro): Google’s API that returns information about places, in this case, author affiliations.
* [Gender API](https://gender-api.com/en/): A name to gender inference system used to find the gender of authors.

Find out how these data sources are linked [here](/schema).
