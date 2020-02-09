# orion
[![Build Status](https://travis-ci.org/orion-search/orion.svg?branch=dev)](https://travis-ci.org/kstathou/orion) [![Total alerts](https://img.shields.io/lgtm/alerts/g/orion-search/orion.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/orion-search/orion/alerts/) [![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/orion-search/orion.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/orion-search/orion/context:python)

Knowledge and meta-knowledge are fragmented across databases, making it difficult to spot gaps and opportunities in research, track emerging topics and find collaborators. Orion depends on a flexible data collection, enrichment, and analysis system that enables users to create and explore research databases.

Read more on Orion [here](https://medium.com/@kstathou/orion-an-open-source-tool-for-the-science-of-science-4259935f91d4).

Orion is an open-source tool. If you want to use our work or parts of it, be a good citizen of the Internet and drop us an acknowledgement. We would also love to know what you are developing so get in touch! 

## Data ##
We are currently collecting data from the following sources:
* [Microsoft Academic](https://docs.microsoft.com/en-us/azure/cognitive-services/academic-knowledge/home): Microsoft’s knowledge base of academic publications. It’s used to enrich the bioarXiv papers.
* [Google Places](https://developers.google.com/places/web-service/intro): Google’s API that returns information about places, in this case, author affiliations.
* [Gender API](https://gender-api.com/en/): A name to gender inference system used to find the gender of authors.

Find out how these data sources are linked [here](/schema).
