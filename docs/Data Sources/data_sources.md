# Data Sources #

## Core data sources ##

Orion depends on Microsoft Academic Graph for retrieving scientific documents.

### Microsoft Academic Graph ###

We collect scientific documents and their metadata from Microsoft Academic Graph (MAG) using the [Academic Knowledge API](https://www.microsoft.com/en-us/research/project/academic-knowledge/). We opted for MAG due to each data coverage and easy to use, expansive API. Find out more about using [MAG for the science of science](https://www.frontiersin.org/articles/10.3389/fdata.2019.00045/full).

### Collecting data from MAG ###

Orion offers multiple entry points to MAG; **users can collect scientific documents by querying MAG with conferences, journals or fields of study** (ie paper keywords). Some examples:

- Query Orion with a journal, `bioRxiv`, to collect all of the papers published in that repository.
- Query Orion with a conference, `ACL`, to collect all of the papers presented there.
- Collect all of the MAG papers containing one of the following Fields of Study; `misinformation`, **`disinformation`*,* `fake news`.

You can also collect scientific documents published within a **timeframe.** 

## Data enrichment ##

We enrich the MAG collection with the following data sources. Orion is **modular**; you can substitute them or choose not to run that part of Orion's ETL.

### Location data ###

We geocode author affiliations using [Google Places API](https://developers.google.com/places/web-service/intro). We use a two-step process to do the matching:

1. Find the unique Place ID that Google assigns to every place by querying its API with the affiliation names (same as you would do with Google Maps).
2. Query the Place ID to retrieve all of its details.

Note that the service is not free but it is cheap for small to medium size projects. An alternative could be [OpenStreetMap](https://wiki.openstreetmap.org/wiki/API_v0.6).

### Gender data ###

We infer the authors’ gender using the [GenderAPI](https://gender-api.com/). Orion removes any authors without a complete first name before feeding them to the API. For any downstream tasks, like the gender diversity indicator, Orion disregards matches with less than 70% accuracy.

A [review of name-to-gender inference systems](https://peerj.com/articles/cs-156/) suggested that the GenderAPI is overall the best performing Python service, however, it also revealed that its performance is not as good with Asian names as with European ones.

>Inferred genderisation assumes that gender identity is both a fixed and binary concept. This **does not** reflect reality as an individual might identify with a different gender from the one assigned at birth. Nevertheless, we decided to include this dimension in Orion since we believe it’s better to provide a partial view of this important issue than simply disregarding it. For those being sceptical about it, it is possible to use Orion without inferring the authors’ gender.

## Contextual data sources ##

We collect data sources that do not directly enrich MAG but provide contextual information about the scientific documents and the indicators we develop. 

### World Bank indicators ###

We collect country-level World Bank indicators using [pandas-datareader](https://github.com/pydata/pandas-datareader), a Python package that gives access to [economic databases](https://pandas-datareader.readthedocs.io/en/latest/readers/index.html). Orion's ETL currently collects the following indicators:

- [GDP (current US$)](https://data.worldbank.org/indicator/NY.GDP.MKTP.CD)
- [Research and development expenditure (% of GDP)](https://data.worldbank.org/indicator/GB.XPD.RSDV.GD.ZS)
- [Government expenditure on education, total (% of GDP)](https://data.worldbank.org/indicator/SE.XPD.TOTL.GD.ZS)
- [Ratio of female to male labour force participation rate (%) (modelled ILO estimate)](https://data.worldbank.org/indicator/SL.TLF.CACT.FM.ZS)

You can substitute or collect additional World Bank indicators by modifying Orion's ETL. 

### Country details ###

We collect country-level details like their population, country code and continent using the [restcountries API](https://github.com/apilayer/restcountries). We also homogenise any discrepancies between the naming conventions used by Google Places API and the World Bank.
