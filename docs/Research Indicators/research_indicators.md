# Research Indicators #
Orion produces metrics of scientific progress. One of the first questions we asked was **“What’s the right level of analysis?”**. We broke it down to **(1)** **time, (2) thematic topics and (3) entities and geography.** We opted for country-level indicators that track annual changes. We leveraged [MAG’s Fields of Study taxonomy](http://export.arxiv.org/pdf/1805.12216) to create a set of topics that are granular enough to make meaningful comparisons and broad enough to capture the diversity of the research topics in the data. 

## **Metrics** ##

### Research specialisation ###

We measure the specialisation of a country in a thematic topic using the [Revealed Comparative Advantage](https://en.wikipedia.org/wiki/Revealed_comparative_advantage) (RCA). RCA is an index used to quantify the relative advantage or disadvantage of a country at a certain class of products or services. Economists usually use it with trade data. 

In Orion, we measure the specialisation of a country in a thematic topic by summing its number of citations on it. Given a topic, a country with an RCA value above 1 is considered relatively specialised in it, while those with an RCA value below 1 are said to have a comparative disadvantage in it.

### Research diversity ###

We measure the **research diversity** (also called interdisciplinarity) of a country within a thematic topic. For a given topic, we recursively collect all of its children Fields of Study, create a count vector and measure the Shannon-Wiener and Simpson diversity indexes.

### Gender diversity ###

We measure the average share of women co-authors of a country for a thematic topic. For a given topic, we first find the share of women in each publication and then average it to create the indicator.
