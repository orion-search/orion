from itertools import chain, combinations
from collections import OrderedDict, Counter
import numpy as np
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3


def unique_dicts(d):
    """Removes duplicate dictionaries from a list.

    Args:
        d (:obj:`list` of :obj:`dict`): List of dictionaries with the same keys.
    
    Returns
       (:obj:`list` of :obj:`dict`)
    
    """
    return [dict(y) for y in set(tuple(x.items()) for x in d)]


def unique_dicts_by_value(d, key):
    """Removes duplicate dictionaries from a list by filtering one of the key values.

    Args:
        d (:obj:`list` of :obj:`dict`): List of dictionaries with the same keys.
    
    Returns
       (:obj:`list` of :obj:`dict`)
    
    """
    return list({v[key]: v for v in d}.values())


def flatten_lists(lst):
    """Unpacks nested lists into one list of elements.

    Args:
        lst (:obj:`list` of :obj:`list`)

    Returns
        (list)
    
    """
    return list(chain(*lst))


def dict2psql_format(d):
    """Transform a dictionary with pandas Series to a list of dictionaries 
    in order to add it in PostgreSQL.

    Args:
        d (dict): Dictionary with pandas Series, usually containing RCA measurement.

    Returns:
        (:obj:`list` of :obj:`dict`)
    
    """
    return flatten_lists(
        [
            [
                {
                    "entity": idx[0],
                    "year": idx[1],
                    "rca_sum": elem,
                    "field_of_study_id": int(fos),
                }
                for idx, elem in series.iteritems()
            ]
            for fos, series in d.items()
        ]
    )


def inverted2abstract(obj):
    """Transforms an inverted abstract to abstract.
    
    Args:
        obj (json): Inverted Abstract.

    Returns:
        (str): Formatted abstract.
    
    """
    if isinstance(obj, dict):
        inverted_index = obj["InvertedIndex"]
        d = {}
        for k, v in inverted_index.items():
            if len(v) == 1:
                d[v[0]] = k
            else:
                for idx in v:
                    d[idx] = k

        return " ".join([v for _, v in OrderedDict(sorted(d.items())).items()])
    else:
        return np.nan


def cooccurrence_graph(elements):
    """Creates a cooccurrence table from a nested list.

    Args:
        elements (:obj:`list` of :obj:`list`): Nested list.

    Returns:
        (`collections.Counter`) of the form Counter({('country_a, country_b), weight})

    """
    # Get a list of all of the combinations you have
    expanded = [tuple(combinations(d, 2)) for d in elements]
    expanded = chain(*expanded)

    # Sort the combinations so that A,B and B,A are treated the same
    expanded = [tuple(sorted(d)) for d in expanded]

    # count the combinations
    return Counter(expanded)


def get_all_children(df, topics, lvl=1):
    """Traverses the Fields of Study tree to collect all the children FoS. For example,
    given a level 1 FoS, it will fetch all the level 2 children (A), the children of A,
    the children of the children of A [...] till it reaches the lowest level.

    Args:
        df (`pd.DataFrame`): Table with FoS IDs and their children.
        topics (:obj:`list` of int | int): Initially, it receives a single FoS. Then a list.
        lvl (int): Level of the initial FoS.

    Returns:
        t (:obj:`list` of int)

    """
    # For the first pass of the recursion, put the topic in a list
    if not isinstance(topics, list):
        topics = [topics]

    t = []
    t.extend(topics)
    t.extend(
        flatten_lists(
            [
                df[df.id == id_]["child_id"].values[0]
                for id_ in topics
                if df[df.id == id_]["child_id"].values[0] is not None
                and df[df.id == id_]["child_id"].values[0]
            ]
        )
    )

    if lvl == 5:
        # t.remove(t[0])
        return t
    else:
        return get_all_children(df, t, lvl + 1)


def average_vectors(vectors):
    """Averages vectors.

    Args:
        vectors (:obj:`list` of `numpy.array`)

    Returns:
        (numpy.ndarray) Average of the vectors of the shape.

    """
    return np.mean([v for v in vectors], axis=0)


def aws_es_client(host, port, region):
    """Create a client with IAM based authentication on AWS.
    Boto3 will fetch the AWS credentials.

    Args:
        host (str): AWS ES domain.
        port (int): AWS ES port (default: 443).
        region (str): AWS ES region.

    Returns:
        es (elasticsearch.client.Elasticsearch): Authenticated AWS client.

    """
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, "es")

    es = Elasticsearch(
        hosts=[{"host": host, "port": port}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )

    return es
