"""
This code is based on the work we have done at Nesta. You can find the original here: https://github.com/nestauk/nesta/blob/dev/nesta/packages/mag/query_mag_api.py
"""
import logging
import requests
from collections import defaultdict
from retrying import retry
from alphabet_detector import AlphabetDetector

ENDPOINT = "https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate"


def prepare_title(title):
    """Replaces non-alphanums from a paper title, allowing foreign characters and cleans
    up multiple spaces and trailing spaces.

    Args:
        title (:obj:`str`): The title of the paper.

    Returns:
        (:obj:`str`): Cleaned title.

    """
    detector = AlphabetDetector()
    if title is None:
        return ""
    result = "".join(
        [
            x if len(detector.detect_alphabet(x)) > 0 or x.isnumeric() else " "
            for x in title.lower()
        ]
    )
    # Recursively remove spaces
    while "  " in result:
        result = result.replace("  ", " ")
    # Remove trailing spaces
    if result[-1] == " ":
        result = result[0:-1]
    return result


def build_expr(query_items, entity_name, max_length=16000):
    """Builds and yields OR expressions for MAG from a list of items. Strings and
    integer items are formatted quoted and unquoted respectively, as per the MAG query
    specification.

    The maximum accepted query length for the api appears to be around 16,000 characters.

    Args:
        query_items (:obj:`list`): All items to be queried.
        entity_name (:obj:`str`): The mag entity to be queried ie 'Ti' or 'Id'.
        max_length (:obj:`int`): Length of the expression which should not be exceeded. Yields
            occur at or below this expression length.

    Returns:
        (:obj:`str`): Expression in the format expr=OR(entity_name=item1, entity_name=item2...).

    """
    expr = []
    length = 0
    query_prefix_format = "expr=OR({})"

    for item in query_items:
        if type(item) == str:
            formatted_item = f"{entity_name}='{item}'"
        elif type(item) == int:
            formatted_item = f"{entity_name}={item}"
        length = (
            sum(len(e) + 1 for e in expr)
            + len(formatted_item)
            + len(query_prefix_format)
        )
        if length >= max_length:
            yield query_prefix_format.format(",".join(expr))
            expr.clear()
        expr.append(formatted_item)

    # pick up any remainder below max_length
    if len(expr) > 0:
        yield query_prefix_format.format(",".join(expr))


@retry(stop_max_attempt_number=10)
def query_mag_api(expr, fields, subscription_key, query_count=1000, offset=0):
    """Posts a query to the Microsoft Academic Graph Evaluate API.

    Args:
        expr (:obj:`str`): Expression as built by build_expr.
        fields: (:obj:`list` of `str`): Codes of fields to return, as per mag documentation.
        query_count: (:obj:`int`): Number of items to return.
        offset (:obj:`int`): Offset in the results if paging through them.

    Returns:
        (:obj:`dict`): JSON response from the api containing 'expr' (the original expression)
                and 'entities' (the results) keys.
                If there are no results 'entities' is an empty list.

    """
    headers = {
        "Ocp-Apim-Subscription-Key": subscription_key,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    query = f"{expr}&count={query_count}&offset={offset}&attributes={','.join(fields)}"

    r = requests.post(ENDPOINT, data=query.encode("utf-8"), headers=headers)
    r.raise_for_status()

    return r.json()


def dedupe_entities(entities):
    """Finds the highest probability match for each title in returned entities from MAG.

    Args:
        entities (:obj:`list` of `dict`): Entities from the MAG api.

    Returns:
        (set): IDs of entities with the highest probability score, one for each title.

    """
    titles = defaultdict(dict)
    for row in entities:
        titles[row["Ti"]].update({row["Id"]: row["logprob"]})

    deduped_mag_ids = set()
    for title in titles.values():
        # find highest probability match for each title
        deduped_mag_ids.add(sorted(title, key=title.get, reverse=True)[0])

    return deduped_mag_ids


def query_fields_of_study(
    subscription_key,
    ids=None,
    levels=None,
    fields=["Id", "DFN", "FL", "FP.FId", "FC.FId"],
    # id, display_name, level, parent_ids, children_ids
    query_count=1000,
    results_limit=None,
):
    """Queries the MAG for fields of study. Expect >650k results for all levels.

    Args:
        subscription_key (str): MAG api subscription key
        ids: (:obj:`list` of `int`): field of study ids to query
        levels (:obj:`list` of `int`): levels to extract. 0 is highest, 5 is lowest
        fields (:obj:`list` of `str`): codes of fields to return, as per mag documentation
        query_count (int): number of items to return from each query
        results_limit (int): break and return as close to this number of results as the
            offset and query_count allow (for testing)

    Returns:
        (:obj:`list` of `dict`): processed results from the api query
    
    """
    if ids is not None and levels is None:
        expr_args = (ids, "Id")
    elif levels is not None and ids is None:
        expr_args = (levels, "FL")
    else:
        raise TypeError("Field of study ids OR levels should be supplied")

    field_mapping = {
        "Id": "id",
        "DFN": "name",
        "FL": "level",
        "FP": "parent_ids",
        "FC": "child_ids",
    }
    fields_to_drop = ["logprob", "prob"]
    fields_to_compact = ["parent_ids", "child_ids"]

    for expr in build_expr(*expr_args):
        count = 1000
        offset = 0
        while True:
            fos_data = query_mag_api(
                expr,
                fields,
                subscription_key=subscription_key,
                query_count=count,
                offset=offset,
            )
            if fos_data["entities"] == []:
                logging.info("Empty entities returned, no more data")
                break

            # clean up and formatting
            for row in fos_data["entities"]:
                for f in fields_to_drop:
                    del row[f]

                for code, description in field_mapping.items():
                    try:
                        row[description] = row.pop(code)
                    except KeyError:
                        pass

                for field in fields_to_compact:
                    try:
                        row[field] = [ids["FId"] for ids in row[field]]
                    except KeyError:
                        # no parents and/or children
                        pass

                logging.info(f"new fos: {row}")
                yield row

            offset += len(fos_data["entities"])
            logging.info(offset)

            if results_limit is not None and offset >= results_limit:
                break


def build_composite_expr(query_values, entity_name):
    """Builds a composite expression with ANDs in OR to be used as MAG query.

    Args:
        query_values (:obj:`list` of str): Phrases to query MAG with. 
        entity_name (str): MAG attribute that will be used in query.

    Returns:
        (str) MAG expression.
    
    """
    query_prefix_format = "expr=OR({})"
    and_queries = [
        "".join([f"Composite({entity_name}='{query_value}')"])
        for query_value in query_values
    ]
    return query_prefix_format.format(", ".join(and_queries))
