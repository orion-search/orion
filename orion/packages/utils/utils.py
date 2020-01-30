from itertools import chain


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
