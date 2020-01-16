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
