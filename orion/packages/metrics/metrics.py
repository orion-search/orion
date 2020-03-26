import numpy as np


def _rca_division(val1, val2, val3, val4):
    """Multi-step division."""
    return (val1 / val2) / (val3 / val4)


def calculate_rca_by_sum(
    data, entity_column, commodity, value, paper_thresh, year_thresh
):
    """Groups a dataframe by entity (country or institution) and
       calculates the Revealed Comparative Advantage (RCA) for each 
       entity, based on a commodity. The value used for measurement is
       usually citations and it's done on an annual basis.

    Args:
        data (:code:`pandas.DataFrame`): DF with the re
        entity_column (str): Label of the column containing countries or institutions.
        commodity (str): The Field of Study to measure RCA for.
        value (str): Label of the column containing the values to use for measurement.
        paper_thresh (int): Calculate RCA for country with more than N number of papers.
        year_thresh (str): Consider only years higher than the threshold.

    Returns:
        (:code:`pandas.DataFrame`): grouped dataframe with entity, year and calculated RCA.

    """
    data = data[data.year > year_thresh]
    entity_sum_topic = (
        data[(data.field_of_study_id == commodity)]
        .groupby([entity_column, "year"])[value]
        .sum()
    )
    entity_sum_all = data.groupby([entity_column, "year"])[value].sum()
    entity_sum_all = entity_sum_all.where(entity_sum_all > paper_thresh)

    world_sum_topic = (
        data[data.field_of_study_id == commodity].groupby("year")[value].sum()
    )
    world_sum_all = data.groupby("year")[value].sum()

    rca = _rca_division(
        entity_sum_topic, entity_sum_all, world_sum_topic, world_sum_all
    )

    return rca.dropna()


def calculate_rca_by_count(data, entity_column, commodity, paper_thresh, year_thresh):
    """Groups a dataframe by entity (country or institution) and
       calculates the Revealed Comparative Advantage (RCA) for each 
       entity, based on a commodity. The value used for measurement is publication 
       volume and it's done on an annual basis.

    Args:
        data (:code:`pandas.DataFrame`): DF with the re
        entity_column (str): Label of the column containing countries or institutions.
        commodity (str): The Field of Study to measure RCA for.
        paper_thresh (int): Calculate RCA for country with more than N number of papers.
        year_thresh (str): Consider only years higher than the threshold.

    Returns:
        (:code:`pandas.DataFrame`): grouped dataframe with entity, year and calculated RCA.
    
    """
    data = data[data.year > year_thresh]
    entity_count_topic = (
        data[(data.field_of_study_id == commodity)]
        .groupby([entity_column, "year"])["paper_id"]
        .count()
    )
    entity_count_all = data.groupby([entity_column, "year"])["paper_id"].count()
    entity_count_all = entity_count_all.where(entity_count_all > paper_thresh)

    world_count_topic = (
        data[data.field_of_study_id == commodity].groupby("year")["paper_id"].count()
    )
    world_count_all = data.groupby("year")["paper_id"].count()

    rca = _rca_division(
        entity_count_topic, entity_count_all, world_count_topic, world_count_all
    )

    return rca.dropna()


def _validate_counts_vector(counts, suppress_cast=False):
    """Validates and converts input to an acceptable counts vector type.
    Note: may not always return a copy of `counts`!
    
    This is taken from scikit-bio.

    """
    counts = np.asarray(counts)

    if not suppress_cast:
        counts = counts.astype(int, casting="safe", copy=False)

    if counts.ndim != 1:
        raise ValueError("Only 1-D vectors are supported.")
    elif (counts < 0).any():
        raise ValueError("Counts vector cannot contain negative values.")

    return counts


def dominance(counts):
    """Calculates dominance.
    
    Dominance is defined as
    .. math::
       \sum{p_i^2}
    where :math:`p_i` is the proportion of the entire community that OTU
    :math:`i` represents.
    Dominance can also be defined as 1 - Simpson's index. It ranges between
    0 and 1.

    Args:
        counts (:obj:`numpy.array` of `int`): Vector of counts.
    
    Returns:
        (float) dominance score.
    
    Notes
    -----
    The implementation here is based on the description given in [1]_.
    References
    ----------
    .. [1] http://folk.uio.no/ohammer/past/diversity.html

    This is taken from scikit-bio.

    """
    counts = _validate_counts_vector(counts)
    freqs = counts / counts.sum()
    return (freqs * freqs).sum()


def shannon(counts, base=2):
    """Calculate Shannon entropy of counts, default in bits.
    Shannon-Wiener diversity index is defined as:
    .. math::
       H = -\sum_{i=1}^s\left(p_i\log_2 p_i\right)
    where :math:`s` is the number of OTUs and :math:`p_i` is the proportion of
    the community represented by OTU :math:`i`.

    
    Args:
        counts (:obj:`numpy.array` of `int`): Vector of counts.
        base (int): Logarithm base to use in the calculations.
    
    Returns:
        (float) Shannon diversity index H.
    
    Notes
    -----
    The implementation here is based on the description given in the SDR-IV
    online manual [1]_ except that the default logarithm base used here is 2
    instead of :math:`e`.
    References
    ----------
    .. [1] http://www.pisces-conservation.com/sdrhelp/index.html

    This is taken from scikit-bio.

    """
    counts = _validate_counts_vector(counts)
    freqs = counts / counts.sum()
    nonzero_freqs = freqs[freqs.nonzero()]
    return -(nonzero_freqs * np.log(nonzero_freqs)).sum() / np.log(base)


def simpson(counts):
    """Calculate Simpson's index.
    Simpson's index is defined as ``1 - dominance``:
    .. math::
       1 - \sum{p_i^2}
    where :math:`p_i` is the proportion of the community represented by OTU
    :math:`i`.

    Args:
        counts (:obj:`numpy.array` of `int`): Vector of counts.

    Returns:
        (float) Simpson's index.

    Notes
    -----
    The implementation here is ``1 - dominance`` as described in [1]_. Other
    references (such as [2]_) define Simpson's index as ``1 / dominance``.
    References
    ----------
    .. [1] http://folk.uio.no/ohammer/past/diversity.html
    .. [2] http://www.pisces-conservation.com/sdrhelp/index.html

    This is taken from scikit-bio.

    """
    counts = _validate_counts_vector(counts)
    return 1 - dominance(counts)


def enspie(counts):
    """Calculate ENS_pie alpha diversity measure.
    ENS_pie is equivalent to ``1 / dominance``:
    .. math::
       ENS_{pie} = \frac{1}{\sum_{i=1}^s{p_i^2}}
    where :math:`s` is the number of OTUs and :math:`p_i` is the proportion of
    the community represented by OTU :math:`i`.
    
    Args:
        counts (:obj:`numpy.array` of `int`): Vector of counts.
    
    Returns:
        (float) ENS_pie alpha diversity measure.

    Notes
    -----
    ENS_pie is defined in [1]_.
    References
    ----------
    .. [1] Chase and Knight (2013). "Scale-dependent effect sizes of ecological
       drivers on biodiversity: why standardised sampling is not enough".
       Ecology Letters, Volume 16, Issue Supplement s1, pgs 17-26.    

    This is taken from scikit-bio.

    """
    counts = _validate_counts_vector(counts)
    return 1 / dominance(counts)


def observed_otus(counts):
    """Calculate the number of distinct OTUs.
    
    Args:
        counts (:obj:`numpy.array` of `int`): Vector of counts.
    
    Returns:
        (int) Distinct OTU count.
    
    """
    counts = _validate_counts_vector(counts)
    return (counts != 0).sum()


def simpson_e(counts):
    """Calculate Simpson's evenness measure E.
    Simpson's E is defined as
    .. math::
       E=\frac{1 / D}{S_{obs}}
    where :math:`D` is dominance and :math:`S_{obs}` is the number of observed
    OTUs.

    Args:
        counts (:obj:`numpy.array` of `int`): Vector of counts.

    Returns:
        (float) Simpson's evenness measure E.

    Notes
    -----
    The implementation here is based on the description given in [1]_.
    References
    ----------
    .. [1] http://www.tiem.utk.edu/~gross/bioed/bealsmodules/simpsonDI.html

    This is taken from scikit-bio.

    """
    counts = _validate_counts_vector(counts)
    return enspie(counts) / observed_otus(counts)
