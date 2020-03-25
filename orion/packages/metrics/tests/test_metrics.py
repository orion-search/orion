import pytest
import pandas as pd
import numpy as np

from orion.packages.metrics.metrics import calculate_rca_by_sum
from orion.packages.metrics.metrics import calculate_rca_by_count
from orion.packages.metrics.metrics import simpson
from orion.packages.metrics.metrics import dominance
from orion.packages.metrics.metrics import shannon
from orion.packages.metrics.metrics import _validate_counts_vector
from orion.packages.metrics.metrics import observed_otus
from orion.packages.metrics.metrics import simpson_e
from orion.packages.metrics.metrics import enspie


def test_calculate_rca_by_sum_calculates_correct_results():
    data = pd.DataFrame(
        {
            "field_of_study_id": [123, 13, 13, 123, 123, 123],
            "country": ["UK", "UK", "US", "US", "US", "AU"],
            "paper_id": [1, 2, 3, 4, 5, 6],
            "citations": [10, 2, 7, 78, 32, 1],
            "year": ["2018", "2018", "2018", "2018", "2018", "2018"],
        }
    )

    expected_result = pd.DataFrame(
        {
            "country": ["UK", "US"],
            "year": ["2018", "2018"],
            "citations": [2.407407, 0.864198],
        }
    ).set_index(["country", "year"])

    result = pd.DataFrame(
        calculate_rca_by_sum(
            data,
            entity_column="country",
            commodity=13,
            value="citations",
            paper_thresh=1,
            year_thresh="2013",
        )
    )

    pd.testing.assert_frame_equal(
        expected_result, result, check_exact=False, check_less_precise=3
    )


def test_calculate_rca_by_count_calculates_correct_results():
    data = pd.DataFrame(
        {
            "field_of_study_id": [123, 13, 13, 123, 123, 123],
            "country": ["UK", "UK", "US", "US", "US", "AU"],
            "paper_id": [1, 2, 3, 4, 5, 6],
            "citations": [10, 2, 7, 78, 32, 1],
            "year": ["2018", "2018", "2018", "2018", "2018", "2018"],
        }
    )

    expected_result = pd.DataFrame(
        {"country": ["UK", "US"], "year": ["2018", "2018"], "paper_id": [1.5, 1]}
    ).set_index(["country", "year"])

    result = pd.DataFrame(
        calculate_rca_by_count(
            data,
            entity_column="country",
            commodity=13,
            paper_thresh=1,
            year_thresh="2013",
        )
    )

    pd.testing.assert_frame_equal(
        expected_result, result, check_exact=False, check_less_precise=3
    )


def test_validate_counts_vector_list():
    obs = _validate_counts_vector([0, 2, 1, 3])

    np.testing.assert_array_equal(obs, np.array([0, 2, 1, 3]))
    assert obs.dtype == int


def test_validate_counts_vector_numpy_array():
    # numpy array (no copy made)
    data = np.array([0, 2, 1, 3])
    obs = _validate_counts_vector(data)

    np.testing.assert_array_equal(obs, data)
    assert obs.dtype == int


def test_validate_counts_vector_single_element():
    obs = _validate_counts_vector([42])

    np.testing.assert_array_equal(obs, np.array([42]))
    assert obs.dtype == int
    assert obs.shape == (1,)


def test_validate_counts_vector_suppress_casting_to_int():
    obs = _validate_counts_vector([42.2, 42.1, 0], suppress_cast=True)

    np.testing.assert_array_equal(obs, np.array([42.2, 42.1, 0]))
    assert obs.dtype == float


def test_validate_counts_vector_all_zeros():
    obs = _validate_counts_vector([0, 0, 0])

    np.testing.assert_array_equal(obs, np.array([0, 0, 0]))
    assert obs.dtype == int


def test_validate_counts_vector_all_zeros_single_value():
    obs = _validate_counts_vector([0])

    np.testing.assert_array_equal(obs, np.array([0]))
    assert obs.dtype == int


def test_validate_counts_vector_invalid_input_wrong_dtype():
    with pytest.raises(Exception):
        _validate_counts_vector([0, 2, 1.2, 3])


def test_validate_counts_vector_invalid_input_wrong_number_of_dimensions():
    with pytest.raises(Exception):
        _validate_counts_vector([[0, 2, 1, 3], [4, 5, 6, 7]])


def test_validate_counts_vector_invalid_input_wrong_number_of_dimensions_scalar():
    with pytest.raises(Exception):
        _validate_counts_vector(1)


def test_validate_counts_vector_invalid_input_negative_values():
    with pytest.raises(Exception):
        _validate_counts_vector([0, 0, 2, -1, 3])


def test_dominance():
    assert dominance(np.array([5])) == 1
    assert pytest.approx(dominance(np.array([1, 0, 2, 5, 2])), 0.34)


def test_shannon():
    assert shannon(np.array([5])) == 0
    assert shannon(np.array([5, 5])) == 1
    assert shannon(np.array([1, 1, 1, 1, 0])) == 2


def test_simpson():
    assert pytest.approx(simpson(np.array([1, 0, 2, 5, 2])), 0.66)
    assert pytest.approx(simpson(np.array([5])), 0)


def test_observed_otus():
    obs = observed_otus(np.array([4, 3, 4, 0, 1, 0, 2]))
    assert obs == 5

    obs = observed_otus(np.array([0, 0, 0]))
    assert obs == 0

    obs = observed_otus(np.array([0, 1, 1, 4, 2, 5, 2, 4, 1, 2]))
    assert obs == 9


def test_enspie():
    # Totally even community should have ENS_pie = number of OTUs.
    assert pytest.approx(enspie(np.array([1, 1, 1, 1, 1, 1])), 6)
    assert pytest.approx(enspie(np.array([13, 13, 13, 13])), 4)

    # Hand calculated.
    arr = np.array([1, 41, 0, 0, 12, 13])
    exp = 1 / ((arr / arr.sum()) ** 2).sum()
    np.testing.assert_almost_equal(enspie(arr), exp)

    # Using dominance.
    exp = 1 / dominance(arr)
    np.testing.assert_almost_equal(enspie(arr), exp)

    arr = np.array([1, 0, 2, 5, 2])
    exp = 1 / dominance(arr)
    np.testing.assert_array_almost_equal(enspie(arr), exp)
