import pytest
import pandas as pd

from orion.packages.metrics.metrics import calculate_rca_by_sum
from orion.packages.metrics.metrics import calculate_rca_by_count


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
