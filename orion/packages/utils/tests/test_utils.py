import pytest
import pandas as pd

from orion.packages.utils.utils import flatten_lists
from orion.packages.utils.utils import unique_dicts
from orion.packages.utils.utils import unique_dicts_by_value
from orion.packages.utils.utils import dict2psql_format

example_list_dict = [
    {"DFN": "Biology", "FId": 86803240},
    {"DFN": "Biofilm", "FId": 58123911},
    {"DFN": "Bacterial growth", "FId": 17741926},
    {"DFN": "Bacteria", "FId": 523546767},
    {"DFN": "Agar plate", "FId": 62643968},
    {"DFN": "Agar", "FId": 2778660310},
    {"DFN": "Agar", "FId": 2778660310},
    {"DFN": "Agar foo bar", "FId": 2778660310},
]


def test_flatten_lists():
    nested_list = [["a"], ["b"], ["c"]]
    result = flatten_lists(nested_list)
    assert result == ["a", "b", "c"]


def test_unique_dicts():
    result = unique_dicts(example_list_dict)
    expected_result = [
        {"DFN": "Biology", "FId": 86803240},
        {"DFN": "Biofilm", "FId": 58123911},
        {"DFN": "Bacterial growth", "FId": 17741926},
        {"DFN": "Bacteria", "FId": 523546767},
        {"DFN": "Agar plate", "FId": 62643968},
        {"DFN": "Agar", "FId": 2778660310},
        {"DFN": "Agar foo bar", "FId": 2778660310},
    ]
    assert sorted([d["FId"] for d in result]) == sorted(
        [d["FId"] for d in expected_result]
    )


def test_unique_dicts_by_value():
    result = unique_dicts_by_value(example_list_dict, "FId")
    expected_result = [
        {"DFN": "Biology", "FId": 86803240},
        {"DFN": "Biofilm", "FId": 58123911},
        {"DFN": "Bacterial growth", "FId": 17741926},
        {"DFN": "Bacteria", "FId": 523546767},
        {"DFN": "Agar plate", "FId": 62643968},
        {"DFN": "Agar foo bar", "FId": 2778660310},
    ]

    assert result == expected_result


def test_dict2postgresql_format():
    data = {}
    data[123] = pd.Series(data=[1, 2, 3], index=[["a", "b", "c"], ["v", "b", "a"]])

    expected_result = [
        {"entity": "a", "year": "v", "rca_sum": 1, "field_of_study_id": 123},
        {"entity": "b", "year": "b", "rca_sum": 2, "field_of_study_id": 123},
        {"entity": "c", "year": "a", "rca_sum": 3, "field_of_study_id": 123},
    ]

    result = dict2psql_format(data)

    assert result == expected_result
