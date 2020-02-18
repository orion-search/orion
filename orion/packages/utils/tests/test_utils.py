import pytest
import pandas as pd
import numpy as np
from collections import Counter

from orion.packages.utils.utils import flatten_lists
from orion.packages.utils.utils import unique_dicts
from orion.packages.utils.utils import unique_dicts_by_value
from orion.packages.utils.utils import dict2psql_format
from orion.packages.utils.utils import inverted2abstract
from orion.packages.utils.utils import cooccurrence_graph
from orion.packages.utils.utils import get_all_children

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


def test_inverted_abstract():
    data = """{"IndexLength": 111, "InvertedIndex": {"In": [0], "comparative": [1], "high-throughput": [2], "sequencing": [3], "assays,": [4], "a": [5, 44, 51, 78], "fundamental": [6], "task": [7], "is": [8, 105], "the": [9, 39, 74, 84, 88], "analysis": [10, 55, 81], "of": [11, 25, 41, 56, 73, 91], "count": [12, 57], "data,": [13, 22], "such": [14, 98], "as": [15, 99, 107], "read": [16], "counts": [17], "per": [18], "gene": [19, 100], "in": [20], "RNA-Seq": [21], "for": [23, 53, 63], "evidence": [24], "systematic": [26], "changes": [27, 67], "across": [28], "experimental": [29], "conditions.": [30], "Small": [31], "replicate": [32], "numbers,": [33], "discreteness,": [34], "large": [35], "dynamic": [36], "range": [37], "and": [38, 65, 71, 94, 102], "presence": [40, 90], "outliers": [42], "require": [43], "suitable": [45], "statistical": [46], "approach.": [47], "We": [48], "present": [49], "DESeq2,": [50], "method": [52], "differential": [54, 92], "data.": [58], "DESeq2": [59, 104], "uses": [60], "shrinkage": [61], "estimation": [62], "dispersions": [64], "fold": [66], "to": [68], "improve": [69], "stability": [70], "interpretability": [72], "estimates.": [75], "This": [76], "enables": [77], "more": [79], "quantitative": [80], "focused": [82], "on": [83], "strength": [85], "rather": [86], "than": [87], "mere": [89], "expression": [93], "facilitates": [95], "downstream": [96], "tasks": [97], "ranking": [101], "visualization.": [103], "available": [106], "an": [108], "R/Bioconductor": [109], "package.": [110]}}"""

    expected_result = "In comparative high-throughput sequencing assays, a fundamental task is the analysis of count data, such as read counts per gene in RNA-Seq data, for evidence of systematic changes across experimental conditions. Small replicate numbers, discreteness, large dynamic range and the presence of outliers require a suitable statistical approach. We present DESeq2, a method for differential analysis of count data. DESeq2 uses shrinkage estimation for dispersions and fold changes to improve stability and interpretability of the estimates. This enables a more quantitative analysis focused on the strength rather than the mere presence of differential expression and facilitates downstream tasks such as gene ranking and visualization. DESeq2 is available as an R/Bioconductor package."

    result = inverted2abstract(data)

    assert result == expected_result


def test_inverted_abstract_empty_field():
    data = None
    result = inverted2abstract(data)

    assert np.isnan(result)


def test_cooccurrence_graph():
    data = [["a", "b"], ["a", "b", "c"]]

    expected_result = Counter({("a", "b"): 2, ("a", "c"): 1, ("b", "c"): 1})
    result = cooccurrence_graph(data)

    assert result == expected_result


def test_get_all_children():
    data = pd.DataFrame(
        {
            "id": [165864922, 114009990, 178809742, 2909274368, 196033, 190796033],
            "child_id": [
                [190796033, 114009990, 178809742],
                [2909274368, 196033],
                [],
                [],
                [114009990, 2909274368],
                [190796033],
            ],
        }
    )

    expected_result = [2909274368, 190796033, 196033, 114009990, 178809742, 165864922]
    result = list(set(get_all_children(data, 165864922)))

    assert result == expected_result
