import pytest

from orion.packages.mag.parsing_mag_data import parse_papers
from orion.packages.mag.parsing_mag_data import parse_affiliations
from orion.packages.mag.parsing_mag_data import parse_authors
from orion.packages.mag.parsing_mag_data import parse_fos
from orion.packages.mag.parsing_mag_data import parse_journal

test_example = {
    "logprob": -17.825,
    "prob": 1.81426557,
    "Id": 2592122940,
    "Ti": "dna fountain enables a robust and efficient storage architecture",
    "Pt": "1",
    "Y": 2017,
    "D": "2017-03-03",
    "CC": 109,
    "RId": [2293000460, 2296125569],
    "DOI": "10.1126/science.aaj2038",
    "PB": "American Association for the Advancement of Science",
    "BT": "a",
    "AA": [
        {
            "DAuN": "Foo",
            "AuId": 2780121452,
            "AfN": "columbia university",
            "AfId": 78577930,
            "S": 1,
        },
        {"DAuN": "Bar", "AuId": 2159352281, "AfId": None, "S": 2},
    ],
    "F": [
        {"DFN": "Petabyte", "FId": 13600138},
        {"DFN": "Oligonucleotide", "FId": 129312508},
    ],
    "J": {"JN": "science", "JId": 3880285},
}


def test_parse_papers():
    expected_result = {
        "id": 2592122940,
        "title": "dna fountain enables a robust and efficient storage architecture",
        "doi": "10.1126/science.aaj2038",
        "prob": 1.81426557,
        "publication_type": "1",
        "year": 2017,
        "date": "2017-03-03",
        "citations": 109,
        "bibtex_doc_type": "a",
        "references": "[2293000460, 2296125569]",
        "publisher": "American Association for the Advancement of Science",
    }
    result = parse_papers(test_example)

    assert result == expected_result


def test_parse_journal():
    expected_result = {"id": 3880285, "journal_name": "science", "paper_id": 2592122940}
    result = parse_journal(test_example, 2592122940)

    assert result == expected_result


def test_parse_authors():
    expected_result_authors = [
        {"id": 2780121452, "name": "Foo"},
        {"id": 2159352281, "name": "Bar"},
    ]
    expected_result_paper_with_authors = [
        {"paper_id": 2592122940, "author_id": 2780121452, "order": 1},
        {"paper_id": 2592122940, "author_id": 2159352281, "order": 2},
    ]
    result_authors, result_paper_with_authors = parse_authors(test_example, 2592122940)

    assert result_authors == expected_result_authors
    assert result_paper_with_authors == expected_result_paper_with_authors


def test_parse_fields_of_study():
    expected_result_paper_with_fos = [
        {"field_of_study_id": 13600138, "paper_id": 2592122940},
        {"field_of_study_id": 129312508, "paper_id": 2592122940},
    ]
    expected_result_fields_of_study = [
        {"id": 13600138, "name": "Petabyte"},
        {"id": 129312508, "name": "Oligonucleotide"},
    ]
    result_paper_with_fos, result_fields_of_study = parse_fos(test_example, 2592122940)

    assert expected_result_paper_with_fos == result_paper_with_fos
    assert expected_result_fields_of_study == result_fields_of_study


def test_parse_affiliations():
    expected_result_affiliations = [
        {"id": 78577930, "affiliation": "columbia university"}
    ]
    expected_result_author_with_aff = [
        {"affiliation_id": 78577930, "author_id": 2780121452}
    ]
