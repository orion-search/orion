import json
import logging
import numpy as np


def parse_papers(response):
    """Parse paper information from a MAG API response.
    
    Args:
        response (json): Response from MAG API in JSON format. Contains paper information.

    Returns:
        d (dict): Paper metadata.

    """
    d = {}
    d["id"] = response["Id"]
    d["doi"] = response["DOI"]
    d["prob"] = response["prob"]
    d["title"] = response["Ti"]
    d["publication_type"] = response["Pt"]
    d["year"] = response["Y"]
    d["date"] = response["D"]
    d["citations"] = response["CC"]
    try:
        d["bibtex_doc_type"] = response["BT"]
    except KeyError as e:
        logging.info(f"{response['Id']}: {e}")
        d["bibtex_doc_type"] = np.nan
    try:
        d["inverted_abstract"] = json.dumps(response["IA"])
    except KeyError as e:
        logging.info(f"{response['Id']}: {e}")
        d["inverted_abstract"] = np.nan
    try:
        d["references"] = json.dumps(response["RId"])
    except KeyError as e:
        logging.info(f"{response['Id']}: {e}")
        d["references"] = np.nan
    try:
        d["publisher"] = response["PB"]
    except KeyError as e:
        logging.info(f"{response['Id']}: {e}")
        d["publisher"] = np.nan

    return d


def parse_journal(response, paper_id):
    """Parse journal information from a MAG API response.
    
    Args:
        response (json): Response from MAG API in JSON format. Contains all paper information.
        paper_id (int): Paper ID.

    Returns:
        d (dict): Journal details.

    """
    return {
        "id": response["J"]["JId"],
        "journal_name": response["J"]["JN"],
        "paper_id": paper_id,
    }


def parse_conference(response, paper_id):
    """Parse conference information from a MAG API response.
    
    Args:
        response (json): Response from MAG API in JSON format. Contains all paper information.
        paper_id (int): Paper ID.

    Returns:
        d (dict): Conference details.

    """
    return {
        "id": response["C"]["CId"],
        "conference_name": response["C"]["CN"],
        "paper_id": paper_id,
    }


def parse_authors(response, paper_id):
    """Parse author information from a MAG API response.

    Args:
        response (json): Response from MAG API in JSON format. Contains all paper information.
        paper_id (int): Paper ID.

    Returns:
        authors (:obj:`list` of :obj:`dict`): List of dictionaries with author information.
            There's one dictionary per author.
        paper_with_authors (:obj:`list` of :obj:`dict`): Matching paper and author IDs.

    """
    authors = []
    paper_with_authors = []
    for author in response["AA"]:
        # mag_paper_authors
        paper_with_authors.append(
            {"paper_id": paper_id, "author_id": author["AuId"], "order": author["S"]}
        )
        # mag_authors
        authors.append({"id": author["AuId"], "name": author["DAuN"]})

    return authors, paper_with_authors


def parse_fos(response, paper_id):
    """Parse the fields of study of a paper from a MAG API response.

    Args:
        response (json): Response from MAG API in JSON format. Contains all paper information.
        paper_id (int): Paper ID.

    Returns:
        fields_of_study (:obj:`list` of :obj:`dict`): List of dictionaries with fields of study information.
            There's one dictionary per field of study.
        paper_with_fos (:obj:`list` of :obj:`dict`): Matching fields of study and paper IDs.

    """
    # two outputs: fos_id with fos_name, fos_id with paper_id
    paper_with_fos = []
    fields_of_study = []
    for fos in response["F"]:
        # mag_fields_of_study
        fields_of_study.append({"id": fos["FId"], "name": fos["DFN"]})
        # mag_paper_fields_of_study
        paper_with_fos.append({"field_of_study_id": fos["FId"], "paper_id": paper_id})

    return paper_with_fos, fields_of_study

def parse_affiliations(response, paper_id):
    """Parse the author affiliations from a MAG API response.

    Args:
        response (json): Response from MAG API in JSON format. Contains all paper information.

    Returns:
        affiliations (:obj:`list` of :obj:`dict`): List of dictionaries with affiliation information.
            There's one dictionary per field of study.
       author_with_aff (:obj:`list` of :obj:`dict`): Matching affiliation and author IDs.

    """
    affiliations = []
    paper_author_aff = []
    for aff in response["AA"]:
        if aff["AfId"]:
            # mag_author_affiliation
            paper_author_aff.append({"affiliation_id": aff["AfId"], "author_id": aff["AuId"], 'paper_id':paper_id})
            # mag_affiliation
            affiliations.append({"id": aff["AfId"], "affiliation": aff["AfN"]})
        else:
            continue
    return affiliations, paper_author_aff
