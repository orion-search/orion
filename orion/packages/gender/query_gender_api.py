import requests
import logging
from retrying import retry
from requests.exceptions import HTTPError

ENDPOINT = "https://gender-api.com/v2/gender"


@retry(stop_max_attempt_number=2)
def query_gender_api(full_names, auth_token):
    """Infers the gender by querying a full name to the GenderAPI.
    
    Args:
        full_names (:obj:`list` of str): Full names.
        auth_token (str): Authorization token.
    
    Returns:
        (json) Person's gender.

    """
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
    }

    data = [{"full_name": fn} for fn in full_names]

    r = requests.post(ENDPOINT, json=data, headers=headers)
    try:
        r.raise_for_status()
        return r.json()
    except HTTPError as h:
        logging.info(full_names, h)
        return None


def parse_response(response):
    """Parses the GenderAPI response.
    
    Args:
        id_ (int): Author MAG ID.
        name (str): Full or first name used to query the GenderAPI.
        response (dict): GenderAPI response.
    
    Returns:
        (dict) Parsed response.
    
    """
    d = {}
    d["full_name"] = response["input"]["full_name"]
    d["samples"] = response["details"]["samples"]
    d["first_name"] = response["first_name"]
    d["probability"] = response["probability"]
    d["gender"] = response["gender"]

    return d
