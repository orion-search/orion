import requests


def parse_country_details(response, pos=0):
    """Parses the country details from the restcountries API.
        More info here: https://github.com/apilayer/restcountries
    
    Args:
        response (:obj:`list` of `dict`): API response.
        
    Returns:
        d (dict): Parsed API response.
        
    """
    remappings = {
        "alpha2Code": "alpha2Code",
        "alpha3Code": "alpha3Code",
        "name": "name",
        "capital": "capital",
        "region": "region",
        "subregion": "subregion",
        "population": "population",
    }

    d = {k: response[pos][v] for k, v in remappings.items()}

    return d


def get_country_details(country_name):
    """Fetches country details from restcountries API.

    Args:
        country_name (str): Country name.

    Returns:
        (dict) API response.

    """
    r = requests.get(f"https://restcountries.eu/rest/v2/name/{country_name}")
    r.raise_for_status()

    return r.json()
