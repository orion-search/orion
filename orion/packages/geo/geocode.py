import logging
import requests
import numpy as np

GEOCODE = "https://maps.googleapis.com/maps/api/geocode/json?"
FIND_PLACE = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?"
PLACE_DETAILS = "https://maps.googleapis.com/maps/api/place/details/json?"


def geocoding(address, key, GEOCODE=GEOCODE):
    """Geocodes a human-readable address.

    Args:
        address (str): Address of a location. For example, it can be country, city,
            postcode or street name.
        key (str): Key for the Google API.
        GEOCODE (str): Endpoint for the Google Geocode API. Note that the service
            must be enabled in order to use it.

    Returns:
        r (:obj:`dict` of :obj:`list`): API response with geocoded information. The returned
            results are ordered by relevance.

    """
    params = {"address": "{}".format(address), "key": key}

    r = requests.get(GEOCODE, params=params)
    r.raise_for_status()

    return r.json()


def reverse_geocoding(lat, lng, key, GEOCODE=GEOCODE):
    """Reverse geocodes geographic coordinates into a human-readable address.

    Args:
        lat (float): Latitude.
        lng (float): Longitude.
        key (str): Key for the Google API.
        GEOCODE  (str): Endpoint for the Google Geocode API. Note that the service
            must be enabled in order to use it.

    Returns:
        (:obj:`dict` of :obj:`list`): API response with the most relevant results
            to the given longitude and latitude.
    
    """
    params = {"latlng": "{},{}".format(lat, lng), "key": key}

    r = requests.get(GEOCODE, params=params)
    r.raise_for_status()
    return r.json()


def place_by_name(place, key, FIND_PLACE=FIND_PLACE):
    """Finds a Google Place ID by searching with its name.

    Args:
        place (str): Name of the place. It can be a restaurant, bar, monument,
            whatever you would normally search in Google Maps.
        key (str): Key for the Google API.
        FIND_PLACE (str): Endpoint for the Google Places API. Note that the
            service must be enabled in order to use it.

    Returns:
        (str) Place ID.
    
    """
    params = {
        "input": "{}".format(place),
        "fields": "place_id",
        "inputtype": "textquery",
        "key": key,
    }

    r = requests.get(FIND_PLACE, params=params)
    r.raise_for_status()

    try:
        return r.json()["candidates"][0]["place_id"]
    except IndexError as e:
        logging.info(f"Failed to find a match for {place}")
        return None


def place_by_id(id, key, PLACE_DETAILS=PLACE_DETAILS):
    """Finds details about a place given its Google Place ID.

    Args:
        id (str): Place ID.
        key (str): Key for the Google API.
        FIND_PLACE_DETAILS (str): Endpoint for the Google Places API. Note that the
            service must be enabled in order to use it.
    
    Returns:
        (dict): Details of a place. See the `fields` parameters to find what's
            being returned in the response.
    
    """
    params = {
        "place_id": id,
        "key": key,
        "fields": "address_components,formatted_address,geometry,name,place_id,type,website",
    }

    r = requests.get(PLACE_DETAILS, params=params)
    r.raise_for_status()

    return r.json()


def parse_response(response):
    """Parses details from a Google Place Details API endpoint response.

    Args:
        response (dict): Response of a request.

    Returns:
        d (dict): Geocoded information for a given Place ID.

    """
    result = response["result"]

    # Store attributes
    d = dict()
    d["lat"] = result["geometry"]["location"]["lat"]
    d["lng"] = result["geometry"]["location"]["lng"]
    d["address"] = result["formatted_address"]
    d["name"] = result["name"]
    d["id"] = result["place_id"]
    d["types"] = result["types"]
    try:
        d["website"] = result["website"]
    except KeyError as e:
        logging.info(f"{d['name']}: {e}")
        d["website"] = np.nan

    for r in result["address_components"]:
        if "postal_town" in r["types"]:
            d["postal_town"] = r["long_name"]
        elif "administrative_area_level_2" in r["types"]:
            d["administrative_area_level_2"] = r["long_name"]
        elif "administrative_area_level_1" in r["types"]:
            d["administrative_area_level_1"] = r["long_name"]
        elif "country" in r["types"]:
            d["country"] = r["long_name"]
        else:
            continue

    return d
