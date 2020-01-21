import pytest
from unittest import mock

from orion.packages.geo.geocode import parse_response
from orion.packages.geo.geocode import geocoding
from orion.packages.geo.geocode import reverse_geocoding
from orion.packages.geo.geocode import place_by_id
from orion.packages.geo.geocode import place_by_name

GEOCODE = "https://maps.googleapis.com/maps/api/geocode/json?"
FIND_PLACE = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?"
PLACE_DETAILS = "https://maps.googleapis.com/maps/api/place/details/json?"


@mock.patch("orion.packages.geo.geocode.requests.get", autospec=True)
def test_google_geocoding_api_queries_correctly(mocked_requests):
    key = "123"
    address = "foo bar"
    geocoding(address, key, GEOCODE)

    expected_call_args = mock.call(GEOCODE, params={"address": "foo bar", "key": "123"})
    assert mocked_requests.call_args == expected_call_args


@mock.patch("orion.packages.geo.geocode.requests.get", autospec=True)
def test_google_reverse_geocoding_api_queries_correctly(mocked_requests):
    key = "123"
    lat = 1.2
    lng = 2.3
    reverse_geocoding(lat, lng, key, GEOCODE)

    expected_call_args = mock.call(GEOCODE, params={"latlng": "1.2,2.3", "key": "123"})
    assert mocked_requests.call_args == expected_call_args


@mock.patch("orion.packages.geo.geocode.requests.get", autospec=True)
def test_google_places_api_queries_correctly(mocked_requests):
    place = "foo bar"
    key = "123"
    place_by_name(place, key, FIND_PLACE)
    expected_call_args = mock.call(
        FIND_PLACE,
        params={
            "input": "foo bar",
            "fields": "place_id",
            "inputtype": "textquery",
            "key": "123",
        },
    )
    assert mocked_requests.call_args == expected_call_args


@mock.patch("orion.packages.geo.geocode.requests.get", autospec=True)
def test_google_places_api_queries_correctly_with_place_ids(mocked_requests):
    id = "abc123"
    key = "123"
    place_by_id(id, key, PLACE_DETAILS)
    expected_call_args = mock.call(
        PLACE_DETAILS,
        params={
            "place_id": "abc123",
            "key": "123",
            "fields": "address_components,formatted_address,geometry,name,place_id,type,website",
        },
    )
    assert mocked_requests.call_args == expected_call_args


def test_parse_google_place_api_response():
    api_response = {
        "html_attributions": [],
        "result": {
            "address_components": [
                {"long_name": "441", "short_name": "441", "types": ["subpremise"]},
                {
                    "long_name": "Metal Box Factory",
                    "short_name": "Metal Box Factory",
                    "types": ["premise"],
                },
                {"long_name": "30", "short_name": "30", "types": ["street_number"]},
                {
                    "long_name": "Great Guildford Street",
                    "short_name": "Great Guildford St",
                    "types": ["route"],
                },
                {
                    "long_name": "London",
                    "short_name": "London",
                    "types": ["postal_town"],
                },
                {
                    "long_name": "Greater London",
                    "short_name": "Greater London",
                    "types": ["administrative_area_level_2", "political"],
                },
                {
                    "long_name": "England",
                    "short_name": "England",
                    "types": ["administrative_area_level_1", "political"],
                },
                {
                    "long_name": "United Kingdom",
                    "short_name": "GB",
                    "types": ["country", "political"],
                },
                {
                    "long_name": "SE1 0HS",
                    "short_name": "SE1 0HS",
                    "types": ["postal_code"],
                },
            ],
            "formatted_address": "441, Metal Box Factory, 30 Great Guildford St, London SE1 0HS, UK",
            "geometry": {
                "location": {"lat": 51.504589, "lng": -0.09708649999999999},
                "viewport": {
                    "northeast": {"lat": 51.5059537802915, "lng": -0.09565766970849796},
                    "southwest": {"lat": 51.5032558197085, "lng": -0.09835563029150202},
                },
            },
            "name": "Mozilla",
            "place_id": "ChIJd7gxxc0EdkgRsxXmeQyR44A",
            "types": ["point_of_interest", "establishment"],
            "website": "https://www.mozilla.org/contact/spaces/london/",
        },
        "status": "OK",
    }

    expected_response = {
        "lat": 51.504589,
        "lng": -0.09708649999999999,
        "address": "441, Metal Box Factory, 30 Great Guildford St, London SE1 0HS, UK",
        "name": "Mozilla",
        "id": "ChIJd7gxxc0EdkgRsxXmeQyR44A",
        "types": ["point_of_interest", "establishment"],
        "website": "https://www.mozilla.org/contact/spaces/london/",
        "postal_town": "London",
        "administrative_area_level_2": "Greater London",
        "administrative_area_level_1": "England",
        "country": "United Kingdom",
    }

    assert parse_response(api_response) == expected_response
