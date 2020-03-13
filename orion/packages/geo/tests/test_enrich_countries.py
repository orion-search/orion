import pytest
from unittest import mock

from orion.packages.geo.enrich_countries import get_country_details
from orion.packages.geo.enrich_countries import parse_country_details


@mock.patch("orion.packages.geo.enrich_countries.requests.get", autospec=True)
def test_get_country_details(mock_requests):
    get_country_details("Laos")

    expected_call_args = mock.call("https://restcountries.eu/rest/v2/name/Laos")
    assert mock_requests.call_args == expected_call_args


def test_parse_rest_countries_api_response():
    data = [
        {
            "name": "United States Minor Outlying Islands",
            "topLevelDomain": [".us"],
            "alpha2Code": "UM",
            "alpha3Code": "UMI",
            "callingCodes": [""],
            "capital": "",
            "altSpellings": ["UM"],
            "region": "Americas",
            "subregion": "Northern America",
            "population": 300,
            "latlng": [],
            "demonym": "American",
            "area": None,
            "gini": None,
            "timezones": ["UTC-11:00", "UTC-10:00", "UTC+12:00"],
            "borders": [],
            "nativeName": "United States Minor Outlying Islands",
            "numericCode": "581",
            "currencies": [
                {"code": "USD", "name": "United States Dollar", "symbol": "$"}
            ],
            "languages": [
                {
                    "iso639_1": "en",
                    "iso639_2": "eng",
                    "name": "English",
                    "nativeName": "English",
                }
            ],
            "translations": {
                "de": "Kleinere Inselbesitzungen der Vereinigten Staaten",
                "es": "Islas Ultramarinas Menores de Estados Unidos",
                "fr": "Îles mineures éloignées des États-Unis",
                "ja": "合衆国領有小離島",
                "it": "Isole minori esterne degli Stati Uniti d'America",
                "br": "Ilhas Menores Distantes dos Estados Unidos",
                "pt": "Ilhas Menores Distantes dos Estados Unidos",
                "nl": "Kleine afgelegen eilanden van de Verenigde Staten",
                "hr": "Mali udaljeni otoci SAD-a",
                "fa": "جزایر کوچک حاشیه\u200cای ایالات متحده آمریکا",
            },
            "flag": "https://restcountries.eu/data/umi.svg",
            "regionalBlocs": [],
            "cioc": "",
        },
        {
            "name": "United States of America",
            "topLevelDomain": [".us"],
            "alpha2Code": "US",
            "alpha3Code": "USA",
            "callingCodes": ["1"],
            "capital": "Washington, D.C.",
            "altSpellings": ["US", "USA", "United States of America"],
            "region": "Americas",
            "subregion": "Northern America",
            "population": 323947000,
            "latlng": [38.0, -97.0],
            "demonym": "American",
            "area": 9629091.0,
            "gini": 48.0,
            "timezones": [
                "UTC-12:00",
                "UTC-11:00",
                "UTC-10:00",
                "UTC-09:00",
                "UTC-08:00",
                "UTC-07:00",
                "UTC-06:00",
                "UTC-05:00",
                "UTC-04:00",
                "UTC+10:00",
                "UTC+12:00",
            ],
            "borders": ["CAN", "MEX"],
            "nativeName": "United States",
            "numericCode": "840",
            "currencies": [
                {"code": "USD", "name": "United States dollar", "symbol": "$"}
            ],
            "languages": [
                {
                    "iso639_1": "en",
                    "iso639_2": "eng",
                    "name": "English",
                    "nativeName": "English",
                }
            ],
            "translations": {
                "de": "Vereinigte Staaten von Amerika",
                "es": "Estados Unidos",
                "fr": "États-Unis",
                "ja": "アメリカ合衆国",
                "it": "Stati Uniti D'America",
                "br": "Estados Unidos",
                "pt": "Estados Unidos",
                "nl": "Verenigde Staten",
                "hr": "Sjedinjene Američke Države",
                "fa": "ایالات متحده آمریکا",
            },
            "flag": "https://restcountries.eu/data/usa.svg",
            "regionalBlocs": [
                {
                    "acronym": "NAFTA",
                    "name": "North American Free Trade Agreement",
                    "otherAcronyms": [],
                    "otherNames": [
                        "Tratado de Libre Comercio de América del Norte",
                        "Accord de Libre-échange Nord-Américain",
                    ],
                }
            ],
            "cioc": "USA",
        },
    ]

    expected_result_pos_0 = {
        "alpha2Code": "UM",
        "alpha3Code": "UMI",
        "name": "United States Minor Outlying Islands",
        "capital": "",
        "region": "Americas",
        "subregion": "Northern America",
        "population": 300,
    }

    expected_result_pos_1 = {
        "alpha2Code": "US",
        "alpha3Code": "USA",
        "name": "United States of America",
        "capital": "Washington, D.C.",
        "region": "Americas",
        "subregion": "Northern America",
        "population": 323947000,
    }

    result_pos_0 = parse_country_details(data, pos=0)
    result_pos_1 = parse_country_details(data, pos=1)

    assert result_pos_0 == expected_result_pos_0
    assert result_pos_1 == expected_result_pos_1
