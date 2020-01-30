import pytest
from unittest import mock

from orion.packages.gender.query_gender_api import query_gender_api
from orion.packages.gender.query_gender_api import parse_response


@mock.patch("orion.packages.gender.query_gender_api.requests.post", autospec=True)
def test_query_gender_api_sends_correct_request(mocked_requests):
    auth_token = 123
    full_name = "foo bar"
    query_gender_api(full_name, auth_token)

    expected_call_args = mock.call(
        "https://gender-api.com/v2/gender",
        headers={
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        },
        json={"full_name": "foo bar"},
    )
    assert mocked_requests.call_args == expected_call_args


def test_parse_response_from_gender_api():
    id_ = 123
    full_name = "Foo Bar"
    response = {
        "input": {"first_name": "Foo"},
        "details": {
            "credits_used": 1,
            "duration": "71ms",
            "samples": 106011,
            "country": None,
            "first_name_sanitized": "foo",
        },
        "result_found": True,
        "first_name": "Foo",
        "probability": 0.98,
        "gender": "female",
    }

    result = parse_response(id_, full_name, response)

    expected_result = {
        "id": 123,
        "full_name": "Foo Bar",
        "samples": 106011,
        "first_name": "Foo",
        "probability": 0.98,
        "gender": "female",
    }

    assert result == expected_result
