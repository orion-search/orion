import pytest
from unittest import mock

from orion.packages.mag.query_mag_api import build_expr
from orion.packages.mag.query_mag_api import query_mag_api
from orion.packages.mag.query_mag_api import dedupe_entities
from orion.packages.mag.query_mag_api import build_composite_expr


class TestBuildExpr:
    def test_build_expr_correctly_forms_query(self):
        assert list(build_expr([1, 2], "Id", 1000)) == ["expr=OR(Id=1,Id=2)"]
        assert list(build_expr(["cat", "dog"], "Ti", 1000)) == [
            "expr=OR(Ti='cat',Ti='dog')"
        ]

    def test_build_expr_respects_query_limit_and_returns_remainder(self):
        assert list(build_expr([1, 2, 3], "Id", 21)) == [
            "expr=OR(Id=1,Id=2)",
            "expr=OR(Id=3)",
        ]


@mock.patch("orion.packages.mag.query_mag_api.requests.post", autospec=True)
def test_query_mag_api_sends_correct_request(mocked_requests):
    sub_key = 123
    fields = ["Id", "Ti"]
    expr = "expr=OR(Id=1,Id=2)"
    query_mag_api(expr, fields, sub_key, query_count=10, offset=0)
    expected_call_args = mock.call(
        "https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate",
        data=b"expr=OR(Id=1,Id=2)&count=10&offset=0&attributes=Id,Ti",
        headers={
            "Ocp-Apim-Subscription-Key": 123,
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    assert mocked_requests.call_args == expected_call_args


def test_dedupe_entities_picks_highest_for_each_title():
    entities = [
        {"Id": 1, "Ti": "test title", "logprob": 44},
        {"Id": 2, "Ti": "test title", "logprob": 10},
        {"Id": 3, "Ti": "another title", "logprob": 5},
        {"Id": 4, "Ti": "another title", "logprob": 10},
    ]

    assert dedupe_entities(entities) == {1, 4}


def test_build_composite_queries_correctly():
    assert (
        build_composite_expr(["bar", "foo"], "F.FN", ("2019-01-01", "2019-02-22"))
        == "expr=OR(And(Composite(F.FN='bar'), D=['2019-01-01', '2019-02-22']), And(Composite(F.FN='foo'), D=['2019-01-01', '2019-02-22']))"
    )
