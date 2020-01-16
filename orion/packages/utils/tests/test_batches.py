import pytest
from unittest import mock

from orion.packages.utils.batches import split_batches


@pytest.fixture
def generate_test_data():
    def _generate_test_data(n):
        return [{"data": "foo", "other": "bar"} for i in range(n)]

    return _generate_test_data


@pytest.fixture
def generate_test_set_data():
    def _generate_test_set_data(n):
        return {f"data{i}" for i in range(n)}

    return _generate_test_set_data


def test_split_batches_when_data_is_smaller_than_batch_size(generate_test_data):
    yielded_batches = []
    for batch in split_batches(generate_test_data(200), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 1


def test_split_batches_yields_multiple_batches_with_exact_fit(generate_test_data):
    yielded_batches = []
    for batch in split_batches(generate_test_data(2000), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 2


def test_split_batches_yields_multiple_batches_with_remainder(generate_test_data):
    yielded_batches = []
    for batch in split_batches(generate_test_data(2400), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 3


def test_split_batches_with_set(generate_test_set_data):
    yielded_batches = []
    for batch in split_batches(generate_test_set_data(2400), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 3
