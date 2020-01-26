import pytest

from orion.packages.utils.nlp_utils import clean_name


def test_clean_name_from_double_initials():
    name = "A. B. FooBar"
    result = clean_name(name)

    expected_result = None

    assert result == expected_result


def test_clean_name_from_single_initial():
    name = "A. FooBar"
    result = clean_name(name)

    expected_result = None

    assert result == expected_result


def test_clean_name_from_single_initial_variation():
    name = "Foo A. FooBar"
    result = clean_name(name)

    expected_result = "Foo FooBar"

    assert result == expected_result


def test_clean_name():
    name = "Foo FooBar"
    result = clean_name(name)

    expected_result = "Foo FooBar"

    assert result == expected_result
