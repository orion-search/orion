import pytest

import torch
import numpy as np

from orion.packages.nlp.text2vec import encode_text
from orion.packages.nlp.text2vec import feature_extraction
from orion.packages.nlp.text2vec import average_vectors


def test_text_encoder():
    text = "Hello world. foo bar!"

    result = encode_text(text)

    expected_shape = torch.Size([1, 9])
    expected_values = torch.tensor([[2, 10975, 126, 9, 4310, 111, 748, 187, 3]])

    assert result.shape == expected_shape
    assert torch.all(result.eq(expected_values))
    # Ensure we're using the right model
    assert result.numpy()[0][0] == expected_values.numpy()[0][0]
    assert result.numpy()[0][1] == expected_values.numpy()[0][1]


def test_feature_extraction():
    input_ids = torch.tensor([[2, 10975, 126, 9, 4310, 111, 748, 187, 3]])

    result = feature_extraction(input_ids)
    expected_shape = torch.Size([1, 9, 768])

    assert result.shape == expected_shape


def test_average_vectors():
    vectors = torch.tensor([[[2, 3, 4], [7, 8, 9]]])

    result = average_vectors(vectors)
    expected_result = np.array([4.5, 5.5, 6.5])

    assert all(result == expected_result)
