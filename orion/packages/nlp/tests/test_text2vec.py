import pytest

import torch
import numpy as np
import pytest
from unittest import mock
import pandas as pd
import tensorflow as tf
import sentencepiece as spm
import tensorflow_hub as hub

from orion.packages.nlp.text2vec import Text2Vector
from orion.packages.nlp.text2vec import use_vectors


def test_text_encoder():
    text = "Hello world. foo bar!"
    tv = Text2Vector()
    result = tv.encode_text(text)

    expected_shape = torch.Size([1, 9])
    expected_values = torch.tensor([[2, 10975, 126, 9, 4310, 111, 748, 187, 3]])

    assert result.shape == expected_shape
    assert torch.all(result.eq(expected_values))
    # Ensure we're using the right model
    assert result.numpy()[0][0] == expected_values.numpy()[0][0]
    assert result.numpy()[0][1] == expected_values.numpy()[0][1]


def test_feature_extraction():
    input_ids = torch.tensor([[2, 10975, 126, 9, 4310, 111, 748, 187, 3]])

    tv = Text2Vector()
    result = tv.feature_extraction(input_ids)
    expected_shape = torch.Size([1, 9, 768])

    assert result.shape == expected_shape


def test_use_vectors():
    documents = ["Water under the bridge.", "Who let the dogs out."]

    values = [4489, 315, 9, 5063, 6, 1945, 456, 9, 112, 70, 6]
    indices = [
        [0, 0],
        [0, 1],
        [0, 2],
        [0, 3],
        [0, 4],
        [1, 0],
        [1, 1],
        [1, 2],
        [1, 3],
        [1, 4],
        [1, 5],
    ]
    dense_shape = (2, 6)

    doc_embeddings = use_vectors(documents)
    assert len(documents) == doc_embeddings.shape[0]
    assert doc_embeddings.shape[1] == 512
