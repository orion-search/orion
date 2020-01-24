import pytest
import numpy as np

from orion.packages.projection.dim_reduction import umap_embeddings


def test_umap_embeddings():
    arr = np.random.uniform(0.1, 10, [10, 4])

    result = umap_embeddings(arr)
    expected_shape = (10, 2)

    assert result.shape == expected_shape
