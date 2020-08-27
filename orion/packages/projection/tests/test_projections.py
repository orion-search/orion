import pytest
import numpy as np

from orion.packages.projection.dim_reduction import umap_embeddings
from orion.packages.projection.faiss_index import faiss_index


def test_umap_embeddings():
    arr = np.random.uniform(0.1, 10, [10, 4])

    _, result = umap_embeddings(arr)
    expected_shape = (10, 2)

    assert result.shape == expected_shape


def test_faiss_index_construction_with_ids():
    vectors = np.array(
        [
            [0.1243, 1.124, 0.21456, 0.123],
            [1.24, 0.765, 0.987, 0.433],
            [0.123, 0.6543, 0.734, 0.235],
        ],
        dtype="float32",
    )
    ids = [11, 22, 33]

    index = faiss_index(vectors, ids)

    D, I = index.search(np.array([vectors[0]]), k=3)
    expected_D = np.array([[0.0, 0.5029817, 2.066431]], dtype="float32")
    expected_I = np.array([[11, 33, 22]])

    expected_index_total = 3
    index_total = index.ntotal

    assert index_total == expected_index_total
    np.testing.assert_almost_equal(D, expected_D)
    np.testing.assert_almost_equal(I, expected_I)


def test_faiss_index_construction_without_ids():
    vectors = np.array(
        [
            [0.1243, 1.124, 0.21456, 0.123],
            [1.24, 0.765, 0.987, 0.433],
            [0.123, 0.6543, 0.734, 0.235],
        ],
        dtype="float32",
    )

    index = faiss_index(vectors)

    D, I = index.search(np.array([vectors[0]]), k=3)
    expected_D = np.array([[0.0, 0.5029817, 2.066431]], dtype="float32")
    expected_I = np.array([[0, 2, 1]])

    expected_index_total = 3
    index_total = index.ntotal

    assert index_total == expected_index_total
    np.testing.assert_almost_equal(D, expected_D)
    np.testing.assert_almost_equal(I, expected_I)
