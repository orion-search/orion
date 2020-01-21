import pytest
import numpy as np

from orion.packages.projection.dim_reduction import umap_embeddings


def test_umap_embeddings():
    arr = np.array(
        [
            [8.17294143, 7.2162937, 2.66418746, 7.80374946],
            [0.77239085, 8.15696308, 1.32678631, 1.06764033],
            [8.55001125, 4.77967993, 5.28321433, 0.43066799],
            [4.62088582, 4.77142547, 6.64696505, 2.17469441],
            [2.72299663, 1.17064131, 3.00076248, 3.48715742],
            [7.70638287, 9.00522287, 6.35119296, 8.45058572],
        ]
    )

    result = umap_embeddings(arr)
    expected_result = np.array(
        [
            [0.29602405, -23.65735],
            [1.2182287, -23.317778],
            [-0.31798708, -22.90279],
            [1.186063, -22.096878],
            [0.22926682, -21.939867],
            [0.62312275, -22.751759],
        ]
    )

    np.testing.assert_allclose(result, expected_result)
