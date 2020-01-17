import umap
import numpy as np


def umap_embeddings(data, n_neighbors, min_dist, n_components, metric):
    """Finds a low dimensional representation of the input embeddings.
    More info: https://umap-learn.readthedocs.io/en/latest/api.html#umap

    Args:
        data (:obj:`numpy.array` of :obj:`float`): Input vectors.
        n_neighbors (int): The size of local neighborhood (in terms of number 
            of neighboring sample points) used for manifold approximation.
        min_dist (float): The effective minimum distance between embedded points.
        n_components (int): The dimension of the space to embed into.
        metric (str): The metric to use to compute distances in high dimensional space.
     
    Returns:
        (numpy.ndarray)
    
    """
    reducer = umap.UMAP(
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        n_components=n_components,
        metric=metric,
        random_state=42,
    )
    return reducer.fit_transform(data)
