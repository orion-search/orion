import numpy as np
import faiss


def faiss_index(vectors, ids=None):
    """Create a brute-force FAISS index.

    Args:
        vectors (:obj:`numpy.array` of `float`): Usually document vectors
        ids (:obj:`list` of `int`, None): FAISS creates a numerical index which
            can be substituted by a list of ids. Here, it can be paper IDs.

    Returns:
        index (`faiss.swigfaiss.IndexIDMap`)

    """
    index = faiss.IndexFlatL2(vectors.shape[1])
    if ids:
        index = faiss.IndexIDMap(index)
        index.add_with_ids(vectors, np.array([i for i in ids]))
    else:
        index.add(vectors)

    return index
