import torch
import numpy as np
from transformers import AlbertModel, AlbertTokenizer

pretrained_weights = "albert-base-v2"


def encode_text(
    text, tokenizer_model=AlbertTokenizer, pretrained_weights=pretrained_weights
):
    """Tokenizes text and finds its indices.
    
    Args:
        text (str): Text input.
        tokenizer_model (transformers.tokenization_albert.AlbertTokenizer): Pretrained tokenizer.
           The tokenizer must match the transformer architecture that will be used.
        pretrained_weights (str): Pretrained weights shortcut.
    
    Returns:
        (torch.Tensor) Indices of input sequence tokens in the vocabulary of the transformer.
    
    """
    tokenizer = tokenizer_model.from_pretrained(pretrained_weights)
    # max_length is equal to 512 because that's the longest input sequence the model takes.
    return torch.tensor(
        [tokenizer.encode(text, add_special_tokens=True, max_length=512)]
    )


def feature_extraction(input_ids, model_class=AlbertModel, pretrained_weights=pretrained_weights):
    """Extracts word embeddings.
    
    Args:
        input_ids (torch.Tensor) Indices of input sequence tokens in the vocabulary of the transformer.
        model_class (transformers.modeling_albert.AlbertModel): Pretrained transformer.
        pretrained_weights (str): Pretrained weights shortcut.
    
    Returns:
        (torch.Tensor) Tensor of shape (batch_size, sequence_length, hidden_size).
    
    """
    model = model_class.from_pretrained(pretrained_weights)
    with torch.no_grad():
        # Keep only the sequence of hidden-states at the output of the last layer of the model.
        last_hidden_states = model(input_ids)[0]

    return last_hidden_states


def average_vectors(vectors):
    """Averages a Tensor with hidden states.
    
    Args:
        vectors (torch.Tensor) Tensor of shape (batch_size, sequence_length, hidden_size).
    
    Returns:
        (numpy.ndarray) Average of the vectors of the shape (hidden_size,).
    
    """
    return np.mean([l for l in vectors.numpy()[0]], axis=0)
