import torch
from transformers import DistilBertModel, DistilBertTokenizer
import numpy as np
import sentencepiece as spm

np.random.seed(42)


class Text2Vector:
    """Transform text to vector using DistilBert from HuggingFace."""

    def __init__(
        self,
        tokenizer_model=DistilBertTokenizer,
        transformer=DistilBertModel,
        pretrained_weights="distilbert-base-uncased",
    ):
        self.tokenizer_model = tokenizer_model
        self.transformer = transformer
        self.pretrained_weights = pretrained_weights
        self.tokenizer = self.tokenizer_model.from_pretrained(self.pretrained_weights)
        self.model = self.transformer.from_pretrained(self.pretrained_weights)

    def encode_text(self, text):
        """Tokenizes text and finds its indices.

        Args:
            text (str): Text input.
            tokenizer_model (transformers.tokenization_distilbert.DistilBertTokenizer): Pretrained tokenizer.
               The tokenizer must match the transformer architecture that will be used.
            pretrained_weights (str): Pretrained weights shortcut.

        Returns:
            (torch.Tensor) Indices of input sequence tokens in the vocabulary of the transformer.

        """

        # max_length is equal to 512 because that's the longest input sequence the model takes.
        return torch.tensor(
            [self.tokenizer.encode(text, add_special_tokens=True, max_length=512)]
        )

    def feature_extraction(self, input_ids):
        """Extracts word embeddings.

        Args:
            input_ids (torch.Tensor) Indices of input sequence tokens in the vocabulary of the transformer.
            model_class (transformers.modeling_distilbert.DistilBertModel): Pretrained transformer.
            pretrained_weights (str): Pretrained weights shortcut.

        Returns:
            (torch.Tensor) Tensor of shape (batch_size, sequence_length, hidden_size).

        """
        with torch.no_grad():
            # Keep only the sequence of hidden-states at the output of the last layer of the model.
            last_hidden_states = self.model(input_ids)[0]

        return last_hidden_states

    def average_vectors(self, vectors):
        """Averages a Tensor with hidden states.

        Args:
            vectors (torch.Tensor) Tensor of shape (batch_size, sequence_length, hidden_size).

        Returns:
            (numpy.ndarray) Average of the vectors of the shape (hidden_size,).

        """
        return np.mean([l for l in vectors.numpy()[0]], axis=0)
