import traceback
import warnings

from fito.model import Model
from fito.model import ModelParameter
from fito.operations.operation import MemoryObject
from fito.specs.base import ToggleField, SpecField, PrimitiveField

try:
    from gensim.models import Word2Vec as GensimWord2Vec
except ImportError:
    traceback.print_exc()
    warnings.warn('Could not import Word2Vec')


class Word2Vec(Model):
    sentences = ToggleField(0)
    size = ModelParameter(1, default=100)
    alpha = ModelParameter(2, default=0.025)
    window = ModelParameter(3, default=5)
    min_count = ModelParameter(4, default=5)
    max_vocab_size = ModelParameter(5, default=None)
    sample = ModelParameter(6, default=0.001)
    seed = ModelParameter(7, default=1)
    workers = ToggleField(8, default=3)
    train_iterator = ToggleField(
        default=None,
        help='When sentences is not a rewindable iterator, you must specify another copy of it here'
    )

    def apply(self, runner):
        kwargs = self.to_kwargs(include_toggle_fields=False)

        model = GensimWord2Vec(**kwargs)
        if self.train_iterator is None:
            self.train_iterator = self.sentences

        model.build_vocab(self.sentences)
        model.train(self.train_iterator)
        return model

