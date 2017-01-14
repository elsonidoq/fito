from fito import OperationRunner
from fito.specs.base import Spec, SpecField


class Operation(Spec):
    out_data_store = SpecField(default=None)

    def execute(self, runner=None):
        # Helps having an optional need for a runner
        return (runner or OperationRunner()).execute(self)

    def apply(self, runner):
        raise NotImplementedError()


