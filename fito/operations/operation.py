from fito import OperationRunner
from fito.specs.base import Spec, SpecField, _no_default


class Operation(Spec):
    out_data_store = SpecField(default=None)

    def execute(self, runner=None):
        # Helps having an optional need for a runner
        return (runner or OperationRunner()).execute(self)

    def apply(self, runner):
        raise NotImplementedError()


def OperationField(pos=None, default=_no_default, base_type=None):
    return SpecField(pos=pos, default=default, base_type=base_type or Operation)
