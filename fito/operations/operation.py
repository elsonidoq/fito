from fito import OperationRunner
from fito.specs.base import Spec, SpecField


class Operation(Spec):
    out_data_store = SpecField(default=None)

    def execute(self, runner=None):
        res = self.apply(runner or OperationRunner())
        if self.out_data_store is not None:
            self.out_data_store[self] = res
        return res

    def apply(self, runner):
        raise NotImplementedError()
