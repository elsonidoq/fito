from fito import OperationRunner
from fito.specs.base import Spec


class Operation(Spec):
    def execute(self, runner=None):
        return self.apply(runner or OperationRunner())

    def apply(self, runner):
        raise NotImplementedError()
