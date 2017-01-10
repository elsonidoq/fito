from fito.specs.base import Spec


class Operation(Spec):
    def apply(self, runner):
        raise NotImplementedError()
