from fito.operations.operation import Operation
from fito.specs.base import get_import_path
from fito.specs.fields import UnbindedPrimitiveField, PrimitiveField, SpecField


class StorageRefactor(Operation):
    doc = UnbindedPrimitiveField(0, serialize=False)
    storage_refactor = SpecField(default=None)

    def add_field(self, field_name, default_value=None):
        return AddField(field_name, default_value, storage_refactor=self)

    def rename_field(self, source, target):
        return RenameField(source, target, storage_refactor=self)

    def remove_field(self, field_name):
        return RemoveField(field_name, storage_refactor=self)

    def change_type(self, operation_class):
        return ChangeType(operation_class, storage_refactor=self)

    def filter_by_type(self, operation_class):
        """
        This will filter all transformations made by self.
        That implies that filter_by_type must be called last on the transformation list

        Example:
        >>> (
        ...    StorageRefactor()
        ...    .add_field('some', 1)
        ...    .remove_field('other')
        ...    .filter_by_type(Operation)
        ... )
        """
        return FilterByType(target_type=get_import_path(operation_class), storage_refactor=self)

    def _bind(self, meth_name, *args, **kwargs):
        res = getattr(super(StorageRefactor, self), meth_name)(*args, **kwargs)
        if self.storage_refactor is not None:
            res.storage_refactor = res.storage_refactor.bind(*args, **kwargs)
        return res

    def bind(self, *args, **kwargs):
        return self._bind('bind', *args, **kwargs)

    def inplace_bind(self, *args, **kwargs):
        return self._bind('inplace_bind', *args, **kwargs)

    def apply(self, runner):
        assert isinstance(self.doc, dict)

        if self.storage_refactor is not None:
            runner.execute(self.storage_refactor)

        self._apply()

    def _apply(self):
        pass


class FilterByType(StorageRefactor):
    target_type = PrimitiveField(0)

    def apply(self, runner):
        if self.doc['type'] == self.target_type:
            super(FilterByType, self).apply(runner)


class AddField(StorageRefactor):
    field_name = PrimitiveField(0)
    default_value = PrimitiveField(1)

    def _apply(self):
        self.doc[self.field_name] = self.default_value


class RenameField(StorageRefactor):
    source = PrimitiveField(0)
    target = PrimitiveField(1)

    def _apply(self):
        self.doc[self.target] = self.doc.pop(self.source)


class RemoveField(StorageRefactor):
    field_name = PrimitiveField(0)

    def _apply(self):
        self.doc.pop(self.field_name, None)


class ChangeType(StorageRefactor):
    operation_class = PrimitiveField(0)

    def _apply(self):
        assert issubclass(self.operation_class, Operation)
        self.doc['type'] = get_import_path(self.operation_class)
