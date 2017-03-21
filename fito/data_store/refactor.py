from fito.operations.operation import Operation, OperationField
from fito.specs.base import get_import_path, Spec
from fito.specs.fields import UnboundPrimitiveField, PrimitiveField, SpecField
from fito.specs.utils import recursive_map


class StorageRefactor(Operation):
    doc = UnboundPrimitiveField(0, serialize=False)
    storage_refactor = SpecField(default=None)

    def add_field(self, spec_type, field_name, default_value=None):
        return AddField(spec_type, field_name, default_value, storage_refactor=self)

    def rename_field(self, spec_type, source, target):
        return RenameField(spec_type, source, target, storage_refactor=self)

    def remove_field(self, spec_type, field_name):
        return RemoveField(spec_type, field_name, storage_refactor=self)

    def change_type(self, spec_type, new_type):
        return ChangeType(spec_type, new_type, storage_refactor=self)

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

        # if 'operation_1' in self.doc['type']: import ipdb;ipdb.set_trace()

        doc = self.chain_transformations(self.doc)
        return recursive_map(doc, self.chain_transformations)

    def transformation(self, doc):
        return doc

    def chain_transformations(self, doc):
        doc = self.transformation(doc)
        if self.storage_refactor is not None:
            doc = self.storage_refactor.chain_transformations(doc)
        return doc


class FilteredStorageRefactor(StorageRefactor):
    spec_type = PrimitiveField(0)

    def matches(self, doc):
        return isinstance(doc, dict) and doc['type'] == self.get_spec_type_string()

    def get_spec_type_string(self):
        if isinstance(self.spec_type, basestring):
            return self.spec_type
        else:
            return get_import_path(self.spec_type)


class AddField(FilteredStorageRefactor):
    field_name = PrimitiveField(1)
    default_value = PrimitiveField(2)

    def transformation(self, doc):
        if self.matches(doc):
            doc = doc.copy()
            doc[self.field_name] = self.default_value

        return doc


class RenameField(FilteredStorageRefactor):
    source = PrimitiveField(1)
    target = PrimitiveField(2)

    def transformation(self, doc):
        if self.matches(doc):
            doc = doc.copy()
            doc[self.target] = doc.pop(self.source)
        return doc


class RemoveField(FilteredStorageRefactor):
    field_name = PrimitiveField(1)

    def transformation(self, doc):
        if self.matches(doc):
            doc = doc.copy()
            doc.pop(self.field_name, None)
        return doc


class ChangeType(FilteredStorageRefactor):
    new_type = PrimitiveField(1)

    def __init__(self, *args, **kwargs):
        super(ChangeType, self).__init__(*args, **kwargs)
        assert issubclass(self.new_type, Spec)

    def transformation(self, doc):
        if self.matches(doc):
            doc = doc.copy()
            doc['type'] = get_import_path(self.new_type)
        return doc
