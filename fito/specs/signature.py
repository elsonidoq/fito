from fito import Spec, SpecField, PrimitiveField
from fito.data_store.refactor import StorageRefactor
from fito.specs.fields import CollectionField


class Diff(Spec):
    added_fields = CollectionField()
    removed_fields = CollectionField()
    changed_fields = CollectionField()

    def create_refactor(self):
        if len(self.changed_fields) > 0:
            raise RuntimeError("Don't know how to handle modified fields yet")

        res = StorageRefactor()
        cls = type(self)

        for field, field_spec in self.added_fields:
            if not field_spec.has_default():
                raise RuntimeError("Can not add field '{}'. It doesn't have a default value".format(field))
            res = res.add_field(cls, field, default_value=field_spec.default)

        for field in self.removed_fields:
            res = res.remove_field(cls, field)

        return res

    def is_empty(self):
        return len(self.added_fields) == 0 and len(self.removed_fields) == 0 and len(self.changed_fields) == 0


class SpecSignature(Spec):
    spec_class = PrimitiveField(0, is_type=True)
    fields = CollectionField(1)

    def diff(self, other):
        assert self.spec_class is other.spec_class

        return Diff(
            added_fields=self.added_fields(other),
            removed_fields=other.added_fields(self),
            changed_fields=self.changed_fields(other)
        )

    def added_fields(self, other):
        return {k: v for k, v in self.fields.iteritems() if k not in other.fields}

    def changed_fields(self, other):
        res = {}
        for name, field in self.fields.iteritems():
            if name not in other.fields: continue
            if field == other.fields[name]: continue

            res[name] = field

        return res
