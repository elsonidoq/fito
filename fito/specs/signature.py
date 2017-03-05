from fito import Spec, SpecField
from fito.specs.fields import CollectionField


class Diff(Spec):
    added_fields = CollectionField()
    removed_fields = CollectionField()
    changed_fields = CollectionField()


class SpecSignature(Spec):
    spec_class = SpecField(0, is_type=True)
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
