from StringIO import StringIO

from fito import Spec
from fito.data_store.refactor import StorageRefactor
from fito.specs.fields import CollectionField, SpecCollection, PrimitiveField


class FieldChange(Spec):
    original_value = PrimitiveField(0)
    new_value = PrimitiveField(1)


class Diff(Spec):
    spec_type = PrimitiveField()
    added_fields = CollectionField()
    removed_fields = CollectionField()
    changed_fields = SpecCollection()
    subdiffs = SpecCollection()

    @classmethod
    def build(cls, old_dict, new_dict):
        added_fields = {k: v for k, v in new_dict.iteritems() if k not in old_dict}
        removed_fields = [k for k in old_dict if k not in new_dict]

        changed_fields = {}
        subdiffs = {}
        for k in old_dict:
            if k not in new_dict: continue

            old_v = old_dict[k]
            new_v = new_dict[k]
            if isinstance(old_v, dict) and 'type' in old_v and isinstance(new_v, dict) and 'type' in new_v:
                # i.e. old_v and new_v refer to specs
                subdiffs[k] = Diff.build(old_v, new_v)

            elif old_v != new_v:
                changed_fields[k] = FieldChange(old_v, new_v)

        return Diff(
            spec_type=old_dict['type'],
            added_fields=added_fields,
            removed_fields=removed_fields,
            changed_fields=changed_fields,
            subdiffs=subdiffs
        )

    def create_refactor(self):
        res = StorageRefactor()
        for field, value in self.added_fields.iteritems():
            res = res.add_field(self.spec_type, field, value)

        for field in self.removed_fields:
            res = res.remove_field(self.spec_type, field)

        for field, field_change in self.changed_fields.iteritems():
            res = res.add_field(self.spec_type, field_change.new_value)

        for field, subdiff in self.subdiffs:
            res = StorageRefactor()


    def __repr__(self):
        res = StringIO()
        template = '{}:\n\t{}'

        if self.added_fields:
            res.write(template.format('Added fields', '\n\t'.join(self.added_fields)))

        if self.removed_fields:
            if res.len > 0: res.write('\n\n')
            res.write(template.format('Removed fields', '\n\t'.join(self.removed_fields)))

        if self.changed_fields:
            if res.len > 0: res.write('\n\n')
            res.write('Changed fields:\n')
            res.write('\t{:<20} {:<20} {:<20}'.format('field', 'from value', 'to value'))
            for field, field_change in self.changed_fields.iteritems():
                res.write('\n\t{:<20} {:<20} {:<20}'.format(field, field_change.original_value, field_change.new_value))

        if self.subdiffs:
            if res.len > 0: res.write('\n\n')
            res.write('\nSub diffs:\n')
            for field, subdiff in self.subdiffs.iteritems():
                res.write('\tField: {}\n'.format(field))
                res.write('\t' + repr(subdiff).replace('\n', '\n\t'))

        return res.getvalue()
