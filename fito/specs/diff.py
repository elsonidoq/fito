from StringIO import StringIO

from fito import Spec
from fito.data_store.refactor import StorageRefactor, ChainedRefactor
from fito.specs.fields import CollectionField, SpecCollection, PrimitiveField


class FieldChange(Spec):
    original_value = PrimitiveField(0)
    new_value = PrimitiveField(1)


class Diff(Spec):
    spec_type = PrimitiveField()
    added_fields = CollectionField()
    removed_fields = CollectionField()
    changed_fields = SpecCollection()
    field = PrimitiveField(default=None, help='Specifies that this diff only applies to this field')

    @classmethod
    def build(cls, old_dict, new_dict, field=None):
        added_fields = {k: v for k, v in new_dict.iteritems() if k not in old_dict}
        removed_fields = [k for k in old_dict if k not in new_dict]

        changed_fields = {}
        subdiffs = []
        for k in old_dict:
            if k not in new_dict: continue

            old_v = old_dict[k]
            new_v = new_dict[k]
            if isinstance(old_v, dict) and 'type' in old_v and isinstance(new_v, dict) and 'type' in new_v:
                # TODO this fails if a param starts being a spec, or a param stops being a spec
                # i.e. old_v and new_v refer to specs

                if field is None:
                    subdiff_field = k
                else:
                    subdiff_field = '{}.{}'.format(field, k)
                subdiff = Diff.build(old_v, new_v, field=subdiff_field)
                subdiffs.append(subdiff)

            elif old_v != new_v:
                changed_fields[k] = FieldChange(old_v, new_v)

        diff = Diff(
            spec_type=old_dict['type'],
            added_fields=added_fields,
            removed_fields=removed_fields,
            changed_fields=changed_fields,
            field=field
        )

        if subdiffs:
            subdiffs.insert(0, diff)
            return ChainedDiff(subdiffs)
        else:
            return diff

    def create_refactor(self):
        res = StorageRefactor()
        for field, value in self.added_fields.iteritems():
            res = res.add_field(self.spec_type, field, value)

        for field in self.removed_fields:
            res = res.remove_field(self.spec_type, field)

        for field, field_change in self.changed_fields.iteritems():
            res = res.add_field(self.spec_type, field_change.new_value)

        if self.field:
            res = res.project(self.field)

        return res

    def __repr__(self):
        res = StringIO()
        if self.field is None:
            template = '{}:\n\t{}'
            join_template = '\n\t'
        else:
            res.write("On field '{}'\n".format(self.field))
            template = '\t{}:\n\t\t{}'
            join_template = '\n\t\t'

        if self.added_fields:
            res.write(
                template.format(
                    'Added fields',
                    join_template.join(
                        '{}:\t{}'.format(k, v) for k, v in self.added_fields.iteritems()
                    )
                )
            )

        if self.removed_fields:
            if res.len > 0: res.write('\n\n')
            res.write(template.format('Removed fields', join_template.join(self.removed_fields)))

        if self.changed_fields:
            if res.len > 0: res.write('\n\n')
            res.write('Changed fields:\n')
            res.write('\t{:<20} {:<20} {:<20}'.format('field', 'from value', 'to value'))
            for field, field_change in self.changed_fields.iteritems():
                res.write('{}{:<20} {:<20} {:<20}'.format(join_template, field, field_change.original_value, field_change.new_value))

        return res.getvalue()


class ChainedDiff(Spec):
    subdiffs = SpecCollection(0)

    def create_refactor(self):
        refactors = []

        for subdiff in self.subdiffs:
            refactors.append(subdiff.create_refactor())

        return ChainedRefactor(*refactors)

    def __repr__(self):
        res = StringIO()

        for i, diff in enumerate(self.subdiffs):
            res.write(repr(diff))
            if i < len(self.subdiffs):
                res.write('\n\n')

        return res.getvalue()
