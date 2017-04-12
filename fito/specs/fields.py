from functools import total_ordering

import base
from fito.specs.utils import is_iterable, general_iterator

# it's a constant that is different from every other object
_no_default = object()


class MockIterable(object):
    def __len__(self): return

    def __getitem__(self, _): return

    def __setitem__(self, _, __): return

    def __delitem__(self, _): return

    def __reversed__(self): return

    def __contains__(self, _): return

    def __setslice__(self, _, __, ___): return

    def __delslice__(self, _, __): return

    def iteritems(self): return


class Field(object):
    """
    Base class for field definition on an :py:class:`Spec`
    """

    def __init__(self, pos=None, default=_no_default, serialize=True, *args, **kwargs):
        """
        :param pos: The position on the argument list
        :param default: The default value
        :param serialize: Whether to include this field in the serialization. A side effect of this field is
        that when set to False, this field is not considered when comparing two specs
        :param args: Helps having them to create on the fly sublcasses of field. See :py:func:Spec:
        :param kwargs:
        """
        self.pos = pos
        self.default = default
        self.serialize = serialize

    @property
    def allowed_types(self):
        raise NotImplementedError()

    def check_valid_value(self, value):
        return any([isinstance(value, t) for t in self.allowed_types])

    def __eq__(self, other):
        return self is other

    def __repr__(self):
        args = []
        if self.pos is not None: args.append('{}'.format(self.pos))
        if self.default is not _no_default: args.append('default={}'.format(self.default))
        if not self.serialize: args.append('serialize={}'.format(self.serialize))
        return '{}({})'.format(type(self).__name__, ', '.join(args))

    def has_default_value(self):
        return self.default is not _no_default


class PrimitiveField(Field):
    """
    Specifies a Field whose value is going to be a python object
    """

    @property
    def allowed_types(self):
        return [object]


class CollectionField(PrimitiveField, MockIterable):
    @property
    def allowed_types(self):
        return list, dict, tuple


@total_ordering
class NumericField(PrimitiveField):
    def __lt__(self, _): return

    def __add__(self, _): return

    def __sub__(self, other): return

    def __mul__(self, other): return

    def __floordiv__(self, other): return

    def __mod__(self, other): return

    def __divmod__(self, other): return

    def __pow__(self, _, modulo=None): return

    def __lshift__(self, other): return

    def __rshift__(self, other): return

    def __and__(self, other): return

    def __xor__(self, other): return

    def __or__(self, other): return

    @property
    def allowed_types(self):
        return int, float


class BaseSpecField(Field):
    """
    Specifies a Field whose value will be an Spec
    """

    def __init__(self, pos=None, default=_no_default, base_type=None, serialize=True, *args, **kwargs):
        super(BaseSpecField, self).__init__(pos=pos, default=default, serialize=serialize, *args, **kwargs)
        self.base_type = base_type or base.Spec
        self.serialize = serialize

    @property
    def allowed_types(self):
        return [self.base_type]


def SpecField(pos=None, default=_no_default, base_type=None, serialize=True, spec_field_subclass=None):
    """
    Builds a SpecField

    :param pos: Position on *args
    :param default: Default value
    :param base_type: Base type, it does some type checkig + avoids some warnings from IntelliJ
    :param serialize: Whether this spec field should be included in the serialization of the object
    :param spec_field_subclass: Sublcass of BaseSpecField, useful to extend the lib

    :return:
    """
    if not serialize and default is _no_default:
        raise RuntimeError("If serialize == False, the field should have a default value")

    spec_field_subclass = spec_field_subclass or BaseSpecField

    if base_type is not None:
        assert issubclass(base_type, base.Spec)
        return_type = type(
            'SpecFieldFor{}'.format(base_type.__name__),
            (spec_field_subclass, base_type),
            {}
        )
    else:
        return_type = spec_field_subclass

    return return_type(pos=pos, default=default, base_type=base_type, serialize=serialize)


class SpecCollection(Field, MockIterable):
    """
    Specifies a Field whose value is going to be a collection of specs
    """

    @property
    def allowed_types(self):
        return list, dict, tuple

    def check_valid_value(self, value):
        if not is_iterable(value): return False

        for k, v in general_iterator(value):
            if not isinstance(v, base.Spec): return False

        return True


class KwargsField(SpecCollection):
    def __init__(self):
        super(KwargsField, self).__init__(default={})

    @property
    def allowed_types(self):
        return [dict]


class ArgsField(SpecCollection):
    def __init__(self):
        super(ArgsField, self).__init__(default=tuple())

    @property
    def allowed_types(self):
        return [tuple, list]


class UnboundField(object):
    pass


class BaseUnboundSpec(BaseSpecField, UnboundField):
    pass


def UnboundSpecField(pos=None, default=_no_default, base_type=None, spec_field_subclass=None):
    spec_field_subclass = spec_field_subclass or BaseUnboundSpec
    assert issubclass(spec_field_subclass, BaseUnboundSpec)
    return SpecField(pos=pos, default=default, base_type=base_type, spec_field_subclass=spec_field_subclass)


class UnboundPrimitiveField(PrimitiveField, UnboundField):
    pass
