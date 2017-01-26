from copy import deepcopy

import yaml
from fito import Spec, DictDataStore
from fito.specs.utils import general_iterator, general_new, is_iterable, recursive_map

cache = DictDataStore()


class ApplicationContext(object):
    objects = None

    @classmethod
    def load(cls, *fnames):
        fnames_contents = []
        for fname in fnames:
            with open(fname) as f:
                fnames_contents.append(f.read())

        return cls.load_from_strings(*fnames_contents)

    @classmethod
    def load_from_strings(cls, *strings):
        if cls.objects is not None:
            raise RuntimeError('Can not load ApplicationContext twice. We need @cache.autosave for instance methods!')

        big_yaml = '\n'.join(strings)

        objects = yaml.load(big_yaml)

        cls.objects = objects

    @cache.autosave(method_type='class')
    def get(cls, name):
        res = cls._get_raw(name)

        if isinstance(res, dict) and 'type' in res:
            res = Spec.dict2spec(res)

        return res

    @cache.autosave(method_type='class')
    def _get_raw(cls, name):
        res = cls.objects[name]

        if is_iterable(res):
            res = resolve(res)

        return res


def resolve(obj):
    res = general_new(obj)

    def try_load(v):
        if isinstance(v, basestring) and v.startswith('$'):
            return ctx._get_raw(v[1:])
        else:
            return v

    for k, v in general_iterator(obj):
        if is_iterable(v):
            v = recursive_map(v, try_load)
        else:
            v = try_load(v)

        res[k] = v
    return res


ctx = ApplicationContext
