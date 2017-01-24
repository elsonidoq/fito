from copy import deepcopy

import yaml
from fito import Spec, DictDataStore
from fito.specs.utils import general_iterator, general_new, is_iterable

cache = DictDataStore()


class ApplicationContext(object):
    objects = {}

    @classmethod
    def load(cls, *fnames):
        fnames_contents = []
        for fname in fnames:
            with open(fname) as f:
                fnames_contents.append(f.read())

        big_yaml = '\n'.join(fnames_contents)

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
    for k, v in general_iterator(obj):
        if isinstance(v, basestring) and v.startswith('$'):
            v = ctx._get_raw(v[1:])
        res[k] = v
    return res


ctx = ApplicationContext
