import yaml
from fito import Spec, DictDataStore
from fito.specs.utils import general_iterator, general_new, is_iterable, recursive_map

cache = DictDataStore()


class ApplicationContext(object):
    def __init__(self, objects):
        self.objects = objects

    @classmethod
    def load(cls, *fnames):
        fnames_contents = []
        for fname in fnames:
            with open(fname) as f:
                fnames_contents.append(f.read())

        return cls.load_from_strings(*fnames_contents)

    @classmethod
    def load_from_strings(cls, *strings):
        big_yaml = '\n'.join(strings)

        objects = yaml.load(big_yaml)

        return cls(objects)

    @cache.autosave(method_type='instance')
    def get(self, name):
        res = self._get_raw(name)

        if isinstance(res, dict) and 'type' in res:
            res = Spec.dict2spec(res)

        return res

    @cache.autosave(method_type='instance')
    def _get_raw(self, name):
        res = self.objects[name]

        if is_iterable(res):
            res = self.resolve(res)

        return res

    def resolve(self, obj):
        res = general_new(obj)

        def try_load(v):
            if isinstance(v, basestring) and v.startswith('$'):
                return self._get_raw(v[1:])
            else:
                return v

        for k, v in general_iterator(obj):
            if is_iterable(v):
                v = recursive_map(v, try_load)
            else:
                v = try_load(v)

            res[k] = v
        return res

