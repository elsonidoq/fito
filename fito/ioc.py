import os

import yaml

from fito import Spec, DictDataStore
from fito.specs.utils import general_iterator, general_new, is_iterable, recursive_map


def recursive_load(strings, paths=None):
    if paths is not None:
        assert len(paths) == len(strings)
        assert all(map(os.path.exists, paths))

    all_objects = map(yaml.load, strings)
    included_files = []
    res = {}
    for i, d in enumerate(all_objects):
        imports = d.pop('import', [])
        if isinstance(imports, basestring): imports = [imports]

        for fname in imports:
            if paths is None: raise RuntimeError('Can not handle imports without paths')
            fname = os.path.join(paths[i], fname)
            included_files.append(fname)

        for obj_name, obj in d.iteritems():
            if obj_name in res:
                raise RuntimeError(
                    "The object name {} is defined more than once.".format(obj_name) +
                    "\nOverrides can only be expressed via imports"
                )
            res[obj_name] = obj

    if included_files:
        tmp_ctx = ApplicationContext.load(*included_files)
        for obj_name, obj in tmp_ctx.objects.iteritems():
            res[obj_name] = obj

    return res


class ApplicationContext(object):
    def __init__(self, objects):
        self.objects = objects

    @classmethod
    def load(cls, *fnames):
        fnames_contents = []
        paths = []
        for fname in fnames:
            paths.append(os.path.abspath(os.path.dirname(fname)))
            with open(fname) as f:
                fnames_contents.append(f.read())

        return cls.load_from_strings(fnames_contents, paths=paths)

    @classmethod
    def load_from_strings(cls, strings, paths=None):
        objects = recursive_load(strings, paths)
        return cls(objects)

    cache = DictDataStore()

    @cache.autosave(method_type='instance')
    def get(self, name):
        res = self._get_raw(name)

        if isinstance(res, dict) and 'type' in res:
            res = Spec.dict2spec(res)

        return res

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
