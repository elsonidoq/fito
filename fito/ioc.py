import os

import yaml

from fito import Spec, DictDataStore
from fito.specs.utils import general_iterator, general_new, is_iterable, recursive_map, general_append


def recursive_load(strings, paths=None):
    """
    Load strings or file contents
    :param strings: Represents yaml strings
    :param paths: Optionally context paths for each string, used for solving imports
    :return: an instance of ApplicationContext
    """
    if paths is not None:
        assert len(paths) == len(strings)
        assert all(map(os.path.exists, paths))

    parsed_strings = map(yaml.load, strings)
    included_files = []
    res = {}
    for i, parsed_string in enumerate(parsed_strings):

        # Collect imports
        imports = parsed_string.pop('import', [])
        if isinstance(imports, basestring): imports = [imports]

        for fname in imports:
            if paths is None: raise RuntimeError('Can not handle imports without paths')
            fname = os.path.join(paths[i], fname)
            included_files.append(fname)

        # Do not allow overrides between files, might be caotic
        for obj_name, obj in parsed_string.iteritems():
            if obj_name in res:
                raise RuntimeError(
                    "The object name {} is defined more than once.".format(obj_name) +
                    "\nOverrides can only be expressed via imports"
                )
            res[obj_name] = obj

    # After having processed everything, let's consider the included files
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

    def alias(self, key, object_name):
        if object_name not in self.objects:
            raise ValueError('Added an alias to a not existing object ("{}")'.format(object_name))

        if key in self.objects:
            raise ValueError('Invalid alias name "{}" already exists'.format(key))

        self.objects[key] = '${}'.format(object_name)

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
        else:
            res = self._try_load(res)

        return res

    def resolve(self, obj):
        res = general_new(obj)

        for k, v in general_iterator(obj):
            if is_iterable(v):
                v = recursive_map(v, self._try_load)
            else:
                v = self._try_load(v)

            general_append(res, k, v)
        return res

    def _try_load(self, v):
        if isinstance(v, basestring) and v.startswith('$'):
            return self._get_raw(v[1:])
        else:
            return v

