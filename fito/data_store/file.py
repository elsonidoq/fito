import json
import mmh3
import os
import pickle
import shutil
import traceback
import warnings
from time import time, sleep

import yaml
from fito import PrimitiveField
from fito import Spec
from fito import SpecField
from fito.data_store.base import BaseDataStore


class Serializer(Spec):
    def save(self, obj, subdir): raise NotImplemented()

    def load(self, subdir): raise NotImplemented()

    def exists(self, subdir): raise NotImplemented()


class SingleFileSerializer(Serializer):
    def get_fname(self, subdir): raise NotImplemented()

    def exists(self, subdir):
        return os.path.exists(self.get_fname(subdir))


class PickleSerializer(SingleFileSerializer):
    def get_fname(self, subdir):
        return os.path.join(subdir, 'obj.pkl')

    def save(self, obj, subdir):
        with open(self.get_fname(subdir), 'w') as f:
            pickle.dump(obj, f, 2)

    def load(self, subdir):
        with open(self.get_fname(subdir)) as f:
            return pickle.load(f)


class RawSerializer(SingleFileSerializer):
    def get_fname(self, subdir):
        return os.path.join(subdir, 'obj.raw')

    def save(self, obj, subdir):
        with open(self.get_fname(subdir), 'w') as f:
            f.write(obj)

    def load(self, subdir):
        with open(self.get_fname(subdir)) as f:
            return f.read()


class FileDataStore(BaseDataStore):
    path = PrimitiveField(0)
    split_keys = PrimitiveField(default=True)
    serializer = SpecField(default=PickleSerializer(), base_type=Serializer)
    use_class_name = PrimitiveField(default=False, help='Whether the first level should be the class name')
    auto_init_file_system = PrimitiveField(
        default=False,
        help='Whether we should create the config files when the data store is instanced',
        serialize=False
    )

    allow_human_readable_dirs = PrimitiveField(default=False)
    store_key = PrimitiveField(default=True)

    def __init__(self, *args, **kwargs):
        super(FileDataStore, self).__init__(*args, **kwargs)
        if self.auto_init_file_system:
            self.init_file_system()

    def init_file_system(self):
        if not os.path.exists(self.path): os.makedirs(self.path)

        conf_file = os.path.join(self.path, 'conf.yaml')
        if os.path.exists(conf_file):

            with open(conf_file) as f:
                conf = yaml.load(f)

            conf_serializer = Spec.dict2spec(conf['serializer'])
            conf_attr_names = 'store_key allow_human_readable_dirs use_class_name'.split()

            for attr_name in conf_attr_names:
                conf[attr_name] = conf.get(attr_name, getattr(type(self), attr_name).default)
                self._check_config(attr_name, conf[attr_name])

            if self.serializer is not None and self.serializer != conf_serializer:
                raise RuntimeError(
                    "This store was initialized with this serializer:\n{}\n\n" +
                    "But was now instanced with this one:\n{}".format(
                        json.dumps(conf['serializer'], indent=2),
                        json.dumps(self.serializer.to_dict(), indent=2)
                    )
                )

        else:
            with open(conf_file, 'w') as f:
                yaml.dump(
                    {
                        'serializer': self.serializer.to_dict(),
                        'use_class_name': self.use_class_name,
                        'allow_human_readable_dirs': self.allow_human_readable_dirs,
                        'store_key': self.store_key
                    },
                    f
                )

    def _check_config(self, attr_name, conf_val):
        attr_val = getattr(self, attr_name)
        if attr_val != conf_val:
            raise RuntimeError(
                'This store was initialized with {} = {} and now was instanced with {}'.format(
                    attr_name,
                    conf_val,
                    attr_val
                )
            )

    def clean(self, cls=None):
        for op in self.iterkeys():
            if cls is None or (cls is not None and isinstance(op, cls)):
                self.remove(op)

    def remove(self, op):
        subdir = self._get_subdir(op)
        shutil.rmtree(subdir)

    def iterkeys(self, raw=False):
        for subdir, _, _ in os.walk(self.path):
            id_fname = os.path.join(subdir, self.get_id_fname())
            if not os.path.exists(id_fname): continue

            with open(id_fname) as f:
                id = f.read()

            if self.store_key:
                spec_dict = Spec.key2dict(id)
            else:
                spec_dict = json.loads(id)

            if raw:
                yield subdir, spec_dict
            else:
                try:
                    yield Spec.dict2spec(spec_dict)
                except Exception, e:  # there might be a key that is not a valid json
                    if len(e.args) > 0 and isinstance(e.args[0], basestring) and e.args[0].startswith(
                            'Unknown spec type'): raise e
                    traceback.print_exc()
                    warnings.warn('Unable to load spec key: {}'.format(key))
                    continue

    def get_by_id(self, subdir):
        assert subdir.startswith(self.path)
        return self.serializer.load(subdir)

    def iteritems(self):
        for op in self.iterkeys():
            try:
                yield op, self.get(op)
            except:
                # TODO: check whether the file exists or not
                continue

    def _get_dir(self, spec):
        is_human_readable = self.allow_human_readable_dirs and len(spec.key) < 50

        if is_human_readable:
            h = spec.key
        else:
            h = str(mmh3.hash(spec.key))

        path = os.path.join(self.path, type(spec).__name__) if self.use_class_name else self.path

        if self.split_keys and not is_human_readable:
            fname = os.path.join(path, h[:3], h[3:6], h[6:])
        else:
            fname = os.path.join(path, h)
        return fname

    def _get_subdir(self, spec):
        dir = self._get_dir(spec)
        if not os.path.exists(dir): raise KeyError("Spec not found")

        subdirs = os.listdir(dir)
        for subdir in subdirs:
            subdir = os.path.join(dir, subdir)
            if not os.path.isdir(subdir): continue

            fname = os.path.join(subdir, self.get_id_fname())
            if not os.path.exists(fname): continue

            with open(fname) as f:
                id = f.read()

            if not self.store_key: id = json.loads(id)

            if self.serializer.exists(subdir) and self.id_eq_spec(id, spec): break

        else:
            raise KeyError("Spec not found")

        return subdir

    def id_eq_spec(self, id, spec):
        return (
            (self.store_key and id == spec.key) or
            (not self.store_key and id == spec.to_dict())
        )

    def get_id_fname(self):
        return 'key' if self.store_key else 'spec_dict'

    def _get(self, spec):
        subdir = self._get_subdir(spec)
        try:
            return self.serializer.load(subdir)
        except Exception:
            traceback.print_exc()
            raise KeyError('Failed to load spec')

    def get_dir_for_saving(self, spec):
        dir = self._get_dir(spec)
        # this accounts for both checking if it not exists, and the fact that there might
        # be another process doing the same thing
        try:
            os.makedirs(dir)
        except OSError:
            pass
        for subdir in os.listdir(dir):
            subdir = os.path.join(dir, subdir)

            id_fname = os.path.join(subdir, self.get_id_fname())
            if not os.path.exists(id_fname): continue
            with open(id_fname) as f:
                id = f.read()

            if self.id_eq_spec(id, spec):
                return subdir
        else:
            while True:
                subdirs = map(int, os.listdir(dir))
                if len(subdirs) == 0:
                    subdir = '0'
                else:
                    subdir = str(max(subdirs) + 1)
                subdir = os.path.join(dir, subdir)
                try:
                    os.makedirs(subdir)
                    return subdir
                except OSError:
                    pass

    def save(self, spec, series):
        if not self.auto_init_file_system: self.init_file_system()

        subdir = self.get_dir_for_saving(spec)
        with open(os.path.join(subdir, self.get_id_fname()), 'w') as f:
            if self.store_key:
                f.write(spec.key)
            else:
                json.dump(spec.to_dict(), f)

        self.serializer.save(series, subdir)

    def __contains__(self, spec):
        try:
            subdir = self._get_subdir(spec)
            return self.serializer.exists(subdir)
        except KeyError:
            return False

