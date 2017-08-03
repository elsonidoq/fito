import json
import mmh3
import os
import pickle
import shutil
import traceback
import warnings
from pprint import pformat
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
    serializer = SpecField(default=PickleSerializer(), base_type=Serializer)
    auto_init_file_system = PrimitiveField(
        default=False,
        help='Whether we should create the config files when the data store is instanced',
        serialize=False
    )

    allow_human_readable_dirs = PrimitiveField(default=True)
    match_using_key = PrimitiveField(
        default=True,
        serialize=False,
        help='Wether to match specs by key or by dict'
    )

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
            conf_attr_names = 'allow_human_readable_dirs'.split()

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
                        'allow_human_readable_dirs': self.allow_human_readable_dirs,
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
            spec_dict_fname = os.path.join(subdir, 'spec_dict')
            if not os.path.exists(spec_dict_fname): continue

            with open(spec_dict_fname) as f:
                spec_dict = json.load(f)

            if raw:
                yield subdir, spec_dict
            else:
                try:
                    yield Spec.dict2spec(spec_dict)
                except Exception, e:  # there might be a key that is not a valid json
                    if len(e.args) > 0 and isinstance(e.args[0], basestring) and e.args[0].startswith('Unknown spec type'): raise e
                    traceback.print_exc()
                    warnings.warn('Unable to load spec key: {}'.format(spec_dict))
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
        """
        Returns the top dir associated with a spec.
        We might have collisions here. Those collisions are handled on `_get_subdir`
        """
        if self.allow_human_readable_dirs:
            h = spec.key
        else:
            h = str(mmh3.hash(spec.key))

        fname = os.path.join(self.path, h)
        return fname

    def _get_subdir(self, spec):
        """
        Returns the exact path containing the value associated with `spec`
        If `spec` is not stored, raises KeyError
        """
        dir = self._get_dir(spec)
        if not os.path.exists(dir): raise KeyError("Spec not found")

        subdirs = os.listdir(dir)
        for subdir in subdirs:
            subdir = os.path.join(dir, subdir)
            if not os.path.isdir(subdir): continue
            if not self.serializer.exists(subdir): continue
            if self._subdir_contains_spec(subdir, spec): break

        else:
            raise KeyError("Spec not found")

        return subdir

    def _subdir_contains_spec(self, subdir, spec):
        """
        Returns true if `spec` is stored in `subdir`
        It matches using key or dict according to self.match_using_key
        """
        spec_dict_fname = os.path.join(subdir, 'spec_dict')
        if not os.path.exists(spec_dict_fname): return False

        with open(spec_dict_fname) as f:
            stored_spec_dict = json.load(f)

        try:
            stored_spec = Spec.dict2spec(stored_spec_dict)

            if self.match_using_key:
                res = stored_spec.key == spec.key
                if res and spec.to_dict() != stored_spec_dict:
                    warnings.warn(
                        'Matching two specs with different dicts: \n {} \n\n with \n\n {}'.format(
                            pformat(spec.to_dict()),
                            pformat(stored_spec_dict)
                        )
                    )
            else:
                res = spec.to_dict() == stored_spec_dict

            return res
        except Exception:
            traceback.print_exc()
            warnings.warn('Unable to load spec from dir: {}'.format(subdir))
            return False

    def _get(self, spec):
        subdir = self._get_subdir(spec)
        try:
            return self.serializer.load(subdir)
        except Exception:
            traceback.print_exc()
            raise KeyError('Failed to load spec')

    def get_dir_for_saving(self, spec):
        dir = self._get_dir(spec)
        try:
            # this accounts for both checking if it not exists, and the fact that there might
            # be another process doing the same thing
            os.makedirs(dir)
        except OSError:
            pass

        for subdir in os.listdir(dir):
            subdir = os.path.join(dir, subdir)
            if self._subdir_contains_spec(subdir, spec): return subdir

        else:
            while True:
                subdirs = [int(e) for e in os.listdir(dir) if e.isdigit()]

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
        with open(os.path.join(subdir, 'spec_dict'), 'w') as f:
            json.dump(spec.to_dict(), f, indent=2)

        self.serializer.save(series, subdir)

    def __contains__(self, spec):
        try:
            self._get_subdir(spec)
            return True
        except KeyError:
            return False

