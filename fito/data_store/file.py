import mmh3
import os
import pickle
import shutil
import traceback
from contextlib import contextmanager
from time import time, sleep

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
    _check_conf = True  # Just to avoid an infinite loop on __init__, see disabled_conf_checking

    def __init__(self, *args, **kwargs):
        super(FileDataStore, self).__init__(*args, **kwargs)

        if not os.path.exists(self.path): os.makedirs(self.path)

        conf_file = os.path.join(self.path, 'conf.yaml')
        if self._check_conf and os.path.exists(conf_file):

            with self.disabled_conf_checking():
                conf = Spec.from_yaml().load(conf_file)

            if conf != self:
                raise RuntimeError(
                    """
This store was initialized with this config:\n{}

But was now instanced with this:\n{}
                    """.format(
                        conf.yaml.dumps(),
                        self.yaml.dumps()
                    )
                )

        else:
            with open(conf_file, 'w') as f:
                self.yaml.dump(f)

    @contextmanager
    def disabled_conf_checking(self):
        try:
            type(self)._check_conf = False
            yield
        finally:
            type(self)._check_conf = True

    def clean(self, cls=None):
        for op in self.iterkeys():
            if cls is None or (cls is not None and isinstance(op, cls)):
                self.remove(op)

    def remove(self, op):
        subdir = self._get_subdir(op)
        shutil.rmtree(subdir)

    def iterkeys(self):
        for subdir, _, _ in os.walk(self.path):
            key_fname = os.path.join(subdir, 'key')
            if not os.path.exists(key_fname): continue

            with open(key_fname) as f:
                key = f.read()

            try:
                op = Spec.key2spec(key)
            except Exception, e:  # there might be a key that is not a valid json
                if len(e.args) > 0 and isinstance(e.args[0], basestring) and e.args[0].startswith(
                        'Unknown spec type'): raise e
                continue

            yield op

    def iteritems(self):
        for op in self.iterkeys():
            try:
                yield op, self.get(op)
            except:
                # TODO: check whether the file exists or not
                continue

    def _get_dir(self, spec):
        h = str(mmh3.hash(spec.key))
        path = os.path.join(self.path, type(spec).__name__) if self.use_class_name else self.path

        if self.split_keys:
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
            key_fname = os.path.join(subdir, 'key')
            if not os.path.exists(key_fname): continue

            with open(key_fname) as f:
                key = f.read()

            if len(key) == 0 and time() - os.path.getctime(key_fname) < 0.1:
                sleep(0.1)
                with open(key_fname) as f:
                    key = f.read()
            if key == spec.key and self.serializer.exists(subdir): break
        else:
            raise KeyError("Spec not found")

        return subdir

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
            key_fname = os.path.join(subdir, 'key')
            if not os.path.exists(key_fname): continue
            with open(key_fname) as f:
                key = f.read()
            if key == spec.key:
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
        subdir = self.get_dir_for_saving(spec)
        with open(os.path.join(subdir, 'key'), 'w') as f:
            f.write(spec.key)

        self.serializer.save(series, subdir)

    def __contains__(self, spec):
        try:
            subdir = self._get_subdir(spec)
            return self.serializer.exists(subdir)
        except KeyError:
            return False

