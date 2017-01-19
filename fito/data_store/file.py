import mmh3
import os
import pickle
import shutil
from time import time, sleep

from fito import PrimitiveField
from fito import Spec
from fito import SpecField
from fito.data_store.base import BaseDataStore


class Serializer(Spec):
    def save(self, obj, subdir): raise NotImplemented()

    def load(self, subdir): raise NotImplemented()

    def exists(self, subdir): raise NotImplemented()


class FileDataStore(BaseDataStore):
    path = PrimitiveField(0)
    split_keys = PrimitiveField(default=True)
    serializer = SpecField(default=None, base_type=Serializer)

    def __init__(self, *args, **kwargs):
        super(FileDataStore, self).__init__(*args, **kwargs)

        if not os.path.exists(self.path): os.makedirs(self.path)

        conf_file = os.path.join(self.path, 'conf.json')
        if os.path.exists(conf_file):
            with open(conf_file) as f:
                conf_serializer = Spec.from_yaml(f.read())

            if self.serializer is None:
                self.serializer = conf_serializer
            else:
                if conf_serializer != self.serializer:
                    raise RuntimeError(
                        'This store was initialized with {} Serializer, but now received {}'.format(
                            conf_serializer,
                            self.serializer)
                    )

        else:
            self.serializer = self.serializer or PickleSerializer()

            with open(conf_file, 'w') as f:
                self.serializer.yaml.dump(f)

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
                if len(e.args) > 0 and isinstance(e.args[0], basestring) and e.args[0].startswith('Unknown spec type'): raise e
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
        if self.split_keys:
            fname = os.path.join(self.path, h[:3], h[3:6], h[6:])
        else:
            fname = os.path.join(self.path, h)
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

            if len(key) == 0 and time() - os.path.getctime(key_fname) < 0.5:
                sleep(0.5)
                print "sleep(0.5)"
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
        except:
            raise KeyError('{} not found'.format(spec))

    def save(self, spec, series):
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
            if key == spec.key: break
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
                    break
                except OSError:
                    pass

        with open(os.path.join(subdir, 'key'), 'w') as f:
            f.write(spec.key)

        self.serializer.save(series, subdir)

    def __contains__(self, spec):
        try:
            subdir = self._get_subdir(spec)
            return self.serializer.exists(subdir)
        except KeyError:
            return False


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
