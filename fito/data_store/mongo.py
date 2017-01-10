import mmh3
from random import random

import pymongo
from fito.data_store.base import BaseDataStore
from fito import Spec
from gridfs import GridFS
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError
from pymongo.mongo_client import MongoClient


def get_collection(client, name):
    dot = name.index('.')
    db, coll = name[:dot], name[dot + 1:]
    return client[db][coll]


global_client = MongoClient()


class MongoHashMap(BaseDataStore):
    """
    Mongo based key value store
    """
    def __init__(self, coll, client=None, add_increlemtal_id=True, get_cache_size=0, operation_runner=None,
                 use_gridfs=False):
        super(MongoHashMap, self).__init__(get_cache_size=get_cache_size, operation_runner=operation_runner)

        client = client or global_client

        if isinstance(coll, basestring):
            coll = get_collection(client, coll)
        else:
            assert isinstance(coll, Collection)
        self.coll = coll

        self.add_incremental_id = add_increlemtal_id
        if add_increlemtal_id: self._init_incremental_id()

        self.use_gridfs = use_gridfs
        if self.use_gridfs:
            self.gridfs = GridFS(coll.database, coll.name + '.fs')
        else:
            self.gridfs = None

    def get_collections(self):
        res = [self.coll, self.coll.conf]
        if self.use_gridfs:
            res.append(self.coll.fs.files)
            res.append(self.coll.fs.chunks)
        return res

    def __len__(self):
        return self.coll.count()

    def _init_incremental_id(self):
        doc = self.coll.conf.find_one({'key': 'id_seq'})
        if doc is None:
            self.coll.conf.insert({'key': 'id_seq', 'value': 0})

    def clean(self):
        self.coll.drop()
        self.coll.conf.drop()
        self.coll.fs.files.drop()
        self.coll.fs.chunks.drop()
        if self.add_incremental_id: self._init_incremental_id()

    def create_indices(self):
        self.coll.create_index('op_hash')
        self.coll.create_index('rnd')

    @classmethod
    def _get_op_hash(cls, spec):
        op_hash = mmh3.hash(spec.key)
        return op_hash

    def _build_doc(self, spec, value):
        doc = {'spec': spec.to_dict(), 'values': value}
        op_hash = self._get_op_hash(spec)
        doc['op_hash'] = op_hash
        doc['rnd'] = random()
        return doc

    def _persist_values(self, docs):
        assert self.use_gridfs

        for doc in docs:
            values = doc.pop('values')
            doc['values'] = self.gridfs.put(values)

    def _insert(self, docs):
        if not self.add_incremental_id:
            if self.use_gridfs: self._persist_values(docs)
            self.coll.insert_many(docs, bypass_document_validation=True)
        else:
            max_id = self.coll.conf.find_and_modify(
                query={'key': 'id_seq'},
                update={'$inc': {'value': len(docs)}},
                projection={'value': 1, '_id': 0},
                new=True
            ).get('value')

            for i, doc in enumerate(docs):
                doc['_id'] = max_id - len(docs) + i

            try:
                if self.use_gridfs: self._persist_values(docs)
                self.coll.insert_many(docs)
            except DuplicateKeyError as e:
                self._insert(docs)

    def _parse_doc(self, doc):
        values = doc['values']
        if self.use_gridfs:
            values = self.gridfs.get(values).read()
        spec = Spec.dict2spec(doc['spec'])
        return spec, values

    def _dict2spec(self, d):
        d = d.copy()
        return Spec.dict2spec(d)

    def iterkeys(self):
        for doc in self.coll.find(no_cursor_timeout=False, projection=['spec']):
            spec = Spec.dict2spec(doc['spec'])
            yield spec

    def iteritems(self):
        for doc in self.coll.find(no_cursor_timeout=False):
            spec, serie = self._parse_doc(doc)
            yield self._dict2spec(doc['spec']), serie

    def _get_doc(self, name_or_spec, projection=None):
        spec = self._get_spec(name_or_spec)
        if projection is not None and 'spec' not in projection:
            projection.append('spec')

        op_hash = self._get_op_hash(spec)
        for doc in self.coll.find({'op_hash': op_hash}, projection=projection):
            # I do not compare the dictionaries, because when there's a nan involved, the comparision is always false
            if self._dict2spec(doc['spec']) == spec: break
        else:
            raise ValueError("Spec not found")

        return doc

    def _get(self, name_or_spec):
        doc = self._get_doc(name_or_spec)
        return self._parse_doc(doc)[1]

    def save(self, name_or_spec, values):
        spec = self._get_spec(name_or_spec)
        doc = self._build_doc(spec, values)
        self._insert([doc])

    def delete(self, name_or_spec):
        if self.use_gridfs:
            projection = ['values']
        else:
            projection = []

        doc = self._get_doc(name_or_spec, projection=projection)

        if self.use_gridfs:
            self.gridfs.delete(doc['values'])

        self.coll.delete_one({'_id': doc['_id']})

    def __delitem__(self, name_or_spec):
        self.delete(name_or_spec)

    def choice(self, n=1, rnd=None):
        while True:
            size = float(n) / self.coll.count()
            if rnd is None:
                rnd_number = random()
            else:
                rnd_number = rnd.random()
            lbound = rnd_number * (1 - size)
            ubound = lbound + size

            cur = self.coll.find({'rnd': {'$gte': lbound, '$lt': ubound}})
            if cur.count() > 0:
                break

        res = map(self._parse_doc, cur)[:n]
        if len(res) == 1:
            res = res[0]
        return res

    def _build_mongo_query(self, q):
        res = {}
        for k, v in q.iteritems():
            if isinstance(v, list):
                new_v = []
                for e in v:
                    new_v.append(e)
                v = new_v

            if not k.startswith('$'): k = 'spec.%s' % k
            res[k] = v
        return res

    def search(self, query):
        query_dict = self._build_mongo_query(query.dict)

        for doc in self.coll.find(query_dict, no_cursor_timeout=False):
            spec, series = self._parse_doc(doc)
            yield spec, series

    def create_index_for_query(self, query):
        index = [('spec.%s' % k, pymongo.ASCENDING) for k in query.dict.keys()]
        self.coll.create_index(index)
