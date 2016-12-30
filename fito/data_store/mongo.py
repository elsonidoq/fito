import mmh3
from random import random

import pymongo
from fito.data_store.base import BaseDataStore
from fito.operations import Operation
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
    def __init__(self, coll, client=None, add_increlemtal_id=True, get_cache_size=0, execute_cache_size=0,
                 use_gridfs=False):
        super(MongoHashMap, self).__init__(get_cache_size=get_cache_size, execute_cache_size=execute_cache_size)
        client = client or global_client
        if isinstance(coll, basestring):
            coll = get_collection(client, coll)
        else:
            assert isinstance(coll, Collection)
        self.coll = coll
        self.add_incremental_id = add_increlemtal_id
        if add_increlemtal_id:
            self._init_incremental_id()

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
        self.coll.remove()
        self.coll.conf.remove()
        self.coll.fs.files.remove()
        self.coll.fs.chunks.remove()
        if self.add_incremental_id: self._init_incremental_id()

    def ensure_indices(self):
        self.coll.ensure_index('op_hash')
        self.coll.ensure_index('rnd')

    @classmethod
    def _get_op_hash(cls, operation):
        op_hash = mmh3.hash(operation.key)
        return op_hash

    def _build_doc(self, operation, value):
        doc = {'operation': operation.to_dict(), 'values': value}
        op_hash = self._get_op_hash(operation)
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
            self.coll.insert(docs, w=0)
        else:
            max_id = self.coll.conf.find_and_modify(
                query={'key': 'id_seq'},
                update={'$inc': {'value': len(docs)}},
                fields={'value': 1, '_id': 0},
                new=True
            ).get('value')

            for i, doc in enumerate(docs):
                doc['_id'] = max_id - len(docs) + i

            try:
                if self.use_gridfs: self._persist_values(docs)
                self.coll.insert(docs)
            except DuplicateKeyError as e:
                self._insert(docs)

    def _parse_doc(self, doc):
        values = doc['values']
        if self.use_gridfs:
            values = self.gridfs.get(values).read()
        operation = Operation.dict2operation(doc['operation'])
        return operation, values

    def _dict2operation(self, d):
        # XXX tal vez seria buena idea que dict2operation pueda ignorar argumentos que estan de mas
        d = d.copy()
        d.pop('involved_series', None)
        return Operation.dict2operation(d)

    def iterkeys(self):
        for doc in self.coll.find(timeout=False, fields=['operation']):
            operation = Operation.dict2operation(doc['operation'])
            yield operation

    def iteritems(self):
        for doc in self.coll.find(timeout=False):
            operation, serie = self._parse_doc(doc)
            yield self._dict2operation(doc['operation']), serie

    def _get_doc(self, series_name_or_operation, fields=None):
        operation = self._get_operation(series_name_or_operation)
        if fields is not None and 'operation' not in fields:
            fields.append('operation')

        op_hash = self._get_op_hash(operation)
        for doc in self.coll.find({'op_hash': op_hash}, fields=fields):
            # I do not compare the dictionaries, because when there's a nan involved, the comparision is always false
            if self._dict2operation(doc['operation']) == operation: break
        else:
            raise ValueError("Operation not found")

        return doc

    def _get(self, series_name_or_operation):
        doc = self._get_doc(series_name_or_operation)
        return self._parse_doc(doc)[1]

    def save(self, series_name_or_operation, values):
        operation = self._get_operation(series_name_or_operation)
        doc = self._build_doc(operation, values)
        self._insert([doc])

    def delete(self, series_name_or_operation):
        if self.use_gridfs:
            fields = ['values']
        else:
            fields = []

        doc = self._get_doc(series_name_or_operation, fields=fields)

        if self.use_gridfs:
            self.gridfs.delete(doc['values'])

        self.coll.remove({'_id': doc['_id']})

    def __delitem__(self, series_name_or_operation):
        self.delete(series_name_or_operation)

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

            if not k.startswith('$'): k = 'operation.%s' % k
            res[k] = v
        return res

    def search(self, query):
        query_dict = self._build_mongo_query(query.dict)

        for doc in self.coll.find(query_dict, timeout=False):
            operation, series = self._parse_doc(doc)
            yield operation, series

    def create_index_for_query(self, query):
        index = [('operation.%s' % k, pymongo.ASCENDING) for k in query.dict.keys()]
        self.coll.ensure_index(index)

    def get_batch(self):
        return MongoHashMap.Batch(self)

    def _save_batch(self, batch):
        if len(batch._docs) > 0:
            self._insert(batch._docs)
            del batch._docs
            batch._docs = []

    class Batch(object):
        def __init__(self, mds):
            self._docs = []
            self.mds = mds

        def append(self, series_name_or_operation, value):
            self._docs.append(self.mds._build_doc(series_name_or_operation, value))

        def commit(self):
            self.mds._save_batch(self)

        def __len__(self):
            return len(self._docs)
