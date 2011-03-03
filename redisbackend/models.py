from base import redis_connection, dump, load, key_maker
from api import identity, lower_str, RedisKeyAttribute, date_str
from django.db import models
from filter import field_converter

def partition(iterable, func):
    result = {}
    for i in iterable:
        j = func(i)
        if type(j) == type([]):
            for k in j:
                result.setdefault(k, []).append(i)
        else:
            result.setdefault(func(i), []).append(i)
    return result

def or_fn(res, asc_value, l, o):
    try:
        if asc_value:
            return res.index(o)
        else:
            return l - res.index(o)
    except ValueError:
        #print o
        pass

class RedisQuerySet(object):
    def __init__(self, model, keymaker, ordering_key):
        self._model = model
        self._redis_ordering_key = ordering_key
        self._keymaker = keymaker
        self.asc = True
        self.clear()
        self.query = self._keymaker.buildkey({})
        self.all()

    def cacheall(self, objects=None):
        self.clear()
        if not objects: objects = self._model.objects
        for o in objects.all(): o.cache()

    def deleteall(self):
        r = redis_connection()
        r.delete(self.redis_ordering_key)
        for key in self.all():
            r.delete(key)
        self.clear()

    def clear(self):
        self._doneresults = False
        self.current = 0
        self.high = 0
        self._results = None

    def all(self):
        self._doneresults = False
        self.results()
        return self

    def filter(self, **kwargs):
        if self._doneresults:
            keys = self._results
        else:
            keys = []
        self._doneresults = False
        dict = {}
        for key, value in kwargs.items():
            k, fn = field_converter(key)
            dict[k] = fn(value) 
        self.query = self._keymaker.buildkey(dict)
        self.results()
        self.intersect(keys)
        return self

    def filter_or(self, **kwargs):
        if self._doneresults:
            keys = self._results
        else:
            keys = []
        self._doneresults = False
        dict = {}
        for key, value in kwargs.items():
            k, fn = field_converter(key)
            dict[k] = fn(value) 
        self.query = self._keymaker.buildkey(dict)
        self.results()
        self.union(keys)
        return self

    def union(self, keys):
        self.results()
        res = self._results
        newres = list(set(keys).union(set(res)))
        self.setresults(newres)
        return self

    def exclude(self, **kwargs):
        if self._doneresults:
            keys = self._results
        else:
            keys = []
        self._doneresults = False
        dict = {}
        for key, value in kwargs.items():
            k, fn = field_converter(key)
            dict[k] = fn(value) 
        self.query = self._keymaker.buildkey(dict)
        self.results()
        self.diffs(keys)
        return self

    def diffs(self, keys):
        self.results()
        res = self._results
        newres = list(set(keys).difference(set(res)))
        self.setresults(newres)
        return self

    def group_by(self, key):
        self._doneresults = False
        self.results()
        k, fn = field_converter(key)
        results = dict(partition(self, lambda e: fn(getattr(e, k))).items())
        self._doneresults = False
        return results

    def reverse(self):
        self.asc = not self.asc
        return self

    def after(self, val):
        keys = redis_connection().zrangebyscore(self._redis_ordering_key, val, '+inf')
        self.intersect(keys)
        return self

    def before(self, val):
        keys = redis_connection().zrangebyscore(self._redis_ordering_key, '-inf', val)
        self.intersect(keys)
        return self

    def intersect(self, keys):
        self.results()
        res = self._results
        newres = list(set(res).intersection(set(keys)))
        self.setresults(newres)
        return self

    def results(self):
        if not self._doneresults:
            self.clear()
            res = redis_connection().keys(self.query)
            if self._redis_ordering_key:
                ordered_results = redis_connection().zrangebyscore(self._redis_ordering_key, '-inf', '+inf')
                res.sort(key = lambda o: or_fn(ordered_results, self.asc, len(res), o))
            self._doneresults = True
            self.setresults(res)

    def setresults(self, newres):
        self._results = newres
        self.high = len(newres)

    def __getitem__(self,index):
        return self._makeobj(self._results[index])

    def __iter__(self):
        return self

    def __len__(self):
        return self.count()

    def count(self):
        if not self._doneresults: self.results()
        return len(self._results)

    def next(self):
        self.high = len(self._results)
        if self.current >= self.high:
            raise StopIteration
        else:
            self.current += 1
            return self._makeobj(self._results[self.current - 1])

    def _makeobj(self, key):
        obj = self._model()
        r = redis_connection()
        res = r.get(key)
        if res: obj.__dict__.update( load(res))
        return obj

class RedisManager(object):
    """
    Adapted from http://www.djangosnippets.org/snippets/562/
    """

    def __init__(self, base, fields, ordering, model):
        self._base = base
        self._fields = fields
        self._ordering = ordering
        self._model = model
        self._keymaker = key_maker(base, fields, model)
        self._redis_ordering_key = None
        self._redis_ordering_fn = None
        if ordering != '': 
            self._redis_ordering_key = self._base + ':' + self._ordering.name
            self._redis_ordering_fn = lambda obj: self._ordering.extracter(obj)

    def get_query_set(self):
        return RedisQuerySet(self._model, keymaker=self._keymaker, ordering_key=self._redis_ordering_key)

    def __getattr__(self, attr, *args):
        return getattr(self.get_query_set(), attr, *args)

    def __repr__(self):
        return '<RedisManager:%s>' % self._base

class RedisModelBase(models.base.ModelBase):

    def __new__(cls, name, bases, dict):
        my_dict = dict.copy()
        new_class = super(RedisModelBase, cls).__new__(cls, name, bases, dict)
        if my_dict.has_key('Meta') and not getattr(my_dict['Meta'], 'abstract', False):
            base = my_dict.get('_redis_base', name)
            fields = my_dict.get('_redis_fields', [])
            ordering = my_dict.get('_redis_ordering', '')
            new_class.redis = RedisManager(base, fields, ordering, new_class)
            new_class.makekey = lambda a: new_class.redis._keymaker.buildkey(a, any=False)
            new_class.redis_ordering_key = new_class.redis._redis_ordering_key
            new_class.redis_ordering_fn = new_class.redis._redis_ordering_fn
        return new_class

class RedisModel(models.Model):
    __metaclass__ = RedisModelBase
    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        super(RedisModel, self).__init__(*args, **kwargs)

    def cached_attributes(self):
        dict = {}
        fields = self._meta.get_all_field_names()
        for field_name in fields:
            try:
                field = self._meta.get_field(field_name)
                if self.__dict__.has_key(field.attname):
                    dict[field.attname] = self.__dict__[field.attname]
            except models.FieldDoesNotExist, e:
                pass
        return dict

    def redis_ordering_value(self):
        if self.redis_ordering_fn:
            return self.redis_ordering_fn()
        else:
            None

    def expires(self):
        return None

    def cache(self):
        key = self.makekey()
        attrs = self.cached_attributes()
        r = redis_connection()
        r.set(key, dump(attrs))
        expires = self.expires()
        if expires: r.expireat(key, expires)
        if self.redis_ordering_key: r.zadd(self.redis_ordering_key, key, self.redis_ordering_value())


