from base import redis_connection, dump, load, key_maker
from django.db import models

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

    def get_query_set(self):
        return RedisQuerySet(self._model, keymaker=self._keymaker)

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
        for key, value in self.__dict__.items():
            if key in fields:
                dict[key] = value
        return dict

    def expires(self):
        return None

    def cache(self):
        key = self.makekey()
        attrs = self.cached_attributes()
        r = redis_connection()
        r.set(key, dump(attrs))
        expires = self.expires()
        if expires: r.expireat(key, expires)

class RedisQuerySet(object):
    def __init__(self, model, keymaker):
        self._model = model
        self._keymaker = keymaker
        self._results = None
        self._doneresults = False
        self.current = 0
        self.high = 0

    def all(self):
        self.query = self._keymaker.buildkey({})
        self._doneresults = False
        return self.results()

    def filter(self, **kwargs):
        self._doneresults = False
        self.query = self._keymaker.buildkey(kwargs)
        return self.results()

    def results(self):
        if not self._doneresults:
            print self.query
            res = redis_connection().keys(self.query)
            self._doneresults = True
            self.setresults(res)
        return [EventRedis(x) for x in self._results]

    def setresults(self, newres):
        self._results = newres
        self.high = len(newres)
"""

    def groupby(self, key):
        results = dict(partition(self.results(), lambda e:getattr(e, key)).items())
        for keyvalue, show_keys in results.items():
            show_keys.sort(key=lambda e: e.datetime)
        return results

    def distinct(self, key):
        return self.groupby(key).keys()

    def after(self, date):
        epoch = int(time.mktime(date.timetuple()))
        keys = redis_connection().zrangebyscore('cms:timedevents', epoch, '+inf')
        self.intersect(keys)
        return self

    def before(self, date):
        epoch = int(time.mktime(date.timetuple()))
        keys = redis_connection().zrangebyscore('cms:timedevents', '-inf', epoch)
        self.intersect(keys)
        return self

    def intersect(self, keys):
        res = self.results()
        newres = list(set(res).intersection(set(keys)))
        self.setresults(newres)
        return self

    def filter(self, **kwargs):
        query = event_key_maker.buildkey(kwargs)
        res = self.results()
        keys = redis_connection().keys(query)
        newres = list(set(res).intersection(set(keys)))
        self.setresults(newres)
        return self

    def __iter__(self):
        return self

    def next(self):
        self.high = len(self.results())
        if self.current >= self.high:
            raise StopIteration
        else:
            self.current += 1
            return self.results()[self.current - 1]

    def count(self):
        return len(self.results())

    def __getitem__(self,index):
        return self.results()[index]


        #int(contents[c.key][-1].epoch()))

class EventRedis(object):
    def __init__(self, key):
        self.key = key

    def __getattr__(self, attr):
        if attr == 'datetime':
            return int('%s%s%s%s%s' % (self.year, self.month, self.day, self.hour, self.minute))
        else:
            return event_key_maker.extractelement(self.key, attr)

    def attributes(self):
        r = redis_connection()
        event = r.get(self.key)
        if event: return load(event)

    def __repr__(self):
        return 'EventRedis:' + self.key

    def epoch(self):
        return time.mktime((int(self.year), int(self.month), int(self.day), int(self.hour), int(self.minute), 0, 0, 0, -1))



"""
