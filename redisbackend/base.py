import redis
import cjson
import re
from django.conf import settings


def dateEncoder(d):
    if isinstance(d, datetime):
        return 'new Date(Date.UTC(%d,%d,%d,%d,%d,%d))'%(d.year, d.month, d.day, d.hour, d.minute, d.second)
    else:
        return d

re_date=re.compile('^new\sDate\(Date\.UTC\(.*?\)\)')
def dateDecoder(json,idx):
    json=json[idx:]
    m=re_date.match(json)
    if not m: raise 'cannot parse JSON string as Date object: %s'%json[idx:]
    args=cjson.decode('[%s]'%json[18:m.end()-2])
    dt=datetime(*args)
    return (dt,m.end()) # must return (object, character_count) tuple

def dump(data):
    return cjson.encode(data, extension=dateEncoder)

def load(data):
    return cjson.decode(data, extension=dateDecoder)

def redis_connection():
    try:
        host = getattr(settings, 'REDIS_HOST', '127.0.0.1')
        port = getattr(settings, 'REDIS_PORT', 6379)
        r = redis.Redis(host=host, port=port)
        return r
    except Exception,e:
        return None

class KeyMaker(object):
    def __init__(self, base='CMS', keys=[]):
        self.keys = keys
        self.demarker = ':'
        self.any = '*'
        self.list_demarker = '@'
        self.end = '!'
        self.base = base
        
    def buildkey(self, obj, any=True):
        key = self.base + self.demarker
        for key_bit in self.keys:
            if not key_bit.list_based:
                key += key_bit.key + self.demarker
                if type(obj) != type({}):
                    print key, key_bit, key_bit.extracter(obj)
                    key += key_bit.formatter(key_bit.extracter(obj))
                    print key
                elif dict.has_key(key_bit.name):
                    key += key_bit.formatter(dict[key_bit.name])
                elif any:
                    key += self.any 
                key += self.demarker
            else:
                key += key_bit.key + self.demarker
                if dict.has_key(key_bit.name):
                    if any: key += self.any
                    if type(obj) != type({}):
                        values = key_bit.extracter(obj)
                    else:
                        values = dict[key_bit.name]
                    for value in values:
                        key += self.list_demarker + key_bit.formatter(value) + self.list_demarker
                    if any: key += self.any
                elif any:
                    key += self.any
                key += self.demarker
        key += self.end
        print 'buildkey %s' % key
        return key

    def extractelement(self, key, bit):
        found = False
        for lst, simple in [(self.simple, True), (self.lists, False)]:
            for attr, attr_key, formatter in lst:
                if bit == attr:
                    found = True
                    break
            if found: break

        if not found: return None

        i = key.split(self.demarker).index(attr_key) + 1
        value = key.split(self.demarker)[i]
        if simple:
            return value
        else:
            return [val for val in value.split(self.list_demarker) if val]

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

class EventResultset(object):
    def __init__(self, **kwargs):
        self.query = event_key_maker.buildkey(kwargs)
        self._results = None
        self._doneresults = False
        self.current = 0
        self.high = 0

    def results(self):
        if not self._doneresults:
            res = redis_connection().keys(self.query)
            self._doneresults = True
            self.setresults(res)
        return [EventRedis(x) for x in self._results]

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

    def setresults(self, newres):
        self._results = newres
        self.high = len(newres)

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
