import redis
import cjson
import re
from datetime import datetime
from django.conf import settings
from api import identity, lower_str, RedisKeyAttribute, date_str
from django.db import models

def dateEncoder(d):
    if isinstance(d, datetime):
        return 'new Date(Date.UTC(%d,%d,%d,%d,%d,%d))'%(d.year, d.month, d.day, d.hour, d.minute, d.second)
    else:
        return d

re_date=re.compile('^new\sDate\(Date\.UTC\(.*?\)\)')
def dateDecoder(json,idx):
    json=json[idx:]
    m=re_date.match(json)
    if not m: 
        raise 'cannot parse JSON string as Date object: %s'%json[idx:]
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

class KeyBuilder(object):
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
                if not isinstance(obj, dict):
                    key += key_bit.formatter(key_bit.extracter(obj))
                elif obj.has_key(key_bit.name):
                    key += key_bit.formatter(obj[key_bit.name])
                elif any:
                    key += self.any 
                key += self.demarker
            else:
                key += key_bit.key + self.demarker
                if not isinstance(obj, dict):
                    if any: 
                        key += self.any
                    values = key_bit.extracter(obj)
                    for value in values:
                        key += self.list_demarker + key_bit.formatter(value) + self.list_demarker
                    if any: 
                        key += self.any
                elif obj.has_key(key_bit.name):
                    if any: 
                        key += self.any
                    values = obj[key_bit.name]
                    for value in values:
                        key += self.list_demarker + key_bit.formatter(value) + self.list_demarker
                    if any: 
                        key += self.any
                elif any:
                    key += self.any
                key += self.demarker
        key += self.end
        return key

    def extractelement(self, key, bit):
        found = False
        for key_bit in self.keys:
            if key_bit.name == bit:
                found = True
                break
            if found: break

        if not found: return None

        i = key.split(self.demarker).index(key_bit.key) + 1
        value = key.split(self.demarker)[i]
        return value

def key_maker(base, redis_fields, model):
    keys = []
    for name in redis_fields:
        if type(name) == RedisKeyAttribute:
            keys.append(name)
        else:
            try:
                field = model._meta.get_field(name)
                if type(field) == models.fields.DateTimeField:
                    keys.append(RedisKeyAttribute(name=name, key=name.upper(), formatter=date_str))
                else:
                    keys.append(RedisKeyAttribute(name=name, key=name.upper(), formatter=lower_str))
            except models.FieldDoesNotExist:
                if hasattr(model, name):
                    keys.append(RedisKeyAttribute(name=name, key=name.upper(), formatter=lower_str, list_based=True)) 
    return KeyBuilder(base=base, keys=keys) 
