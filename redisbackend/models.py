from base import redis_connection, dump, load, KeyMaker
from api import identity, lower_str, RedisKeyAttribute
from django.db import models, backend

class RedisQuerySet(models.query.QuerySet):
    def __init__(self, model=None, fields=None):
        super(RedisQuerySet, self).__init__(model)
        self._redis_fields = fields

    def redis(self, query):
        meta = self.model._meta

        # Get the table name and column names from the model
        # in `table_name`.`column_name` style
        columns = [meta.get_field(name, many_to_many=False).column for name in self._redis_fields]
        full_names = ["%s.%s" %
                    (backend.quote_name(meta.db_table),
                     backend.quote_name(column))
                     for column in columns]

        # Create the MATCH...AGAINST expressions 
        fulltext_columns = ", ".join(full_names)
        match_expr = ("MATCH(%s) AGAINST (%%s)" %
                            fulltext_columns)

        # Add the extra SELECT and WHERE options
        return self.extra(select={'relevance': match_expr},
                             where=[match_expr],
                             params=[query, query])

class RedisManager(models.Manager):
    def __init__(self, fields):
        self._redis_fields = fields
        super(RedisManager, self).__init__()

    def get_query_set(self):
        return RedisQuerySet(self.model, self._redis_fields)

    def redis(self, query):
        return self.get_query_set().redis(query)

class RedisModel(models.Model):
    class Meta:
        abstract = True

    _redis_fields = None
    #objects = RedisManager(_redis_fields)

    def __init__(self, *args, **kwargs):
        self.key_maker = self.keymaker()    
        super(RedisModel, self).__init__(*args, **kwargs)

    def keymaker(self):
        keys = []
        for name in self._redis_fields:
            if type(name) == RedisKeyAttribute:
                keys.append(name)
            else:
                try:
                    field = self._meta.get_field(name)
                    if type(field) == models.fields.DateTimeField:
                        keys.append(RedisKeyAttribute(name=name, key=name.upper(), formatter=lower_str, extracter=lambda obj: getattr(obj, name).strftime('%Y%m%d%H%M')))
                    else:
                        keys.append(RedisKeyAttribute(name=name, key=name.upper(), formatter=lower_str, extracter=lambda obj: getattr(obj, name)))
                except models.FieldDoesNotExist:
                    if hasattr(self, name):
                       keys.append(RedisKeyAttribute(name=name, key=name.upper(), formatter=lower_str, extracter=lambda obj: getattr(obj, name), list_based=True)) 
        key_maker = KeyMaker(base=self._meta.db_table, keys=keys) 
        return key_maker

    def cached_attributes(self):
        return {}

    def expires(self):
        return None

    def makekey(self):
        return self.key_maker.buildkey(self,any=False)

    def cache(self):
        key = self.makekey()
        attrs = self.cached_attributes()
        r = redis_connection()
        r.set(key, dump(attrs))
        expires = self.expires()
        if expires: r.expireat(key, expires)

        #int(contents[c.key][-1].epoch()))
