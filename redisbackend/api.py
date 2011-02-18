identity = lambda a: str(a)
lower_str = lambda a: str(a).lower()

class RedisKeyAttribute(object):
    def __init__(self, name, key, extracter, list_based=False, formatter=identity):
        self.name = name
        self.key = key
        self.extracter = extracter
        self.formatter = formatter
        self.list_based = list_based

    def _extracter(self, obj):
        return getattr(obj, self.name)

    def __repr__(self):
        return '%s %s %s' % (self.name, self.extracter, self.list_based)
