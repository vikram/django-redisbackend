identity = lambda a: str(a)
lower_str = lambda a: unicode(a).lower()
date_str = lambda a: a.strftime('%Y%m%d%H%M')

class RedisKeyAttribute(object):
    def __init__(self, name, key, extracter=None, list_based=False, formatter=identity):
        self.name = name
        self.key = key
        if extracter:
            self.extracter = extracter
        else:
            self.extracter = self._extracter
        self.formatter = formatter
        self.list_based = list_based

    def _extracter(self, obj):
        return getattr(obj, self.name)

    def __repr__(self):
        return '%s %s ' % (self.name, self.list_based)
