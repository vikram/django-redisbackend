import re

# value conversion for (i)regex works via special code
VALUE_CONVERSION = {
    'iexact': lambda value: value.lower(),
    'istartswith': lambda value: value.lower(),
    'endswith': lambda value: value[::-1],
    'iendswith': lambda value: value[::-1].lower(),
    'year': lambda value: value.year,
    'month': lambda value: value.month,
    'day': lambda value: value.day,
    'date': lambda value: value.date(),
    'week_day': lambda value: value.isoweekday(),
    'contains': lambda value: contains_indexer(value),
    'icontains': lambda value: [val.lower() for val in contains_indexer(value)],
    # TODO: clean $default case
    '$default': lambda value: value,
}

def field_converter(key):
    bits = key.split('__', 1)
    field_name = bits[0]
    if len(bits)>1 and VALUE_CONVERSION.has_key(bits[1]):
        return (bits[0], VALUE_CONVERSION[bits[1]])
    else:
        return (bits[0], VALUE_CONVERSION['$default'])
    
