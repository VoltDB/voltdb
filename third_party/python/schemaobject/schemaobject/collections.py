class OrderedDict(dict):
    """
    A Dictionary whose items are returned in the order they were first added
    """

    def __init__(self):
        self._sequence = []
        self._current = 0
        super(OrderedDict, self).__init__()

    def __setitem__(self, item, value):
        self._sequence.append(item)
        super(OrderedDict, self).__setitem__(item, value)

    def iterkeys(self):
        for key in self._sequence:
            yield key

    def keys(self):
        return self._sequence

    def iteritems(self):
        for key in self._sequence:
            yield (key, super(OrderedDict, self).__getitem__(key))

    def items(self):
        return [(k, super(OrderedDict, self).__getitem__(k)) for k in self._sequence]

    def __iter__(self):
        return self

    def next(self):
        i = self._current
        self._current += 1
        if self._current > len(self._sequence):
            self._current = 0
            raise StopIteration

        return self._sequence[i]

    def index(self, item):
        try:
            return self._sequence.index(item)
        except ValueError:
            raise

    def insert(self, pos, key_value_tuple):
        key, value = key_value_tuple
        super(OrderedDict, self).__setitem__(key, value)
        self._sequence.insert(pos, key)

    def __delitem__(self, key):
        self._sequence.remove(key)
        super(OrderedDict, self).__delitem__(key)
