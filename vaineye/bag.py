class Bag(object):
    """
    Represents a Bag data structure, where a container can hold a number
    of items of the same type (like a set with counts).
    """
    def __init__(self, items=None):
        self._data = {}
        if items:
            for item in items:
                self.add(item)
    def add(self, item):
        if item in self._data:
            self._data[item] += 1
        else:
            self._data[item] = 1
    def __len__(self):
        return sum(self._data.values())
    def __iter__(self):
        for item, count in self._data.items():
            for i in xrange(count):
                yield item
    def __contains__(self, item):
        return item in self._data
    def count(self, item):
        return self._data.get(item, 0)
    def counts(self):
        return [(count, item) for item, count in self._data.iteritems()]
    def counts_most_frequent(self):
        counts = self.counts()
        counts.sort(reverse=True)
        return counts
    def __repr__(self):
        if len(self) < 20:
            return 'Bag(%r)' % list(self)
        else:
            return 'Bag([%s, ...])' % ', '.join([repr(x) for x in list(self)[:20]])
    def count_dict(self):
        return self._data.copy()
