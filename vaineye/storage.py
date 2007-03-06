import os
from cPickle import load, dump

class PickleStorage(object):

    def __init__(self, dir):
        self.dir = os.path.normpath(dir)

    def filename_for_name(self, name):
        fn = os.path.join(self.dir, name) + '.pickle'
        fn = os.path.normpath(fn)
        if not fn.startswith(self.dir):
            raise ValueError(
                "Bad name: %r (leads to pickle file %s)"
                % (name, fn))
        return fn

    def read_pickle(self, name, default=None):
        fn = self.filename_for_name(name)
        if not os.path.exists(fn):
            return default
        f = open(fn, 'rb')
        try:
            return load(f)
        finally:
            f.close()

    def write_pickle(self, name, value):
        fn = self.filename_for_name(name)
        f = open(fn, 'wb')
        try:
            dump(value, f)
        finally:
            f.close()
