import time
from paste.response import header_value

class Tracker(object):
    """
    Abstract base class for trackers
    """

    name = None

    def __init__(self, watcher, name, storage):
        self.watcher = watcher
        self.name = name
        self.storage = storage
        self._data_initialized = False
        self._data = None
        self._pending = []

    def empty_data(self):
        return {}

    def track_request(self, url, status, headers):
        raise NotImplementedError

    def serialize(self):
        if not self._data_initialized:
            self._initialize_data()
        self._merge_data()
        self._write_data()

    def _initialize_data(self):
        self._data = self.storage.read_pickle(self.name, self.empty_data())

    def _merge_data(self):
        for item in self._pending:
            self._merge_item(self._data, item)
        self._pending = []

    def _merge_item(self, data, item):
        raise NotImplementedError

    def _write_data(self):
        self.storage.write_pickle(self.name, self._data)

    @property
    def data(self):
        self.serialize()
        return self._data

class NotFound(Tracker):

    def track_request(self, url, status, headers):
        if status.startswith('404'):
            self._pending.append(url)

    def _merge_item(self, data, url):
        data[url] = data.get(url, 0)+1
        
class Redirect(Tracker):

    def track_request(self, url, status, headers):
        if status.startswith('301'):
            self._pending.append(
                (url, header_value(headers, 'Location')))

    def _merge_item(self, data, (url, location)):
        data[(url, location)] = data.get((url, location), 0)+1

class Hits(Tracker):

    def track_request(self, url, status, headers):
        self._pending.append(url)

    def _merge_item(self, data, url):
        data[url] = data.get(url, 0)+1

def week_number(t):
    return int(time.strftime('%U', time.localtime(t)))    

class HitsWeekly(Tracker):

    def track_request(self, url, status, headers):
        self._pending.append((url, time.time()))

    def _merge_item(self, data, (url, t)):
        week = week_number(t)
        if week not in data:
            data[week] = {}
        data[week][url] = data[week].get(url, 0)+1
