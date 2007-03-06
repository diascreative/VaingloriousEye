import atexit
import threading
import time
from paste.request import construct_url
from paste.util.import_string import simple_import
from vaineye.storage import PickleStorage

class StatusWatcher(object):

    def __init__(self, app, data_store, trackers,
                 serialize_time=120, serialize_requests=100):
        self.app = app
        self.storage = PickleStorage(data_store)
        if hasattr(trackers, 'items'):
            trackers = trackers.items()
        self.trackers = {}
        for tracker in trackers:
            self.add_tracker(tracker)
        self.serialize_time = serialize_time
        self.serialize_requests = serialize_requests
        self.serialize_lock = threading.Lock()
        self.last_serialize = time.time()
        self.request_count = 0
        atexit.register(self.serialize)

    def add_tracker(self, tracker):
        if isinstance(tracker, tuple):
            name, tracker = tracker
        elif isinstance(tracker, basestring):
            name = tracker
            tracker = self.load_tracker(name)
        else:
            name = tracker.name
        if name in self.trackers:
            raise ValueError(
                "Trying to add tracker %r with name %r, when there is already a "
                "tracker (%r) with that name"
                % (tracker, name, self.trackers[name]))
        self.trackers[name] = tracker(self, name, self.storage)

    def load_tracker(self, name):
        if '.' not in name:
            name = 'vaineye.trackers.%s' % name
        return simple_import(name)

    def tracker(self, name):
        return self.trackers[name]

    def serialize(self):
        if not self.serialize_lock.acquire(False):
            # Someone else is currently serializing
            return
        try:
            self.last_serialize = time.time()
            self.request_counts = 0
            for tracker in self.trackers.values():
                tracker.serialize()
        finally:
            self.serialize_lock.release()

    def serialize_in_thread(self):
        t = threading.Thread(target=self.serialize)
        t.start()

    def __call__(self, environ, start_response):
        url = construct_url(environ)
        self.request_count += 1
        if (self.request_count > self.serialize_requests
            or time.time() - self.last_serialize > self.serialize_time):
            self.serialize_in_thread()
        def repl_start_response(status, headers, exc_info=None):
            self.track_request(url, status, headers)
            return start_response(status, headers, exc_info)
        return self.app(environ, repl_start_response)

    def track_request(self, url, status, headers):
        for tracker in self.trackers.values():
            tracker.track_request(url, status, headers)
    
