import atexit
import threading
import time
from paste.request import construct_url
from paste.util.import_string import simple_import
from sqlalchemy import MetaData
from sqlalchemy import create_engine

class StatusWatcher(object):

    def __init__(self, app, db, trackers,
                 serialize_time=120, serialize_requests=100,
                 _synchronous=False):
        self.app = app
        self.sql_metadata = MetaData()
        self.sql_engine = create_engine(db, echo=True)
        if hasattr(trackers, 'values'):
            trackers = trackers.values()
        self.trackers = {}
        self._setting_up = True
        for tracker in trackers:
            self.add_tracker(tracker)
        self._setting_up = False
        self.sql_metadata.create_all(self.sql_engine)
        self.serialize_time = serialize_time
        self.serialize_requests = serialize_requests
        self._synchronous = _synchronous
        self.write_pending_lock = threading.Lock()
        self.last_written = time.time()
        self.request_count = 0
        if not _synchronous:
            atexit.register(self.write_pending)

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
        self.trackers[name] = tracker(self, name, self.sql_metadata, self.sql_engine)
        self.trackers[name].setup_database()
        if not self._setting_up:
            self.sql_metadata.create_all(self.sql_engine)

    def load_tracker(self, name):
        if '.' not in name:
            name = 'vaineye.trackers.%s' % name
        return simple_import(name)

    def tracker(self, name):
        return self.trackers[name]

    def write_pending(self):
        if not self.write_pending_lock.acquire(False):
            # Someone else is currently serializing
            return
        try:
            conn = self.sql_engine.connect()
            self.last_written = time.time()
            self.request_counts = 0
            for tracker in self.trackers.values():
                tracker.write_pending(conn)
        finally:
            self.write_pending_lock.release()

    def write_in_thread(self):
        t = threading.Thread(target=self.write_pending)
        t.start()

    def __call__(self, environ, start_response):
        url = construct_url(environ)
        self.request_count += 1
        if not self._synchronous and (
            self.request_count > self.serialize_requests
            or time.time() - self.last_written > self.serialize_time):
            self.write_in_thread()
        def repl_start_response(status, headers, exc_info=None):
            self.track_request(url, status, environ, headers)
            if self._synchronous:
                self.write_pending()
            return start_response(status, headers, exc_info)
        return self.app(environ, repl_start_response)

    def track_request(self, url, status, environ, headers):
        for tracker in self.trackers.values():
            tracker.track_request(url, status, environ, headers)
    

def make_status_watcher(app, global_conf, db=None,
                        trackers=None, serialize_time=120,
                        serialize_requests=100):
    """
    Adds a status tracker.  You must give it a data_dir (where it will
    store data), and a trackers setting.

    trackers should be a series of trackers like::

      trackers = tracker1
                 tracker2
                 name:tracker3

    You can use things like ``name:tracker3`` to give alternate names
    to a tracker.

    Look in ``vaineye.trackers`` to see the available trackers.
    """
    if not db:
        raise ValueError('You must give a value for db')
    if isinstance(trackers, basestring):
        t = []
        for tracker in trackers.split():
            if ':' in tracker:
                name, tracker = tracker.split(':', 1)
                name = name.strip()
                tracker = tracker.split()
                t.append((name, tracker))
            else:
                t.append(tracker)
        trackers = t
    return StatusWatcher(
        app, db=db, trackers=trackers,
        serialize_time=int(serialize_time),
        serialize_requests=int(serialize_requests))
