"""
Middleware that tracks requests
"""
import atexit
import threading
import time
from datetime import datetime
from paste.request import construct_url
from paste.util.import_string import simple_import
from sqlalchemy import MetaData, Table
from sqlalchemy import Integer, String, DateTime, Float
from sqlalchemy import create_engine
from vaineye.model import RequestTracker

class StatusWatcher(object):
    """Middleware that tracks requests"""

    def __init__(self, app, db,
                 serialize_time=120, serialize_requests=100,
                 _synchronous=False):
        """This wraps the `app` and saves data about each request.

        data is stored in `vaineye.model.RequestTracker`, instantiated
        with the `db` SQLAlchemy connection string.

        Periodically data is written to the database (every
        `serialize_time` seconds, or `serialize_requests` requests,
        whichever comes first).  This writing happens in a background
        thread.

        For debugging purposes you can set `_synchronous` to True to
        have requests written out every request without spawning a
        thread."""
        self.app = app
        self.request_tracker = RequestTracker(db)
        self.serialize_time = serialize_time
        self.serialize_requests = serialize_requests
        self._synchronous = _synchronous
        self.write_pending_lock = threading.Lock()
        self.last_written = time.time()
        self.request_count = 0
        if not _synchronous:
            atexit.register(self.write_pending)

    def write_pending(self):
        """Write all pending requests"""
        if not self.write_pending_lock.acquire(False):
            # Someone else is currently serializing
            return
        try:
            self.request_tracker.write_pending()
            self.last_written = time.time()
            self.request_counts = 0
        finally:
            self.write_pending_lock.release()

    def write_in_thread(self):
        """Write all pending requests, in a background thread"""
        t = threading.Thread(target=self.write_pending)
        t.start()

    def __call__(self, environ, start_response):
        """WSGI interface"""
        self.request_count += 1
        if not self._synchronous and (
            self.request_count > self.serialize_requests
            or time.time() - self.last_written > self.serialize_time):
            self.write_in_thread()
        start_time = time.time()
        def repl_start_response(status, headers, exc_info=None):
            end_time = time.time()
            self.request_tracker.add_request(
                environ=environ,
                start_time=start_time,
                end_time=end_time,
                status=status,
                response_headers=headers)
            if self._synchronous:
                self.write_pending()
            return start_response(status, headers, exc_info)
        return self.app(environ, repl_start_response)

def make_status_watcher(app, global_conf, db=None,
                        serialize_time=120,
                        serialize_requests=100,
                        _synchronous=False):
    """
    Adds a status tracker.  You must give it a database description
    and a data_dir (where it will store file-based data)
    """
    if not db:
        raise ValueError('You must give a value for db')
    from paste.deploy.converters import asbool
    return StatusWatcher(
        app, db=db,
        serialize_time=int(serialize_time),
        serialize_requests=int(serialize_requests),
        _synchronous=asbool(_synchronous))
