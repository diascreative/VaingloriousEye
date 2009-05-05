import time
import urlparse
from datetime import datetime
from sqlalchemy import MetaData, Table
from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy import create_engine, select

class RequestTracker(object):

    def __init__(self, db):
        self.engine = create_engine(db, echo=True)
        self.sql_metadata = MetaData()
        self.table = Table(
            'requests', self.sql_metadata,
            Column('id', Integer, primary_key=True),
            Column('ip', String(15)),
            Column('date', DateTime),
            Column('processing_time', Float),
            Column('request_method', String(15)),
            Column('scheme', String),
            Column('host', String),
            Column('path', String),
            Column('query_string', String),
            Column('user_agent', String),
            Column('referrer', String),
            Column('response_code', Integer),
            Column('response_bytes', Integer),
            Column('content_type', String),
            Column('ip_location', String),
            )
        self.table_insert = self.table.insert()
        self.sql_metadata.create_all(self.engine)
        self._pending = []

    def add_request(self, environ, start_time, end_time,
                    status, response_headers):
        request = {
            'REMOTE_ADDR': environ['REMOTE_ADDR'],
            'vaineye.date': datetime.now(),
            'vaineye.start_time': time.time(),
            'REQUEST_METHOD': environ['REQUEST_METHOD'],
            'wsgi.url_scheme': environ['wsgi.url_scheme'],
            'HTTP_HOST': environ.get('HTTP_HOST', ''),
            'SCRIPT_NAME': environ.get('SCRIPT_NAME', ''),
            'PATH_INFO': environ.get('PATH_INFO', ''),
            'QUERY_STRING': environ.get('QUERY_STRING', ''),
            'HTTP_USER_AGENT': environ.get('HTTP_USER_AGENT', ''),
            'HTTP_REFERER': environ.get('HTTP_REFERER', ''),
            }
        request['vaineye.response_code'] = int(status.split(None, 1)[0])
        for header_name, header_value in response_headers:
            header_name = header_name.lower()
            if header_name == 'content-length':
                request['vaineye.response_bytes'] = int(header_value)
            elif header_name == 'content-type':
                request['vaineye.content_type'] = header_value
        request['vaineye.end_time'] = time.time()
        ## FIXME: should I just be creating SQLAlchemy bound inserts
        ## that I can execute later?
        self._pending.append(request)

    def write_pending(self):
        conn = self.engine.connect()
        for request in self._pending:
            ins = self.table_insert.values({
                'ip': request['REMOTE_ADDR'],
                'date': datetime.fromtimestamp(request['vaineye.start_time']),
                'processing_time': request['vaineye.end_time'] - request['vaineye.start_time'],
                'request_method': request['REQUEST_METHOD'],
                'scheme': request['wsgi.url_scheme'],
                'host': request.get('HTTP_HOST'),
                ## urllib.quote?:
                'path': request.get('SCRIPT_NAME', '')+request.get('PATH_INFO', ''),
                'query_string': request.get('QUERY_STRING', ''),
                'user_agent': request.get('HTTP_USER_AGENT', ''),
                'referrer': request.get('HTTP_REFERER', ''),
                'response_code': request['vaineye.response_code'],
                'response_bytes': request.get('vaineye.response_bytes'),
                'content_type': request.get('vaineye.content_type'),
                'ip_location': request.get('vaineye.ip_location'),
                })
            conn.execute(ins)
        self._pending = []

    def requests_since(self, d):
        conn = self.engine.connect()
        q = select([self.table],
                   self.table.c.date >= d)
        for row in conn.execute(q):
            row = dict(row)
            url = urlparse.urlunsplit((row['scheme'],
                                       row['host'],
                                       row['path'],
                                       row['query_string'],
                                       ''))
            row['url'] = url
            yield row
