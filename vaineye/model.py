import time
import urlparse
from datetime import datetime, timedelta
import re
from sqlalchemy import MetaData, Table
from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy import create_engine, select, and_

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
            if request.get('vaineye.end_time'):
                processing_time = request['vaineye.end_time'] - request['vaineye.start_time']
            else:
                processing_time = None
            date = request.get('vaineye.date')
            if not date:
                date = datetime.fromtimestamp(request['vaineye.start_time'])
            ins = self.table_insert.values({
                'ip': request['REMOTE_ADDR'],
                'date': date,
                'processing_time': processing_time,
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

    def requests_during(self, start, end):
        conn = self.engine.connect()
        q = select([self.table],
                   and_(self.table.c.date >= start,
                        self.table.c.date < end))
        for row in conn.execute(q):
            row = dict(row)
            url = urlparse.urlunsplit((row['scheme'],
                                       row['host'],
                                       row['path'],
                                       row['query_string'],
                                       ''))
            row['url'] = url
            yield row

    apache_line_re = re.compile(r'''
    (?P<ip>[\d.:a-fA-F]+)          \s+  # IP Address
    (?P<ident>[^\s]+)              \s+  # ident (usually -)
    (?P<user>[^\s]+)               \s+  # logged-in user (usually -)
    \[(?P<date>[^\]]*)\]           \s+  # date
    "(?P<method>[A-Z]+)            \s+  # Start of the request string
    (?P<path>[^ ])+                \s+  # Requested path
    HTTP/(?P<http_version>[\d.]*)" \s+  # The version
    (?P<status>\d+)                \s+  # Response status version
    (?P<bytes>\d+|-)               \s+  # Bytes in response (- is 0)
    "(?P<referrer>[^"]*)"          \s+  # Referrer
    "(?P<user_agent>[^"]*)"             # User-Agent
    ''', re.VERBOSE)

    apache_date_format = '%d/%b/%Y:%H:%M:%S'

    def import_apache_line(self, line, default_scheme='http', default_host='localhost'):
        match = self.apache_line_re.match(line)
        if not match:
            raise ValueError("Bad line, cannot parse: %r" % line)
        d = match.groupdict()
        date = datetime.fromtimestamp(
            time.mktime(time.strptime(d['date'].split(None, 1)[0], self.apache_date_format)))
        date = date + timedelta(hours=int(d['date'].split(None, 1)[1]))
        if '?' in d['path']:
            path, query_string = d.path.split('?', 1)
        else:
            path = d['path']
            query_string = ''
        if not d.get('bytes') or d['bytes'] == '-':
            bytes = 0
        else:
            bytes = int(d['bytes'])
        referrer = d['referrer']
        if referrer == '-':
            referrer = ''
        user_agent = d['user_agent']
        if user_agent == '-':
            user_agent = '-'
        request = {
            'REMOTE_ADDR': d['ip'],
            'vaineye.date': date,
            'vaineye.start_time': None,
            'vaineye.end_time': None,
            'REQUEST_METHOD': d['method'],
            'wsgi.url_scheme': default_scheme,
            'HTTP_HOST': default_host,
            'SCRIPT_NAME': '',
            'PATH_INFO': path,
            'QUERY_STRING': query_string,
            'HTTP_USER_AGENT': user_agent,
            'HTTP_REFERER': referrer,
            'vaineye.response_code': int(d['status']),
            'vaineye.response_bytes': bytes,
            'vaineye.content_type': None,
            'ip_location': None,
            }
        self._pending.append(request)
