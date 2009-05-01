import time
from paste.response import header_value
from sqlalchemy.sql import select
from sqlalchemy import Table, Column, String, Integer

class Tracker(object):
    """
    Abstract base class for trackers
    """

    name = None

    def __init__(self, watcher, name, metadata, engine):
        self.watcher = watcher
        self.name = name
        self._pending = self.empty_pending()
        self.metadata = metadata
        self.engine = engine

    def track_request(self, url, status, environ, headers):
        raise NotImplementedError

    def write_pending(self, conn):
        raise NotImplementedError

    def merge_values(self, conn, data, table, key):
        key_column = getattr(table.c, key)
        count_column = table.c.count
        result = conn.execute(
            select([key_column],
                   key_column.in_(data.keys())))
        work = []
        for (key_value,) in result:
            work.append(
                table.update(count_column).where(
                    key_column==key_value).values(
                    {count_column: count_column+data[key_value]}))
            del data[key_value]
        for key_value, count in data.items():
            work.append(table.insert().values({key_column: url, count_column: count}))
        conn.execute(work)
        self._pending = self.empty_pending()

class NotFound(Tracker):

    def empty_pending(self):
        return {}

    def setup_database(self):
        self.not_found_urls = Table(
            'not_found', self.metadata,
            Column('url', String(255), primary_key=True),
            Column('count', Integer))

    def track_request(self, url, status, environ, headers):
        if status.startswith('404'):
            self._pending[url] = self._pending.get(url, 0)+1

    def write_pending(self, conn):
        T = self.not_found_urls
        result = conn.execute(
            select([T.c.url],
                   T.c.url.in_(self._pending.keys())))
        for (url,) in result:
            conn.execute(
                T.update(T.c.count).where(
                    T.c.url==url).values(
                    {T.c.count: T.c.count+self._pending[url]}))
            del self._pending[url]
        values = []
        for url, count in self._pending.items():
            values.append({'url': url, 'count': count})
        if values:
            conn.execute(T.insert(), values)
        self._pending = self.empty_pending()

    def select(self, conn):
        T = self.not_found_urls
        return conn.execute(select([T.c.url, T.c.count])).fetchall()
        
class Redirect(Tracker):

    def empty_pending(self):
        return {}

    def track_request(self, url, status, environ, headers):
        if status.startswith('301'):
            location = header_value(headers, 'Location')
            url = self._pending.setdefault(url, {})
            url[location] = url.get(location, 0)+1

    def setup_database(self):
        self.redirects = Table(
            'redirect', self.metadata,
            Column('url', String(255), primary_key=True),
            Column('location', String(255), primary_key=True),
            Column('count', Integer))
    
    def write_pending(self, conn):
        T = self.redirects
        result = conn.execute(
            select([T.c.url, T.c.location],
                   T.c.url.in_(self._pending.keys())))
        for (url, location) in result:
            if location not in self._pending[url]:
                continue
            conn.execute(
                T.update(T.c.count).where(
                    (T.c.url==url).and_(T.c.location==location)).values(
                    {T.c.count: T.c.count+self._pending[url][location]}))
            del self._pending[url][location]
        values = []
        for url, locations in self._pending.items():
            for location, count in locations.items():
                values.append({'url': url, 'location': location, 'count': count})
        if values:
            conn.execute(T.insert(), values)
        self._pending = self.empty_pending()

    def select(self, conn):
        T = self.redirects
        return conn.execute(select([T.c.url, T.c.location, T.c.count])).fetchall()


class Hits(Tracker):

    def empty_pending(self):
        return {}

    def track_request(self, url, status, environ, headers):
        self._pending[url] = self._pending.get(url, 0)+1

    def setup_database(self):
        self.hits = Table(
            'hit', self.metadata,
            Column('url', String(255), primary_key=True),
            Column('count', Integer))
        self.insert_hits = self.hits.insert()

    def write_pending(self, conn):
        T = self.hits
        result = conn.execute(
            select([T.c.url],
                   T.c.url.in_(self._pending.keys())))
        for (url,) in result:
            conn.execute(
                T.update(T.c.count).where(
                    T.c.url==url).values(
                    {T.c.count: T.c.count+self._pending[url]}))
            del self._pending[url]
        values = []
        for url, count in self._pending.items():
            values.append({'url': url, 'count': count})
        if values:
            conn.execute(T.insert(), values)
        self._pending = self.empty_pending()

    def select(self, conn):
        T = self.hits
        return conn.execute(select([T.c.url, T.c.count])).fetchall()

def week_number(t):
    return int(time.strftime('%U', time.localtime(t)))    

class HitsWeekly(Tracker):

    def track_request(self, url, status, environ, headers):
        self._pending.append((url, time.time()))

    def _merge_item(self, data, (url, t)):
        week = week_number(t)
        if week not in data:
            data[week] = {}
        data[week][url] = data[week].get(url, 0)+1

class HitsWeeklyByReferrer(Tracker):
    """Track the weekly hits, but group by referrer"""

    def track_request(self, url, status, environ, headers):
        referrer = environ.get("HTTP_REFERER", None)
        if referrer is not None:
            self._pending.append((url, referrer, time.time()))

    def _merge_item(self, data, (url, referrer, t)):
        week = week_number(t)
        if week not in data:
            data[week] = {}
        if url not in data[week]:
            data[week][url] = {}
        data[week][url][referrer] = data[week][url].get(referrer, 0)+1
