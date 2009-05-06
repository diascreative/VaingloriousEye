"""
Stat viewer app for Vainglorious Eye stats (reads the tracker's database)
"""

from webob import Request, Response
from webob import exc
import os
import threading
from datetime import datetime
from cPickle import load, dump
import urlparse
import fnmatch
import re
import urllib
from mako.lookup import TemplateLookup
from vaineye.model import RequestTracker
from waitforit import WaitForIt
from paste.urlparser import StaticURLParser
from dateutil.parser import parse as parse_date
from topp.utils.pretty_date import prettyDate as pretty_date
from vaineye.ziptostate import unabbreviate_state
from pygooglechart import MapChart
from sqlalchemy import and_
from paste.lint import middleware as lint
from vaineye.bag import Bag

class Bag(object):
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

class wsgi_wrap(object):
    def __init__(self, func):
        self.func = func
    def __get__(self, obj, type=None):
        return self.__class__(self.func.__get__(obj, type))
    def wsgi_app(self, environ, start_response):
        req = Request(environ)
        try:
            resp = self.func(req)
        except exc.HTTPException, resp:
            pass
        return resp(environ, start_response)
    def __call__(self, *args, **kw):
        return self.func(*args, **kw)

class wsgi_unwrap(object):
    def __init__(self, wsgi_app):
        self.wsgi_app = wsgi_app
    def __call__(self, req):
        return self.wsgi_app

class VaineyeView(object):

    summary_classes = {}

    static_app = StaticURLParser(os.path.join(os.path.dirname(__file__), 'static'))

    def __init__(self, db, data_dir, _synchronous=False,
                 site_title='The Vainglorious Eye: '):
        self.request_tracker = RequestTracker(db)
        self.data_dir = data_dir
        self.lookup = TemplateLookup(directories=[os.path.join(os.path.dirname(__file__), 'templates')])
        self._synchronous = _synchronous
        self.site_title = site_title
        if _synchronous:
            self.view_summary = self.summary
        else:
            self.view_summary = wsgi_unwrap(WaitForIt(lint(wsgi_wrap(self.summary).wsgi_app),
                                                      time_limit=1, poll_time=5))

    def __call__(self, environ, start_response):
        req = Request(environ, charset='utf8')
        req.base_url = req.application_url
        return wsgi_wrap(self.app).wsgi_app(environ, start_response)

    def render(self, template_name, req, title, **args):
        tmpl = self.lookup.get_template(template_name)
        args['controller'] = self
        args['req'] = req
        args['unabbreviate_state'] = unabbreviate_state
        return tmpl.render(title=title, **args)

    def app(self, req):
        next = req.path_info_pop() or 'index'
        try:
            meth = getattr(self, 'view_'+next)
        except AttributeError:
            raise exc.HTTPNotFound('No view for %r' % next).exception
        return meth(req)

    def view_index(self, req):
        return Response(self.render('index.html', req, title='View stats'))

    def summary(self, req):
        next_name = req.path_info_pop()
        if next_name not in self.summary_classes:
            raise exc.HTTPNotFound('No summary with the name %r' % next_name).exception
        cls = self.summary_classes[next_name]
        summary = cls(self, req)
        return summary.app(req)

    def view_static(self, req):
        return self.static_app

    def view_clear_cached(self, req):
        assert req.method == 'POST'
        for filename in os.listdir(self.data_dir):
            if filename.endswith('.pickle'):
                os.unlink(os.path.join(self.data_dir, filename))
        raise exc.HTTPFound(location=req.base_url).exception

class Summary(object):

    only_200 = False

    def __init__(self, controller, req):
        self.controller = controller
        assert self.name
        self.pickle_write_lock = threading.Lock()
        self.req = req
        self.id = self.name
        self.start_date, self.end_date = self.parse_date_range(req.GET.get('date_range'))
        if self.start_date:
            self.id += '_start-%s' % self.start_date.strftime('%Y%m%d')
            if self.start_date == self.end_date:
                self.description += ' on %s' % pretty_date(self.start_date)
            else:
                self.description += ' from %s' % pretty_date(self.start_date)
        if self.end_date:
            self.id += '_end-%s' % self.end_date.strftime('%Y%m%d')
            if self.start_date != self.end_date:
                self.description += ' until %s' % pretty_date(self.end_date)
        self.all_content = bool(req.GET.get('all_content'))
        if self.all_content:
            self.id += '_all-content'
            self.description += ' including images, etc'
        path = req.GET.get('path')
        if path:
            self.path_regex = re.compile(fnmatch.translate(path))
            self.id += '_path-%s' % urllib.quote(path, '')
            self.description += ' for path %s' % path
        else:
            self.path_regex = None

    @classmethod
    def view_form(cls, base):
        form = '''
        <form action="%(base)s/summary/%(name)s" method="GET">
        View date range: <input class="daterange" name="date_range" value=""><br>
        <label for="hits-all_content">
        Include images etc: <input type="checkbox" name="all_content" id="hits-all_content">
        </label>
        <br>
        Restrict to path (wildcards OK):
        <input type="text" name="path" style="width: 20em"><br>
        <input type="submit" value="View %(description)s">
        </form>
        ''' % dict(base=base, description=cls.description, name=cls.name)
        return form

    def merge_request(self, request, data):
        raise NotImplementedError

    def blank_data(self):
        raise NotImplementedError

    def filter_request(self, request, data):
        if (not self.all_content
            and request['content_type']
            and request['content_type'].split(';')[0] not in self.content_types):
            return True
        if (self.start_date
            and request['date'] < self.start_date):
            return True
        if (self.end_date
            and request['date'] > self.end_date):
            return True
        if self.only_200 and request['response_code'] >= 300:
            return True
        if self.path_regex and not self.path_regex.match(request['path']):
            return True

    content_types = [
        'text/html',
        'application/xhtml+xml',
        'text/xhtml',
        'application/pdf',
        'text/plain',
        None,
        ]

    def app(self, req):
        if 'waitforit.progress' in req.environ:
            progress = req.environ['waitforit.progress']
            def callback(index=None, total=None):
                if index is None:
                    progress['message'] = 'Saving'
                else:
                    index += 1
                    if not index % 100:
                        progress['message'] = 'Reviewed %i requests' % index
        else:
            callback = None
        data = self.update_data(callback)
        return Response(self.controller.render(
            self.name + '.html',
            req,
            title='Summary: %s' % self.description,
            summary=self,
            data=data,
            **self.vars(req, data)))

    def vars(self, req, data):
        return {}

    def update_data(self, callback):
        rt = self.controller.request_tracker
        data = self.load_data()
        end = new_date = datetime.now()
        start = data.time_updated
        if self.end_date and self.end_date < end:
            end = self.end_date
        if self.start_date and self.start_date > start:
            start = self.start_time
        query = and_(rt.table.c.date >= start,
                     rt.table.c.date < end)
        if self.only_200:
            query = and_(query, rt.table.c.response_code < 300)
        query = self.ammend_query(query, rt)
        for index, request in enumerate(rt.requests(query, callback)):
            if self.filter_request(request, data):
                #print 'filtered', request
                continue
            self.merge_request(request, data)
        if callback:
            callback()
        data.time_updated = new_date
        self.save_data(data)
        return data

    def ammend_query(query, rt):
        return query

    @property
    def pickle_filename(self):
        filename = os.path.join(self.controller.data_dir, self.id + '.pickle')
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        return filename

    def load_data(self):
        if not os.path.exists(self.pickle_filename):
            return self.blank_data()
        fp = open(self.pickle_filename, 'rb')
        data = load(fp)
        fp.close()
        return data

    def save_data(self, data):
        fp = open(self.pickle_filename, 'wb')
        dump(data, fp)
        fp.close()

    def parse_date_range(self, range):
        if not range or not range.strip():
            return None, None
        if '-' not in range:
            date = parse_date(range)
            return date, date
        start, end = range.split('-', 1)
        if not start.strip():
            start = None
        else:
            start = parse_date(start.strip())
        if not end.strip():
            end = None
        else:
            end = parse_date(end.strip())
        return start, end

    def is_search_domain(self, domain):
        if 'google.' in domain:
            return True

class HitsSummary(Summary):
    name = 'hits'
    description = 'Hits'
    only_200 = True

    def merge_request(self, request, data):
        url = request['url']
        data.requests.add(url)

    def blank_data(self):
        data = Data()
        data.requests = Bag()
        return data

class ReferrerSummary(Summary):
    name = 'referrers'
    description = 'Referrers'
    only_200 = True

    def merge_request(self, request, data):
        referrer = request['referrer']
        if not referrer.strip():
            return
        url = request['url']
        ref_domain = urlparse.urlsplit(referrer)[1]
        if self.is_search_domain(ref_domain):
            return
        url_domain = urlparse.urlsplit(url)[1]
        if url_domain and url_domain == ref_domain:
            return
        data.referrers.add((referrer, url))

    def blank_data(self):
        data = Data()
        data.referrers = Bag()
        return data

    def ammend_query(self, query, rt):
        # Can't filter out local requests here
        return and_(query, rt.table.c.referrer != '')

class LocationSummary(Summary):
    name = 'location'
    description = 'Location'
    only_200 = True

    def merge_request(self, request, data):
        country_name = request['ip_country_name']
        country_code = request['ip_country_code']
        if country_name and country_code:
            data.countries.add((country_name, country_code))
        state = request['ip_state']
        if state:
            data.states.add(state)
        city = request['ip_city']
        if state and city:
            data.cities.add((state, city))

    def ammend_query(self, query, rt):
        return and_(query, rt.table.c.ip_country_code != '')

    def blank_data(self):
        data = Data()
        data.countries = Bag()
        data.states = Bag()
        data.cities = Bag()
        return data

    def vars(self, req, data):
        v = {}
        # 440x220 is the max size
        us_map = MapChart(440, 220)
        us_map.geo_area = 'usa'
        items = data.states.counts()
        us_map.set_codes([state for count, state in items])
        us_map.add_data([count for count, state in items])
        us_map.set_colours(('EEEEEE', '0000FF', '000033'))
        v['us_map_url'] = us_map.get_url()
        country_map = MapChart(440, 220)
        items = data.countries.counts()
        country_map.set_codes([country_code for count, (country_name, country_code) in items])
        country_map.add_data([count for count, (country_name, country_code) in items])
        country_map.set_colours(('EEEEEE', '9999FF', '000033'))
        v['country_map_url'] = country_map.get_url()
        return v

class Data(object):
    def __init__(self, time_updated=datetime(1990, 1, 1, 0, 0, 0)):
        if time_updated is None:
            time_updated = datetime.now()
        self.time_updated = time_updated

def make_vaineye_view(global_conf, db=None, data_dir=None,
                      _synchronous=False, site_title='The Vainglorious Eye: '):
    assert db, 'You must give a db parameter'
    assert data_dir, 'You must give a data_dir parameter'
    from paste.deploy.converters import asbool
    return VaineyeView(db=db, data_dir=data_dir,
                       _synchronous=asbool(_synchronous),
                       site_title=site_title)

for name, value in globals().items():
    if (isinstance(value, type) and issubclass(value, Summary)
        and value is not Summary):
        VaineyeView.summary_classes[value.name] = value
