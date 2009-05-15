"""
Stat viewer app for Vainglorious Eye stats (reads the tracker's database)
"""

import os
import threading
from datetime import datetime
from cPickle import load, dump
import urlparse
import fnmatch
import re
import urllib
from mako.lookup import TemplateLookup
from waitforit import WaitForIt
from paste.urlparser import StaticURLParser
from dateutil.parser import parse as parse_date
from topp.utils.pretty_date import prettyDate as pretty_date
from pygooglechart import MapChart
from sqlalchemy import and_
from webob import Request, Response
from webob import exc
from vaineye.model import RequestTracker
from vaineye.ziptostate import unabbreviate_state
from vaineye.bag import Bag
from vaineye.helpers import wsgi_wrap, wsgi_unwrap, fnum

class VaineyeView(object):
    """
    This is a WSGI application that displays the statistic results
    captured and saved by RequestTracker.
    """

    # These are subclasses of Summary; this is automatically filled
    # out later in the module:
    summary_classes = {}

    # The app that serves up CSS, Javascript, etc:
    static_app = StaticURLParser(os.path.join(os.path.dirname(__file__), 'static'))

    def __init__(self, db, data_dir, table_prefix='', _synchronous=False,
                 site_title='The Vainglorious Eye: '):
        """Instantiate/configure the object.

        `db` is a SQLAlchemy connection string

        `data_dir` is a directory where pickle caches are kept

        `_synchronous` can be set to True to avoid spawning any
        threads (even when summaries are slow)

        `site_title` is used in templates, a simple view customization
        """
        self.request_tracker = RequestTracker(db, table_prefix=table_prefix)
        self.data_dir = data_dir
        self.lookup = TemplateLookup(directories=[os.path.join(os.path.dirname(__file__), 'templates')])
        self._synchronous = _synchronous
        self.site_title = site_title
        if _synchronous:
            self.view_summary = self.summary
        else:
            self.view_summary = wsgi_unwrap(WaitForIt(wsgi_wrap(self.summary).wsgi_app,
                                                      time_limit=10, poll_time=5))

    def __call__(self, environ, start_response):
        """WSGI Interface"""
        req = Request(environ, charset='utf8')
        req.base_url = req.application_url
        return wsgi_wrap(self.app).wsgi_app(environ, start_response)

    def app(self, req):
        """Main non-WSGI entry point"""
        next = req.path_info_pop() or 'index'
        try:
            meth = getattr(self, 'view_'+next)
        except AttributeError:
            raise exc.HTTPNotFound('No view for %r' % next).exception
        return meth(req)

    def render(self, template_name, req, title, **args):
        """Render a template.  Some variables are populated
        automatically."""
        tmpl = self.lookup.get_template(template_name)
        args['controller'] = self
        args['req'] = req
        args['unabbreviate_state'] = unabbreviate_state
        args['fnum'] = fnum
        return tmpl.render(title=title, **args)

    def view_index(self, req):
        """Simple view for /"""
        return Response(self.render('index.html', req, title='View stats'))

    def summary(self, req):
        """The summary view.

        This may be wrapped with WaitForIt"""
        next_name = req.path_info_pop()
        if next_name not in self.summary_classes:
            raise exc.HTTPNotFound('No summary with the name %r' % next_name).exception
        cls = self.summary_classes[next_name]
        summary = cls(self, req)
        return summary.app(req)

    def view_static(self, req):
        """Serve static (CSS, etc) content"""
        return self.static_app

    def view_clear_cached(self, req):
        """Clear all the summary pickle caches"""
        assert req.method == 'POST'
        for filename in os.listdir(self.data_dir):
            if filename.endswith('.pickle'):
                os.unlink(os.path.join(self.data_dir, filename))
        raise exc.HTTPFound(location=req.base_url).exception

class Summary(object):
    """Abstract base class for summaries

    Each subclass of this class summarizes something different about
    requests.
    """

    # If this is true, all requests that result in a non-2xx response
    # code are filtered out:
    only_200 = False

    def __init__(self, controller, req):
        """Instantiate the summary per request, bound to the parent
        (`VaineyeView`) controller

        This automatically looks for the filter parameters:

        `start_date`: everything after this date (inclusive)

        `end_date`: everything before this date (inclusive)

        `all_content`: if true, then include content like text/css

        `path`: a wildcard expression to match against paths
        """
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
        """The form displayed on the index form

        `base` is the application base URL
        """
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
        """Abstract method; merge one request into the data

        Subclasses should add the request to the data"""
        raise NotImplementedError

    def blank_data(self):
        """Abstract method; create blank data

        Typical subclasses instantiate `Data()` and set attributes"""
        raise NotImplementedError

    def filter_request(self, request, data):
        """Return true if the request should be ignored/filtered.

        This tests all the values set up in `__init__`
        """
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

    # Content-types that represent "real" content, as opposed to
    # images, etc:
    content_types = [
        'text/html',
        'application/xhtml+xml',
        'text/xhtml',
        'application/pdf',
        'text/plain',
        None,
        ]

    def app(self, req):
        """The main view entry point, displays the results of this summary

        Subclasses primarily use the template (named after the `name`
        attribute) to customize the display, and need not override
        this method.
        """
        if 'waitforit.progress' in req.environ:
            progress = req.environ['waitforit.progress']
            def callback(index=None, total=None, total_callback=None):
                if total is None and index > 1000:
                    total = total_callback()
                if index is None:
                    progress['message'] = 'Saving'
                else:
                    index += 1
                    if not index % 100:
                        if not total or total == -1:
                            progress['message'] = 'Reviewed %s requests' % fnum(index)
                        else:
                            progress['message'] = 'Reviewed %s/%s requests' % (fnum(index), fnum(total))
                    if total and total != -1:
                        progress['percent'] = 100*index/total
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
        """Returns variables to be passed to the template
        """
        return {}

    def update_data(self, callback):
        """Updates the data, getting any unprocessed requests and
        merging them in"""
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

    def ammend_query(self, query, rt):
        """Ammends the SQLAlchemy query to add any parameters that are
        specific to the summary, e.g., to require data that the
        summary uses."""
        return query

    @property
    def pickle_filename(self):
        """The filename where the cache pickle is kept"""
        filename = os.path.join(self.controller.data_dir, self.id + '.pickle')
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        return filename

    def load_data(self):
        """Loads and returns the cache pickle data"""
        if not os.path.exists(self.pickle_filename):
            return self.blank_data()
        fp = open(self.pickle_filename, 'rb')
        data = load(fp)
        fp.close()
        return data

    def save_data(self, data):
        """Saves cache pickle data"""
        fp = open(self.pickle_filename, 'wb')
        dump(data, fp)
        fp.close()

    def parse_date_range(self, range):
        """Parse the ``date_range`` variable, which is a value like:

        ``mm/dd/YYYY - mm/dd/YYYY``
        (but may leave out some of those parameters)

        and returns ``(start_date, end_date)``, where either or both
        value may be None"""
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
        """Is the given domain a search engine?"""
        ## FIXME: obvious naive:
        if 'google.' in domain:
            return True

class HitsSummary(Summary):
    """Summarizes hits on a per-URL basis"""
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
    """Summarizes referrers, using both the referrer and the
    destination (local) URL"""
    name = 'referrers'
    description = 'Referrers'
    only_200 = True

    def __init__(self, controller, req):
        super(ReferrerSummary, self).__init__(controller, req)
        self.minimum_count = int(req.params.get('minimum_count') or '0')
        if self.minimum_count > 1:
            self.description += ' with more than %s hits' % self.minimum_count
        self.by_domain = bool(req.params.get('by_domain'))
        if self.by_domain:
            self.id += '_by-domain'
            self.description += ' by domains'
        self.no_ip = bool(req.params.get('no_ip'))
        if self.no_ip:
            self.id += '_no-ip'
            self.description += ' excluding IPs'

    @classmethod
    def view_form(cls, base):
        """The form displayed on the index form

        `base` is the application base URL
        """
        form = '''
        <form action="%(base)s/summary/%(name)s" method="GET">
        View date range: <input class="daterange" name="date_range" value=""><br>
        <label for="hits-all_content">
        Include images etc: <input type="checkbox" name="all_content" id="hits-all_content">
        </label>
        <br>
        Restrict to path (wildcards OK):
        <input type="text" name="path" style="width: 20em"><br>
        <label for="referrer-minimum">
        Minimum number of referrals to display:
        <input type="text" name="minimum_count" value="1" id="referrer-minimum">
        </label> <br>
        <label for="referrer-domains">
        Show referrers only by domain:
        <input type="checkbox" name="by_domain" id="referrer-domains">
        </label> <br>
        <label for="referrer-no-ip">
        Exclude IP referrers (only allow domains):
        <input type="checkbox" name="no_ip" id="referrer-no-ip">
        </label> <br>
        <input type="submit" value="View %(description)s">
        </form>
        ''' % dict(base=base, description=cls.description, name=cls.name)
        return form

    _no_ip_regex = re.compile(r'[0-9:\.]+$')

    def merge_request(self, request, data):
        referrer = request['referrer']
        if not referrer.strip():
            return
        ref_domain = urlparse.urlsplit(referrer)[1]
        if self.no_ip and self._no_ip_regex.match(ref_domain):
            return
        if self.by_domain:
            referrer = 'http://' + ref_domain
        url = request['url']
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
        # Can't filter out local requests here, but can filter out
        # no-referrer requests
        return and_(query, rt.table.c.referrer != '')

class LocationSummary(Summary):
    """Summarizes the location of visitors"""
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
        # Filter out requests without location data:
        return and_(query, rt.table.c.ip_country_code != '')

    def blank_data(self):
        data = Data()
        data.countries = Bag()
        data.states = Bag()
        data.cities = Bag()
        return data

    def vars(self, req, data):
        # Sets up the variables for the google charts used in the
        # template
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
    """
    Holds the per-summary data.  This is basically just a dumb
    container object, that has attributes set on it; only
    `time_updated` is common to all instances.
    """
    def __init__(self, time_updated=datetime(1990, 1, 1, 0, 0, 0)):
        if time_updated is None:
            time_updated = datetime.now()
        self.time_updated = time_updated

def make_vaineye_view(global_conf, db=None, table_prefix='', data_dir=None,
                      _synchronous=False, site_title='The Vainglorious Eye: ',
                      htpasswd=None):
    """Create the Vaineye viewer

    You must give a `db` parameter, a SQLAlchemy connection string
    (like ``sqlite:////path/to/file.db``)

    You must give a `data_dir` parameter, a location where cache files
    can be kept.

    If you give `htpasswd`, it should be the name of a file created
    with the ``htpasswd`` command.  Only users listed in this file
    will be allowed to view this application.
    """
    assert db, 'You must give a db parameter'
    assert data_dir, 'You must give a data_dir parameter'
    from paste.deploy.converters import asbool
    app = VaineyeView(db=db, table_prefix=table_prefix, data_dir=data_dir,
                      _synchronous=asbool(_synchronous),
                      site_title=site_title)
    if htpasswd:
        if not os.path.exists(htpasswd):
            raise ValueError('The htpasswd file %r does not exist' % htpasswd)
        from paste.auth.form import AuthFormHandler
        app = AuthFormHandler(app, CheckHtpasswd(htpasswd))
    return app

class CheckHtpasswd(object):
    def __init__(self, filename):
        self.filename = filename
    def __call__(self, environ, username, password):
        from vaineye.htpasswd import check_password, NoSuchUser
        try:
            return check_password(username, password, self.filename)
        except NoSuchUser:
            return False

# Populate VaineyeView.summary_classes:
for name, value in globals().items():
    if (isinstance(value, type) and issubclass(value, Summary)
        and value is not Summary):
        VaineyeView.summary_classes[value.name] = value
del name, value
