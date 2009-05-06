from webob import Request, Response
from webob import exc
import os
import threading
from datetime import datetime
from cPickle import load, dump
from mako.lookup import TemplateLookup
from vaineye.model import RequestTracker
from waitforit import WaitForIt

class VaineyeView(object):

    summary_classes = []

    def __init__(self, db, data_dir, _synchronous=False):
        self.request_tracker = RequestTracker(db)
        self.data_dir = data_dir
        self.lookup = TemplateLookup(directories=[os.path.join(os.path.dirname(__file__), 'templates')])
        self.summarizers = [
            cls(self) for cls in self.summary_classes]
        self._synchronous = _synchronous

    def __call__(self, environ, start_response):
        req = Request(environ, charset='utf8')
        try:
            resp = self.app(req)
        except exc.HTTPException, resp:
            pass
        return resp(environ, start_response)

    def render(self, template_name, req, title, **args):
        tmpl = self.lookup.get_template(template_name)
        args['controller'] = self
        args['req'] = req
        return tmpl.render(title=title, **args)

    def app(self, req):
        next = req.path_info_peek() or 'index'
        try:
            meth = getattr(self, 'view_'+next)
        except AttributeError:
            raise exc.HTTPNotFound('No view for %r' % next)
        return meth(req)

    def view_index(self, req):
        return Response(self.render('index.html', req, title='View stats'))

    def view_summary(self, req):
        next_name = req.path_info.split('/', 2)[2]
        for summary in self.summarizers:
            if summary.name == next_name:
                if not self._synchronous:
                    return WaitForIt(summary.wsgi_application)
                else:
                    return summary.wsgi_application
        raise exc.HTTPNotFound('No summary with the name %r' % next_name)

class Summary(object):

    def __init__(self, controller):
        self.controller = controller
        assert self.name
        self.pickle_write_lock = threading.Lock()

    def merge_request(self, request, data):
        raise NotImplementedError

    def blank_data(self):
        raise NotImplementedError

    def summarize(self, data):
        return data

    def wsgi_application(self, environ, start_response):
        try:
            resp = self.app(Request(environ))
        except exc.HTTPException, resp:
            pass
        assert not isinstance(resp, basestring)
        return resp(environ, start_response)

    def app(self, req):
        data = self.update_data()
        data.summarized = self.summarize(data)
        return Response(self.controller.render(
            self.name + '.html',
            req,
            title='Summary: %s' % self.description,
            summary=self,
            data=data))

    def update_data(self):
        data = self.load_data()
        new_date = datetime.now()
        for request in self.controller.request_tracker.requests_during(
            data.time_updated, new_date):
            print 'Merging', request
            self.merge_request(request, data)
        data.time_updated = new_date
        self.save_data(data)
        return data

    def load_data(self):
        filename = os.path.join(self.controller.data_dir, self.name + '.pickle')
        if not os.path.exists(filename):
            return self.blank_data()
        fp = open(filename, 'rb')
        data = load(fp)
        fp.close()
        return data

    def save_data(self, data):
        filename = os.path.join(self.controller.data_dir, self.name + '.pickle')
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        fp = open(filename, 'wb')
        dump(data, fp)
        fp.close()

class HitsSummary(Summary):
    name = 'hits'
    description = 'Summarize Hits'

    def merge_request(self, request, data):
        url = request['url']
        data.requests[url] = data.requests.get(url, 0)+1

    def blank_data(self):
        data = Data()
        data.requests = {}
        return data

    def summarize(self, data):
        all_reqs = [(c, url) for url, c in data.requests.items()]
        all_reqs.sort()
        data.popular_requests = all_reqs
        
class Data(object):
    def __init__(self, time_updated=datetime(1990, 1, 1, 0, 0, 0)):
        if time_updated is None:
            time_updated = datetime.now()
        self.time_updated = time_updated

def make_vaineye_view(global_conf, db=None, data_dir=None,
                      _synchronous=False):
    assert db, 'You must give a db parameter'
    assert data_dir, 'You must give a data_dir parameter'
    from paste.deploy.converters import asbool
    return VaineyeView(db=db, data_dir=data_dir,
                       _synchronous=asbool(_synchronous))

for name, value in globals().items():
    if (isinstance(value, type) and issubclass(value, Summary)
        and value is not Summary):
        VaineyeView.summary_classes.append(value)
