from webob import Request

class wsgi_wrap(object):
    """
    Wraps a Request-taking Response-returning webob.exc-raising
    function as a WSGI app
    """
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
    """
    Takes a WSGI app and makes it into a Request-taking
    Response-returning object

    (This ends up being kind of zen)
    """
    def __init__(self, wsgi_app):
        self.wsgi_app = wsgi_app
    def __call__(self, req):
        return self.wsgi_app
