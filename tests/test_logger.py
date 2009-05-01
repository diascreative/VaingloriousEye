import os
import shutil
import time
from vaineye import statuswatch
from vaineye.trackers import week_number
from webtest import TestApp

here = os.path.dirname(__file__)

def status_app(environ, start_response):
    path_info = environ.get('PATH_INFO', '')
    status = '200 OK'
    headers = [('Content-type', 'text/html')]
    if path_info.startswith('/notfound'):
        status = '404 Not Found'
    elif path_info.startswith('/redir'):
        headers.append(('Location', '/newloc'))
        status = '301 Moved Permanently'
    content = 'This is a page'
    start_response(status, headers)
    return [content]

wsgi_app = statuswatch.StatusWatcher(
    status_app, db='sqlite:///:memory:', _synchronous=True,
    trackers=['NotFound', 'Redirect', 'Hits'])#, 'HitsWeekly'])

app = TestApp(wsgi_app)

def test_logs():
    conn = wsgi_app.sql_engine.connect()
    app.get('/')
    app.get('/')
    assert not wsgi_app.tracker('NotFound').select(conn)
    assert dict(wsgi_app.tracker('Hits').select(conn)) == {'http://localhost/': 2}
    app.get('/notfound', status=404)
    app.get('/notfound/2', status=404)
    app.get('/notfound', status=404)
    assert wsgi_app.tracker('NotFound').select(conn)
    week = week_number(time.time())
    #assert wsgi_app.tracker('HitsWeekly').data == {week: {'http://localhost/': 2}}
    assert dict(wsgi_app.tracker('NotFound').select(conn)) == {
        'http://localhost/notfound': 2,
        'http://localhost/notfound/2': 1,
        }, wsgi_app.tracker('NotFound').select(conn)
    assert not wsgi_app.tracker('Redirect').select(conn)
    app.get('/redir')
    app.get('/redir/2')
    print wsgi_app.tracker('Redirect').select(conn)
    # @@: Should the URLs be completely normalized here?  I.e., fully qualified
    assert sorted(wsgi_app.tracker('Redirect').select(conn)) == [
        (u'http://localhost/redir', u'/newloc', 1),
        (u'http://localhost/redir/2', u'/newloc', 1),
        ]

    
