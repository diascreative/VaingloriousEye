import os
import shutil
import time
from vaineye import statuswatch
from vaineye.trackers import week_number
from paste.fixture import TestApp

here = os.path.dirname(__file__)
test_data = os.path.join(here, 'test-data')
if os.path.exists(test_data):
    shutil.rmtree(test_data)
os.mkdir(test_data)

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
    status_app, data_store=os.path.join(test_data),
    trackers=['NotFound', 'Redirect', 'Hits', 'HitsWeekly'])

app = TestApp(wsgi_app)

def test_logs():
    app.get('/')
    app.get('/')
    assert not wsgi_app.tracker('NotFound').data
    assert wsgi_app.tracker('Hits').data == {'http://localhost/': 2}
    week = week_number(time.time())
    assert wsgi_app.tracker('HitsWeekly').data == {week: {'http://localhost/': 2}}
    app.get('/notfound', status=404)
    app.get('/notfound/2', status=404)
    app.get('/notfound', status=404)
    assert wsgi_app.tracker('NotFound').data == {
        'http://localhost/notfound': 2,
        'http://localhost/notfound/2': 1,
        }
    assert not wsgi_app.tracker('Redirect').data
    app.get('/redir')
    app.get('/redir/2')
    print wsgi_app.tracker('Redirect').data
    # @@: Should the URLs be completely normalized here?  I.e., fully qualified
    assert wsgi_app.tracker('Redirect').data == {
        ('http://localhost/redir', '/newloc'): 1,
        ('http://localhost/redir/2', '/newloc'): 1,
        }
    
