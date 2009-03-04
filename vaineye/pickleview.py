from cPickle import load
import os
import pprint

def make_pickleview(global_conf, data_dir, **kw):
    def vainglorious_pickleview(environ, start_response):
        result = []
        result.append('<html><body>')
        result.append('<h1>Behold! The Vain Glorious Eye!</h1>')
        pickles = os.listdir(data_dir)
        for fname in pickles:
            result.append('<h2>%s</h2>' % fname)
            f = open(os.path.join(data_dir, fname))
            data = load(f)
            f.close()
            result.append('<pre>%s</pre>' % pprint.pformat(data))
        result.append('</body></html>')
        start_response('200 OK', [('Content-type','text/html')])
        return result
    return vainglorious_pickleview
