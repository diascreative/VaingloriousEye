"""
Imports Apache log files
"""
import optparse
import sys
from vaineye.model import RequestTracker

parser = optparse.OptionParser(
    usage='%prog [OPTIONS] DB_CONNECTION < apache/access.log'
    )
parser.add_option(
    '-H', '--host',
    metavar='HOST',
    help='The default value for the host (normally recorded in vaineye, but not present in Apache logs)')

parser.add_option(
    '--scheme',
    metavar='SCHEME',
    help='The default value for the scheme (default "http")',
    default='http')

parser.add_option(
    '--table-prefix',
    metavar='PREFIX',
    help='The prefix to prepend on the table(s) created by the system',
    default='')

parser.add_option(
    '-b', '--batch',
    metavar='COUNT',
    help='The number of entries to insert at once',
    default='240000')

def main(args=None, stdin=sys.stdin):
    if args is None:
        args = sys.argv[1:]
    options, args = parser.parse_args(args)
    if len(args) < 1:
        parser.error('You must give a DB_CONNECTION string')
    insert_count = int(options.batch)
    request_tracker = RequestTracker(args[0], options.table_prefix)
    done = False
    while not done:
        done = True
        for index, line in enumerate(stdin):
            try:
                request_tracker.import_apache_line(
                    line,
                    default_host=options.host,
                    default_scheme=options.scheme)
            except ValueError, e:
                print >> sys.stdout, str(e)
            if not index % 1000:
                sys.stdout.write('.')
                sys.stdout.flush()
            if index > insert_count:
                done = False
                break
        sys.stdout.write('writing db...\n')
        sys.stdout.flush()
        def writer(i=None, total=0):
            if i and not i % 1000:
                sys.stdout.write('.')
                sys.stdout.flush()
            if i is None:
                sys.stdout.write('write...')
                sys.stdout.flush()
        request_tracker.write_pending(writer)
        if not done:
            sys.stdout.write('\ncontinuing...')
            sys.stdout.flush()
            import gc
            gc.collect()
    print 'done.'

def first_lines(stream, count):
    for i in xrange(count):
        yield i, stream.readline()

if __name__ == '__main__':
    sys.exit(main())
    
