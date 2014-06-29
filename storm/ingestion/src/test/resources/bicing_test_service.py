#!/usr/bin/env python

'''
For Python 2.7
'''
import re, sys, os, glob, time
from twisted.web import server, resource
from twisted.internet import reactor

timestamp_re = re.compile("<!\[CDATA\[(\d+)\]\]>")
timestamp_format = "<![CDATA[{timestamp}]]>"

class BicingSimResource(resource.Resource):
    isLeaf = True

    def __init__(self):
        data_files_dir = os.path.dirname(os.path.realpath(__file__))
        data_files_paths = glob.glob(os.path.join(data_files_dir, "bicing*_UTC.xml"))

        self._files = []
        # take just first file
        for data_file_path in data_files_paths:
            with open(data_file_path, 'r') as data_file:
                self._files.append(data_file.read())
        print "Loaded files", data_files_paths
        # -1 to start at 0 in first request
        self._current_file = -1
    
    def render_GET(self, request):
        request.setHeader("content-type", "text/xml")
        now_timestamp = int(time.time())
        self._current_file = (self._current_file + 1) % len(self._files)
        print "Using file", self._current_file
        return timestamp_re.sub(timestamp_format.format(timestamp = now_timestamp), self._files[self._current_file])

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage:', sys.argv[0], '<port>'
        print '\texample:', sys.argv[0], '9999'
        sys.exit(1)
    port = int(sys.argv[1])

    print 'Running service at http://localhost:' + str(port)
    reactor.listenTCP(port, server.Site(BicingSimResource()))
    reactor.run()
