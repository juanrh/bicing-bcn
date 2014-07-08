#!/usr/bin/env python

'''
For Python 2.7
'''
import re, sys, os, glob, time
from twisted.web import server, resource
from twisted.internet import reactor
import json

timestamp_re = re.compile("<!\[CDATA\[(\d+)\]\]>")
timestamp_format = "<![CDATA[{timestamp}]]>"

class TestResource(resource.Resource):
    isLeaf = True

    def __init__(self):
        self._responses = []
        self._responses.append({'name' : 'pepe', 'age' : 30, 'sex' : 'male'})
        self._responses.append({'name' : 'maria', 'age' : 30, 'sex' : 'female'})
        self._responses.append({'name' : 'juan', 'age' : 31, 'sex' : 'male'})
        # -1 to start at 0 in first request
        self._current_register = -1
    
    def render_GET(self, request):
        request.setHeader("content-type", "application/json")
        now_timestamp = int(time.time())
        self._current_register = (self._current_register + 1) % len(self._responses)
        register = dict(self._responses[self._current_register])
        register['timestamp'] = now_timestamp
        return json.dumps(register, indent=3)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage:', sys.argv[0], '<port>'
        print '\texample:', sys.argv[0], '9999'
        sys.exit(1)
    port = int(sys.argv[1])

    print 'Running service at http://localhost:' + str(port)
    reactor.listenTCP(port, server.Site(TestResource()))
    reactor.run()
