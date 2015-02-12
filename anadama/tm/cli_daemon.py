#!/usr/bin/env python
import tasks, optparse, sys, json, pprint, parser
import os
import tm as TaskManager
import tm_daemon
import signal
import tempfile
from pprint import pprint
#import tornado.httpserver
#import tornado.websocket
#import tornado.ioloop
#import tornado.web 
#from tornado.web import RequestHandler
import re
import globals


HELP="""%prog [-p port] [-c configuration ] 

%prog - TMD (Task Manager Daemon) runs the tornado web service on a 
given port and listens for both HTTP connections and websocket
connections from clients.  

Currently, the daemon only listens on localhost to prevent being
exposed too much.

It will serve static web content (the AnADAMA status page) to 
webbrowsers.  It will also communicate over a websocket from
javascript in the webbrowser or from the cli.py interface.
Communication via the websocket is json encoded.

-p gives the port that the daemon will listen on.
-c 
"""

opts_list = [
    optparse.make_option('-v', '--verbose', action="store_true",
                         dest="verbose", default=False,
                         help="Turn on verbose output (to stderr)"),
    optparse.make_option('-p', '--port', action="store", type="string",
                         dest="port", default=8888, help="Specify port daemon listens on"),
    optparse.make_option('-c', '--configuration', action="store", type="string",
                         dest="directory", default=os.getcwd(), help="Specify configuration file "),
]


def optionHandling():
    global opts

    if opts.port is None:
        opts.port = 8888

def main():
    global opts, p, argParser, tmgrs

    def sigtermSetup():
        signal.signal(signal.SIGTERM, sigtermHandler)
        signal.signal(signal.SIGINT, sigtermHandler)

    def sigtermHandler(signum, frame):
        print "caught signal " + str(signum)
        print "cleaning up..."
        for tm in tmgrs:
            tm.cleanup()
        print "shutting down webserver..."
        sys.exit(0)

    argParser = optparse.OptionParser(option_list=opts_list,
                                   usage=HELP)
    (opts, args) = argParser.parse_args()
    optionHandling()

    # load configuration parameters (if they exist)
    print "loading globals"
    globals.init(os.path.dirname(opts.directory))

    # signals
    sigtermSetup()

    # get our installed location
    # currentFile = os.path.realpath(__file__)
    # path = os.path.dirname(currentFile)
    # web_install_path = os.path.join(path, "anadama_flows")

    tm_daemon = Tm_daemon()
    tm_daemon.run(opts.port)

if __name__ == '__main__':
    main()
