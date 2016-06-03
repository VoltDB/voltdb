#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.

# A command line tool for getting job statistics via jenkins REST API
# usage:  stats help

import jenkins
import inspect
import re
import json
import sys
import getopt
import time
import getpass
import os
from six.moves.http_client import BadStatusLine
from six.moves.urllib.error import HTTPError
from six.moves.urllib.error import URLError
from six.moves.urllib.parse import quote, urlencode, urljoin, urlparse
from six.moves.urllib.request import Request, urlopen
import httplib
from optparse import OptionParser


class Stats():
    """Provide functions for analyzing jobs, branches, and tests."""

    def __init__(self):
        self.username = None ;
        self.passwd = None;
        self.jhost='http://ci.voltdb.lan'
        self._server = None

    def getOpts(self) :
        parser = OptionParser()
        parser.add_option('-u','--username', dest='username', default=None);
        parser.add_option('-p','--passwd', dest='passwd', default=None);
        return parser.parse_args();

    def runCommand(self, command, branch=None, job=None, build=None, build_range=None):
        cmdHelp = """usage: stats [options] help|jobs|history|report|test-history
        options:
        -u <username> defaults to local user
        -p <passwd>
        commands:
        help - display help
        jobs <branch> - displays jobs running on the branch
        history <branch> <job> - displays history of job on the branch
        report <branch> <job> [build] - displays test report using build number. defaults to last completed build
        test-history <branch> <job> [range] - displays test history ranging for range of builds
        """
        #print("command: "+command);
        if command == 'help':
            print(cmdHelp)
        elif command == 'jobs':
            self.jobs(branch)
        elif command == 'history':
            self.history(branch, job)
        elif command == 'report':
            self.report(branch, job, build)
        elif command == 'test-history':
            self.test_history(branch, job, build_range)
        else:
            print(cmdHelp);
            exit(0)

    def getServer(self):
        if self._server == None:
            self.connect()
        return self._server

    def connect(self):
        # check if we can log in with kerberos
        # sudo pip install kerberos
        #self._server = jenkins.Jenkins(self.jhost)

        if self._server == None:
            self._server = jenkins.Jenkins(self.jhost, username=self.username, password=self.passwd)

        # test it
        # print self._server.jobs_count()

    def jobs(self, branch):
        if branch == None:
            self.runCommand('help')
            return
        server = self.getServer()
        url = self.jhost + '/view/Branch-jobs/view/' + branch + '/api/python'
        branch_job = eval(urlopen(url).read())
        print(json.dumps(branch_job, indent=2))

if __name__ == '__main__':
    stats = Stats()
    stats.username = getpass.getuser()
    branch = None
    job = None
    build = None
    build_range = None
    command = 'help'

    (options,args) = stats.getOpts()
    if options.username != None:
        stats.username = options.username;

    if options.passwd != None:
       stats.passwd = options.passwd

    if len(args) > 0:
        command = args[0]

    if len(args) > 1:
        branch = args[1]

    if len(args) > 2:
        job = args[2]

    if len(args) > 3:
        build = args[3]
        build_range = args[3]

    stats.runCommand(command, branch, job, build, build_range)
