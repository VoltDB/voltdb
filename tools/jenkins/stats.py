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
        history <branch> <job> - displays job history
        report <branch> [build] - displays test report using build number. defaults to last completed build
        """
        #print("command: "+command);
        if command == 'help':
            print(cmdHelp)
        elif command == 'history':
            self.history(branch, job)
        elif command == 'report':
            self.report(branch, build)
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

    def history(self, branch, job):
        if branch == None or job == None:
            self.runCommand('help')
            return
        server = self.getServer()
        url = self.jhost + '/view/Branch-jobs/view/' + branch + '/api/python'
        branch_job = eval(urlopen(url).read())
        jobs = branch_job['jobs']

        for j in jobs:
            if j['name'] == job:
                print j['url']
                job_history = eval(urlopen(j['url'] + 'api/python').read())
                print(json.dumps(job_history, indent=2))
                return
        print('Could not find job %s under branch %s' %(job, branch))

    def report(self, branch, build):
        if branch == None:
            self.runCommand('help')
            return
        if build == None:
            build = 'lastCompletedBuild'
        server = self.getServer()
        url = self.jhost + '/view/Branch-jobs/view/' + branch + '/api/python'
        branch_job = eval(urlopen(url).read())
        jobs = branch_job['jobs']
        all_jobs = {}

        for job in jobs:
            if job['color'] == 'red' or job['color'] == 'red_anime':
                all_jobs[job['name']] = 'fatal'
            elif job['color'] == 'yellow' or job['color'] == 'yellow_anime':
                all_jobs[job['name']] = 'unstable'
            elif job['color'] != 'disabled':
                all_jobs[job['name']] = 'good'

        for job in all_jobs:
            url = self.jhost + '/view/Branch-jobs/view/' + branch + '/job/' + job + '/' + \
                  build + '%s/api/python'

            if all_jobs[job] == 'fatal':
                filename = job.replace('.', '-') + '-fatal.txt'
            elif all_jobs[job] == 'unstable':
                filename = job.replace('.', '-') + '-unstable.txt'
            else:
                filename = job.replace('.', '-') + '-good.txt'

            report = {"report":"no info"}

            try:
                test_url = url % '/testReport'
                print test_url
                report = eval(urlopen(test_url).read())
            except (HTTPError, URLError) as e:
                print url
                report = eval(urlopen(url).read())
                print(e)

            with open(filename, 'w') as tempfile:
                tempfile.write(json.dumps(report, indent=2))

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
