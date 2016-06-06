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
        cmdHelp = """usage: stats [options] help|history|build-report|build-history
        options:
        -u <username> defaults to local user
        -p <passwd>
        commands:
        help - display help
        history <branch> <job> - displays job history
        build-report <branch> [build] - displays a report using build number. defaults to last completed build
        build-history <branch> <job> <range> - analyze job history for a range of builds, ex: 500-512
        """
        #print("command: "+command);
        if command == 'help':
            print(cmdHelp)
        elif command == 'history':
            self.history(branch, job)
        elif command == 'build-report':
            self.build_report(branch, build)
        elif command == 'build-history':
            self.build_history(branch, job, build_range)
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

    def build_report(self, branch, build):
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

            # '/testReport' only works for some jobs depending on plugins installed on Jenkins
            try:
                test_url = url % '/testReport'
                print test_url
                report = eval(urlopen(test_url).read())
            except (HTTPError, URLError) as e:
                try:
                    test_url = url % ''
                    print test_url
                    report = eval(urlopen(test_url).read())
                except (HTTPError, URLError) as e:
                    print(e)
                    print("Could not retrieve report")

            with open(filename, 'w') as outfile:
                outfile.write(json.dumps(report, indent=2))

    def build_history(self, branch, job, build_range):
        if branch == None or job == None or build_range == None:
            self.runCommand('help')
            return
        else:
            try:
                builds = build_range.split('-')
                build_low = int(builds[0])
                build_high = int(builds[1])
                if build_high < build_low: raise Exception('left number must be lesser than or equal to right')
            except:
                print sys.exc_info()[0]
                self.runCommand('help')
                return

        server = self.getServer()
        test_map = {}

        for build in range(build_low, build_high+1):
            url = self.jhost + '/view/Branch-jobs/view/' + branch + '/job/' + job + '/' + str(build) + '%s/api/python'

            report = {"report":"no info"}

            # '/testReport' only works for some jobs depending on plugins installed
            try:
                test_url = url % '/testReport'
                report = eval(urlopen(test_url).read())
                childReports = report['childReports']
                for child in childReports:
                    suites = child['result']['suites']
                    for suite in suites:
                        cases = suite['cases']
                        for case in cases:
                            name = case['className'] + '.' + case['name']
                            if test_map.get(name, None) == None:
                                test_map[name] = []
                            test_map[name].append(case['status'])

                # for case in all_cases:
                #     name = test_case['className'] + '.' + test_case['name']
                #     if test_map.get(name, None) == None:
                #         test_map[name] = []
                #     test_map[name].append(test_case['status'])

                # for cases in all_cases:
                #     print json.dumps(cases, indent=2)
                #     return
                #     for test_case in cases:
                #         print json.dumps(test_case, indent=2)
                #         return
                #         name = test_case['className'] + '.' + test_case['name']
                #         if test_map.get(name, None) == None:
                #             test_map[name] = []
                #         test_map[name].append(test_case['status'])

            # doesn't have '/testReport' so try to get normal report
            except (HTTPError, URLError) as e:
                try:
                    test_url = url % ''
                    report = eval(urlopen(test_url).read())
                except (HTTPError, URLError) as e:
                    print(e)
                    print("Could not retrieve report")
            except AttributeError as e:
                print('Error retriving test data')
                print(e)

        for test in test_map:
            failure_tally = 0
            regression_tally = 0
            passed_tally = 0
            other_tally = 0
            for status in test_map[test]:
                if status == 'FAILURE' or status == 'FAILED':
                    failure_tally += 1
                elif status == 'REGRESSION':
                    regression_tally += 1
                elif status == 'PASSED':
                    passed_tally += 1
                else:
                    print(status)
                    other_tally += 1
            with open('temp.txt', 'a') as outfile:
                outfile.write(json.dumps(
                {
                    'build range': build_range,
                    'test': test,
                    'failure tally': failure_tally,
                    'regression tally': regression_tally,
                    'passed tally': passed_tally,
                    'other tally': other_tally
                }, indent=2
                ))

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
        build = args[2]

    if len(args) > 3:
        build_range = args[3]

    stats.runCommand(command, branch, job, build, build_range)
