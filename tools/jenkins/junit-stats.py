#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.

# A command line tool for getting junit job statistics from CI
# usage:  junit-stats help

import re
import json
import sys
import time
import getpass
import mysql.connector
from six.moves.urllib.error import HTTPError
from six.moves.urllib.error import URLError
from six.moves.urllib.request import urlopen
import httplib
from optparse import OptionParser


class Stats():
    """Provide functions for analyzing jobs, branches, and tests."""

    def __init__(self):
        self.username = None
        self.passwd = None
        self.jhost='http://ci.voltdb.lan'
        self.dbhost='volt2.voltdb.lan'
        self._server = None

    def getOpts(self):
        parser = OptionParser()
        parser.add_option('-u','--username', dest='username', default=None)
        parser.add_option('-p','--passwd', dest='passwd', default=None)
        return parser.parse_args()

    # TODO: print what filenames data are saved to
    def runCommand(self, command, branch=None, job=None, build=None, build_range=None):
        cmdHelp = """usage: stats [options] help|job-history|build-report|build-history
        options:
        -u <username> defaults to local user
        -p <passwd>
        commands:
        help - display help
        job-detail <branch> <job> - displays job details
        last-build-report <branch> - displays a report for last completed build
        build-history <branch> <job> <range> - analyze job history for a range of builds, ex: 500-512, 434-434
        """
        #print("command: "+command);
        if command == 'help':
            print(cmdHelp)
        elif command == 'job-detail':
            self.job_history(branch, job)
        elif command == 'last-build-report':
            self.last_build_report(branch)
        elif command == 'build-history':
            self.build_history(branch, job, build_range)
        else:
            print(cmdHelp);
            exit(0)

    def read_url(self, url):
        """
        Open a url and evaluate it. Return it as (hopefully) as JSON.
        """
        data = None
        try:
            data = eval(urlopen(url).read())
        except (HTTPError, URLError) as e:
            print(e)
            print('Could not open data from url: %s. The url may not be formed correctly.' % url)
            print
        except IOError as e:
            print(e)
            print('Could not read data from url: %s. The data at the url may not be readable.' % url)
            print
        return data

    def job_detail(self, branch, job):
        """
        Display job detail which shows if the job is failing or not, and the target of the failure.
        """
        if branch is None or job is None:
            self.runCommand('help')
            return

        url = self.jhost + '/view/Branch-jobs/view/' + branch + '/job/' + job + '/api/python'
        j = self.read_url(url)
        if j is None:
            return

        filename = 'job-history-' + branch + '-' + job + '%s.txt'
        if j['color'] != 'blue' and j['color'] != 'blue_anime':
            status = '-failed'
        else:
            status = '-passed'
        filename = filename % status

        job_detail = {}

        for conf in j['activeConfigurations']:
            if conf['color'] != 'blue' and conf['color'] != 'blue_anime':
                job_detail[conf['url']] = 'FAILED'
            else:
                job_detail[conf['url']] = 'PASSED'

        with open(filename, 'w') as outfile:
            outfile.write(j['url'] + '\n')
            outfile.write(json.dumps(job_detail, indent=2))

    # TODO: create a summary
    def last_build_report(self, branch):
        """
        Create a report of the last build (for each job) on a branch. Creates file for each job. Indicates whether or
        not the job has passed or failed. Contents of file includes name, # of failures, # of passes, # of tests, and
        failure rate.
        For a report of a job on a particular build, ex: 213, do `build-history <branch> <job> 213-213`
        """
        if branch is None:
            self.runCommand('help')
            return

        url = self.jhost + '/view/Branch-jobs/view/' + branch + '/api/python'
        branch_job = self.read_url(url)
        if branch_job is None:
            return

        jobs = branch_job['jobs']
        all_jobs = {}

        for job in jobs:
            if job['color'] != 'blue' and job['color'] != 'blue_anime' and job['color'] != 'disabled':
                all_jobs[job['name']] = 'failed'
            elif job['color'] != 'disabled':
                all_jobs[job['name']] = 'passed'

        for job in all_jobs:
            if all_jobs[job] == 'failed':
                filename = 'last-build-report-' + branch + '-' + job.replace('.', '-') + '-failed.txt'
            else:
                filename = 'last-build-report-' + branch + '-' + job.replace('.', '-') + '-passed.txt'

            url = self.jhost + '/view/Branch-jobs/view/' + branch + '/job/' + job + '/lastCompletedBuild/testReport/api/python'
            report = self.read_url(url)

            if report is None:
                continue
            failCount = report.get('failCount', 0)
            skipCount = report.get('skipCount', 0)
            passCount = report.get('passCount', 0)
            totalCount = report.get('totalCount', 0)
            if totalCount == 0:
                totalCount = failCount + skipCount + passCount
            report = {
                'job': job,
                'failures': failCount,
                'passes': totalCount - failCount,
                'tests': totalCount,
                'failure %': failCount/totalCount*100.0
            }

            with open(filename, 'w') as outfile:
                outfile.write(json.dumps(report, indent=2))

    def build_history(self, branch, job, build_range):
        """
        Displays build history for a job on a branch. Can specify an inclusive build range.
        For every build specified on the branch, prints the full test results of the
        """
        if branch is None or job is None or build_range is None:
            self.runCommand('help')
            return
        else:
            try:
                builds = build_range.split('-')
                build_low = int(builds[0])
                build_high = int(builds[1])
                if build_high < build_low:
                    raise Exception('Error: Left number must be lesser than or equal to right')
            except:
                print(sys.exc_info()[1])
                self.runCommand('help')
                return

        url = self.jhost + '/view/Branch-jobs/view/' + branch + '/job/' + job + '/lastCompletedBuild/api/python'
        build = self.read_url(url)
        if build is None:
            return

        test_map = {}
        latestBuild = build['number']
        host = build['builtOn']

        filename = 'build-history-' + branch + '-' + build_range + '.txt'

        db = mysql.connector.connect(host=self.dbhost, user='oolukoya', password='oolukoya', database='qa')
        cursor = db.cursor()

        for build in range(build_low, build_high+1):
            url = self.jhost + '/view/Branch-jobs/view/' + branch + '/job/' + job + '/' + str(build) + '/testReport/api/python'
            report = self.read_url(url)
            if report is None:
                print('Could not retrieve report because url is invalid. This may be because the build %d might not '
                'exist on Jenkins' % build)
                print('Latest build for this job is %d' % latestBuild)
                print
                continue
            try:
                childReports = report['childReports']
                for child in childReports:
                    suites = child['result']['suites']
                    # failCount = child['result']['failCount']
                    # passCount = child['result']['passCount']
                    target_url = child['child']['url'] + 'testReport'
                    for suite in suites:
                        cases = suite['cases']
                        timestamp = time.strptime(suite['timestamp'].replace('T', ' '), '%Y-%m-%d %H:%M:%S')
                        for case in cases:
                            name = case['className'] + '.' + case['name']
                            status = case['status']
                            test_data = {
                                'name': name,
                                'job': job,
                                'status': status,
                                'timestamp': timestamp,
                                'url': target_url,
                                'build': build,
                                'host': host
                            }

                            add_test = ('INSERT INTO `junit-results` '
                                        '(name, job, status, stamp, url, build, host) '
                                        'VALUES (%(name)s, %(job)s, %(status)s, %(timestamp)s, %(url)s, %(build)s, %(host)s)')

                            cursor.execute(add_test, test_data)
                            db.commit()

                            if test_map.get(name, None) is None:
                                test_map[name] = []
                            test_map[name].append(test_data)
            except KeyError as e:
                print(e)
                print('Error retriving test data for this particular build: %d' % build)
                print

        cursor.close()
        db.close()

        for test in test_map:
            test_summary = {
                '_failures': 0,
                '_passes': 0,
                '_name': test,
                'data': []
            }
            for test_data in test_map[test]:
                if test_data['status'] == 'PASSED':
                    test_summary['_passes'] += 1
                else:
                    test_summary['_failures'] += 1
                test_summary['data'].append(test_data)

            with open(filename, 'a') as outfile:
                outfile.write(json.dumps(test_summary, indent=2, sort_keys=True))


if __name__ == '__main__':
    stats = Stats()
    stats.username = getpass.getuser()
    branch = None
    job = None
    build = None
    build_range = None
    command = 'help'

    (options,args) = stats.getOpts()
    stats.username = options.username;
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
