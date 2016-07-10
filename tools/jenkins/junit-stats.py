#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.

# A command line tool for getting junit job statistics from Jenkins CI

import os
import sys

from datetime import datetime, timedelta
from mysql.connector.errors import Error as MySQLError
from six.moves.urllib.error import HTTPError
from six.moves.urllib.error import URLError
from six.moves.urllib.request import urlopen

import mysql.connector


class Stats(object):
    def __init__(self):
        self.jhost = 'http://ci.voltdb.lan'
        self.dbhost = 'volt2.voltdb.lan'
        self.dbuser = os.environ.get('dbuser', None)
        self.dbpass = os.environ.get('dbpass', None)
        self.cmdhelp = """
        usage: junit-stats <job> <range>
        ex: junit-stats branch-2-pro-junit-master 800-802
        ex: junit-stats branch-2-community-junit-master 550-550
        """

    def read_url(self, url):
        """
        Download (hopefully) a json object from a url and evaluate it.
        """

        data = None
        try:
            data = eval(urlopen(url).read())
        except (HTTPError, URLError) as error:
            print(error)
            print('Could not open data from url: %s. The url may not be formed correctly.\n' % url)
        except IOError as error:
            print(error)
            print('Could not read data from url: %s. The data at the url may not be readable.\n' % url)
        except Exception as error:
            print('Something unexpected went wrong.\n')
            print(error)
        return data

    def build_history(self, job, build_range):
        """
        Displays build history for a job. Can specify an inclusive build range.
        For every build specified on the job, saves the test results
        """

        if job is None or build_range is None:
            print(self.cmdhelp)
            return

        try:
            builds = build_range.split('-')
            build_low = int(builds[0])
            build_high = int(builds[1])
            if build_high < build_low:
                raise Exception('Error: Left number must be lesser than or equal to right')
        except Exception as error:
            print(error)
            print(self.cmdhelp)
            return

        url = self.jhost + '/job/' + job + '/lastCompletedBuild/api/python'
        build = self.read_url(url)
        if build is None:
            print('Could not retrieve last completed build build. Job: %s' % job)
            return

        latestBuild = build['number']
        host = build['builtOn']

        try:
            db = mysql.connector.connect(host=self.dbhost, user=self.dbuser, password=self.dbpass, database='qa')
            cursor = db.cursor()
        except MySQLError as error:
            print('Could not connect to qa database. User: %s. Pass: %s' % (self.dbuser, self.dbpass))
            print(error)
            return

        for build in range(build_low, build_high + 1):
            test_url = self.jhost + '/job/' + job + '/' + str(build) + '/testReport/api/python'
            test_report = self.read_url(test_url)
            if test_report is None:
                print('Could not retrieve report because url is invalid. This may be because the build %d might not '
                      'exist on Jenkins' % build)
                print('Last completed build for this job is %d\n' % latestBuild)
                continue

            try:
                fails = test_report.get('failCount', 0)
                skips = test_report.get('skipCount', 0)
                passes = test_report.get('passCount', 0)
                total = test_report.get('totalCount', 0)
                if total == 0:
                    total = fails + skips + passes
                if total == 0:
                    percent = 0
                else:
                    percent = fails*100.0/total

                # Get timestamp job ran on.
                job_url = self.jhost + '/job/' + job + '/' + str(build) + '/api/python'
                job_report = self.read_url(job_url)
                if job_report is None:
                    print('Could not retrieve report because url is invalid. This may be because the build %d might not '
                          'exist on Jenkins' % build)
                    print('Last completed build for this job is %d\n' % latestBuild)
                    continue
                # Already in EST
                job_stamp = datetime.fromtimestamp(job_report['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')

                # Compile job data to write to database
                job_data = {
                    'name': job,
                    'timestamp': job_stamp,
                    'url': job_report['url'] + 'testReport',
                    'build': build,
                    'fails': fails,
                    'total': total,
                    'percent': percent
                }
                add_job = ('INSERT INTO `junit-builds` '
                           '(name, stamp, url, build, fails, total, percent) '
                           'VALUES (%(name)s, %(timestamp)s, %(url)s, %(build)s, %(fails)s, %(total)s, %(percent)s)')
                try:
                    cursor.execute(add_job, job_data)
                    db.commit()
                except MySQLError as error:
                    print(error)

                # Some of the test results are structured differently, depending on the matrix configurations.
                childReports = test_report.get('childReports', None)
                if childReports is None:
                    childReports = [
                        {
                            'result': test_report,
                            'child': {
                                'url': test_url.replace('testReport/api/python', '')
                            }
                        }
                    ]

                # Traverse through reports into test suites and get failed test case data to write to database.
                for child in childReports:
                    suites = child['result']['suites']
                    report_url = child['child']['url'] + 'testReport'
                    for suite in suites:
                        cases = suite['cases']
                        timestamp = suite.get('timestamp', None)
                        if timestamp is None or timestamp == 'None':
                            timestamp = job_stamp
                        else:
                            timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
                            # Convert from GMT to EST
                            timestamp = (timestamp - timedelta(hours=4)).strftime('%Y-%m-%d %H:%M:%S')
                        for case in cases:
                            name = case['className'] + '.' + case['name']
                            status = case['status']

                            # Record tests that don't pass.
                            if status != 'PASSED':
                                test_data = {
                                    'name': name,
                                    'job': job,
                                    'status': status,
                                    'timestamp': timestamp,
                                    'url': report_url,
                                    'build': build,
                                    'host': host
                                }
                                add_test = ('INSERT INTO `junit-test-failures` '
                                            '(name, job, status, stamp, url, build, host) '
                                            'VALUES (%(name)s, %(job)s, %(status)s, %(timestamp)s, %(url)s, %(build)s, %(host)s)')
                                try:
                                    cursor.execute(add_test, test_data)
                                    db.commit()
                                except MySQLError as error:
                                    print(error)

            except KeyError as error:
                print(error)
                print('Error retrieving test data for this particular build: %d\n' % build)
            except Exception as error:
                # Catch all errors to avoid causing a failing build for the upstream project
                print(error)

        cursor.close()
        db.close()

if __name__ == '__main__':
    stats = Stats()
    job = os.environ.get('job', None)
    build_range = os.environ.get('build_range', None)

    args = sys.argv

    if len(args) > 1:
        job = args[1]

    if len(args) > 2:
        build_range = args[2]

    stats.build_history(job, build_range)
