#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.

# A command line tool for getting junit job statistics from Jenkins CI

import mysql.connector
import sys
import os
import time

from datetime import datetime
from six.moves.urllib.error import HTTPError
from six.moves.urllib.error import URLError
from six.moves.urllib.request import urlopen


class Stats():
    def __init__(self):
        self.jhost='http://ci.voltdb.lan'
        self.dbhost='volt2.voltdb.lan'
        self.cmdHelp = """
        usage: junit-stats <job> <range>
        ex: junit-stats branch-2-pro-junit-master 800-802
        ex: junit-stats branch-2-community-junit-master 550-550
        """

    def read_url(self, url):
        """
        Open a url and evaluate it. Return it as (hopefully) as JSON.
        """

        data = None
        try:
            data = eval(urlopen(url).read())
        except (HTTPError, URLError) as e:
            print(e)
            print('Could not open data from url: %s. The url may not be formed correctly.\n' % url)
        except IOError as e:
            print(e)
            print('Could not read data from url: %s. The data at the url may not be readable.\n' % url)
        return data

    def build_history(self, job, build_range):
        """
        Displays build history for a job. Can specify an inclusive build range.
        For every build specified on the job, saves the test results
        """

        if job is None or build_range is None:
            print(self.cmdHelp)
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
                print(self.cmdHelp)
                return

        url = self.jhost + '/job/' + job + '/lastCompletedBuild/api/python'
        build = self.read_url(url)
        if build is None:
            return

        latestBuild = build['number']
        host = build['builtOn']

        db = mysql.connector.connect(host=self.dbhost, user='oolukoya', password='oolukoya', database='qa')
        cursor = db.cursor()

        for build in range(build_low, build_high+1):
            url = self.jhost + '/job/' + job + '/' + str(build) + '/testReport/api/python'
            report = self.read_url(url)
            if report is None:
                print('Could not retrieve report because url is invalid. This may be because the build %d might not '
                'exist on Jenkins' % build)
                print('Last completed build for this job is %d\n' % latestBuild)
                continue

            try:
                fails = report.get('failCount', 0)
                skips = report.get('skipCount', 0)
                passes = report.get('passCount', 0)
                total = report.get('totalCount', 0)
                if total == 0:
                    total = fails + skips + passes
                if total == 0:
                    percent = 0
                else:
                    percent = fails*100.0/total

                # Get timestamp job ran on.
                url = self.jhost + '/job/' + job + '/' + str(build) + '/api/python'
                report = self.read_url(url)
                if report is None:
                    print('Could not retrieve report because url is invalid. This may be because the build %d might not '
                    'exist on Jenkins' % build)
                    continue
                job_stamp = datetime.fromtimestamp(report['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')

                # Compile job data to write to database
                job_data = {
                    'name': job,
                    'stamp': job_stamp,
                    'url': report['url'] + 'testReport',
                    'build': build,
                    'fails': fails,
                    'total': total,
                    'percent': percent
                }
                add_job = ('INSERT INTO `junit-job-results` '
                            '(name, stamp, url, build, fails, total, percent) '
                            'VALUES (%(name)s, %(stamp)s, %(url)s, %(build)s, %(fails)s, %(total)s, %(percent)s)')
                cursor.execute(add_job, job_data)
                db.commit()

                # Some of the test results are structured differently, depending on the matrix configurations.
                childReports = report.get('childReports', None)
                if childReports is None:
                    childReports = [
                        {
                            'result': report,
                            'child': {
                                'url': url.replace('testReport/api/python','')
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
                            timestamp = time.strptime(timestamp.replace('T', ' '), '%Y-%m-%d %H:%M:%S')
                        for case in cases:
                            name = case['className'] + '.' + case['name']
                            status = case['status']
                            test_data = {
                                'name': name,
                                'job': job,
                                'status': status,
                                'timestamp': timestamp,
                                'url': report_url,
                                'build': build,
                                'host': host
                            }

                            if status != 'PASSED':
                                add_test = ('INSERT INTO `junit-test-failures` '
                                            '(name, job, status, stamp, url, build, host) '
                                            'VALUES (%(name)s, %(job)s, %(status)s, %(timestamp)s, %(url)s, %(build)s, %(host)s)')
                                cursor.execute(add_test, test_data)
                                db.commit()
            except KeyError as e:
                print(e)
                print('Error retriving test data for this particular build: %d\n' % build)
            except:
                print(sys.exc_info()[1])

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
