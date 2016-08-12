#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.

# A command line tool for getting junit job statistics from Jenkins CI

import logging
import os
import sys

import mysql.connector

from datetime import datetime, timedelta
from jenkinsbot import JenkinsBot
from mysql.connector.errors import Error as MySQLError
from urllib2 import HTTPError, URLError, urlopen

# Get job names from environment variables
COMMUNITY = os.environ.get('community', None)
PRO = os.environ.get('pro', None)

# Number of failures in a row for a test needed to trigger a new Jira issue
TOLERANCE = 3

# Slack channel to notify if and when issue is reported
JUNIT = os.environ.get('junit', None)


class Stats(object):
    def __init__(self):
        self.jhost = 'http://ci.voltdb.lan'
        self.dbhost = 'volt2.voltdb.lan'
        self.dbuser = os.environ.get('dbuser', None)
        self.dbpass = os.environ.get('dbpass', None)
        self.cmdhelp = """
        usage: junit-stats <job> <build_range>
        ex: junit-stats branch-2-pro-junit-master 800-990
        ex: junit-stats branch-2-community-junit-master 550-550
        You can also specify 'job' and 'build_range' environment variables
        """
        logging.basicConfig(stream=sys.stdout)

    def read_url(self, url):
        """
        :param url: url to download data from
        :return: Dictionary representation of json object
        """

        data = None
        try:
            data = eval(urlopen(url).read())
        except (HTTPError, URLError):
            logging.exception('Could not open data from url: %s. The url may not be formed correctly.' % url)
        except IOError:
            logging.exception('Could not read data from url: %s. The data at the url may not be readable.' % url)
        except:
            logging.exception('Something unexpected went wrong.')
        return data

    def get_build_data(self, job, build_range):
        """
        Gets build data for a job. Can specify an inclusive build range.
        For every build specified on the job, saves the test results
        :param job: Full job name on Jenkins
        :param build_range: Build range that exists for the job on Jenkins, ie "700-713", "1804-1804"
        """

        if job is None or build_range is None:
            print('Either a job or build range was not specified.')
            print(self.cmdhelp)
            return

        try:
            builds = build_range.split('-')
            build_low = int(builds[0])
            build_high = int(builds[1])
            if build_high < build_low:
                raise Exception('Error: Left number must be lesser than or equal to right number.')
        except:
            logging.exception('Couldn\'t extrapolate build range.')
            print(self.cmdhelp)
            return

        url = self.jhost + '/job/' + job + '/lastCompletedBuild/api/python'
        build = self.read_url(url)
        if build is None:
            logging.warn('Could not retrieve last completed build. Job: %s' % job)
            build = {
                'number': 'unknown',
                'builtOn': 'unknown'
            }

        latest_build = build['number']
        host = build['builtOn']

        issues = []

        try:
            db = mysql.connector.connect(host=self.dbhost, user=self.dbuser, password=self.dbpass, database='qa')
            cursor = db.cursor()
        except MySQLError:
            logging.exception('Could not connect to qa database. User: %s. Pass: %s' % (self.dbuser, self.dbpass))
            return

        for build in range(build_low, build_high + 1):
            build_url = self.jhost + '/job/' + job + '/' + str(build) + '/api/python'
            build_report = self.read_url(build_url)
            if build_report is not None:
                host = build_report.get('builtOn', 'unknown')

            test_url = self.jhost + '/job/' + job + '/' + str(build) + '/testReport/api/python'
            test_report = self.read_url(test_url)
            if test_report is None:
                logging.warn(
                    'Could not retrieve report because url is invalid. This may be because the build %d might not '
                    'exist on Jenkins' % build)
                logging.warn('Last completed build for this job is %s\n' % latest_build)
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
                    percent = fails * 100.0 / total

                # Get timestamp job ran on.
                # Appears to be same url as build_url
                job_url = self.jhost + '/job/' + job + '/' + str(build) + '/api/python'
                job_report = self.read_url(job_url)
                if job_report is None:
                    logging.warn(
                        'Could not retrieve report because url is invalid. This may be because the build %d might not '
                        'exist on Jenkins' % build)
                    logging.warn('Last completed build for this job is %s\n' % latest_build)
                    continue

                # Job stamp is already in EST
                job_stamp = datetime.fromtimestamp(job_report['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')

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
                except MySQLError:
                    logging.exception('Could not add job data to database')

                # Some of the test results are structured differently, depending on the matrix configurations.
                child_reports = test_report.get('childReports', None)
                if child_reports is None:
                    child_reports = [
                        {
                            'result': test_report,
                            'child': {
                                'url': test_url.replace('testReport/api/python', '')
                            }
                        }
                    ]

                # Traverse through reports into test suites and get failed test case data to write to database.
                for child in child_reports:
                    suites = child['result']['suites']
                    for suite in suites:
                        cases = suite['cases']
                        test_stamp = suite.get('timestamp', None)
                        if test_stamp is None or test_stamp == 'None':
                            test_stamp = job_stamp
                        else:
                            # Create datetime object from test_stamp string
                            test_stamp = datetime.strptime(test_stamp, '%Y-%m-%dT%H:%M:%S')
                            # Convert test stamp from GMT to EST and store as string
                            test_stamp = (test_stamp - timedelta(hours=4)).strftime('%Y-%m-%d %H:%M:%S')
                        for case in cases:
                            name = case['className'] + '.' + case['name']
                            status = case['status']
                            testcase_url = child['child']['url'] + 'testReport/' + name
                            testcase_url.replace('.test', '/test').replace('.Test', '/Test').replace('-', '_')
                            # Record tests that don't pass.
                            if status != 'PASSED':
                                test_data = {
                                    'name': name,
                                    'job': job,
                                    'status': status,
                                    'timestamp': test_stamp,
                                    'url': testcase_url,
                                    'build': build,
                                    'host': host
                                }

                                if status == 'FAILED':
                                    issues.append(test_data)

                                add_test = ('INSERT INTO `junit-test-failures` '
                                            '(name, job, status, stamp, url, build, host) '
                                            'VALUES (%(name)s, %(job)s, %(status)s, %(timestamp)s, '
                                            '%(url)s, %(build)s, %(host)s)')

                                try:
                                    cursor.execute(add_test, test_data)
                                    db.commit()
                                except MySQLError:
                                    logging.exception('Could not add test data to database')

            except KeyError:
                logging.exception('Error retrieving test data for this particular build: %d\n' % build)
            except Exception:
                # Catch all errors to avoid causing a failing build for the upstream job in case this is being
                # called from the junit-test-branch on Jenkins
                logging.exception('Catching unexpected errors to avoid causing a failing build for the upstream job')

        cursor.close()
        db.close()

        if job != PRO and job != COMMUNITY:
            return

        try:
            jenkinsbot = JenkinsBot()
            for issue in issues:
                # Only report pro and community job

                error_url = issue['url']
                error_report = self.read_url(error_url + '/api/python')

                if error_report is None:
                    continue

                age = error_report['age']
                yesterday = datetime.now() - timedelta(days=1)
                timestamp = issue['timestamp']
                old = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') < yesterday

                # Don't file ticket if age within tolerance and this failure happened over one day ago
                if age < TOLERANCE and old:
                    continue

                failed_since = error_report['failedSince']
                summary = issue['name'] + ' is failing since build ' + str(failed_since) + ' on ' + job
                description = error_url + '\n' + error_report['errorStackTrace']
                current_version = str(self.read_url('https://raw.githubusercontent.com/VoltDB/voltdb/'
                                                    'master/version.txt'))
                jenkinsbot.create_bug_issue(JUNIT, summary, description, 'Core', current_version,
                                            ['junit-consistent-failure', 'automatic'])
        except:
            logging.exception('Error with creating issue')


if __name__ == '__main__':
    stats = Stats()
    job = os.environ.get('job', None)
    build_range = os.environ.get('build_range', None)

    args = sys.argv

    if len(args) > 1:
        job = args[1]

    if len(args) > 2:
        build_range = args[2]

    stats.get_build_data(job, build_range)
