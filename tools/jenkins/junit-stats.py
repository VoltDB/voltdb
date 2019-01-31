#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2019 VoltDB Inc.

# A command line tool for getting junit job statistics from Jenkins CI

import logging
import os
import sys

import mysql.connector

from datetime import datetime, timedelta
from jenkinsbot import JenkinsBot
from mysql.connector.errors import Error as MySQLError
from numpy import std, mean
from re import sub
from urllib2 import HTTPError, URLError, urlopen

# Slack channel to notify if and when issue is reported
JUNIT = os.environ.get('junit', None)

# set to True if you need to suppress updating the database or JIRA
DRY_RUN = False

# set threshold (greater than or equal to) of failures in a row to be significant
FAIL_THRESHOLD = 2

from string import maketrans
TT = maketrans("[]-<>", "_____")

QUERY1 = """
    SELECT count(*) AS fails
    FROM `junit-test-failures` m
    WHERE m.job = %(job)s
        AND m.name = %(name)s
        AND m.status in ('FAILED', 'REGRESSION')
        AND m.stamp > %(stamp)s - INTERVAL 30 DAY
        AND m.build <= %(build)s
"""

QUERY2 = """
    SELECT count(*) AS fixes
    FROM `junit-test-failures` m
    WHERE m.job = %(job)s
        AND m.name = %(name)s
        AND m.status in ('FIXED')
        AND m.stamp > %(stamp)s - INTERVAL 30 DAY
        AND m.build <= %(build)s
    HAVING fixes > 0
    LIMIT 1
"""

QUERY3 = """
    SELECT count(*) as runs
    FROM `junit-builds` m
    WHERE m.name = %(job)s
        AND m.stamp > %(stamp)s - INTERVAL 30 DAY
        AND m.stamp <= %(stamp)s
"""

QUERY4 = """
    SELECT job, build, name, ord-1-COALESCE(pre, 0) AS runs, current
    FROM
        (SELECT job, build, name, status, ord, stamp,
                LAG(ord) OVER w2 AS pre,
                LEAD(ord) OVER w2 AS post,
                (SELECT last-MAX(ord)
                FROM
                    (SELECT job, name, status, stamp,
                            ROW_NUMBER() OVER w1 AS ord,
                            (SELECT count(*)
                            FROM `junit-test-failures` n
                            WHERE n.job=%(job)s
                                AND n.name=%(name)s
                                AND n.stamp > %(stamp)s - INTERVAL 30 DAY
                                AND n.build <= %(build)s
                            ) last
                    FROM `junit-test-failures` n
                    WHERE n.job=%(job)s
                        AND n.name=%(name)s
                        AND n.stamp > %(stamp)s - INTERVAL 30 DAY
                        AND n.build <= %(build)s
                    WINDOW w1 AS (ORDER BY build)
                    ) q1
                WHERE q1.status in ('FIXED')
                LIMIT 1
                ) current
        FROM
            (SELECT job, build, name, status, stamp,
                    ROW_NUMBER() OVER w1 AS ord
            FROM `junit-test-failures` n
            WHERE n.job=%(job)s
                AND n.name=%(name)s
                AND n.stamp > %(stamp)s - INTERVAL 30 DAY
                AND n.build <= %(build)s
            WINDOW w1 AS (ORDER BY build)
            ) q2
        WHERE q2.status in ('FIXED')
        WINDOW w2 AS (ORDER BY ord)
        ) q3;
"""

class Stats(object):
    def __init__(self):
        self.jhost = 'http://ci.voltdb.lan'
        self.dbhost = 'junitstatsdb.voltdb.lan'
        self.dbuser = os.environ.get('dbuser', None)
        self.dbpass = os.environ.get('dbpass', None)
        self.dbname = os.environ.get('dbname', 'qa')
        self.cmdhelp = """
        usage: junit-stats <job> <build_range>
        ex: junit-stats branch-2-pro-junit-master 800-990
        ex: junit-stats branch-2-community-junit-master 550-550
        You can also specify 'job' and 'build_range' environment variables
        """
        log_format = '%(asctime)s %(module)14s:%(lineno)-6d %(levelname)-8s [%(threadName)-10s] %(message)s'
        # logging.basicConfig(stream=sys.stdout, level=logging.INFO)
        # file = logging.FileHandler("junit-stats.log", mode='w')
        # file.setLevel(logging.INFO)
        # formatter = logging.Formatter(log_format)
        # file.setFormatter(formatter)
        # logging.getLogger('').handlers = []
        # logging.getLogger('').addHandler(file)
        loglevel = logging.INFO
        console_loglevel = loglevel
        logfile = "junit-stats.log"
        logger = logging.getLogger()
        logger.setLevel(logging.NOTSET)
        logger.propogate = True
        file = logging.FileHandler(logfile, mode='a')
        console = logging.StreamHandler()
        file.setLevel(loglevel)
        console.setLevel(console_loglevel)
        formatter = logging.Formatter(log_format)
        file.setFormatter(formatter)
        console.setFormatter(formatter)
        logging.getLogger('').handlers = []
        logging.getLogger('').addHandler(file)
        logging.getLogger('').addHandler(console)
        logging.info("starting... %s" % sys.argv)

    def read_url(self, url, ignore404=False):
        """
        :param url: url to download data from
        :return: Dictionary representation of json object
        """

        data = None
        try:
            data = eval(urlopen(url).read())
        except (HTTPError, URLError) as e:
            if not (e.code == 404 and ignore404):
                logging.exception('Could not open data from url: %s. The url may not be formed correctly.' % url)
        return data

    def file_jira_issue(self, issue, DRY_RUN=False):
        jenkinsbot = JenkinsBot()
        error_url = issue['url']
        error_report = self.read_url(error_url + '/api/python')
        if error_report is None:
            return

        # throw a link to the query for the test history into the ticket to aid in checking/debugging the filer
        note = 'http://junitstatsdb.voltdb.lan/adminer.php?server=junitstatsdb.voltdb.lan&username=qaquery&db=qa&sql=select+%2A+from+%60junit-test-failures%60+WHERE+name+%3D+%27'+issue['name']+'%27+and+job+%3D+%27'+job+'%27order+by+build+desc'
        # and a link to the test history on jenkins by last completed build
        history = sub('/\d+/testReport/', '/lastCompletedBuild/testReport/', error_url) + '/history/'

        failed_since = error_report['failedSince']
        failure_percent = issue['failurePercent']
        summary = issue['name'] + ' is failing ~' + failure_percent + '% of the time on ' + job + ' (' + issue['type'] + ')'
        description = error_url + '\n\n-----------------\-stack trace\----------------------------\n\n' \
                      + str(error_report['errorStackTrace']) \
                      + '\n\n----------------------------------------------\n\n' \
                      + 'Failing since build ' + str(failed_since) + '\n' \
                      + '[query history|' + note + ']\n' \
                      + '!' + job + 'CountGraph.png!' \
                      + '\nNOTE: this graph is from when this ticket was filed, click [here|' + history + '] for an updated graph\n'

        current_version = str(self.read_url('https://raw.githubusercontent.com/VoltDB/voltdb/'
                                    'master/version.txt'))
        new_issue_url = None
        attachments = {
            # filename : location
            job + 'CountGraph.png' : error_url + '/history/countGraph/png?start=0&amp;end=25'
        }

        try:
            new_issue = jenkinsbot.create_bug_issue(JUNIT, summary, description, 'Core', current_version,
                                                    ['junit-consistent-failure', 'automatic'],
                                                    attachments,
                                                    DRY_RUN=DRY_RUN)

            if new_issue:
                new_issue_url = "https://issues.voltdb.com/browse/" + new_issue.key

        except:
            logging.exception('Error with creating issue')
            new_issue_url = None

        return new_issue_url

    def get_build_data(self, job, build_range, file_jira_ticket=True):
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

        # we only process ticket filing for master jobs.
        from re import match, IGNORECASE
        if not(match(r'test-nextrelease-', job, IGNORECASE) \
               or match(r'branch-.*-master', job, IGNORECASE)):
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

        if latest_build == 'unknown':
            raise Exception("Error: could not determine latest build in jenkins, job name in error?")

        try:
            db = mysql.connector.connect(host=self.dbhost, user=self.dbuser, password=self.dbpass, database='qa')
            cursor = db.cursor()
        except MySQLError:
            logging.exception('Could not connect to qa database. User: %s. Pass: %s' % (self.dbuser, self.dbpass))
            return

        query_last_build = ("select max(build) FROM `junit-builds` where name='%s'" % job)
        cursor.execute(query_last_build)
        last_build_recorded = cursor.fetchone()[0]

        try:
            builds = build_range.split('-')
            build_low = last_build_recorded + 1
            if len(builds) > 1 and len(builds[1]) > 0:
                build_high = int(builds[1])
                build_low = min(int(builds[0]), last_build_recorded + 1)
                if build_high < build_low:
                    raise Exception('Error: Left number must be lesser than or equal to right number.')
            else:
                build_low = last_build_recorded + 1
                build_high = latest_build

        except:
            logging.exception('Couldn\'t extrapolate build range.')
            print(self.cmdhelp)
            return

        logging.info("effective parms for: %s %s-%s" % (job, build_low, build_high))

        for build in range(build_low, build_high + 1):
            build_url = self.jhost + '/job/' + job + '/' + str(build) + '/api/python'
            build_report = self.read_url(build_url)
            if build_report is not None:
                host = build_report.get('builtOn', 'unknown').split('-')[0]  # split for hosts which are voltxx-controller-...

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

                delete_job = ('DELETE FROM `junit-builds` where name=%(name)s and build=%(build)s')
                delete_target = (
                    'DELETE FROM `junit-build-targets` WHERE name=%(name)s AND build=%(build)s')
                delete_test = (
                    'DELETE FROM `junit-test-failures` where job=%(name)s AND build=%(build)s')

                add_job = ('INSERT INTO `junit-builds` '
                           '(name, stamp, url, build, fails, total, percent) '
                           'VALUES (%(name)s, %(timestamp)s, %(url)s, %(build)s, %(fails)s, %(total)s, %(percent)s)')

                logging.debug(add_job % job_data)

                if not DRY_RUN:
                    try:
                        cursor.execute(delete_job, job_data)
                    except MySQLError as e:
                        logging.exception('Could not delete job data to database')
                    try:
                        cursor.execute(delete_target, job_data)
                        db.commit()
                    except MySQLError as e:
                        logging.exception('Could not delete target data to database')
                    try:
                        cursor.execute(delete_test, job_data)
                        db.commit()
                    except MySQLError as e:
                        logging.exception('Could not delete test data to database')

                if not DRY_RUN:
                    try:
                        cursor.execute(add_job, job_data)
                        db.commit()
                    except MySQLError as e:
                        if e.errno != 1062:
                            logging.exception('Could not add job data to database')

                # in a multiconfiguration job we have multiple job_reports under 'runs', otherwise, just the
                # single job report we already retrieved.
                for run in job_report.get('runs', [job_report]):

                    # very strange but sometimes there a run from a different build?
                    if run['number'] != build:
                        continue

                    if build != int(run['url'].split('/')[-2]):
                        raise RuntimeError("build # not correct %s %s" % (build, run['url']))

                    tr_url = run['url'] + "testReport/api/python"
                    child = self.read_url(tr_url)

                    if child is None:
                        child = {'duration': None,
                                 'empty': True,
                                 'failCount': None,
                                 'passCount': None,
                                 'skipCount': None
                                 }

                    # Compile job data to write to database
                    target_data = {
                        'name': job,
                        'build': build,
                        'target': run['url'].split('/')[-3],
                        'duration': child['duration'],
                        'empty': child['empty'],
                        'failcount': child['failCount'],
                        'passcount': child['passCount'],
                        'skipcount': child['skipCount'],
                    }

                    add_target = ('INSERT INTO `junit-build-targets` '
                                  '(name, build, target, duration, `empty`, failcount, passcount, skipcount) '
                                  'VALUES (%(name)s, %(build)s, %(target)s, SEC_TO_TIME(%(duration)s), %(empty)s, %(failcount)s, %(passcount)s, %(skipcount)s)')

                    logging.debug(add_target % target_data)

                    if not DRY_RUN:
                        try:
                            cursor.execute(add_target, target_data)
                            db.commit()
                        except MySQLError as e:
                            if e.errno != 1062:  # duplicate entry
                                logging.exception('Could not add target data to database')

                    # if there is no test report, for some reason, maybe the build timed out, just move on
                    if child['empty'] is True:
                        logging.warning("No test report found for url: %s" % tr_url)
                        continue

                    # Traverse through reports into test suites and get failed test case data to write to database.
                    # for child in child_reports:
                    suites = child['suites']

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
                            #logging.debug(name)

                            assert status in ['PASSED', 'FAILED', 'FIXED', 'REGRESSION'], status

                            # we need to record 'FIXED' and 'FAILED' and 'REGRESSION'
                            if status == 'PASSED':
                                continue

                            name = name.translate(TT)
                            if "_init_" in name or "-vdm-" in run['url']:
                                testcase_url = run['url'] + 'testReport/(root)/' + name
                            else:
                                testcase_url = run['url'] + 'testReport/' + name
                            n = testcase_url.count('.')
                            # find the testcase url that "works"
                            for i in range(n+1):
                                try:
                                    page = self.read_url(testcase_url+"/api/python", ignore404=True)
                                except:
                                    pass
                                if type(page) == dict and 'status' in page:
                                    break
                                testcase_url = '/'.join(testcase_url.rsplit('.', 1))
                            if page is None:
                                logging.error("testcase url invalid: " + testcase_url)
                                ####raise RuntimeError("testcase url invalid: " + testcase_url)

                            logging.debug("working on: %s %s %s %s %s" % (job,build,name,status,url))

                            test_data = {
                                'name': name,
                                'job': job,
                                'status': status,
                                'timestamp': test_stamp,
                                'url': testcase_url,
                                'build': build,
                                'host': host,
                                'new_issue_url': None   # unused
                            }

                            # record test results to database (with issue url)
                            # if an issue was filed, its url will be recorded in the database
                            add_test = ('INSERT INTO `junit-test-failures` '
                                        '(name, job, status, stamp, url, build, host, issue_url) '
                                        'VALUES (%(name)s, %(job)s, %(status)s, %(timestamp)s, '
                                        '%(url)s, %(build)s, %(host)s, %(new_issue_url)s)')

                            logging.debug("%s" % (add_test % test_data))

                            if not DRY_RUN:
                                try:
                                    cursor.execute(add_test, test_data)
                                    db.commit()
                                except MySQLError as e:
                                    if e.errno != 1062:
                                        logging.exception('Could not add test data to database')

                            # we do not need to file a jira ticket if test is currently fixed
                            if status == 'FIXED':
                                continue

                            params1 = {
                                'name': test_data['name'],
                                'stamp': test_data['timestamp'],
                                'job': test_data['job'],
                                'build': test_data['build']
                            }

                            # query to get number of failures in a row
                            logging.debug("Q1 %s" % (QUERY1 % params1))

                            cursor.execute(QUERY1, params1)
                            numFails = float(cursor.fetchone()[0])

                            if (numFails >= FAIL_THRESHOLD):
                                # query to see if job was fixed in the past 30 days
                                logging.debug("Q2 %s" % (QUERY2 % params1))

                                cursor.execute(QUERY2, params1)
                                everFixed = cursor.fetchone()

                                # query to count number of builds of certain job
                                logging.debug("Q3 %s" % (QUERY3 % params1))

                                cursor.execute(QUERY3, params1)
                                runs = float(cursor.fetchone()[0])
                                test_data['failurePercent'] = "%.2f" % (numFails / runs * 100)

                                if not everFixed:
                                    # if first time failure sequence in past 30 days, file ticket
                                    logging.info("will file: %s %s %s %s" % (job, build, name, testcase_url))
                                    test_data['type'] = "INTERMITTENT"
                                    try:
                                        test_data['new_issue_url'] = self.file_jira_issue(test_data, DRY_RUN=(not file_jira_ticket))
                                    except:
                                        logging.exception("failed to file a jira ticket")
                                else:
                                    # computes failure sequences over the past 30 days
                                    logging.debug("Q4 %s" % (QUERY4 % params1))

                                    cursor.execute(QUERY4, params1)
                                    results = cursor.fetchall()
                                    values = [int(v[3]) for v in results]
                                    current = results[0][4]

                                    # if current failure sequence exceeds 2SD from mean, file ticket
                                    if (current > mean(values) + 2*std(values)):
                                        logging.info("will file: %s %s %s %s" % (job, build, name, testcase_url))
                                        test_data['type'] = "CONSISTENT"
                                        try:
                                            test_data['new_issue_url'] = self.file_jira_issue(test_data, DRY_RUN=(not file_jira_ticket))
                                        except:
                                            logging.exception("failed to file a jira ticket")

            except KeyError:
                logging.exception('Error retrieving test data for this particular build: %d\n' % build)
            except Exception:
                # Catch all errors to avoid causing a failing build for the upstream job in case this is being
                # called from the junit-test-branch on Jenkins
                logging.exception('Catching unexpected errors to avoid causing a failing build for the upstream job')
                # re-raise the exception, cause the build to fail (until the bugs are worked out)
                raise

        cursor.close()
        db.close()
        logging.info("done")


import unittest
class Tests(unittest.TestCase):

    def create_db(self):
        self.cursor.execute("DROP DATABASE IF EXISTS junitstatstests;")
        self.cursor.execute("CREATE DATABASE junitstatstests;")
        self.db.database = "junitstatstests"
        self.db.autocommit = True

    def drop_db(self):
        self.cursor.execute("DROP DATABASE junitstatstests;")

    def create_table(self):
        self.cursor.execute("""CREATE TABLE `junit-test-failures` (
                                                `job`         varchar(64) NOT NULL,
                                                `name`        varchar(256) NOT NULL,
                                                `build`       int(2) NOT NULL,
                                                `status`      varchar(16) NOT NULL,
                                                `stamp`       timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                                `url`         varchar(256) NOT NULL,
                                                `host`        varchar(64) NOT NULL,
                                                `issue_url`   varchar(1024) DEFAULT NULL,
                                                              KEY `build` (`build`),
                                                              KEY `status` (`status`),
                                                              KEY `name` (`name`),
                                                              KEY `job` (`job`)
                            );""")

    def set_env(self):
        self.dbuser = os.environ.get('dbuser', None)
        self.dbpass = os.environ.get('dbpass', None)
        self.dbname = os.environ.get('dbname', "junitstatstests")
        self.dbhost = 'junitstatsdb.voltdb.lan'

    def open_db(self):
        self.set_env()
        self.db = mysql.connector.connect(host=self.dbhost, user=self.dbuser, password=self.dbpass)
        self.cursor = self.db.cursor()

    def close_db(self):
        self.cursor.close()
        self.db.close()
        pass

    def load_table(self, sql, data):
        self.cursor.execute("TRUNCATE TABLE `junit-test-failures`;")
        self.cursor.execute(sql, data)

    def setUp(self):
        self.open_db()
        self.create_db()
        self.create_table()

    def tearDown(self):
        self.drop_db()
        self.close_db()

    def test_1(self):
        add_test = ('INSERT INTO `junit-test-failures` '
                    '(name, job, status, url, build, host, issue_url, stamp) '
                    'VALUES (%(name)s, %(job)s, %(status)s, '
                    '%(url)s, %(build)s, %(host)s, %(new_issue_url)s, %(stamp)s)')
        data = [
         dict(job='test-nextrelease-1', name='class-1', build=1, status='FAILED', url='url-1', host='host-1', new_issue_url=None, stamp='2018-05-28 10:10:10'),
         dict(job='test-nextrelease-1', name='class-1', build=2, status='FAILED', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-06 10:10:10'),
         dict(job='test-nextrelease-1', name='class-1', build=3, status='FIXED', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-06 10:10:10'),
         dict(job='test-nextrelease-1', name='class-1', build=4, status='REGRESSION', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-06 10:10:10'),
         dict(job='test-nextrelease-1', name='class-1', build=5, status='FIXED', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-06 10:10:10'),
         dict(job='test-nextrelease-1', name='class-1', build=6, status='REGRESSION', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-06 10:10:10'),
         dict(job='test-nextrelease-1', name='class-1', build=7, status='FAILED', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-06 10:10:10'),
         dict(job='test-nextrelease-1', name='class-1', build=8, status='FAILED', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-06 10:10:10'),
         dict(job='test-nextrelease-1', name='class-1', build=9, status='FAILED', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-06 10:10:10'),
         dict(job='test-nextrelease-1', name='class-1', build=10, status='FAILED', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-26 10:10:10'),

         dict(job='test-nextrelease-2', name='class-1', build=1, status='FAILED', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-04 10:10:10'),
         dict(job='test-nextrelease-2', name='class-1', build=2, status='FAILED', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-05 10:10:10'),
         dict(job='test-nextrelease-2', name='class-1', build=3, status='FAILED', url='url-1', host='host-1', new_issue_url=None, stamp='2018-06-06 10:10:10'),
        ]

        for d in data:
            self.cursor.execute(add_test, d)

        param = {
            'name': 'class-1',
            'stamp': '2018-06-26 10:10:10',
            'job': 'test-nextrelease-1',
            'build': 9
        }

        self.cursor.execute(QUERY1, param)
        numFails = self.cursor.fetchone()[0]

        self.cursor.execute(QUERY2, param)
        everFixed = self.cursor.fetchone()

        if (numFails >= FAIL_THRESHOLD):
            if not everFixed:
                print("FILE TICKET")
                return
            else:
                self.cursor.execute(QUERY4, param)
                results = self.cursor.fetchall()
                values = [int(v[3]) for v in results]
                current = results[0][4]
                if (current > mean(values) + 2*std(values)):
                    print("FILE TICKET")
                    return

        print("DO NOT FILE")

if __name__ == '__main__':

    if sys.argv[1] == 'unittest':
        unittest.main(argv=['', 'Tests.test_1'], failfast=False)
        #unittest.main(failfast=False)
        sys.exit()

    stats = Stats()
    job = os.environ.get('job', None)
    build_range = os.environ.get('build_range', None)

    args = sys.argv

    if len(args) > 1:
        job = args[1]

    if len(args) > 2:
        build_range = args[2]

    stats.get_build_data(job, build_range, file_jira_ticket=not DRY_RUN)
