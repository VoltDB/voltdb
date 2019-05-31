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

# Constants used in posting messages on Slack
JUNIT = os.environ.get('junit', None)
AUTO_FILED = os.environ.get('auto-filed', None)
# For now, this should work (?); need a constant for the 'auto-filed' channel
SLACK_CHANNEL_FOR_AUTO_FILING = JUNIT

# set to True if you need to suppress updating the 'qa' database or JIRA
DRY_RUN = False

# Default is None (null); but once initialized, it may be reused
JENKINSBOT = None

# Constants used to determine which test failures are considered Consistent,
# Intermittent or New failures
NEW_FAILURE_WINDOW_SIZE             = 5
NEW_NUM_FAILURES_THRESHOLD          = 1  # 1 out of 5 failures is 'New' - if not seen recently
INTERMITTENT_FAILURE_WINDOW_SIZE    = 25
INTERMITTENT_NUM_FAILURES_THRESHOLD = 3  # 3 out of 25 failures is 'Intermittent'
CONSISTENT_FAILURE_WINDOW_SIZE      = 3
CONSISTENT_NUM_FAILURES_THRESHOLD   = 3  # 3 out of 3 (consecutive) failures is 'Consistent'

# Constants used to determine after how many passing it should be closed, or
# when its status should be changed to Old, Inconsistent or Intermittent.
# Note: "Inconsistent" means formerly deemed Consistent, but we're not yet
# certain whether it is fixed or actually Intermittent.
CHANGE_NEW_TO_OLD_WINDOW_SIZE            = 5
CHANGE_NEW_TO_OLD_NUM_FAILURES_THRESHOLD = 1  # if less than 1 in 5 failed, downgrade to 'Old'
CHANGE_INTERMITTENT_TO_OLD_WINDOW_SIZE            = 10
CHANGE_INTERMITTENT_TO_OLD_NUM_FAILURES_THRESHOLD = 1  # if less than 1 in 10 failed, downgrade to 'Old'
CHANGE_INTERMITTENT_TO_CONSISTENT_WINDOW_SIZE            = 5
CHANGE_INTERMITTENT_TO_CONSISTENT_NUM_FAILURES_THRESHOLD = 5  # if 5 out of 5 failed, upgrade to 'Consistent'
CHANGE_CONSISTENT_TO_INCONSISTENT_WINDOW_SIZE            = 1
CHANGE_CONSISTENT_TO_INCONSISTENT_NUM_FAILURES_THRESHOLD = 1  # if less than 1 in 1 failed, downgrade to 'Inconsistent'
CHANGE_INCONSISTENT_TO_INTERMITTENT_WINDOW_SIZE            = 1
CHANGE_INCONSISTENT_TO_INTERMITTENT_NUM_FAILURES_THRESHOLD = 1  # if 1 out of 1 failed, change to 'Intermittent'

# Constants used to determine after how many passing it should be closed
CLOSE_INCONSISTENT_WINDOW_SIZE            = 5
CLOSE_INCONSISTENT_NUM_FAILURES_THRESHOLD = 1  # if less than 1 in 5 failed, close the ticket
CLOSE_OLD_WINDOW_SIZE            = 10
CLOSE_OLD_NUM_FAILURES_THRESHOLD = 1  # if less than 1 in 10 failed, close the ticket

# Constants used in filing (or modifying) Jira tickets
JIRA_PRIORITY_FOR_CONSISTENT_FAILURES   = 'Critical'
JIRA_PRIORITY_FOR_INCONSISTENT_FAILURES = 'Major'
JIRA_PRIORITY_FOR_INTERMITTENT_FAILURES = 'Major'
JIRA_PRIORITY_FOR_NEW_FAILURES = 'Minor'
JIRA_PRIORITY_FOR_OLD_FAILURES = 'Trivial'
JIRA_LABEL_FOR_AUTO_FILING     = 'auto-filed'
JIRA_LABEL_FOR_CONSISTENT_FAILURES   = 'junit-consistent-failure'
JIRA_LABEL_FOR_INCONSISTENT_FAILURES = 'junit-intermittent-failure'
JIRA_LABEL_FOR_INTERMITTENT_FAILURES = 'junit-intermittent-failure'
JIRA_LABEL_FOR_NEW_FAILURES          = 'junit-intermittent-failure'
JIRA_LABEL_FOR_OLD_FAILURES          = 'junit-intermittent-failure'
MAX_NUM_ATTACHMENTS_PER_JIRA_TICKET  = 8

# Used in Jira ticket descriptions:
STACK_TRACE_LINE = '\n-------------------------\-Stack Trace\--------------------------\n\n'
SEPARATOR_LINE   = '\n-------------------------------------------------------------\n\n'
JENKINS_JOB_NICKNAMES = {
    'branch-2-community-junit-master'         : 'community-junit',
    'branch-2-pro-junit-master'               : 'pro-junit',
    'test-nextrelease-debug-pro'              : 'debug-pro',
    'test-nextrelease-memcheck-pro'           : 'memcheck-pro',
    'test-nextrelease-memcheck-nodebug-pro'   : 'memcheck-nodebug',
    'test-nextrelease-fulljmemcheck-pro-junit': 'fulljmemcheck',
    'test-nextrelease-nonflaky-pro-junit'     : 'nonflaky-pro',
    'test-nextrelease-pool-community-junit'   : 'pool-community',
    'test-nextrelease-pool-pro-junit'         : 'pool-pro',
    }

# Used for getting the preferred URL prefix; we prefer the latter to the former,
# because it works even over the VPN
BAD_URL_PREFIX  = 'ci:8080'
GOOD_URL_PREFIX = 'ci.voltdb.lan:8080'

# Used to count errors and warnings encountered during execution
ERROR_COUNT   = 0
WARNING_COUNT = 0

# Use to modify URLs by changing problematic characters into underscores
from string import maketrans
TT = maketrans("[]-<> ", "______")

# Print a log (info) message after every group of this many test cases are processed
# (in each "run" of a build, e.g. junit_other_p4 vs. junit_regression_h2)
LOG_MESSAGE_EVERY_NUM_TEST_CASES = 200

# TODO: possibly obsolete?? :
# set threshold (greater than or equal to) of failures in a row to be significant
FAIL_THRESHOLD = 2

# TODO: probably obsolete:
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


    def error(self, message, caused_by=None):
        """TODO
        :param 
        """
        global ERROR_COUNT
        ERROR_COUNT = ERROR_COUNT + 1
        if caused_by:
            message = message + '\nCaused by:\n' + str(caused_by)
        logging.error(message)


    def warn(self, message, caused_by=None):
        """TODO
        :param 
        """
        global WARNING_COUNT
        WARNING_COUNT = WARNING_COUNT + 1
        if caused_by:
            message = message + '\nCaused by:\n' + str(caused_by)
        logging.warn(message)


    def fix_url(self, url):
        """
        :param url: url to download data from
        :return: TODO
        """
        if not url:
            return None
        return GOOD_URL_PREFIX.join(url.split(BAD_URL_PREFIX))


    def read_url(self, url, ignore404=False):
        """
        :param url: url to download data from
        :return: Dictionary representation of json object
        """
        logging.debug('In read_url:')
        logging.debug('    url: '+url)

        url = self.fix_url(url)
        logging.debug('    url: '+url)

        data = None
        try:
            data = eval(urlopen(url).read())
        except Exception as e:
            if (ignore404 and type(e) is HTTPError and e.code == 404):
                logging.debug('Ignoring HTTPError (%s) at URL:\n    %s' % (str(e), str(url)))
            else:
                self.error('Exception trying to open data from URL:\n    %s'
                           '\n    The URL may not be formed correctly.'
                           % str(url), e )
        return data


    def get_number_of_jenkins_failures(self, cursor, testName, jenkins_job,
                                       last_build, num_builds):
        """TODO
        """
        logging.debug('In get_number_of_jenkins_failures:')
        logging.debug('    cursor      : '+str(cursor))
        logging.debug('    testName    : '+str(testName))
        logging.debug('    jenkins_job : '+str(jenkins_job))
        logging.debug('    last_build  : '+str(last_build))
        logging.debug('    num_builds  : '+str(num_builds))

        query_base = """SELECT count(*) as numfails
                        FROM `junit-test-failures` f
                        WHERE f.name = '%s'
                          AND f.job  = '%s'
                          AND f.status in ('FAILED', 'REGRESSION')
                          AND f.build <= %s
                          AND f.build  > %s
                     """
        query = query_base % (testName, jenkins_job, last_build,
                              (last_build - num_builds))
        logging.debug('    query       :\n    '+str(query))

        cursor.execute(query)
        num_failures = float(cursor.fetchone()[0])
        #num_failures = int(cursor.fetchone()[0])

        logging.debug('    num_failures: '+str(num_failures))

        return num_failures


    def get_intermittent_failure_percent(self, cursor=None, testName=None,
                                         jenkins_job=None, last_build=None,
                                         num_failures=None):
        """TODO
        """
        logging.debug('In get_intermittent_failure_percent...')

        if not num_failures:
            num_failures = self.get_number_of_jenkins_failures(cursor, testName,
                            jenkins_job, last_build, INTERMITTENT_FAILURE_WINDOW_SIZE)

        return (100.0 * num_failures
                / INTERMITTENT_FAILURE_WINDOW_SIZE)


    def qualifies_as_new_failure(self, cursor, testName,
                                 jenkins_job, last_build, status):
        """TODO
        """
        logging.debug('In qualifies_as_new_failure...')

        # Possible shortcut to skip querying the 'qa' database,
        # if we're just checking the most recent build
        if (status is 'REGRESSION' and
                NEW_FAILURE_WINDOW_SIZE is 1 and
                NEW_NUM_FAILURES_THRESHOLD is 1):
            logging.debug('...qualifies as new, via shortcut (assuming 4% failure percent)')
            return 4.0    # assume 1 failure out of the last 25 builds

        num_failures = self.get_number_of_jenkins_failures(cursor, testName,
                        jenkins_job, last_build, NEW_FAILURE_WINDOW_SIZE)

        if num_failures >= NEW_NUM_FAILURES_THRESHOLD:
            # recompute failure percent, as if an intermittent failure
            return self.get_intermittent_failure_percent(cursor, testName,
                                                         jenkins_job, last_build)
        else:
            return 0  # does not qualify


    def qualifies_as_intermittent_failure(self, cursor, testName,
                                          jenkins_job, last_build):
        """TODO
        """
        logging.debug('In qualifies_as_intermittent_failure...')

        num_failures = self.get_number_of_jenkins_failures(cursor, testName,
                        jenkins_job, last_build, INTERMITTENT_FAILURE_WINDOW_SIZE)

        if num_failures >= INTERMITTENT_NUM_FAILURES_THRESHOLD:
            # compute failure percent, as an intermittent failure
            return self.get_intermittent_failure_percent(num_failures=num_failures)
        else:
            return 0  # does not qualify


    def qualifies_as_consistent_failure(self, cursor, testName,
                                        jenkins_job, last_build):
        """TODO
        """
        logging.debug('In qualifies_as_consistent_failure...')

        num_failures = self.get_number_of_jenkins_failures(cursor, testName,
                        jenkins_job, last_build, CONSISTENT_FAILURE_WINDOW_SIZE)

        if num_failures >= CONSISTENT_NUM_FAILURES_THRESHOLD:
            failurePercent = (100.0 * num_failures
                              / CONSISTENT_FAILURE_WINDOW_SIZE)
            return failurePercent
        else:
            return 0  # does not qualify


    def change_intermittent_failure_to_consistent(self, cursor, testName,
                                                  jenkins_job, last_build):
        """TODO
        """
        logging.debug('In change_intermittent_failure_to_consistent_...')

        num_failures = self.get_number_of_jenkins_failures(cursor, testName,
                        jenkins_job, last_build, CHANGE_INTERMITTENT_TO_CONSISTENT_WINDOW_SIZE)

        if num_failures >= CHANGE_INTERMITTENT_TO_CONSISTENT_NUM_FAILURES_THRESHOLD:
            failurePercent = (100.0 * num_failures
                              / CHANGE_INTERMITTENT_TO_CONSISTENT_WINDOW_SIZE)
            return failurePercent
        else:
            return 0  # do not change


    def change_new_failure_to_old(self, cursor, testName,
                                  jenkins_job, last_build):
        """TODO
        """
        logging.debug('In change_new_failure_to_old...')

        num_failures = self.get_number_of_jenkins_failures(cursor, testName,
                        jenkins_job, last_build, CHANGE_NEW_TO_OLD_WINDOW_SIZE)

        if num_failures < CHANGE_NEW_TO_OLD_NUM_FAILURES_THRESHOLD:
            # recompute failure percent, as if an intermittent failure
            return self.get_intermittent_failure_percent(cursor, testName,
                                                         jenkins_job, last_build)
        else:
            return 0  # do not change


    def change_intermittent_failure_to_old(self, cursor, testName,
                                           jenkins_job, last_build):
        """TODO
        """
        logging.debug('In change_intermittent_failure_to_old...')

        num_failures = self.get_number_of_jenkins_failures(cursor, testName,
                        jenkins_job, last_build, CHANGE_INTERMITTENT_TO_OLD_WINDOW_SIZE)

        if num_failures < CHANGE_INTERMITTENT_TO_OLD_NUM_FAILURES_THRESHOLD:
            # recompute failure percent, as if still an intermittent failure
            return self.get_intermittent_failure_percent(cursor, testName,
                                                         jenkins_job, last_build)
        else:
            return 0  # do not change


    def change_consistent_failure_to_inconsistent(self, cursor, testName,
                                                  jenkins_job, last_build, status):
        """TODO
        """
        logging.debug('In change_consistent_failure_to_inconsistent...')

        # Possible shortcut to skip querying the 'qa' database,
        # if we're just checking the most recent build
        if (status is 'FIXED' and
                CHANGE_CONSISTENT_TO_INCONSISTENT_WINDOW_SIZE is 1 and
                CHANGE_CONSISTENT_TO_INCONSISTENT_NUM_FAILURES_THRESHOLD is 1):
            logging.debug('...do change to inconsistent, via shortcut (recompute failure percent)')
            # recompute failure percent, as if an intermittent failure
            return self.get_intermittent_failure_percent(cursor, testName,
                                                         jenkins_job, last_build)

        num_failures = self.get_number_of_jenkins_failures(cursor, testName,
                        jenkins_job, last_build, CHANGE_CONSISTENT_TO_INCONSISTENT_WINDOW_SIZE)

        if num_failures < CHANGE_CONSISTENT_TO_INCONSISTENT_NUM_FAILURES_THRESHOLD:
            # recompute failure percent, as if an intermittent failure
            return self.get_intermittent_failure_percent(cursor, testName,
                                                         jenkins_job, last_build)
        else:
            return 0  # do not change


    def change_inconsistent_failure_to_intermittent(self, cursor, testName,
                                                    jenkins_job, last_build, status):
        """TODO
        """
        logging.debug('In change_inconsistent_failure_to_intermittent...')

        # Possible shortcut to skip querying the 'qa' database,
        # if we're just checking the most recent build
        if (status is 'REGRESSION' and
                CHANGE_INCONSISTENT_TO_INTERMITTENT_WINDOW_SIZE is 1 and
                CHANGE_INCONSISTENT_TO_INTERMITTENT_NUM_FAILURES_THRESHOLD is 1):
            logging.debug('...do change to intermittent, via shortcut (recompute failure percent)')
            # recompute failure percent, as an intermittent failure
            return self.get_intermittent_failure_percent(cursor, testName,
                                                         jenkins_job, last_build)

        num_failures = self.get_number_of_jenkins_failures(cursor, testName,
                        jenkins_job, last_build, CHANGE_INCONSISTENT_TO_INTERMITTENT_WINDOW_SIZE)

        if num_failures >= CHANGE_INCONSISTENT_TO_INTERMITTENT_NUM_FAILURES_THRESHOLD:
            # recompute failure percent, as an intermittent failure
            return self.get_intermittent_failure_percent(cursor, testName,
                                                         jenkins_job, last_build)
        else:
            return 0  # do not change


    def should_close_inconsistent_failure(self, cursor, testName,
                                          jenkins_job, last_build,
                                          ticket_description=''):
        """TODO
        """
        logging.debug('In should_close_intermittent_failure...')

        num_failures = self.get_number_of_jenkins_failures(cursor, testName,
                        jenkins_job, last_build, CLOSE_INCONSISTENT_WINDOW_SIZE)

        if (num_failures < CLOSE_INCONSISTENT_NUM_FAILURES_THRESHOLD
                and jenkins_job in ticket_description):
            return True
        else:
            return False  # do not close


    def should_close_old_failure(self, cursor, testName,
                                 jenkins_job, last_build,
                                 ticket_description=''):
        """TODO
        """
        logging.debug('In should_close_old_failure...')

        num_failures = self.get_number_of_jenkins_failures(cursor, testName,
                        jenkins_job, last_build, CLOSE_OLD_WINDOW_SIZE)

        if (num_failures < CLOSE_OLD_NUM_FAILURES_THRESHOLD
                and jenkins_job in ticket_description):
            return True
        else:
            return False  # do not close


    def file_jira_issue(self, issue, DRY_RUN=False, failing_consistently=False):
        global JENKINSBOT
        if not JENKINSBOT:
            JENKINSBOT = JenkinsBot()
        error_url  = issue['url']
        error_report = self.read_url(error_url + '/api/python')
        if error_report is None:
            return None

        fullTestName = issue['packageName']+'.'+issue['className']+'.'+issue['testName']
        summary_keys = [issue['className'], issue['testName']]
        channel      = issue['channel']
        labels       = issue['labels']
        priority     = issue['priority']
        build_number = issue['build']
        jenkins_job  = issue['job']
        jenkins_job_nickname = JENKINS_JOB_NICKNAMES.get(jenkins_job, jenkins_job)
        existing_ticket = issue['existing_ticket']

        logging.debug('In file_jira_issue:')
        logging.debug('  issue        : '+str(issue))
        logging.debug('  fullTestName : '+str(fullTestName))
        logging.debug('  summary_keys : '+str(summary_keys))
        logging.debug('  labels       : '+str(labels))
        logging.debug('  priority     : '+str(priority))
        logging.debug('  build_number : '+str(build_number))
        logging.debug('  jenkins_job  : '+str(jenkins_job))
        logging.debug('  jenkins_job_nickname: '+str(jenkins_job_nickname))
        logging.debug('  existing_ticket: '+str(existing_ticket))
        logging.debug('  DRY_RUN      : '+str(DRY_RUN))
        logging.debug('  failing_consistently: '+str(failing_consistently))

        # TODO: not currently used; perhaps add back as a comment?? (to a Jira ticket)
        # throw a link to the query for the test history into the ticket to aid in checking/debugging the filer
        note = 'http://junitstatsdb.voltdb.lan/adminer.php?server=junitstatsdb.voltdb.lan&username' \
                + '=qaquery&db=qa&sql=select+%2A+from+%60junit-test-failures%60+WHERE+name+%3D+%27' \
                + fullTestName+'%27+and+job+%3D+%27'+jenkins_job+'%27order+by+build+desc'

        # Add a link to the test history on jenkins by last completed build
        history = sub('/\d+/testReport/', '/lastCompletedBuild/testReport/', error_url) + '/history/'

        failed_since = error_report.get('failedSince')
        failure_percent = issue['failurePercent']
#         summary = fullTestName + ' is failing ~' + failure_percent + '% of the time on ' + job + ' (' + issue['type'] + ')'

        logging.debug('  failed_since : '+str(failed_since))
        logging.debug('  failure_percent: '+str(failure_percent))
        logging.debug('  note         : '+str(note))
        logging.debug('  history(0)   : '+str(history))

        history = self.fix_url(history)
        logging.debug('  history(1)   : '+str(history))

        summary = issue['type']+' '+failure_percent+'%: '+issue['className']+'.'+issue['testName']
        descriptions = []
        descriptions.append('Failing Test:\n' + fullTestName + '\n')
        descriptions.append(SEPARATOR_LINE   + 'Failure history, in ' + jenkins_job_nickname + ':\n' + history + '\n')
        if error_report.get('errorStackTrace'):
            descriptions.append(STACK_TRACE_LINE + str(error_report['errorStackTrace']))
        if failed_since:
            consistently_or_intermittently = 'intermittently'
            if failing_consistently:
                consistently_or_intermittently = 'consistently'
            descriptions.append('\nFailing %s since %s build #%s'
                                % (consistently_or_intermittently,
                                   jenkins_job_nickname, str(failed_since) ))

        current_version = str(self.read_url('https://raw.githubusercontent.com/VoltDB/voltdb/'
                                    'master/version.txt'))

        attachments = {
            # filename : location
            jenkins_job + 'CountGraph.png' : error_url + '/history/countGraph/png?start=0&amp;end=25'
        }

        logging.debug('  summary      : '+str(summary))
        logging.debug('  descriptions :\n'+str(descriptions))
        logging.debug('  current_version: '+str(current_version))
        logging.debug('  attachments  : '+str(attachments))

        new_issue = None
        try:
            new_issue = JENKINSBOT.create_or_modify_jira_bug_ticket(
                            channel, summary_keys, summary,
                            jenkins_job, build_number,
                            descriptions, current_version, labels,
                            priority, attachments, existing_ticket,
                            max_num_attachments=MAX_NUM_ATTACHMENTS_PER_JIRA_TICKET,
                            DRY_RUN=DRY_RUN)
        except Exception as e:
            self.error('Error with filing issue, in file_jira_issue / create_or_modify_jira_bug_ticket', e)
            new_issue = None

        return new_issue


    def file_jira_new_issue(self, issue, failure_percent, DRY_RUN=False):
        logging.debug('In file_jira_new_issue...')
        issue['type'] = 'NEW'
        issue['labels'] = [JIRA_LABEL_FOR_AUTO_FILING, JIRA_LABEL_FOR_NEW_FAILURES]
        issue['priority'] = JIRA_PRIORITY_FOR_NEW_FAILURES
        issue['failurePercent'] = "%.0f" % failure_percent
        return self.file_jira_issue(issue, DRY_RUN)


    def file_jira_intermittent_issue(self, issue, failure_percent, DRY_RUN=False):
        logging.debug('In file_jira_intermittent_issue...')
        issue['type'] = 'INTERMITTENT'
        issue['labels'] = [JIRA_LABEL_FOR_AUTO_FILING, JIRA_LABEL_FOR_INTERMITTENT_FAILURES]
        issue['priority'] = JIRA_PRIORITY_FOR_INTERMITTENT_FAILURES
        issue['failurePercent'] = "%.0f" % failure_percent
        return self.file_jira_issue(issue, DRY_RUN)


    def file_jira_consistent_issue(self, issue, failure_percent, DRY_RUN=False):
        logging.debug('In file_jira_consistent_issue...')
        issue['type'] = 'CONSISTENT'
        issue['labels'] = [JIRA_LABEL_FOR_AUTO_FILING, JIRA_LABEL_FOR_CONSISTENT_FAILURES]
        issue['priority'] = JIRA_PRIORITY_FOR_CONSISTENT_FAILURES
        issue['failurePercent'] = "%.0f" % failure_percent
        return self.file_jira_issue(issue, DRY_RUN, failing_consistently=True)


    def file_jira_inconsistent_issue(self, issue, failure_percent, DRY_RUN=False):
        logging.debug('In file_jira_inconsistent_issue...')
        issue['type'] = 'INCONSISTENT'
        issue['labels'] = [JIRA_LABEL_FOR_AUTO_FILING, JIRA_LABEL_FOR_INCONSISTENT_FAILURES]
        issue['priority'] = JIRA_PRIORITY_FOR_INCONSISTENT_FAILURES
        issue['failurePercent'] = "%.0f" % failure_percent
        return self.file_jira_issue(issue, DRY_RUN)


    def file_jira_old_issue(self, issue, failure_percent, DRY_RUN=False):
        logging.debug('In file_jira_old_issue...')
        issue['type'] = 'OLD'
        issue['labels'] = [JIRA_LABEL_FOR_AUTO_FILING, JIRA_LABEL_FOR_OLD_FAILURES]
        issue['priority'] = JIRA_PRIORITY_FOR_OLD_FAILURES
        issue['failurePercent'] = "%.0f" % failure_percent
        return self.file_jira_issue(issue, DRY_RUN)


    def close_jira_issue(self, issue, DRY_RUN=False):
        global JENKINSBOT
        if not JENKINSBOT:
            JENKINSBOT = JenkinsBot()

        summary_keys = [issue['className'], issue['testName']]
        channel      = issue['channel']
        labels       = issue['labels']
        build_number = issue['build']

        closed_issue_url = None
        try:
            closed_issue = JENKINSBOT.close_jira_bug_ticket(
                            channel, summary_keys,
                            jenkins_job, build_number,
                            labels, DRY_RUN)
            if closed_issue:
                closed_issue_url = "https://issues.voltdb.com/browse/" + closed_issue.key
        except Exception as e:
            self.error('Error with closing issue', e)
            closed_issue_url = None

        return closed_issue_url


    def get_build_data(self, job, build_range, process_passed=False,
                       post_to_slack=False, file_jira_ticket=True):
        """
        Gets build data for a job. Can specify an inclusive build range.
        For every build specified on the job, saves the test results
        :param job: Full job name on Jenkins
        :param build_range: Build range that exists for the job on Jenkins, e.g. "700-713", "1804-1804"
        """
        logging.debug('In get_build_data:')

        if job is None or build_range is None:
            self.error('Either the job ('+str(job)+') or build range ('+str(build_range)+') was not specified.')
            print(self.cmdhelp)
            return

        logging.debug('  Jenkins job   : '+str(job))
        logging.debug('  build_range   : '+str(build_range))
        logging.debug('  process_passed: '+str(process_passed))
        logging.debug('  post_to_slack : '+str(post_to_slack))
        logging.debug('  file_jira_ticket: '+str(file_jira_ticket))

        # we only process ticket filing for master jobs.
        from re import match, IGNORECASE
        if not(match(r'test-nextrelease-', job, IGNORECASE) \
               or match(r'branch-.*-master', job, IGNORECASE)):
            logging.warn("Ignoring this job ("+str(job)+"), since it is neither 'branch-...-master' nor 'test-nextrelease-...'.")
            return

        url = self.jhost + '/job/' + job + '/lastCompletedBuild/api/python'
        build = self.read_url(url)
        if build is None:
            self.warn('Could not retrieve last completed build. Job: %s' % job)
            build = {
                'number': 'unknown',
                'builtOn': 'unknown'
            }

        latest_build = build['number']
        host = build['builtOn']
        logging.debug('Job %s: latest build %s; host %s' % (job, latest_build, host))

        logging.debug('  url          : '+str(url))
        #logging.debug('  build        : '+str(build))
        logging.debug('  latest_build : '+str(latest_build))
        logging.debug('  host         : '+str(host))

        if latest_build == 'unknown':
            raise Exception('Error: could not determine latest build in jenkins, job name ('+job+') in error?')

        try:
            db = mysql.connector.connect(host=self.dbhost, user=self.dbuser, password=self.dbpass, database='qa')
            cursor = db.cursor()
        except MySQLError as e:
            self.error('Could not connect to qa database. User: %s. Pass: %s' % (self.dbuser, self.dbpass), e)
            return

        query_last_build = ("select max(build) FROM `junit-builds` where name='%s'" % job)
        cursor.execute(query_last_build)
        last_build_recorded = cursor.fetchone()[0]
        logging.info('Last build recorded (in qa database): %s' % last_build_recorded)

        try:
            builds = build_range.split('-')
            if len(builds) > 1 and len(builds[1]) > 0:
                build_high = int(builds[1])
                build_low = min(int(builds[0]), last_build_recorded + 1)
                if build_high < build_low:
                    raise Exception('Error: Left build range number must be less than or equal to right number.')
            else:
                build_low = last_build_recorded + 1
                build_high = latest_build
        except Exception as e:
            self.error("Couldn't extrapolate build range, from %s, with latest %s, so %s - %s." \
                              & (build_range, latest_build, build_low, build_high), e )
            print(self.cmdhelp)
            return

        logging.info("Effective builds for job %s: %s-%s" % (job, build_low, build_high))

        for build in range(build_low, build_high + 1):
            build_url = self.jhost + '/job/' + job + '/' + str(build) + '/api/python'
            build_report = self.read_url(build_url)
            if build_report is not None:
                host = build_report.get('builtOn', 'unknown').split('-')[0]  # split for hosts which are voltxx-controller-...

            test_url = self.jhost + '/job/' + job + '/' + str(build) + '/testReport/api/python'
            test_report = self.read_url(test_url)

            logging.info('')
            logging.info('Build #: '+str(build))
            logging.debug('  build_url    : '+str(build_url))
            logging.debug('  test_url     : '+str(test_url))
            #logging.debug('  build_report : '+str(build_report))
            #logging.debug('  test_report  : '+str(test_report))

            if test_report is None:
                self.warn(
                    'Could not retrieve report because URL (%s) is invalid. This may be '
                    'because build #%d might not exist on Jenkins' % (test_url, build))
                self.warn('Last completed build for this job is: %s\n' % latest_build)
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
                # Appears to be same URL as build_url
                job_url = self.jhost + '/job/' + job + '/' + str(build) + '/api/python'
                job_report = self.read_url(job_url)
                if job_report is None:
                    self.warn(
                        'Could not retrieve report because URL (%s) is invalid. This may be '
                        'because the build %d might not exist on Jenkins' % (job_url, build))
                    self.warn('Last completed build for this job is: %s\n' % latest_build)
                    continue

                # Job stamp is already in EST (or EDT)
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

                delete_job = ('DELETE FROM `junit-builds` WHERE name=%(name)s and build=%(build)s')
                delete_target = (
                    'DELETE FROM `junit-build-targets` WHERE name=%(name)s AND build=%(build)s')
                delete_test = (
                    'DELETE FROM `junit-test-failures` WHERE job=%(name)s AND build=%(build)s')

                add_job = ('INSERT INTO `junit-builds` '
                           '(name, stamp, url, build, fails, total, percent) '
                           'VALUES (%(name)s, %(timestamp)s, %(url)s, %(build)s, %(fails)s, %(total)s, %(percent)s)')

                logging.debug('  fails        : '+str(fails))
                logging.debug('  skips        : '+str(skips))
                logging.debug('  passes       : '+str(passes))
                logging.debug('  total        : '+str(total))
                logging.debug('  percent      : '+str(percent))
                logging.debug('  job_url      : '+str(job_url))
                #logging.debug('  job_report   : '+str(job_report))
                logging.debug('  job_stamp    : '+str(job_stamp))
                logging.debug('  job_data     : '+str(job_data))
                logging.debug('  delete_job % job_data:\n    '   +(delete_job % job_data))
                logging.debug('  delete_target % job_data:\n    '+(delete_target % job_data))
                logging.debug('  delete_test % job_data:\n    '  +(delete_test % job_data))
                logging.debug('  add_job % job_data:\n    '      +(add_job % job_data))

                if not DRY_RUN:
                    try:
                        cursor.execute(delete_job, job_data)
                    except MySQLError as e:
                        self.error('Could not delete job data from database', e)
                    try:
                        cursor.execute(delete_target, job_data)
                        db.commit()
                    except MySQLError as e:
                        self.error('Could not delete target data from database', e)
                    try:
                        cursor.execute(delete_test, job_data)
                        db.commit()
                    except MySQLError as e:
                        self.error('Could not delete test data from database', e)

                    try:
                        cursor.execute(add_job, job_data)
                        db.commit()
                    except MySQLError as e:
                        if e.errno != 1062:
                            self.error('Could not add job data to database', e)

                logging.debug('  deletes & add_job complete.')
                logging.debug('  job_report size: '+str(len(job_report)))

                # in a multiconfiguration job we have multiple job_reports under 'runs', otherwise, just the
                # single job report we already retrieved.
                for run in job_report.get('runs', [job_report]):

                    run['url'] = self.fix_url(run.get('url'))
                    logging.info('run:  number: %s;  url: %s'
                                 % (str(run.get('number')), str(run.get('url')) ))

                    # very strange but sometimes there is a run from a different build??
                    if run['number'] != build:
                        self.warn('Build number is %s, but run number is %s !?! (ignoring)'
                                     % (build, run['number']))
                        continue

                    if build != int(run['url'].split('/')[-2]):
                        raise RuntimeError("build # not correct %s %s" % (build, run['url']))

                    tr_url = run['url'] + "testReport/api/python"
                    child = self.read_url(tr_url, ignore404=True)

                    if child is None:
                        child = {'duration': None,
                                 'empty': True,
                                 'failCount': None,
                                 'passCount': None,
                                 'skipCount': None
                                 }
                        self.warn('child: '+str(child))

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

                    logging.debug('  tr_url       : '+str(tr_url))
                    logging.debug('  target_data  : '+str(target_data))
                    logging.debug('  add_target % target_data:\n    '+(add_target % target_data))

                    if not DRY_RUN:
                        try:
                            cursor.execute(add_target, target_data)
                            db.commit()
                        except MySQLError as e:
                            if e.errno != 1062:  # duplicate entry
                                self.error('Could not add duplicate target data to database', e)
                            else:
                                raise e

                    # if there is no test report, for some reason, maybe the build timed out, just move on
                    if child['empty'] is True:
                        self.warn("No test report found for URL: %s" % tr_url)
                        continue

                    # Traverse through reports into test suites and get failed test case data to write to database.
                    # for child in child_reports:
                    suites = child['suites']
                    #logging.debug('suites: %s' % suites)

                    count_test_cases = 0
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
                            status        = case['status']
                            fullClassName = case['className'].translate(TT)
                            testName      = case['name'].translate(TT)
                            lastDot       = fullClassName.rfind('.')
                            packageName   = fullClassName[:lastDot]
                            className     = fullClassName[lastDot+1:]
                            fullTestName  = fullClassName + '.' + testName

                            logging.debug('')
                            logging.debug('  FullClassName: '+str(fullClassName))
                            logging.debug('  testName     : '+str(testName))
                            logging.debug('  packageName  : '+str(packageName))
                            logging.debug('  className    : '+str(className))
                            logging.debug('  fullTestName : '+str(fullTestName))
                            logging.debug('  status       : '+str(status))

                            assert status in ['PASSED', 'FAILED', 'FIXED', 'REGRESSION'], status

                            # We normally ignore 'PASSED' tests, when process_passed
                            # is False (as it is by default); when it's True, this
                            # script runs much slower, but without running it that
                            # way once in a while, there is no way to Close, or
                            # even to "downgrade" (e.g. from INTERMITTENT or NEW
                            # to OLD), Jira tickets for tests that have not failed
                            # for a while
                            if status == 'PASSED' and not process_passed:
                                continue

                            if "_init_" in fullTestName or "-vdm-" in run['url']:
                                testcase_url = run['url'] + 'testReport/(root)/' + fullTestName
                            else:
                                testcase_url = run['url'] + 'testReport/' + fullTestName
                            orig_testcase_url = testcase_url

                            # find the testcase url that "works"
                            dot_count = fullTestName.count('.')
                            separator = '/'
                            # Special case for "junit framework" failures:
                            if fullTestName.startswith('junit.framework.TestSuite.'):
                                testcase_url = testcase_url.replace('junit.framework.TestSuite.',
                                                                    'junit.framework/TestSuite/')
                                separator = '_'
                            for i in range(dot_count):
                                page = None
                                try:
                                    page = self.read_url(testcase_url+'/api/python', ignore404=True)
                                except Exception as e:
                                    self.warn("Failed to find URL, with dot_count %d, i %d, original "
                                              "URL '%s':\n    %s\n    current URL:\n    %s"
                                              % (dot_count, i, orig_testcase_url,
                                                 testcase_url+' [/api/python]'), e )
                                    pass
                                if type(page) == dict and 'status' in page:
                                    break
                                testcase_url = separator.join(testcase_url.rsplit('.', 1))
                            if page is None:
                                self.error("Testcase URL invalid:\n    %s"
                                           "\n    Skipping (in build %d) test: %s"
                                           % (testcase_url+' [/api/python]', build, fullTestName) )
                                continue

                            test_data = {
                                'fullTestName': fullTestName,
                                'packageName': packageName,
                                'className': className,
                                'testName': testName,
                                'job': job,
                                'status': status,
                                'timestamp': test_stamp,
                                'url': testcase_url,
                                'build': build,
                                'host': host,
                                'existing_ticket': None,
                                'new_issue_url': None   # unused
                            }
                            if post_to_slack:
                                test_data['channel'] = SLACK_CHANNEL_FOR_AUTO_FILING
                            else:
                                test_data['channel'] = None

                            logging.debug('  testcase_url : '+str(testcase_url))
                            logging.debug('  dot_count    : '+str(dot_count))
                            logging.debug('  test_data    : '+str(test_data))

                            # We need to record 'FIXED' and 'FAILED' and 'REGRESSION'
                            # in the 'qa' database, but not 'PASSED'
                            if status != 'PASSED':
                                logging.info('working on  : %s, %s, %s; %s; %s'
                                             % (build, status, fullTestName, job, url) )

                                # record test results to database (with issue url)
                                # if an issue was filed, its url will be recorded in the database
                                add_test = ("INSERT INTO `junit-test-failures` "
                                            "(name, job, status, stamp, url, build, host, issue_url) "
                                            "VALUES ( %(fullTestName)s, "
                                            "%(job)s, %(status)s, %(timestamp)s, %(url)s, "
                                            "%(build)s, %(host)s, %(new_issue_url)s )")

                                logging.debug('  add_test     : '+str(add_test))
                                logging.debug('  add_test % test_data:\n    '+(add_test % test_data))

                                if not DRY_RUN:
                                    try:
                                        cursor.execute(add_test, test_data)
                                        db.commit()
                                    except MySQLError as e:
                                        if e.errno != 1062:
                                            self.error('Could not add test data to database', e)



                            # TODO: rewrite logic, below (jtc):
                            global JENKINSBOT
                            if not JENKINSBOT:
                                JENKINSBOT = JenkinsBot()

                            summary_keys = [className, testName]
                            labels = [JIRA_LABEL_FOR_AUTO_FILING]

                            previous_ticket_summary = None
                            previous_ticket_failure_type = None
                            previous_ticket = JENKINSBOT.find_jira_bug_ticket(summary_keys, labels)
                            logging.debug('  previous_ticket             : '+str(previous_ticket))
                            if previous_ticket:
                                test_data['existing_ticket'] = previous_ticket
                                previous_ticket_summary = previous_ticket.fields.summary
                                previous_ticket_description = previous_ticket.fields.description
                                previous_ticket_failure_type = previous_ticket_summary.split(' ')[0]
                                # Get the "type" of tickets auto-filed before recent changes
                                if (previous_ticket_failure_type.startswith('org.volt')
                                            and '(' in previous_ticket_summary):
                                    previous_ticket_failure_type = previous_ticket_summary \
                                                                   .split('(')[1] \
                                                                   .split(' ')[0] \
                                                                   .split(')')[0]
                                logging.debug('  previous_ticket.key         : '+str(previous_ticket.key))
                                logging.debug('  previous_ticket_summary     : '+str(previous_ticket_summary))
                                logging.debug('  previous_ticket_failure_type: '+str(previous_ticket_failure_type))
                                logging.debug('  previous_ticket_description : '+str(previous_ticket_description))


                            # When no current, open Jira bug ticket exists
                            # (or the type is unrecognized), file a ticket,
                            # either of type 'NEW' or 'INTERMITTENT'
                            if (not previous_ticket_failure_type) or (previous_ticket_failure_type not in
                                        ['NEW', 'INTERMITTENT', 'CONSISTENT', 'INCONSISTENT', 'OLD']):
                                if (status in ['REGRESSION', 'FAILED']):
                                    failure_percent = self.qualifies_as_intermittent_failure(
                                                                cursor, fullTestName, job, build)
                                    if failure_percent:
                                        self.file_jira_intermittent_issue(test_data, failure_percent, DRY_RUN)
                                    else:
                                        failure_percent = self.qualifies_as_new_failure(
                                                                cursor, fullTestName, job, build, status)
                                        if failure_percent:
                                            self.file_jira_new_issue(test_data, failure_percent, DRY_RUN)
                                        else:
                                            logging.info(('No Jira ticket filed for test %s, '
                                                          'in job %s, build #%s')
                                                          % (fullTestName, job, str(build)) )

                            # When a current, open Jira bug ticket exists, of type 'NEW',
                            # consider upgrading it to 'CONSISTENT' or 'INTERMITTENT',
                            # or possibly downgrading it to 'OLD'
                            elif previous_ticket_failure_type == 'NEW':
                                if (status in ['REGRESSION', 'FAILED']):
                                    failure_percent = self.qualifies_as_consistent_failure(
                                                            cursor, fullTestName, job, build)
                                    if failure_percent:
                                        self.file_jira_consistent_issue(test_data, failure_percent, DRY_RUN)
                                    else:
                                        failure_percent = self.qualifies_as_intermittent_failure(
                                                            cursor, fullTestName, job, build)
                                        if failure_percent:
                                            self.file_jira_intermittent_issue(test_data, failure_percent, DRY_RUN)
                                elif (status in ['FIXED', 'PASSED']):
                                    failure_percent = self.change_new_failure_to_old(
                                                            cursor, fullTestName, job, build)
                                    if failure_percent:
                                        self.file_jira_old_issue(test_data, failure_percent, DRY_RUN)

                            # When a current, open Jira bug ticket exists, of type
                            # 'INTERMITTENT', consider upgrading it to 'CONSISTENT',
                            # or possibly downgrading it to 'OLD'
                            elif previous_ticket_failure_type == 'INTERMITTENT':
                                if (status in ['REGRESSION', 'FAILED']):
                                    failure_percent = self.change_intermittent_failure_to_consistent(
                                                            cursor, fullTestName, job, build)
                                    if failure_percent:
                                        self.file_jira_consistent_issue(test_data, failure_percent, DRY_RUN)
                                    else:
                                        # Update the 'INTERMITTENT' ticket's failure percent
                                        failure_percent = self.qualifies_as_intermittent_failure(
                                                            cursor, fullTestName, job, build)
                                        if failure_percent:
                                            self.file_jira_intermittent_issue(test_data, failure_percent, DRY_RUN)
                                elif (status in ['FIXED', 'PASSED']):
                                    failure_percent = self.change_intermittent_failure_to_old(
                                                            cursor, fullTestName, job, build)
                                    if failure_percent:
                                        self.file_jira_old_issue(test_data, failure_percent, DRY_RUN)

                            # When a current, open Jira bug ticket exists, of type
                            # 'CONSISTENT', consider downgrading it to 'INCONSISTENT'
                            elif previous_ticket_failure_type == 'CONSISTENT':
                                if (status in ['FIXED', 'PASSED']):
                                    failure_percent = self.change_consistent_failure_to_inconsistent(
                                                            cursor, fullTestName, job, build, status)
                                    if failure_percent:
                                        self.file_jira_inconsistent_issue(test_data, failure_percent, DRY_RUN)

                            # When a current, open Jira bug ticket exists, of type
                            # 'INCONSISTENT' (i.e., formerly, but no longer,
                            # 'CONSISTENT'), consider changing it to 'INTERMITTENT',
                            # or closing it
                            elif previous_ticket_failure_type == 'INCONSISTENT':
                                if (status in ['REGRESSION', 'FAILED']):
                                    failure_percent = self.change_inconsistent_failure_to_intermittent(
                                                            cursor, fullTestName, job, build, status)
                                    if failure_percent:
                                        self.file_jira_intermittent_issue(test_data, failure_percent, DRY_RUN)
                                elif (status in ['FIXED', 'PASSED']):
                                    if self.should_close_inconsistent_failure(
                                                            cursor, fullTestName, job, build,
                                                            previous_ticket_description):
                                        self.close_jira_issue(test_data, DRY_RUN)

                            # When a current, open Jira bug ticket exists, of type 'OLD',
                            # consider upgrading it to 'INTERMITTENT', or closing it
                            elif previous_ticket_failure_type == 'OLD':
                                if (status in ['REGRESSION', 'FAILED']):
                                    failure_percent = self.qualifies_as_intermittent_failure(
                                                            cursor, fullTestName, job, build)
                                    if failure_percent:
                                        self.file_jira_intermittent_issue(test_data, failure_percent, DRY_RUN)
                                elif (status in ['FIXED', 'PASSED']):
                                    if self.should_close_old_failure(
                                                            cursor, fullTestName, job, build,
                                                            previous_ticket_description):
                                        self.close_jira_issue(test_data, DRY_RUN)

                            # It should be impossible to get here; but just in case we
                            # miss a type of ticket failure, and somehow slip through to
                            # here, as used to happen with older Jira tickets, auto-filed
                            # before recent changes, whose type could not be identified
                            # in the same way: then issue a warning
                            else:
                                self.warn("Unrecognized failure type '%s' ignored, for ticket: %s"
                                             + (str(previous_ticket_failure_type),
                                                str(previous_ticket.get(key)) ))

                            count_test_cases = count_test_cases + 1
                            if not (count_test_cases % LOG_MESSAGE_EVERY_NUM_TEST_CASES):
                                logging.info('     - processed %d test cases (in %s)'
                                             % (count_test_cases, str(run.get('url'))) )

            except KeyError as ke:
                self.error('Error retrieving test data for this particular build: %d\n' % build, ke)
            except Exception as e:
                # Catch all errors to avoid causing a failing build for the upstream job in case this is being
                # called from the junit-test-branch on Jenkins
                self.error('Catching unexpected errors to avoid causing a failing build for the upstream job', e)
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


def isTruthy(value):
    """Converts any value to a boolean value; just uses the 'bool' built-in function,
       except that strings like 'FALSE', 'false' and 'False', and strings that are
       numeric values equal to 0, return False.
    """
    if str(value).lower() == 'false':
        return False

    try:
        return bool(float(value))
    except:
        return bool(value)


if __name__ == '__main__':

    args = sys.argv
    logging.info('junit-stats.__main__; args: '+str(args))

    if len(args) > 1 and args[1] == 'unittest':
        unittest.main(argv=['', 'Tests.test_1'], failfast=False)
        #unittest.main(failfast=False)
        sys.exit()

    stats = Stats()
    job = os.environ.get('job', None)
    build_range = os.environ.get('build_range', None)
    process_passed = os.environ.get('process_passed', None)
    post_to_slack  = os.environ.get('post_to_slack', None)

    if len(args) > 1:
        job = args[1]

    if len(args) > 2:
        build_range = args[2]

    if len(args) > 3:
        process_passed = args[3]

    if len(args) > 4:
        post_to_slack = args[4]

    process_passed = isTruthy(process_passed)
    post_to_slack  = isTruthy(post_to_slack)

    logging.info('Processing job: %s;\n    build range: %s;'
                 '  process tests that passed: %s;  post to slack: %s'
                 % (job, build_range, str(process_passed), str(post_to_slack)) )

    stats.get_build_data(job, build_range, process_passed,
                         post_to_slack, file_jira_ticket=not DRY_RUN)

    if ERROR_COUNT:
        logging.info("Processing of Jenkins builds failed with %d ERROR(s) "
                     "(and %d WARNING(s))" % (ERROR_COUNT, WARNING_COUNT) )
        sys.exit(11)
    elif WARNING_COUNT:
        logging.info("Processing of Jenkins builds failed with %d WARNING(s) "
                     "(and %d ERROR(s))" % (WARNING_COUNT, ERROR_COUNT) )
        sys.exit(10)
    else:
        logging.info("Processing of Jenkins build(s) succeeded, without ERRORs or (major) WARNINGs.")
