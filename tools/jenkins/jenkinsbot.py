#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.

# Script that runs jenkinsbot for VoltDB Slack

import logging
import os
import sys
import thread
from threading import Thread

import mysql.connector

from jira import JIRA
from logging import handlers
from mysql.connector.errors import Error as MySQLError
from slackclient import SlackClient
from tabulate import tabulate  # Used for pretty printing tables
from urllib import urlretrieve

# Get job names from environment variables
COMMUNITY = os.environ.get('community', None)
PRO = os.environ.get('pro', None)
VDM = os.environ.get('vdm', None)
MEMVALDEBUG = os.environ.get('memvaldebug', None)
DEBUG = os.environ.get('debug', None)
MEMVAL = os.environ.get('memval', None)
FULLMEMCHECK = os.environ.get('fullmemcheck', None)
MASTER_JOBS = [PRO, COMMUNITY, VDM]
CORE_JOBS = [MEMVALDEBUG, DEBUG, MEMVAL, FULLMEMCHECK]

# Channel to message in case something goes wrong
ADMIN_CHANNEL = os.environ.get('admin', None)

# Other channels
GENERAL_CHANNEL = os.environ.get('general', None)
RANDOM_CHANNEL = os.environ.get('random', None)
JUNIT = os.environ.get('junit', None)

# Jira credentials and info
JIRA_USER = os.environ.get('jirauser', None)
JIRA_PASS = os.environ.get('jirapass', None)
# TODO: change this back to 'ENG', before merging to master
JIRA_PROJECT = os.environ.get('jiraproject', 'ENG')

# Queries

# System Leaderboard - Leaderboard for master system tests on apprunner.
SL_QUERY = ("""
  SELECT job_name AS 'Job name',
         workload AS 'Workload',
         fails AS 'Fails',
         total AS 'Total',
         fails/total*100. AS 'Fail %',
         latest AS 'Latest'
    FROM
        (
           SELECT job_name,
                  workload,
                  COUNT(*) AS fails,
                  (
                   SELECT COUNT(*)
                     FROM `apprunnerfailures` AS run
                    WHERE NOW() - INTERVAL 30 DAY <= run.datetime AND
                          run.job_name=failure.job_name AND
                          run.workload=failure.workload AND
                          run.branch_name=failure.branch_name
                  ) AS total,
                  MAX(failure.datetime) AS latest
            FROM `apprunnerfailures` AS failure
            WHERE NOW() - INTERVAL 30 DAY <= datetime AND
                  result='FAIL' AND
                  failure.branch_name='master'
         GROUP BY job_name,
                  workload
        ) AS intermediate
GROUP BY 6 DESC
""")

# Tests since leaderboard - See all the tests that have been failing the most since a certain build and view them as
# most frequently occurring.
TS_QUERY = ("""
  SELECT tf.name AS 'Test name',
         COUNT(*) AS 'Failures'
    FROM `junit-test-failures` AS tf
   WHERE NOT tf.status='FIXED' AND
         tf.build >= %(beginning)s AND
         tf.job=%(job)s
GROUP BY tf.name
ORDER BY 2 DESC
""")

# Days leaderboard - See the tests that have been failing the most in the past certain amount of days and view them as
# most frequently occurring.
DL_QUERY = ("""
  SELECT tf.name AS 'Test name',
         COUNT(*) AS 'Failures'
    FROM `junit-test-failures` AS tf
   WHERE NOT tf.status='FIXED' AND
         NOW() - INTERVAL %(days)s DAY <= tf.stamp AND
         tf.job=%(job)s
GROUP BY tf.name
ORDER BY 2 DESC
""")

# Build range leaderboard - See the tests that have been failing the most in a range of builds and view them as most
# frequently occurring.
BR_QUERY = ("""
    SELECT tf.name AS 'Test name',
           COUNT(*) AS 'Number of failures in this build range'
      FROM `junit-test-failures` AS tf
INNER JOIN `junit-builds` AS jb
        ON NOT tf.status='FIXED' AND
           jb.name=tf.job AND
           jb.build=tf.build AND
           jb.name=%(job)s AND
           %(build_low)s <= jb.build AND
           jb.build <= %(build_high)s
  GROUP BY tf.name,
           tf.job
  ORDER BY 2 DESC
""")

# All failures leaderboard - See all failures for a job over time and view them as most recent.
AF_QUERY = ("""
  SELECT tf.name AS 'Test name',
         tf.build AS 'Build',
         tf.stamp AS 'Time'
    FROM `junit-test-failures` AS tf
   WHERE NOT STATUS='FIXED' AND
         tf.job=%(job)s
ORDER BY 2 DESC
""")

# Latest build - See the latest build for a job in the database.
LB_QUERY = ("""
    SELECT name AS 'Job name',
           stamp AS 'Latest run',
           url AS 'Build url',
           build AS 'Build number'
      FROM `junit-builds`
     WHERE stamp = (
              SELECT MAX(jb.stamp)
                FROM `junit-builds` AS jb
               WHERE jb.name=%(job)s
           ) AND
           name=%(job)s
""")

# Recent failure - See if test is failing and how recently.
RF_QUERY = ("""
    SELECT MAX(tf.build) AS 'Most recent failure',
           MAX(jb.build) AS 'Most recent build of job'
      FROM `junit-test-failures` AS tf
INNER JOIN `junit-builds` AS jb
        ON NOT tf.status='FIXED' AND
           jb.name=tf.job AND
           tf.name=%(test)s AND
           jb.name=%(job)s
""")

# Recent status - See the most recent status for a test.
RS_QUERY = ("""
    SELECT name AS 'Test name',
           status AS 'Latest status',
           stamp AS 'Latest run',
           build AS 'Latest build'
      FROM `junit-test-failures`
     WHERE stamp = (
              SELECT MAX(tf.stamp)
                FROM `junit-test-failures` AS tf
               WHERE tf.name=%(test)s AND
                     tf.job=%(job)s
           ) AND
           name=%(test)s AND
           job=%(job)s
""")

# Add alias - Add an alias for the user.
AA_QUERY = ("""
    INSERT INTO `jenkinsbot-user-aliases`
                (slack_user_id, command, alias)
         VALUES (%(slack_user_id)s, %(command)s, %(alias)s)
""")

# Remove alias - Remove an alias for the user.
RA_QUERY = ("""
    DELETE FROM `jenkinsbot-user-aliases`
          WHERE slack_user_id=%(slack_user_id)s AND
                alias=%(alias)s
""")

# Get alias - Get the command for an alias for the user.
GA_QUERY = ("""
    SELECT command
      FROM `jenkinsbot-user-aliases`
     WHERE alias=%(alias)s AND
           slack_user_id=%(slack_user_id)s
""")

# See aliases - Get all aliases for the user.
SA_QUERY = ("""
    SELECT alias,
           command
      FROM `jenkinsbot-user-aliases`
     WHERE slack_user_id=%(slack_user_id)s
""")


class WorkerThread(Thread):
    def __init__(self, incoming_data, jenkins_bot):
        Thread.__init__(self)
        self.incoming = incoming_data
        self.jenkins_bot = jenkins_bot

    def run(self):
        jenkins_bot = self.jenkins_bot

        try:

            if not self.can_reply():
                return

            text = self.incoming.get('text', None)
            channel = self.incoming.get('channel', None)
            user = self.incoming.get('user', None)

            if 'shut-down' in text:
                # Keyword command for shutting down jenkinsbot. Script has to be run again with proper
                # environment variables to turn jenkinsbot on.
                jenkins_bot.post_message(channel, 'Shutting down..')
                thread.interrupt_main()
            elif 'help' in text:
                jenkins_bot.post_message(channel, jenkins_bot.help_text)
            else:
                query = None
                params = None
                filename = None
                try:
                    # TODO get insert option from parse_text rather than inferring it
                    (query, params, filename) = jenkins_bot.parse_text(text, channel, user)
                except IndexError:
                    jenkins_bot.logger.exception('Incorrect number or formatting of arguments')
                    if channel:
                        jenkins_bot.post_message(channel, 'Incorrect number or formatting of arguments\n\n' +
                                                 jenkins_bot.help_text)
                if query and params and filename:
                    jenkins_bot.post_message(channel, 'Please wait..')
                    jenkins_bot.query_and_response(query, params, [channel], filename)
                elif query and params:
                    jenkins_bot.query([channel], query, params, insert=True)
        except KeyboardInterrupt:
            # Propagate the keyboard interrupt from the worker thread that received the shut-down command
            raise
        except:
            jenkins_bot.logger.exception('Something unexpected went wrong')

            # Try to reconnect
            if not jenkins_bot.connect_to_slack():
                jenkins_bot.logger.info('Could not connect to Slack')
                jenkins_bot.post_message(ADMIN_CHANNEL, 'Cannot connect to Slack')

    def can_reply(self):
        """
        :return: true if bot can act on incoming data - There is incoming data, it is text data, it's not from a bot,
        the text isn't in a file, the channel isn't in #general, #random, #junit which jenkinsbot is part of
        """
        # TODO rather than check all channels, just check if this is an direct message
        return (self.incoming.get('text', None) is not None and self.incoming.get('bot_id', None) is None
                and self.incoming.get('file', None) is None and self.incoming['channel'] != GENERAL_CHANNEL
                and self.incoming['channel'] != RANDOM_CHANNEL and self.incoming['channel'] != JUNIT)


class JenkinsBot(object):
    def __init__(self):
        self.client = None
        self.help_text = '\n'.join(
            ['*Instructions:*',
             'Alias a command:\n\tmy alias = valid command',
             'Remove an alias:\n\t`unalias` my alias',
             'See your aliases:\n\t`aliases`',
             'See which tests are failing the most since this build:\n\t`tests-since` <job> <build #>',
             'See which tests are failing in the past x days:\n\t`days` <job> <days>',
             'Failing the most in this build range:\n\t`build-range` <job> <build #>-<build #>',
             'Most recent failure of a non-passing test:\n\t`recent-failure` <job> <testname>'
             ' (ex. testname: org.voltdb.iv2..)',
             'Most recent status of a non-passing test:\n\t`recent-status` <job> <testname>',
             'All failures for a job:\n\t`all-failures` <job>',
             'Display this help:\n\t`help`',
             'For <job>, you can specify *pro* or *com* for the respective junit master jobs',
             'Examples: `tests-since pro 860`, `days com 14`, now = `days pro 1`']
        )
        self.logger = self.setup_logging()

    def setup_logging(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        # Limit log file to 1GB, no rotation
        handler = handlers.RotatingFileHandler('jenkinsbot.log', maxBytes=1 << 30)
        handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logger.addHandler(handler)

        return logger

    def connect_to_slack(self):
        """
        :return: True if token for bot exists, client was created, and bot connected to Real Time Messaging
        """
        token = os.environ.get('token', None)
        if token is None:
            self.logger.info('Could not retrieve token for jenkinsbot')
            return False

        self.client = SlackClient(token)

        # This might only be required for listen()
        # Connect to real time messaging
        if not self.client.rtm_connect():
            self.logger.info('Could not connect to real time messaging (Slack).')
            return False

        return True

    def listen(self):
        """
        Establishes session and responds to commands
        """

        while True:
            try:
                incoming = []
                try:
                    incoming = list(self.client.rtm_read())
                except KeyboardInterrupt:
                    # Propagate the keyboard interrupt from the worker thread that received the shut-down command
                    raise
                except:
                    self.logger.exception('Could not read from Real Time Messaging. Connection may have been closed')

                    # Try to reconnect
                    if not self.connect_to_slack():
                        self.logger.info('Could not connect to Slack')
                        self.post_message(ADMIN_CHANNEL, 'Cannot connect to Slack')

                if len(incoming) == 0:
                    continue

                workers = []
                for data in incoming:
                    workers.append(WorkerThread(data, self))

                for worker in workers:
                        worker.start()

            except KeyboardInterrupt:
                # Raised by a worker thread that calls thread.interrupt_main()
                self.logger.info('Shutting down due to "shut-down" command or Control C')
                self.post_message(ADMIN_CHANNEL, 'Shutting down due to `shut-down` command or Control C')
                return

    def parse_text(self, text, channel, user):
        """
        Parses the text in valid incoming data to determine what command it is. Could raise an IndexError
        :param text: The text
        :param channel: The channel this text is coming from
        :param user: The user who wrote the text
        :return: The query, parameters, and filename derived from the text
        """
        # TODO replace with argparse or something similar

        query = ''
        params = {}
        filename = ''

        # Check to see if text is an alias
        alias_params = {
            'alias': text.strip(),
            'slack_user_id': user
        }
        table = self.query([channel], GA_QUERY, alias_params)
        if len(table) == 2:
            rows = table[1]
            if len(rows) > 0:
                command = rows[0]
                text = command[0]
        # Check to see setting alias or getting aliases
        if '=' in text:
            args = text.split('=')
            if len(args) != 2:
                self.post_message(channel, 'Couldn\'t parse alias.')
                return query, params, filename
            alias = args[0]
            command = args[1]
            query = AA_QUERY
            params = {
                'slack_user_id': user,
                'command': command.strip(),
                'alias': alias.strip()
            }
            return query, params, filename
        elif 'unalias' in text:
            args = text.split(' ')
            if len(args) != 2:
                return query, params, filename
            alias = args[1]
            query = RA_QUERY
            params = {
                'slack_user_id': user,
                'alias': alias.strip()
            }
            return query, params, filename
        elif 'aliases' in text:
            query = SA_QUERY
            params = {
                'slack_user_id': user
            }
            filename = 'aliases.txt'
            return query, params, filename

        # Check to see if dealing with normal command
        args = text.split(' ')
        if 'pro' in text:
            job = PRO
        elif 'com' in text:
            job = COMMUNITY
        else:
            if len(args) > 1:
                job = args[1]
            else:
                job = args[0]

        if 'recent-failure' in text:
            query = RF_QUERY
            params = {
                'job': job,
                'test': args[2],
            }
            filename = '%s-recent-failure.txt' % (args[2])
        elif 'recent-status' in text:
            query = RS_QUERY
            params = {
                'job': job,
                'test': args[2],
            }
            filename = '%s-recent-status.txt' % (args[2])
        elif 'all-failures' in text:
            query = AF_QUERY
            params = {
                'job': job
            }
            filename = '%s-allfailures.txt' % job
        elif 'days' in text:
            query = DL_QUERY
            params = {
                'job': job,
                'days': args[2]
            }
            filename = '%s-leaderboard-past-%s-days.txt' % (job, args[2])
        elif 'tests-since' in text:
            query = TS_QUERY
            params = {
                'job': job,
                'beginning': args[2]
            }
            filename = '%s-testssince-%s.txt' % (job, args[2])
        elif 'build-range' in text:
            builds = args[2].split('-')
            query = BR_QUERY
            params = {
                'job': job,
                'build_low': builds[0],
                'build_high': builds[1]
            }
            filename = '%s-buildrange-%s-to-%s.txt' % (job, builds[0], builds[1])
        else:
            self.post_message(channel, 'Couldn\'t parse: `' + text + '`\nType `help` to see command usage')

        return query, params, filename

    def query(self, channels, query, params, is_retry=False, insert=False):
        """
        Make a query and return a table
        :param channels: Channels this query is for
        :param query: Query to execute
        :param params: Parameters for the query
        :param is_retry: If this call of the query is a retry. Will not attempt to retry after calling.
        :param insert: This query is an insert so the data needs to be committed
        :return: Tuple of (headers, rows) as results
        """

        table = ()
        cursor = None
        database = None

        try:
            database = mysql.connector.connect(host=os.environ.get('dbhost', None),
                                               user=os.environ.get('dbuser', None),
                                               password=os.environ.get('dbpass', None),
                                               database=os.environ.get('dbdb', None))
            cursor = database.cursor()
            cursor.execute(query, params)

            if insert:
                database.commit()
            else:
                headers = list(cursor.column_names)  # List of strings
                rows = cursor.fetchall()  # List of tuples
                table = (headers, rows)

        except MySQLError:
            self.logger.exception('Either could not connect to database or execution error')
            for channel in channels:
                self.post_message(channel, 'Something went wrong with getting database information.')
                if query == AA_QUERY:
                    self.post_message(channel, 'Are you sure this alias isn\'t defined? Type `aliases` to see aliases.')
        except:
            self.logger.exception('Something unexpected went wrong')

            # Try to reconnect only once
            if self.connect_to_slack() and not is_retry:
                table = self.query(query, params, True, insert=insert)
        finally:
            if cursor is not None:
                cursor.close()
            if database is not None:
                database.close()
        return table

    def response(self, tables, channels, filename, vertical=False, edit=False, log=""):
        """
        Respond to a file to a channel
        :param tables: List of (header, rows) tuples i.e. tables to construct leaderboards from
        :param channels: The channels to post this file to
        :param filename: The filename to respond with
        :param vertical: Whether a vertical version of the table should be included
        :param edit: Whether the row entries should be edited
        :param log: Message prepended to the response
        """

        filecontent = ""
        for i, table in enumerate(tables):
            if len(table) != 2:
                continue
            headers = table[0]
            rows = table[1]
            content = ""

            if vertical:
                # If this is set generate a vertical leaderboard. Append to end of normal leaderboard.
                content = '\n\n*Vertical Leaderboard*:\n\n' + self.vertical_leaderboard(rows, headers)

            if edit:
                # Generate ordinal numbers (1st, 2nd, 3rd..)
                n = i + 1
                suffix = {1: "st", 2: "nd", 3: "rd"}.get(n if (n < 20) else (n % 10), 'th')
                log += '\n*Names in %d%s table might be shortened to fit on screen*' % (n, suffix)
                # Do some specific edits.
                self.edit_rows(rows)

            # Prepend leaderboard which might have edited rows.
            content = tabulate(rows, headers) + content
            filecontent = filecontent + content

        filecontent = log + '\n\n' + filecontent

        self.client.api_call(
            'files.upload', channels=channels, content=filecontent, filetype='text', filename=filename
        )

    def generate_html(self, tables, filename, message=''):
        with open(filename, 'r+') as html_file:
            table_html = """
<style style="text/css">
table {
    border-collapse: collapse;
    width: 100%;
    font-family: verdana,arial,sans-serif;
}
th, td {
    padding: 8px;
    border-bottom: 1px solid #ddd;
}
tr:hover{
    background-color:#f5f5f5
}
</style>
"""
            for table in tables:
                if len(table) != 2:
                    continue
                headers = table[0]
                rows = table[1]
                table_html += tabulate(rows, headers, tablefmt='html')
                html_file.write(table_html)

        if message:
            self.post_message(JUNIT, message)

    def vertical_leaderboard(self, rows, headers):
        """
        Displays each row in the table as one over the other. Similar to mysql's '\G'
        :param headers: Column names for the table
        :param rows: List of tuples representing the rows
        :return: A string representing the table vertically
        """
        # TODO (Femi) encapsulate in Leaderboard class
        table = ''
        for i, row in enumerate(rows):
            rows[i] = list(row)
            table += '%d\n' % (i + 1)
            for j, entry in enumerate(row):
                table += headers[j] + ': ' + str(entry) + '\n'
            table += '\n\n'
        return table

    def edit_rows(self, rows):
        """
        Edit the rows to fit on most screens.
        :param rows: Rows to edit. This method is specific to leaderboards.
        """
        # TODO (Femi) encapsulate in Leaderboard class
        for i, row in enumerate(rows):
            rows[i] = list(row)
            rows[i][0] = rows[i][0].replace('branch-2-', '')
            rows[i][0] = rows[i][0].replace('test-', '')
            rows[i][0] = rows[i][0].replace('nextrelease-', '')
            rows[i][1] = rows[i][1].replace('org.voltdb.', '')
            rows[i][1] = rows[i][1].replace('org.voltcore.', '')
            rows[i][1] = rows[i][1].replace('regressionsuites.', '')

    def leaderboard_query(self, jobs, days=30):
        """
        This constructs a completed leaderboard query which doesn't need parameters. Returns () for parameters
        :param jobs: The jobs to coalesce into a leaderboard
        :param days: Number of days to go back in query
        :return: A completed leaderboard query for the jobs and empty params
        """
        # TODO (Femi) encapsulate in Leaderboard class
        jobs_filter = map(lambda j: 'job="%s"' % j, jobs)
        job_params = ' OR '.join(jobs_filter)
        job_params = '(' + job_params + ')'

        # Leaderboard - See a leaderboard for jobs.
        junit_leaderboard_query = (
                          """
          SELECT job AS 'Job name',
                 name AS 'Test name',
                 fails AS 'Fails',
                 total AS 'Total',
                 fails/total*100. AS 'Fail %',
                 latest AS 'Latest failure'
            FROM
                (
                   SELECT job,
                          name,
                          COUNT(*) AS fails,
                          (
                           SELECT COUNT(*)
                             FROM `junit-builds` AS jb
                            WHERE jb.name = tf.job AND
                                  NOW() - INTERVAL 30 DAY <= jb.stamp
                          ) AS total,
                          MAX(tf.stamp) AS latest
                     FROM `junit-test-failures` AS tf
                    WHERE NOT status='FIXED' AND
                          """ + job_params + """ AND
                          NOW() - INTERVAL """ + str(days) + """ DAY <= tf.stamp
                 GROUP BY job,
                          name,
                          total
                ) AS intermediate
        ORDER BY 6 DESC
        """)

        return junit_leaderboard_query

    def post_message(self, channel, text):
        """
        Post a message on the channel.
        :param channel: Channel to post message to
        :param text: Text in message
        """
        self.client.api_call(
            'chat.postMessage', channel=channel, text=text, as_user=True
        )

    def get_log(self, jobs):
        """
        Get a log of Jenkins jobs. Logs to file and also returns a log message. Adds a query for each job. For now
        only works for junit jobs.
        :param jobs: List of job names
        :return: Log message string
        """
        tables = []
        for job in jobs:
            tables.append(self.query([ADMIN_CHANNEL], LB_QUERY, {'job': job}))

        log_message = ['Status of jobs:']
        for table in tables:
            if len(table) != 2:
                continue
            headers = table[0]
            rows = table[1][0]
            log = []
            for field, value in zip(headers, rows):
                log.append('%s: %s' % (field, value))
            log_message.append(', '.join(log))

        log = '\n'.join(log_message)
        self.logger.info(log)
        return log

    def query_and_response(self, query, params, channels, filename, vertical=False, edit=False, jobs=None):
        """
        Perform a single query and response
        :param query: Query to run
        :param params: Parameters for query
        :param channels: Channels to respond to
        :param filename: Filename for the post, or the html file
        :param vertical: Whether a vertical version of the table should be included
        :param edit: Whether the row entries should be edited
        :param jobs: Generate status logs for these jobs
        """
        table = self.query(channels, query, params)
        log = ''

        if jobs is not None:
            log = self.get_log(jobs)
            #self.response([table], channels, filename, vertical, edit, log=log)
        else:
            self.response_html()


    def get_jira_interface(self, username=JIRA_USER, password=JIRA_PASS):
        """ TODO
        """
        if not (username and password):
            self.logger.error('Did not provide either a Jira username ('
                              +username+') or a Jira password ('+password+').')

        try:
            jira_interface = JIRA(server='https://issues.voltdb.com/', basic_auth=(username, password), options=dict(verify=False))
#             jira_interface = JIRA(server='https://issues.voltdb.com/',
#                                    basic_auth=(username, password),
#                                    options=dict(verify=False))
        except Exception as e:
            self.logger.exception('Could not connect to Jira!!! Exception is:\n'+str(e))
            return None
        return jira_interface


    def find_jira_bug_tickets(self, summary_keys, labels,
                              jira=None, user=JIRA_USER, passwd=JIRA_PASS,
                              project=JIRA_PROJECT):
        """
        Finds one or more existing, open bug tickets in Jira.
        :param summary_keys: One or more substrings of the Summary, used to
               find a related, open Jira ticket; typically the first one is the Test Suite in which the failed
               test exists, and the second is the name of the test itself.
        :param labels: The Labels to list in the Jira ticket.
        :param jira: A JIRA access object, used to create a Jira ticket; if not
               specified, the user and passwd will be used to create one.
        :param user: The Jira Username used to access Jira.
        :param passwd: The Jira Password for that User.
        :param project: The Jira Project in which the Jira ticket should be created.
        """
        logging.debug('In find_jira_bug_tickets:')
        logging.debug('    summary_keys: '+str(summary_keys))
        logging.debug('    labels      : '+str(labels))
        logging.debug('    jira        : '+str(jira))
        logging.debug('    user        : '+str(user))
        logging.debug('    passwd      : '+str(passwd))
        logging.debug('    project     : '+str(project))

        if not jira:
            jira = self.get_jira_interface(user, passwd)
            logging.debug('    jira        : '+str(jira))

        labels_partial_query = ""
        if labels:
            labels_partial_query = "' AND labels = '" + "' AND labels = '".join(labels)
        summary_partial_query = " AND summary ~ '" + "' AND summary ~ '".join(summary_keys)
        full_jira_query = ("project = %s AND status != Closed"
                           + summary_partial_query + labels_partial_query
                           + "' ORDER BY key ASC"
                           ) % str(project)
        tickets = []
        try:
            tickets = jira.search_issues(full_jira_query)
        except Exception as e:
            logging.exception('Jira ticket query failed with Exception:'
                              '\n    %s\n    using Jira query:\n    %s'
                              % (str(e), full_jira_query) )

        logging.debug('    summary_partial_query: '+summary_partial_query)
        logging.debug('    full_jira_query:\n    '+str(full_jira_query))
        logging.debug('    tickets     : '+str(tickets))

        return tickets


    def find_jira_bug_ticket(self, summary_keys, labels,
                             jira=None, user=JIRA_USER, passwd=JIRA_PASS,
                             project=JIRA_PROJECT):
        """
        Finds (exactly) one existing, open bug ticket in Jira.
        :param summary_keys: One or more substrings of the Summary, used to
               find a related, open Jira ticket; typically the first one is the Test Suite in which the failed
               test exists, and the second is the name of the test itself.
        :param labels: The Labels to list in the Jira ticket.
        :param jira: A JIRA access object, used to create a Jira ticket; if not
               specified, the user and passwd will be used to create one.
        :param user: The Jira Username used to access Jira.
        :param passwd: The Jira Password for that User.
        :param project: The Jira Project in which the Jira ticket should be created.
        """
        logging.debug('In find_jira_bug_ticket:')
        logging.debug('    summary_keys: '+str(summary_keys))
        logging.debug('    labels      : '+str(labels))
        logging.debug('    jira        : '+str(jira))
        logging.debug('    user        : '+str(user))
        logging.debug('    passwd      : '+str(passwd))
        logging.debug('    project     : '+str(project))

        ticket = None

        existing_tickets = self.find_jira_bug_tickets(summary_keys, labels,
                                                      jira, user, passwd, project)
        if existing_tickets:
            if len(existing_tickets) > 1:
                logging.warn('More than 1 Jira ticket found; using first one listed below:\n'
                             +str(existing_tickets))
                for et in existing_tickets:
                    logging.warn("    %s: '%s'" % (et.key, et.fields.summary))

            ticket = existing_tickets[0]

        return ticket


    def add_attachments(self, jira, ticket_id, attachments):
        added_attachments = []
        for file in attachments:
            urlretrieve(attachments[file], file)
            a = jira.add_attachment(ticket_id, os.getcwd() + '/' + file, file)
            added_attachments.append(a)
            os.unlink(file)
        return added_attachments

    def enforce_max_num_attachments(self, jira, ticket, max_num_attachments=10):
        if len(ticket.fields.attachment) > max_num_attachments:
            attachment_ids = [a.id for a in ticket.fields.attachment]
            attachment_ids.sort(reverse=True)
            for i in range(max_num_attachments, len(attachment_ids)):
                jira.delete_attachment(attachment_ids[i])
                logging.info('Deleted, from ticket %s, attachment: %s'
                             % (str(ticket.key), str(attachment_ids[i])) )


    def is_number(self, s):
        try:
            return float(s)
        except ValueError:
            return False


    def get_jira_component_list(self, jira, component='Core',
                                project=JIRA_PROJECT):
        jira_component = component

        components = jira.project_components(project)
        for c in components:
            if c.name == component:
                jira_component = {
                    'name': c.name,
                    'id': c.id
                }
                break
        return [jira_component]


    def get_jira_version_list(self, jira, version='Autofiled',
                              project=JIRA_PROJECT):
        jira_version = version

        if not version.startswith('V') and self.is_number(version):
            version = 'V' + version

        versions = jira.project_versions(project)
        for v in versions:
            if str(v.name) == version.strip():
                jira_version = {
                    'name': v.name,
                    'id': v.id
                }
                break
        return [jira_version]


    def create_jira_bug_ticket(self, channel, test_suite, summary,
                               jenkins_job, build_number,
                               description, version, labels,
                               priority='Major', attachments={},
                               jira=None, user=JIRA_USER, passwd=JIRA_PASS,
                               project=JIRA_PROJECT, component='Core',
                               DRY_RUN=False):
        """
        Creates a new bug ticket in Jira.
        :param channel: A slack channel to be notified.
        :param test_suite: The Test Suite in which the failed test exists;
               used to find other bug tickets in the same Test Suite, which
               are marked 'is related to'.
        :param summary: The Summary to be used in the Jira ticket that is to
               be created.
        :param description: The Description for the new Jira ticket.
        :param version: The (VoltDB) Version that this bug affects.
        :param labels: The Labels to list in the Jira ticket.
        :param priority: The Priority of the Jira ticket.
        :param attachments: Any Attachments for the Jira ticket.
        :param component: The Component to be used in the Jira ticket, i.e.,
               the Component affected by this bug.
        :param jira: A JIRA access object, used to create a Jira ticket; if not
               specified, the user and passwd will be used to create one.
        :param user: The Jira Username used to access Jira.
        :param passwd: The Jira Password for that User.
        :param project: The Jira Project in which the Jira ticket should be created.
        :param DRY_RUN: When set to True, no Jira ticket will be created.
        """

        logging.debug('In create_jira_bug_ticket:')
        logging.debug('  channel     : '+str(channel))
        logging.debug('  test_suite  : '+str(test_suite))
        logging.debug('  summary     : '+str(summary))
        logging.debug('  description :\n'+str(description)+'\n')
        logging.debug('  version     : '+str(version))
        logging.debug('  labels      : '+str(labels))
        logging.debug('  priority    : '+str(priority))
        logging.debug('  attachments : '+str(attachments))
        logging.debug('  component   : '+str(component))
        logging.debug('  jira        : '+str(jira))
        logging.debug('  project     : '+str(project))
        logging.debug('  DRY_RUN     : '+str(DRY_RUN))

        if not jira:
            jira = self.get_jira_interface(user, passwd)
            logging.debug('  jira        : '+str(jira))

        issue_dict = {
            'project': project,
            'summary': summary,
            'description': description,
            'issuetype': {
                'name': 'Bug'
            },
            'labels': labels
        }

        issue_dict['components'] = self.get_jira_component_list(jira, component, project)
        issue_dict['versions']   = self.get_jira_version_list(jira, version, project)

        issue_dict['fixVersions'] = [{'name':'Autofiled'}]
        issue_dict['priority']    = {'name': priority}

        logging.debug("Filing ticket with summary:\n%s" % summary)
        logging.debug('  issue_dict  :\n    '+str(issue_dict))

        if DRY_RUN:
            new_issue = None
        # Kludge to prevent too many of these tickets:
        elif 'TestFixedSQLSuite' in summary:
            logging.warn("NOT creating the following ticket, with summary:"
                         +"\n    '%s'\nbecause it is for 'TestFixedSQLSuite':\n%s    "
                         % (summary, str(issue_dict)))
        else:
            try:
                new_issue = jira.create_issue(fields=issue_dict)
            except Exception as e:
                logging.exception("Jira ticket creation failed with Exception:"
                                  "\n    %s\n    using:\n    %s"
                                  % (str(e), str(issue_dict)) )
                raise e

            # Add attachments to the Jira ticket
            with_attachments = ''
            if attachments:
                try:
                    new_attachments = self.add_attachments(jira, new_issue.id, attachments)
                    with_attachments = ', with attachment' + ' (ID ' + ', '.join(
                        new_attachments[i].id for i in range(len(new_attachments)) ) + ')'
                except Exception as e:
                    with_attachments = ', without specified attachment'
                    logging.warn("Unable (in create_jira_bug_ticket) to add attachment(s):"
                                 "\n    '%s'\n  due to Exception:\n    %s"
                                 % (str(attachments), str(e)) )

            # Add a comment to the Jira ticket; and log a message
            logging_message = ("Filed ticket %s (https://issues.voltdb.com/browse/%s)%s, "
                               "with summary:\n    '%s'"
                               % (new_issue.key, new_issue.key,
                                  with_attachments, summary) )
            comment = ("Filed ticket due to %s, build #%s%s."
                       % (jenkins_job, build_number, with_attachments) )
            jira.add_comment(new_issue.key, comment)
            logging.info(logging_message)

            # Post a message in the specified slack channel (if any)
            try:
                if channel and self.connect_to_slack():
                    self.post_message(channel, logging_message)
            except Exception as e:
                logging.warn('Unable to connect to Slack!! (in create_jira_bug_ticket)')

            # Find all tickets within the same test suite and link them
            labels_partial_query = ""
            if labels:
                labels_partial_query = " AND labels = '" + "' AND labels = '".join(labels) + "'"
            full_jira_query = ("project = %s AND status != Closed AND summary ~ '%s'"
                               + labels_partial_query
                               ) % (str(project), str(test_suite))
            link_tickets = []
            try:
                link_tickets = jira.search_issues(full_jira_query)
            except TypeError as e:
                logging.warn('Caught TypeError('+str(e)+'), in create_jira_bug_ticket, using:'
                             '\n    labels_partial_query: '+str(labels_partial_query)+
                             '\n    test_suite          : '+str(test_suite) )

            for ticket in link_tickets:
                if ticket.key != new_issue.key:
                    jira.create_issue_link('Related', new_issue.key, ticket)
                    logging.debug('Linked ticket: %s' % str(ticket))

        return new_issue


    def summary_differs_significantly(self, old_summary, new_summary):
        """Determines whether the old and new (Jira ticket) summaries are
           'significantly' different: if they are identical, or if they are
           identical except for the failure percentage, returns False;
           otherwise, returns True.
        """
        if old_summary == new_summary:
            return False

        percent_sign = '%'
        if (percent_sign in old_summary and percent_sign in new_summary):
            old_index = old_summary.index(percent_sign)
            new_index = new_summary.index(percent_sign)
            if (old_summary[0:max(0,old_index-3)] == new_summary[0:max(0,new_index-3)]
                      and old_summary[old_index:] == new_summary[new_index:]):
                return False

        return True


    def modify_jira_bug_ticket(self, channel, summary_keys, summary,
                               jenkins_job, build_number,
                               description, version, labels,
                               priority='Major', attachments={}, ticket_to_modify=None,
                               jira=None, user=JIRA_USER, passwd=JIRA_PASS,
                               project=JIRA_PROJECT, component='Core',
                               max_num_attachments=10, DRY_RUN=False):
        """
        Modifies an existing bug ticket in Jira.
        # TODO: finish this doc:
        :param channel: A slack channel to be notified
        :param summary_keys ????: One or more substrings of the Summary, used to
               determine whether a Jira ticket already exists for this issue
        :param summary: The Summary to be used in the Jira ticket that is to
               be created or modified
        :param jenkins_job ????: One or more substrings of the Summary, used to
               determine whether a Jira ticket already exists for this issue
        :param description: The Description for the modified Jira ticket.
        :param version ???: The (VoltDB) Version that this bug affects
        :param labels ??: The Labels to list in the Jira ticket
        :param priority: The Priority of the Jira ticket
        :param attachments: Any Attachments for the Jira ticket
        :param component ??: The Component to be used in the Jira ticket, i.e.,
               the Component affected by this bug
        :param jira: A JIRA access object, used to modify a Jira ticket; if not
               specified, the user and passwd will be used to create one.
        :param user: The Jira Username used to access Jira.
        :param passwd: The Jira Password for that User.
        :param project: The Jira Project in which the Jira ticket should be modified
        :param DRY_RUN: When set to True, no Jira ticket will be modified
        """

        logging.debug('In modify_jira_bug_ticket:')
        logging.debug('  channel     : '+str(channel))
        logging.debug('  summary_keys: '+str(summary_keys))
        logging.debug('  summary     : '+str(summary))
        logging.debug('  jenkins_job : '+str(jenkins_job))
        logging.debug('  build_number: '+str(build_number))
        logging.debug('  description : '+str(description))
        logging.debug('  version     : '+str(version))
        logging.debug('  labels      : '+str(labels))
        logging.debug('  priority    : '+str(priority))
        logging.debug('  attachments : '+str(attachments))
        logging.debug('  component   : '+str(component))
        logging.debug('  jira        : '+str(jira))
        logging.debug('  project     : '+str(project))
        logging.debug('  max_num_attachments: '+str(max_num_attachments))
        logging.debug('  DRY_RUN     : '+str(DRY_RUN))

        if not jira:
            jira = self.get_jira_interface(user, passwd)
            logging.debug('    jira        : '+str(jira))

        if not ticket_to_modify:
            ticket_to_modify = self.find_jira_bug_ticket(summary_keys, labels,
                                                         jira, user, passwd, project)
            logging.debug('    ticket_to_modify: '+str(ticket_to_modify))

        if ticket_to_modify and not DRY_RUN:
            # Update the Jira ticket's summary, description, etc.
            previous_summary  = ticket_to_modify.fields.summary
            old_description   = ticket_to_modify.fields.description
            previous_priority = ticket_to_modify.fields.priority

            # If ticket has been marked as a "Blocker" (presumably manually),
            # then do not downgrade it
            if previous_priority == 'Blocker':
                priority = previous_priority

            # Try to update the Jira ticket without email notification; but if
            # that fails (as seems to happen fairly often, but unpredictably),
            # update it with email notification (which is the default)
            exception = None
            exception_count = 0
            with_attachments = ''
            for notification in [False, True]:
                try:
                    if notification:
                        with_attachments = ' (notify=True)'
                        ticket_to_modify.update(fields={'summary'    : summary,
                                                        'description': description,
                                                        'labels'     : labels,
                                                        'priority'   : {'name': priority}
                                                        }
                                                )
                    else:
                        with_attachments = ' (notify=False)'
                        ticket_to_modify.update(notify=False,
                                                fields={'summary'    : summary,
                                                        'description': description,
                                                        'labels'     : labels,
                                                        'priority'   : {'name': priority}
                                                        },
                                                )
                    break
                except Exception as e:
                    exception = e
                    exception_count += 1
                    logging.warn("Jira ticket update (notify=%s) failed with Exception:"
                                 "\n    %s"
                                 "\n    for Jira ticket %s, using:"
                                 "\n        version '%s', priority '%s', labels %s;"
                                 "\n    old and new summaries:"
                                 "\n        '%s'"
                                 "\n        '%s'"
                                 "\n    old description:"
                                 "\n        %s"
                                 "\n    new (updated) description:"
                                 "\n        %s\n"
                                 % (str(notification), str(e),
                                    str(ticket_to_modify.key),
                                    version, priority, str(labels),
                                    previous_summary, summary,
                                    old_description, description) )
            # If an exception was thrown for both values of 'notification',
            # throw the latter exception
            if exception_count > 1:
                raise exception

            # Add attachments to the Jira ticket
            if attachments:
                try:
                    new_attachments = self.add_attachments(jira, ticket_to_modify.id, attachments)
                    with_attachments += ', with attachment' + ' (ID ' + ', '.join(
                        new_attachments[i].id for i in range(len(new_attachments)) ) + ')'
                except Exception as e:
                    with_attachments += ', without specified attachment'
                    logging.warn("Unable (in modify_jira_bug_ticket) to add attachment(s):"
                                 "\n    '%s'\n  due to Exception:\n    %s"
                                 % (str(attachments), str(e)) )
                try:
                    self.enforce_max_num_attachments(jira, ticket_to_modify,
                                                     max_num_attachments - len(new_attachments) )
                except Exception as e:
                    logging.warn("Unable (in modify_jira_bug_ticket) to enforce max. number "
                                 "of attachments (%d) for %s, due to Exception:\n    %s"
                                 % (max_num_attachments, str(ticket_to_modify.key), str(e)) )

            # Add a comment to the Jira ticket, if appropriate; and log a message
            message1 = ("Modified ticket %s (https://issues.voltdb.com/browse/%s)"
                        % (ticket_to_modify.key, ticket_to_modify.key) )
            if previous_summary == summary:
                logging_message = ("%s%s, with summary unchanged:\n    '%s'"
                                   % (message1, with_attachments, summary))
            else:
                message2 = ("%s, with summary changed from/to:\n    '%s'\n    '%s'"
                            % (with_attachments, previous_summary, summary) )
                logging_message = message1 + message2
                if self.summary_differs_significantly(previous_summary, summary):
                    comment = ("Modified ticket due to %s, build #%s%s"
                               % (jenkins_job, build_number, message2) )
                    jira.add_comment(ticket_to_modify.key, comment)
            logging.info(logging_message)

            # Post a message in the specified slack channel (if any)
            try:
                if channel and self.connect_to_slack():
                    self.post_message(channel, logging_message)
            except Exception as e:
                logging.warn('Unable to connect to Slack!! (in modify_jira_bug_ticket)')

        return ticket_to_modify


    def create_or_modify_jira_bug_ticket(self, channel, summary_keys, summary,
                                         jenkins_job, build_number,
                                         description, version, labels,
                                         priority='Major', attachments={}, existing_ticket=None,
                                         jira=None, user=JIRA_USER, passwd=JIRA_PASS,
                                         project=JIRA_PROJECT, component='Core',
                                         max_num_attachments=10, DRY_RUN=False):
        """
        Creates a new bug ticket in Jira, or modifies an existing one.
        :param channel: A slack channel to be notified.
        :param summary_keys: One or more substrings of the Summary, used to
               determine whether a Jira ticket already exists for this issue;
               typically the first one is the Test Suite in which the failed
               test exists, and the second is the name of the test itself.
        :param summary: The Summary to be used in the Jira ticket that is to
               be created or modified.
        :param jenkins_job ????
        :param description: The Description for the new or modified Jira ticket.
        :param version: The (VoltDB) Version that this bug affects.
        :param labels: The Labels to list in the Jira ticket.
        :param priority: The Priority of the Jira ticket.
        :param attachments: Any Attachments for the Jira ticket.
        :param component: The Component to be used in the Jira ticket, i.e.,
               the Component affected by this bug.
        :param jira: A JIRA access object, used to create a Jira ticket; if not
               specified, the user and passwd will be used to create one.
        :param user: The Jira Username used to access Jira.
        :param passwd: The Jira Password for that User.
        :param project: The Jira Project in which the Jira ticket should be reported.
        :param DRY_RUN: When set to True, no Jira ticket will be created or modified.
        """
        logging.debug('In create_or_modify_jira_bug_ticket:')

        if not project:
            self.logger.error('Did not provide a Jira project ('+project+').')
            return

        if not jira:
            jira = self.get_jira_interface(user, passwd)
            logging.debug('  jira        : '+str(jira))

        logging.debug('  channel     : '+str(channel))
        logging.debug('  summary_keys: '+str(summary_keys))
        logging.debug('  summary     : '+str(summary))
        logging.debug('  jenkins_job : '+str(jenkins_job))
        logging.debug('  description : '+str(description))
        logging.debug('  version     : '+str(version))
        logging.debug('  labels      : '+str(labels))
        logging.debug('  priority    : '+str(priority))
        logging.debug('  attachments : '+str(attachments))
        logging.debug('  component   : '+str(component))
        logging.debug('  user        : '+str(user))
        logging.debug('  passwd      : '+str(passwd))
        logging.debug('  project     : '+str(project))
        logging.debug('  DRY_RUN     : '+str(DRY_RUN))

        # Check for existing tickets for the same issue; if there are any,
        # we'll modify the existing ticket, rather than creating a new one
        if len(summary_keys) < 1:
            self.logger.exception('No summary_keys ('+summary_keys+') specified')
            return None

        if not existing_ticket:
            existing_ticket = self.find_jira_bug_ticket(summary_keys, labels,
                                                        jira, user, passwd, project)
            logging.debug('  existing_ticket: '+str(existing_ticket))

        # There is an existing ticket, so modify it
        if existing_ticket:
            self.logger.debug("Found open issue(s) for " + str(summary_keys) + "': "
                             + str(existing_ticket))

            return self.modify_jira_bug_ticket(channel, summary_keys, summary,
                                               jenkins_job, build_number,
                                               description, version, labels,
                                               priority, attachments, existing_ticket,
                                               jira, user, passwd,
                                               project, component,
                                               max_num_attachments, DRY_RUN)

        # There is no existing ticket, so create one
        else:
            self.logger.debug("Found no open issues for " + str(summary_keys))

            return self.create_jira_bug_ticket(channel, summary_keys[0], summary,
                                               jenkins_job, build_number,
                                               description, version, labels,
                                               priority, attachments,
                                               jira, user, passwd,
                                               project, component, DRY_RUN)


    def close_jira_bug_ticket(self, channel, summary_keys,
                              jenkins_job, build_number,
                              labels, ticket_to_close=None,
                              jira=None, user=JIRA_USER, passwd=JIRA_PASS,
                              project=JIRA_PROJECT, DRY_RUN=False):

        if not jira:
            jira = self.get_jira_interface(user, passwd)
            logging.debug('    jira        : '+str(jira))

        if not ticket_to_close:
            ticket_to_close = self.find_jira_bug_ticket(summary_keys, labels,
                                                        jira, user, passwd, project)
            logging.debug('    ticket_to_close: '+str(ticket_to_close))

        if ticket_to_close and not DRY_RUN:
            transitions = jira.transitions(ticket_to_close)
            logging.info("Available transitions for ticket '"+str(ticket_to_close)+"':\n"+str(transitions))
            try:
                jira.transition_issue(ticket_to_close, transition='Close Issue')
            except Exception as e:
                logging.exception("Closing Jira ticket %s failed with Exception:\n    %s"
                                  % (str(ticket_to_close.key), str(e)) )
                raise e

            logging_message = ("Closed ticket %s (https://issues.voltdb.com/browse/%s), "
                               "with summary:\n    '%s'"
                               % (ticket_to_close.key, ticket_to_close.key,
                                  ticket_to_close.field.summary) )
            comment = ("Closed ticket after %s, build #%s."
                       % (jenkins_job, build_number) )
            jira.add_comment(ticket_to_close.key, comment)
            logging.info(logging_message)

            # Post a message in the specified slack channel (if any)
            try:
                if channel and self.connect_to_slack():
                    self.post_message(channel, message)
            except Exception as e:
                logging.warn('Unable to connect to Slack!! (in close_jira_bug_ticket)')

        return ticket_to_close


    # TODO: this may be obsolete, but I'm not sure yet
    def create_bug_issue(self, channel, summary, description, component, version, labels, attachments={},
                         user=JIRA_USER, passwd=JIRA_PASS, project=JIRA_PROJECT, DRY_RUN=False):
        """
        Creates a bug ticket in Jira
        :param channel: The channel to notify
        :param summary: The title summary
        :param description: Description field
        :param component: Component bug affects
        :param version: Version this bug affects
        :param labels: Labels to attach to the issue
        :param user: User to report bug as
        :param passwd: Password
        :param project: Jira project
        """

        def add_attachments(jira, ticketId, attachments):
            for file in attachments:
                urlretrieve(attachments[file], file)
                jira.add_attachment(ticketId, os.getcwd() + '/' + file, file)
                os.unlink(file)

        if user and passwd and project:
            try:
                jira = JIRA(server='https://issues.voltdb.com/', basic_auth=(user, passwd), options=dict(verify=False))
            except:
                self.logger.exception('Could not connect to Jira')
                return
        else:
            self.logger.error('Did not provide either a Jira user, a Jira password or a Jira project')
            return

        # Check for existing bugs for the same test case, if there are any, suppress filing another
        test_case = summary.split(' ')[0]
        existing = jira.search_issues('summary ~ \'%s\' and labels = automatic and status != Closed' % test_case)
        if len(existing) > 0:
            self.logger.info('Found open issue(s) for "' + test_case + '" ' + ' '.join([k.key for k in existing]))

            # Check if new failure is on different job than existing ticket, if so comments
            job = summary.split()[-2]
            existing_ticket = jira.issue(existing[0].id)
            if job not in existing_ticket.fields.summary:
                comments = jira.comments(existing[0].id)
                for comment in comments:
                    # Check for existing comment for same job, if there are any, suppress commenting another
                    if job in comment.body:
                        self.logger.info('Found existing comment(s) for "' + job + '" on open issue')
                        return

                self.logger.info('Commenting about separate job failure for %s on open issue' % test_case)
                if not DRY_RUN:
                    jira.add_comment(existing[0].id, summary + '\n\n' + description)
                    add_attachments(jira, existing[0].id, attachments)
            return

        issue_dict = {
            'project': project,
            'summary': summary,
            'description': description,
            'issuetype': {
                'name': 'Bug'
            },
            'labels': labels
        }

        jira_component = None
        components = jira.project_components(project)
        for c in components:
            if c.name == component:
                jira_component = {
                    'name': c.name,
                    'id': c.id
                }
                break
        if jira_component:
            issue_dict['components'] = [jira_component]
        else:
            # Components is still a required field
            issue_dict['components'] = ['Core']

        jira_version = None
        versions = jira.project_versions(project)
        version = 'V' + version
        for v in versions:
            if str(v.name) == version.strip():
                jira_version = {
                    'name': v.name,
                    'id': v.id
                }
                break
        if jira_version:
            issue_dict['versions'] = [jira_version]
        else:
            # Versions is still a required field
            issue_dict['versions'] = ['DEPLOY-Integration']

        issue_dict['fixVersions'] = [{'name':'Autofiled'}]
        issue_dict['priority'] = {'name': 'Critical'}

        self.logger.info("Filing ticket: %s" % summary)
        if not DRY_RUN:
            new_issue = jira.create_issue(fields=issue_dict)
            add_attachments(jira, new_issue.id, attachments)
            #self.logger.info('NEW: Reported issue with summary "' + summary + '"')
            if self.connect_to_slack():
                self.post_message(channel, 'Opened issue at https://issues.voltdb.com/browse/' + new_issue.key)
            suite = summary.split('.')[-3]
            # Find all tickets within same test suite and link them
            link_tickets = jira.search_issues('summary ~ \'%s\' and labels = automatic and status != Closed and reporter in (voltdbci)' % suite)
            for ticket in link_tickets:
                jira.create_issue_link('Related', new_issue.key, ticket)
        else:
            new_issue = None

        return new_issue


if __name__ == '__main__':
    jenkinsbot = JenkinsBot()
    help_text = """
            usage: jenkinsbot <listen|master|core|system|html>
                   listen - bring jenkinsbot online (do not use if already running)
                   master - post the master branch junit leaderboard on Slack
                   core - post the core extended junit leaderboard on Slack
                   system - post the master systems test leaderboard on Slack
                   html - generate html files of the leaderboards
            """
    if not jenkinsbot.connect_to_slack():
        print 'Not able to connect to Slack. Is the "token" environment variable set?'
        sys.exit(1)
    if len(sys.argv) == 2:
        if sys.argv[1] == 'listen':
            jenkinsbot.listen()
        elif sys.argv[1] == 'master':
            query = jenkinsbot.leaderboard_query(MASTER_JOBS, days=30)
            jenkinsbot.query_and_response(
                query,
                (),
                [JUNIT],
                'master-past30days.txt',
                vertical=True,
                edit=True,
                jobs=MASTER_JOBS
            )
        elif sys.argv[1] == 'core':
            query = jenkinsbot.leaderboard_query(CORE_JOBS, days=7)
            jenkinsbot.query_and_response(
                query,
                (),
                [JUNIT],
                'coreextended-past7days.txt',
                vertical=True,
                edit=True,
                jobs=CORE_JOBS
            )
        elif sys.argv[1] == 'system':
            jenkinsbot.query_and_response(
                SL_QUERY,
                (),
                [JUNIT],
                'systems-master-past30days.txt',
                vertical=True
            )
        elif sys.argv[1] == 'html':
            master_query = jenkinsbot.leaderboard_query(MASTER_JOBS, days=30)
            core_query = jenkinsbot.leaderboard_query(CORE_JOBS, days=7)
            system_query = SL_QUERY
            #master_table = jenkinsbot.query(ADMIN_CHANNEL, master_query, ())
            #core_table = jenkinsbot.query(ADMIN_CHANNEL, core_query, ())
            #system_table = jenkinsbot.query(ADMIN_CHANNEL, system_query, ())
            #jenkinsbot.generate_html([master_table], 'master-junit-leaderboard.html', 'Leaderboard updated at http://ci.voltdb.lan/job/leaderboards/lastSuccessfulBuild/artifact/master-junit-leaderboard.html')
            #jenkinsbot.generate_html([core_table], 'core-junit-leaderboard.html', 'Leaderboard updated at http://ci.voltdb.lan/job/leaderboards/lastSuccessfulBuild/artifact/core-junit-leaderboard.html')
            #jenkinsbot.generate_html([system_table], 'systems-master-leaderboard.html', 'Leaderboard updated at http://ci.voltdb.lan/job/leaderboards/lastSuccessfulBuild/artifact/systems-master-leaderboard.html')
        else:
            print 'Command %s not found' % sys.argv[1]
            print help_text
    else:
        print 'Incorrect number of arguments'
        print help_text
