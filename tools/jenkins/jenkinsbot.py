#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.

# Script that runs jenkinsbot for VoltDB Slack

import logging
import os
import sys
import time

import mysql.connector

from jira import JIRA
from logging import handlers
from mysql.connector.errors import Error as MySQLError
from slackclient import SlackClient
from tabulate import tabulate  # Used for pretty printing tables

# Get job names from environment variables
COMMUNITY = os.environ.get('community', None)
PRO = os.environ.get('pro', None)
VDM = os.environ.get('vdm', None)
MEMVALDEBUG = os.environ.get('memvaldebug', None)
DEBUG = os.environ.get('debug', None)
MEMVAL = os.environ.get('memval', None)
FULLMEMCHECK = os.environ.get('fullmemcheck', None)

# Channel to message in case something goes wrong
ADMIN_CHANNEL = os.environ.get('admin', None)

# Other channels
GENERAL_CHANNEL = os.environ.get('general', None)
RANDOM_CHANNEL = os.environ.get('random', None)
JUNIT = os.environ.get('junit', None)

# Jira credentials and info
JIRA_USER = os.environ.get('jirauser', None)
JIRA_PASS = os.environ.get('jirapass', None)
JIRA_PROJECT = os.environ.get('jiraproject', None)

# Queries

# Test Leaderboard - See the tests that have been failing the most since a certain build.
TL_QUERY = ("""
  SELECT tf.name AS 'Test name',
         COUNT(*) AS 'Failures'
    FROM `junit-test-failures` AS tf
   WHERE NOT tf.status='FIXED' AND
         tf.build >= %(beginning)s AND
         tf.job=%(job)s
GROUP BY tf.name
ORDER BY 2 DESC
""")

# Days Leaderboard - See the tests that has been failing the most in the past amount of days.
D_QUERY = ("""
  SELECT tf.name AS 'Test name',
         COUNT(*) AS 'Failures'
    FROM `junit-test-failures` AS tf
   WHERE NOT tf.status='FIXED' AND
         NOW() - INTERVAL %(days)s DAY <= tf.stamp AND
         tf.job=%(job)s
GROUP BY tf.name
ORDER BY 2 DESC
""")

# Build Range Leaderboard - See the tests that have been failing the most in a range of builds.
BR_QUERY = ("""
    SELECT tf.name AS 'Test name',
           COUNT(*) AS 'Number of failures in this build range'
      FROM `junit-test-failures` AS tf
INNER JOIN `junit-builds` AS jr
        ON NOT tf.status='FIXED' AND
           jr.name=tf.job AND
           jr.build=tf.build AND
           jr.name=%(job)s AND
           %(build_low)s <= jr.build AND
           jr.build <= %(build_high)s
  GROUP BY tf.name,
           tf.job
  ORDER BY 2 DESC
""")

# Test On Master - See if test is also failing on master and how recently.
TOM_QUERY = ("""
    SELECT MAX(tf.build) AS 'Most recent failure on master',
           MAX(jr.build) AS 'Most recent build of master'
      FROM `junit-test-failures` AS tf
RIGHT JOIN `junit-builds` AS jr
        ON NOT tf.status='FIXED' AND
           jr.name=tf.job AND
           tf.name=%(test)s AND
           jr.name=%(job)s
""")

# Master Leaderboard - See a specific leaderboard for three jobs.
ML_QUERY = ("""
  SELECT job AS 'Job name',
         name AS 'Test name',
         fails AS 'Fails',
         total AS 'Total',
         fails/total*100. AS "Fail %",
         latest AS 'Latest failure'
    FROM
        (
           SELECT job,
                  name,
                  (
                   SELECT COUNT(*)
                     FROM `junit-builds` AS jr
                    WHERE jr.name = tf.job AND
                          NOW() - INTERVAL 30 DAY <= jr.stamp
                  ) AS total,
                  COUNT(*) AS fails,
                  MAX(tf.stamp) AS latest
             FROM `junit-test-failures` AS tf
            WHERE NOT status='FIXED' AND
                  (job=%(jobA)s OR job=%(jobB)s OR job=%(jobC)s) AND
                  NOW() - INTERVAL 30 DAY <= tf.stamp
         GROUP BY job,
                  name,
                  total
        ) AS intermediate
ORDER BY 5 DESC
""")

# Core Extended Leaderboard - See a specific leaderboard for three jobs.
CL_QUERY = ("""
  SELECT job AS 'Job name',
         name AS 'Test name',
         fails AS 'Fails',
         total AS 'Total',
         fails/total*100. AS "Fail %",
         latest AS 'Latest failure'
    FROM
        (
           SELECT job,
                  name,
                  (
                   SELECT COUNT(*)
                     FROM `junit-builds` AS jr
                    WHERE jr.name = tf.job AND
                          NOW() - INTERVAL 2 DAY <= jr.stamp
                  ) AS total,
                  COUNT(*) AS fails,
                  MAX(tf.stamp) AS latest
             FROM `junit-test-failures` AS tf
            WHERE NOT status='FIXED' AND
                  (job=%(jobA)s OR job=%(jobB)s OR job=%(jobC)s OR job=%(jobD)s) AND
                  NOW() - INTERVAL 2 DAY <= tf.stamp
         GROUP BY job,
                  name,
                  total
        ) AS intermediate
ORDER BY 5 DESC
""")

# All Failures - See all failures for a job over time and view them as most recent.
AF_QUERY = ("""
  SELECT tf.name AS 'Test name',
         tf.build AS 'Build',
         tf.stamp AS 'Time'
    FROM `junit-test-failures` AS tf
   WHERE NOT STATUS='FIXED' AND
         tf.job=%(job)s
ORDER BY 2 DESC
""")

# Add alias - Add an alias for the user.
AA_QUERY = ("""
    INSERT INTO `jenkinsbot-user-aliases`
                (slack_user_id, command, alias)
         VALUES (%(slack_user_id)s, %(command)s, %(alias)s)
""")

# Get alias - Get the command for an alias for the user.
GA_QUERY = ("""
    SELECT command
      FROM `jenkinsbot-user-aliases`
     WHERE alias=%(alias)s AND
           slack_user_id=%(slack_user_id)s
""")


class JenkinsBot(object):
    def __init__(self):
        self.client = None
        self.help_text = \
            ['*help*\n',
             'Alias a command:\n\tmy alias=*valid command*\n',
             'See which tests are failing the most since this build:\n\t*test-leaderboard* <job> <build #>\n',
             'See which tests are failing in the past x days:\n\t*days* <job> <days> \n',
             'Failing the most in this build range:\n\t*build-range* <job> <build #>-<build #>\n',
             'Most recent failure on master:\n\t*test-on-master* <job> <testname> (ex. testname: org.voltdb.iv2'
             '..)\n',
             'All failures for a job:\n\t*all-failures* <job>\n',
             'Display this help:\n\t*help*\n'
             'For any <job>, you can specify *"pro"* or *"com"* for the master jobs\n',
             'Examples: test-leaderboard pro 860, days com 14, now=days pro 1\n']
        self.logger = self.setup_logging()

    def setup_logging(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        # Limit log file to 1GB
        handler = handlers.RotatingFileHandler('jenkinsbot.log', maxBytes=1 << 30)
        handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logger.addHandler(handler)

        return logger

    def connect_to_slack(self):
        """
        :return: true if token for bot exists, client was created, and bot connected to Real Time Messaging
        """
        token = os.environ.get('token', None)
        if token is None:
            self.logger.info('Could not retrieve token for jenkinsbot')
            return False

        self.client = SlackClient(token)

        # Connect to real time messaging
        if not self.client.rtm_connect():
            self.logger.info('Could not connect to real time messaging')
            return False

        return True

    def can_reply(self, incoming):
        """
        :param incoming: A dictionary describing what data is incoming to the bot
        :return: true if bot can act on incoming data - There is incoming data, it is text data, it's not from a bot,
        the text isn't in a file, the channel isn't in #general,  #random, #junit which jenkinsbot is part of
        """
        # TODO rather than check if is channel, just check this is an instant message
        return (len(incoming) > 0 and incoming[0].get('text', None) is not None
                and incoming[0].get('bot_id', None) is None and incoming[0].get('file', None) is None
                and incoming[0]['channel'] != GENERAL_CHANNEL and incoming[0]['channel'] != RANDOM_CHANNEL
                and incoming[0]['channel'] != JUNIT)

    def listen(self):
        """
        Establishes session and responds to commands
        """
        # Don't edit or make vertical tables in listen mode
        os.environ['vertical'] = ''
        os.environ['edit'] = ''

        while True:
            channel = ""
            try:
                # Wait for and respond to incoming data that is: text, not from a bot, not a file.
                # Could possibly loop through incoming to address each incoming messsage rather than using just
                # incoming[0]
                incoming = list(self.client.rtm_read())
                if self.can_reply(incoming):
                    text = incoming[0].get('text', None)
                    channel = incoming[0].get('channel', None)
                    user = incoming[0].get('user', None)
                    if 'end-session' in text:
                        # Command for making jenkinsbot inactive. Script has to be run again with proper environment
                        # variables to turn jenkinsbot on.
                        self.post_message(channel, 'Leaving...')
                        sys.exit(0)
                    elif 'help' in text:
                        self.post_message(channel, ''.join(self.help_text))
                    else:
                        (query, params, filename) = self.parse_text(text, channel, user)
                        if query and params and filename:
                            self.query_and_response(query, params, [channel], filename)
                        elif query and params:
                            self.query([channel], query, params, insert=True)
            except (KeyboardInterrupt, SystemExit):
                self.logger.info('Turning off the bot due to "end-session" command')
                self.post_message(ADMIN_CHANNEL, 'Turning off the bot due to "end-session" command')
                return
            except IndexError:
                self.logger.exception('Incorrect number or formatting of arguments')
                if channel:
                    self.post_message(channel, 'Incorrect number or formatting of arguments\n\n' +
                                      ''.join(self.help_text))
            except:
                self.logger.exception('Something unexpected went wrong')

                # Try to reconnect
                if not self.connect_to_slack():
                    self.logger.info('Could not connect to Slack')
                    self.logger.info('Turning off the bot. Cannot connect to Slack')
                    self.post_message(ADMIN_CHANNEL, 'Turning off the bot. Cannot connect to Slack')
                    return

            # Slow but reconfigurable
            time.sleep(1)

    def parse_text(self, text, channel, user):
        """
        Parses the text in valid incoming data to determine what command it is
        :param text: The text
        :param channel: The channel this text is coming from
        :param user: The user who wrote the text
        :return: The query, parameters, and filename derived from the text
        """
        # TODO replace with argparse or something similar

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

        query = ''
        params = {}
        filename = ''

        if 'pro' in text:
            job = PRO
        elif 'com' in text:
            job = COMMUNITY
        else:
            args = text.split(' ')
            if len(args) > 1:
                job = args[1]
            else:
                self.post_message(channel, "Couldn't parse: " + text)
                self.post_message(channel, self.help_text)
                return query, params, filename

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
                'command': command,
                'alias': alias
            }
            filename = ''
        elif 'test-on-master' in text:
            args = text.split(' ')
            query = TOM_QUERY
            params = {
                'job': job,
                'test': args[2],
            }
            filename = '%s-testonmaster.txt' % (args[2])
        elif 'all-failures' in text:
            query = AF_QUERY
            params = {
                'job': job
            }
            filename = '%s-allfailures.txt' % job
        elif 'days' in text:
            args = text.split(' ')
            query = D_QUERY
            params = {
                'job': job,
                'days': args[2]
            }
            filename = '%s-leaderboard-past-%s-days.txt' % (job, args[2])
        elif 'test-leaderboard' in text:
            args = text.split(' ')
            query = TL_QUERY
            params = {
                'job': job,
                'beginning': args[2]
            }
            filename = '%s-testleaderboard-from-%s.txt' % (job, args[2])
        elif 'build-range' in text:
            args = text.split(' ')
            builds = args[2].split('-')
            query = BR_QUERY
            params = {
                'job': job,
                'build_low': builds[0],
                'build_high': builds[1]
            }
            filename = '%s-buildrange-%s-to-%s.txt' % (job, builds[0], builds[1])

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

            if not insert:
                headers = list(cursor.column_names)
                rows = cursor.fetchall()  # List of tuples
                table = (headers, rows)
            else:
                database.commit()

        except MySQLError:
            self.logger.exception('Either could not connect to database or execution error')
            for channel in channels:
                self.post_message(channel, 'Something went wrong with the query.')
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

    def response(self, tables, channels, filename, vertical=False, edit=False):
        """
        Respond to a file to a channel
        :param tables: List of (header, rows) tuples i.e. tables to construct leaderboards from
        :param channels: The channels to post this file to
        :param filename: The filename to respond with
        :param vertical: whether a vertical version of the table should be included
        :param edit: whether the row entries should be edited
        """

        filecontent = ""
        for table in tables:
            if len(table) != 2:
                continue
            headers = table[0]
            rows = table[1]
            content = ""

            if vertical:
                # If this is set generate a vertical leaderboard. Append to end of normal leaderboard.
                content = '\n\n*Vertical Leaderboard*:\n\n' + self.vertical_leaderboard(rows, headers)

            if edit:
                # Do some specific edits.
                self.edit_rows(rows)

            # Prepend leaderboard which might have edited rows.
            content = tabulate(rows, headers) + content
            filecontent = filecontent + content

        self.client.api_call(
            'files.upload', channels=channels, content=filecontent, filetype='text', filename=filename
        )

    def vertical_leaderboard(self, rows, headers):
        """
        Displays each row in the table as one over the other. Similar to mysql's '\G'
        :param headers: Column names for the table
        :param rows: List of tuples representing the rows
        :return: A string representing the table vertically
        """
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
        :param rows: Rows to edit. This method is specific to ML_QUERY and CL_QUERY
        """
        for i, row in enumerate(rows):
            rows[i] = list(row)
            rows[i][0] = rows[i][0].replace('branch-2-', '')
            rows[i][0] = rows[i][0].replace('test-', '')
            rows[i][0] = rows[i][0].replace('nextrelease-', '')
            rows[i][1] = rows[i][1].replace('org.voltdb.', '')
            rows[i][1] = rows[i][1].replace('org.voltcore.', '')
            rows[i][1] = rows[i][1].replace('regressionsuites.', '')

    def post_message(self, channel, text):
        """
        Post a message on the channel.
        :param channel: Channel to post message to
        :param text: Text in message
        """
        self.client.api_call(
            'chat.postMessage', channel=channel, text=text, as_user=True
        )

    def query_and_response(self, query, params, channels, filename, vertical=False, edit=False):
        """
        Perform a single query and response
        :param query: Query to run
        :param params: Parameters for query
        :param channels: Channels to respond to
        :param filename: filename for the post
        :param vertical: whether a vertical version of the table should be included
        :param edit: whether the row entries should be edited
        """
        table = self.query(channels, query, params)
        self.response([table], channels, filename, vertical, edit)

    def create_bug_issue(self, channel, summary, description, component, version, label,
                         user=JIRA_USER, passwd=JIRA_PASS, project=JIRA_PROJECT):
        """
        Creates a bug issue on Jira
        :param channel: The channel to notify
        :param summary: The title summary
        :param description: Description field
        :param component: Component bug affects
        :param version: Version this bug affects
        :param label: Label to attach to the issue
        :param user: User to report bug as
        :param passwd: Password
        :param project: Jira project
        """
        if user and passwd and project:
            try:
                jira = JIRA(server='https://issues.voltdb.com/', basic_auth=(user, passwd))
            except:
                self.logger.exception('Could not connect to Jira')
                return
        else:
            self.logger.error('Did not provide either a Jira user, a Jira password or a Jira project')
            return

        # Check for existing bug with same summary
        existing = jira.search_issues('summary ~ \'%s\'' % summary)
        if len(existing) > 0:
            # Already reported
            self.logger.info('OLD: Already reported issue with summary "' + summary + '"')
            return

        issue_dict = {
            'project': project,
            'summary': summary,
            'description': description,
            'issuetype': {
                'name': 'Bug'
            },
            'labels': [label]
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

        jira_version = None
        versions = jira.project_versions(project)
        for v in versions:
            if v.name == version:
                jira_version = {
                    'name': v.name,
                    'id': v.id
                }
                break
        if jira_version:
            issue_dict['versions'] = [jira_version]

        new_issue = jira.create_issue(fields=issue_dict)

        self.logger.info('NEW: Reported issue with summary "' + summary + '"')

        if self.connect_to_slack():
            self.post_message(channel, 'Opened issue at https://issues.voltdb.com/browse/' + new_issue.key)


if __name__ == '__main__':
    jenkinsbot = JenkinsBot()
    if jenkinsbot.connect_to_slack() and len(sys.argv) == 2:
        if sys.argv[1] == 'listen':
            jenkinsbot.listen()
        elif sys.argv[1] == 'master-leaderboard':
            jenkinsbot.query_and_response(
                ML_QUERY,
                {'jobA': PRO, 'jobB': COMMUNITY, 'jobC': VDM},
                [JUNIT],
                'master-past30days.txt',
                vertical=True,
                edit=True
            )
        elif sys.argv[1] == 'core-leaderboard':
            jenkinsbot.query_and_response(
                CL_QUERY,
                {'jobA': MEMVALDEBUG, 'jobB': DEBUG, 'jobC': MEMVAL, 'jobD': FULLMEMCHECK},
                [JUNIT],
                'coreextended-past2days.txt',
                vertical=True,
                edit=True
            )
