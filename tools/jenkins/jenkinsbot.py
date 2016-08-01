#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.

# Script that runs jenkinsbot for VoltDB Slack

import os
import sys
import time
from datetime import datetime

import mysql.connector

from mysql.connector.errors import Error as MySQLError
from slackclient import SlackClient
# Used for pretty printing tables
from tabulate import tabulate

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

# Core Extended Leaderboard - See a specific leaderboard for two jobs.
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


class JenkinsBot(object):
    def __init__(self):
        self.client = None
        self.logfile = 'jenkinslog.txt'

    def log(self, message):
        message = str(message)
        try:
            size = os.path.getsize(self.logfile)
        except OSError:
            message = self.logfile + ' does not exist. Creating..\n' + message
            size = 0
        if size < (1 << 30):
            # Limit log file to 1G
            with open(self.logfile, 'a') as logfile:
                logfile.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                logfile.write(' ' + message + '\n')
        else:
            self.post_message(ADMIN_CHANNEL, 'Log file too large.')

    def connect_to_slack(self):
        """
        :return: true if token for bot exists, client was created, and bot connected to Real Time Messaging
        """
        token = os.environ.get('token', None)
        if token is None:
            self.log('Could not retrieve token for jenkinsbot')
            return False

        self.client = SlackClient(token)

        # Connect to real time messaging
        if not self.client.rtm_connect():
            self.log('Could not connect to real time messaging')
            return False

        return True

    def can_reply(self, incoming):
        """
        :param incoming: A dictionary describing what data is incoming to the bot
        :return: true if bot can act on incoming data
        """
        return (len(incoming) > 0 and incoming[0].get('text', None) is not None
                and incoming[0].get('bot_id', None) is None and incoming[0].get('file', None) is None
                and incoming[0]['channel'] != GENERAL_CHANNEL and incoming[0]['channel'] != RANDOM_CHANNEL)

    def listen(self):
        """
        Establishes session and responds to commands
        """

        help_text = ['*help*\n',
                     'See which tests are failing the most since this build:\n\t*test-leaderboard* <job> <build #>\n',
                     'See which tests are failing in the past x days:\n\t*days* <job> <days> \n',
                     'Failing the most in this build range:\n\t*build-range* <job> <build #>-<build #>\n',
                     'Most recent failure on master:\n\t*test-on-master* <job> <testname> (ex. testname: org.voltdb.iv2'
                     '..)\n',
                     'All failures for a job:\n\t*all-failures* <job>\n',
                     'Display this help:\n\t*help*\n'
                     'For any <job>, you can specify *"pro"* or *"com"* for the master jobs\n',
                     'Examples: test-leaderboard pro 860, days com 14\n']

        while True:
            channel = ""
            try:
                # Wait for and respond to incoming data that is: text, not from a bot, not a file.
                incoming = list(self.client.rtm_read())
                if self.can_reply(incoming):
                    text = incoming[0]['text']
                    channel = incoming[0]['channel']
                    if 'end-session' in text:
                        # Keyword for making jenkinsbot inactive. Script has to be run again to turn jenkinsbot on.
                        self.post_message(channel, 'Leaving...')
                        sys.exit(0)
                    elif 'help' in text:
                        self.post_message(channel, ''.join(help_text))
                    else:
                        if 'pro' in text:
                            job = PRO
                        elif 'com' in text:
                            job = COMMUNITY
                        elif len(text) > 1:
                            job = text.split(' ')[1]
                        if 'test-on-master' in text:
                            args = text.split(' ')
                            params = {
                                'job': job,
                                'test': args[2],
                            }
                            table = self.query([channel], TOM_QUERY, params)
                            self.response(table, [channel], '%s-testonmaster.txt' % (args[2]))
                        elif 'all-failures' in text:
                            params = {
                                'job': job
                            }
                            table = self.query([channel], AF_QUERY, params)
                            self.response(table, [channel], '%s-allfailures.txt' % job)
                        elif 'days' in text:
                            args = text.split(' ')
                            params = {
                                'job': job,
                                'days': args[2]
                            }
                            table = self.query([channel], D_QUERY, params)
                            self.response(table, [channel], '%s-leaderboard-past-%s-days.txt' % (job, args[2]))
                        elif 'test-leaderboard' in text:
                            args = text.split(' ')
                            params = {
                                'job': job,
                                'beginning': args[2]
                            }
                            table = self.query([channel], TL_QUERY, params)
                            self.response(table, [channel], '%s-testleaderboard-from-%s.txt' % (job, args[2]))
                        elif 'build-range' in text:
                            args = text.split(' ')
                            builds = args[2].split('-')
                            params = {
                                'job': job,
                                'build_low': builds[0],
                                'build_high': builds[1]
                            }
                            table = self.query([channel], BR_QUERY, params)
                            self.response(table, [channel], '%s-buildrange-%s-to-%s.txt' % (job, builds[0], builds[1]))
            except (KeyboardInterrupt, SystemExit):
                self.log('Turning off the bot')
                self.post_message(ADMIN_CHANNEL, 'Turning off the bot')
                return
            except IndexError as error:
                self.log(error)
                if channel:
                    self.post_message(channel, 'Incorrect number or formatting of arguments\n\n' + ''.join(help_text))
            except Exception as error:
                self.log('Something unexpected went wrong')
                self.log(error)

                # Try to reconnect
                if not self.connect_to_slack():
                    self.log('Could not connect to Slack')
                    self.log('Turning off the bot')
                    self.post_message(ADMIN_CHANNEL, 'Turning off the bot')
                    return

            # Slow but reconfigurable
            time.sleep(1)

    def query(self, channels, query, params, is_retry=False):
        """
        Make a query and return a table
        :param channels: Channels this query is for
        :param query: Query to execute
        :param params: Parameters for the query
        :param is_retry: If this call of the query is a retry
        :return: Tuple of (headers, rows) as results
        """

        table = ""
        cursor = None
        database = None

        try:
            database = mysql.connector.connect(host=os.environ.get('dbhost', None),
                                               user=os.environ.get('dbuser', None),
                                               password=os.environ.get('dbpass', None),
                                               database=os.environ.get('dbdb', None))
            cursor = database.cursor()
            cursor.execute(query, params)
            headers = list(cursor.column_names)
            rows = cursor.fetchall()
            table = (headers, rows)

        except MySQLError as error:
            self.log('Could not connect to database')
            self.log(error)
            for channel in channels:
                self.post_message(channel, 'Something went wrong with the query.')
        except Exception as error:
            self.log('Something unexpected went wrong')
            self.log(error)

            # Try to reconnect only once
            if self.connect_to_slack() and not is_retry:
                table = self.query(query, params, True)
        finally:
            if cursor is not None:
                cursor.close()
            if database is not None:
                database.close()

        return table

    def response(self, tables, channels, filename):
        """
        Respond to a file to a channel
        :param tables: List of (header, rows) tuples i.e. tables to construct leaderboards from
        :param channels: The channels to post this file to
        :param filename: The filename to respond with
        """

        filecontent = ""
        for headers, rows in tables:
            content = ""
            if os.environ.get('vertical', False):
                # If this is set generate a vertical leaderboard. Append to end of normal leaderboard.
                content = '\n\n*Vertical Leaderboard*:\n\n' + self.vertical_leaderboard(rows, headers)

            if os.environ.get('edit', False):
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

    def query_and_response(self, query, params, channels, filename):
        """
        Perform a single query and response
        :param query: Query to run
        :param params: Parameters for query
        :param channels: Channels to respond to
        :param filename: filename for the post
        """
        table = self.query(channels, query, params)
        self.response([table], channels, filename)


if __name__ == '__main__':
    jenkinsbot = JenkinsBot()
    if jenkinsbot.connect_to_slack() and len(sys.argv) == 3:
        if sys.argv[1] == 'listen':
            jenkinsbot.logfile = sys.argv[2]
            jenkinsbot.listen()
        elif sys.argv[1] == 'master-leaderboard':
            jenkinsbot.logfile = sys.argv[2]
            jenkinsbot.query_and_response(
                ML_QUERY,
                {'jobA': PRO, 'jobB': COMMUNITY, 'jobC': VDM},
                [ADMIN_CHANNEL],
                'master-past30days.txt'
            )
        elif sys.argv[1] == 'core-leaderboard':
            jenkinsbot.logfile = sys.argv[2]
            jenkinsbot.query_and_response(
                CL_QUERY,
                {'jobA': MEMVALDEBUG, 'jobB': DEBUG, 'jobC': MEMVAL, 'jobD': FULLMEMCHECK},
                [ADMIN_CHANNEL],
                'coreextended-past2days.txt'
            )
