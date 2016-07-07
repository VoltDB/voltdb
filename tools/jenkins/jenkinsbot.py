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
from tabulate import tabulate

COMMUNITY = os.environ.get('community', None)
PRO = os.environ.get('pro', None)

# Channel to message in case something goes wrong
ADMIN_CHANNEL = 'D1JG6N2A2'

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

L_QUERY = ("""
  SELECT job AS 'Job name',
         name AS 'Test name',
         fails AS 'Fails',
         total AS 'Total',
         fails/total*100. AS "Fail %",
         latest AS 'Latest Failure'
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
                  (job=%(jobA)s OR job=%(jobB)s) AND
                  NOW() - INTERVAL 30 DAY <= tf.stamp
         GROUP BY job,
                  name,
                  total
        ) AS intermediate
ORDER BY 5 DESC
""")

AF_QUERY = ("""
  SELECT tf.name AS 'Test name',
         tf.build as 'Build',
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
        size = 1 << 30
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

    def connect(self):
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

    def listen(self):
        """
        Establishes session and responds to commands
        """

        help_text = ['*Help*\n',
                     'See which tests are failing the most since this build:\n\ttest-leaderboard <job> <build #>\n',
                     'See which tests are failing in the past x days:\n\tdays <job> <days> \n',
                     'Failing the most in this build range:\n\tbuild-range <job> <build #>-<build #>\n',
                     'Most recent failure on master:\n\ttest-on-master <job> <testname> (ex. testname: org.voltdb.iv2'
                         '..)\n',
                     'All failures for a job:\n\tall-failures <job>\n',
                     'For any <job>, you can specify "pro" or "com" for the master jobs\n',
                     'Examples: test-leaderboard pro 860, days com 14\n',
                     'help\n']

        while True:
            try:
                # Wait for and respond to incoming data that is: text, not from a bot, not a file.
                incoming = list(self.client.rtm_read())
                if (len(incoming) > 0 and incoming[0].get('text', None) is not None and
                    incoming[0].get('bot_id', None) is None and incoming[0].get('file', None) is None):
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
                            self.query_and_response(TOM_QUERY, params, [channel],
                                                    '%s-testonmaster.txt' % (args[2]))
                        elif 'all-failures' in text:
                            params = {
                                'job': job
                            }
                            self.query_and_response(AF_QUERY, params, [channel],
                                                    '%s-allfailures.txt' % (job))
                        elif 'days' in text:
                            args = text.split(' ')
                            params = {
                                'job': job,
                                'days': args[2]
                            }
                            self.query_and_response(D_QUERY, params, [channel],
                                                    '%s-leaderboard-past-%s.txt' % (job, args[2]))
                        elif 'test-leaderboard' in text:
                            args = text.split(' ')
                            params = {
                                'job': job,
                                'beginning': args[2]
                            }
                            self.query_and_response(TL_QUERY, params, [channel],
                                                    '%s-testleaderboard-from-%s.txt' % (job, args[2]))
                        elif 'build-range' in text:
                            args = text.split(' ')
                            builds = args[2].split('-')
                            params = {
                                'job': job,
                                'build_low': builds[0],
                                'build_high': builds[1]
                            }
                            self.query_and_response(BR_QUERY, params, [channel],
                                                    '%s-buildrange-%s-to-%s.txt' % (job, builds[0], builds[1]))
            except (KeyboardInterrupt, SystemExit):
                self.log('Turning off the bot')
                self.post_message(ADMIN_CHANNEL, 'Turning off the bot')
                return
            except IndexError as error:
                self.log(error)
                self.post_message(channel, 'Incorrect number or formatting of arguments\n\n' + ''.join(help_text))
            except Exception as error:
                self.log('Something unexpected went wrong')
                self.log(error)

                # Try to reconnect
                if not self.connect():
                    self.log('Could not connect to Slack')
                    self.log('Turning off the bot')
                    self.post_message(ADMIN_CHANNEL, 'Turning off the bot')
                    return

            # Slow but reconfigurable
            time.sleep(1)

    def query_and_response(self, query, params, channels, filename, retry=False):
        """
        Make a query then upload a text file with tables to the channels.
        """
        try:
            database = mysql.connector.connect(host=os.environ.get('dbhost', None),
                                               user=os.environ.get('dbuser', None),
                                               password=os.environ.get('dbpass', None),
                                               database=os.environ.get('dbdb', None))
            cursor = database.cursor()

            cursor.execute(query, params)
            headers = list(cursor.column_names)
            table = cursor.fetchall()

            if query == L_QUERY:
            # Do some specific replacement for long rows in this query.
                for i, row in enumerate(table):
                    table[i] = list(row)
                    table[i][0] = table[i][0].replace('branch-2-', '')
                    table[i][1] = table[i][1].replace('org.voltdb.', '')
                    table[i][1] = table[i][1].replace('org.voltcore.', '')

            self.client.api_call(
                'files.upload', channels=channels, content=tabulate(table, headers), filetype='text', filename=filename
            )

        except MySQLError as error:
            self.log('Could not connect to database')
            self.log(error)
            for channel in channels:
                self.post_message(channel, 'Something went wrong with the query.')
        except Exception as error:
            self.log('Something unexpected went wrong')
            self.log(error)

            # Try to reconnect only once
            if self.connect() and retry == False:
                self.query_and_response(query, params, channels, filename, retry=True)
        finally:
            cursor.close()
            database.close()

    def post_message(self, channel, text):
        """
        Post a message on the channel.
        """
        self.client.api_call(
            'chat.postMessage', channel=channel, text=text, as_user=True
        )

if __name__ == '__main__':
    jenkinsbot = JenkinsBot()
    if jenkinsbot.connect() and len(sys.argv) == 3:
        if sys.argv[1] == 'listen':
            jenkinsbot.logfile = sys.argv[2]
            jenkinsbot.listen()
        elif sys.argv[1] == 'leaderboard':
            jenkinsbot.logfile = sys.argv[2]
            jenkinsbot.query_and_response(
                L_QUERY,
                {'jobA': PRO, 'jobB': COMMUNITY},
                ['#junit'],
                'leaderboard-past30days.txt'
            )
