#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.

# Script that runs jenkinsbot for VoltDB Slack

import os
import sys
import time

import mysql.connector

from mysql.connector.errors import Error as MySQLError
from slackclient import SlackClient
from tabulate import tabulate

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

BR_QUERY = ("""
    SELECT tf.name AS 'Test name',
           COUNT(*) AS 'Number of failures in this build range'
      FROM `junit-test-failures` AS tf
INNER JOIN `junit-job-results` AS jr
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
    SELECT MAX(t.build) AS 'Most recent failure on master',
           MAX(j.build) AS 'Most recent build on master'
      FROM `junit-test-failures` AS tf
RIGHT JOIN `junit-job-results` AS jr
        ON NOT tf.status='FIXED' AND
           jr.name=tf.job AND
           tf.name=%(test)s
""")

PL_QUERY = ("""
  SELECT job,
         name,
         fails,
         total,
         fails/total*100. AS "Percent failure"
    FROM
        (
           SELECT job,
                  name,
                  (
                   SELECT COUNT(*)
                     FROM `junit-job-results` jr
                    WHERE jr.name = tf.job
                  ) AS total,
                  COUNT(*) AS fails
             FROM `junit-test-failures` AS tf
            WHERE NOT status='FIXED' AND
                  job=%(job)s AND
                  %(build_low)s <= build AND
                  build <= %(build_high)s
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
        self.cursor = None
        self.client = None

    def connect(self):
        token = os.environ.get('token', None)
        if token is None:
            print('Could not retrieve token for jenkinsbot')
            return False
        self.client = SlackClient(token)
        return True

    def jenkins_session(self):
        """
        Establishes session and responds to commands
        """

        help_text = ['See which tests are failing the most since this build: test-leaderboard <job> <build>\n',
                     'Failing the most in this build range: build-range <job> <build #>-<build #>\n',
                     'Most recent failure on master: test-on-master <test>\n',
                     'See failure percentages per test: percent-leaderboard <job> <build #>-<build #>\n',
                     'All failures for a job: all-failures <job>\n'
                     'help\n']

        if not self.connect():
            return

        try:
            database = mysql.connector.connect(host=os.environ.get('dbhost', None),
                                               user=os.environ.get('dbuser', None),
                                               password=os.environ.get('dbpass', None),
                                               database=os.environ.get('dbdb', None))
            self.cursor = database.cursor()
        except MySQLError as error:
            print('Could not connect to database')
            print(error)
            return

        # Connect to real time messaging
        if not self.client.rtm_connect():
            print "Connection Failed, invalid token?"
            return

        while True:
            try:
                # Wait for and respond to commands.
                incoming = list(self.client.rtm_read())
                if len(incoming) > 0 and incoming[0].get('text', None) is not None and incoming[0].get('bot_id', None) is None:
                    text = incoming[0]['text']
                    channel = incoming[0]['channel']
                    if 'end-session' in text:
                        self.post_message(channel, 'Leaving...')
                        sys.exit(0)
                    elif 'help' in text:
                        self.post_message(channel, ''.join(help_text))
                    elif 'test-leaderboard' in text:
                        params = {
                            'job': text.split(' ')[1],
                            'beginning': text.split(' ')[2]
                        }
                        self.query_and_response(TL_QUERY, params, [channel], 'testleaderboard.txt')
                    elif 'build-range' in text:
                        args = text.split(' ')
                        builds = args[2]
                        params = {
                            'job': args[1],
                            'build_low': builds.split('-')[0],
                            'build_high': builds.split('-')[1]
                        }
                        self.query_and_response(BR_QUERY, params, [channel], 'buildrange.txt')
                    elif 'test-on-master' in text:
                        params = {
                            'test': text.split(' ')[1]
                        }
                        self.query_and_response(TOM_QUERY, params, [channel], 'testonmaster.txt')
                    elif 'percent-leaderboard' in text:
                        args = text.split(' ')
                        builds = args[2]
                        params = {
                            'job': args[1],
                            'build_low': builds.split('-')[0],
                            'build_high': builds.split('-')[1]
                        }
                        self.query_and_response(PL_QUERY, params, [channel], 'percentleaderboard.txt')
                    elif 'all-failures' in text:
                        params = {
                            'job': text.split(' ')[1]
                        }
                        self.query_and_response(AF_QUERY, params, [channel], 'allfailures.txt')
                time.sleep(1)
            except (KeyboardInterrupt, SystemExit):
                # Turning off the bot
                self.cursor.close()
                database.close()
                break
            except IndexError as error:
                print(error)
                self.post_message(channel, 'Hint: Incorrect number of arguments\n\n' + ''.join(help_text))
            except MySQLError as error:
                print(error)
                self.post_message(channel, 'Something went wrong with the query.')
            except Exception as error:
                # Something unexpected went wrong
                print(error)


    def query_and_response(self, query, params, channels, filename):
        """
        Make a query then upload a text file with tables to the channels.
        """
        self.cursor.execute(query, params)
        headers = list(self.cursor.column_names)
        table = list(self.cursor.fetchall())
        self.client.api_call(
            'files.upload', channels=channels, content=tabulate(table, headers), filetype='text', filename=filename
        )


    def post_message(self, channel, text):
        """
        Post a message on the channel.
        """
        self.client.api_call(
            'chat.postMessage', channel=channel, text=text, as_user=True
        )

if __name__ == '__main__':
    jenkinsbot = JenkinsBot()
    jenkinsbot.jenkins_session()
