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
SELECT t.name AS 'Test name', COUNT(*) AS 'Failures'
FROM `junit-test-failures` AS t
WHERE NOT t.status='FIXED' AND t.build >= %(beginning)s AND t.job=%(job)s
GROUP BY t.name
ORDER BY COUNT(*) DESC
""")

BR_QUERY = ("""
SELECT t.name AS 'Test name', COUNT(*) AS 'Number of failures in this build range'
FROM `junit-test-failures` AS t
INNER JOIN `junit-job-results` AS j
ON NOT t.status='FIXED' AND j.name=t.job AND j.build=t.build AND j.name=%(job)s AND %(build_low)s <= j.build AND j.build <= %(build_high)s
GROUP by t.name, t.job
ORDER BY COUNT(*) DESC
""")

TOM_QUERY = ("""
SELECT MAX(t.build) AS 'Most recent failure on master', MAX(j.build) AS 'Most recent build on master'
FROM `junit-test-failures` AS t
RIGHT JOIN `junit-job-results` AS j
ON j.name=t.job AND t.name=%(test)s
""")

class JenkinsBot(object):

    def __init__(self):
        self.cursor = None
        self.client = None

    def jenkins_session(self):
        """
        Establishes session and responds to commands
        """

        help_text = ['test-leaderboard <job> <build>\n',
                     'build-range <job> <build #>-<build #>\n',
                     'test-on-master <test>\n',
                     'help\n']

        token = os.environ.get('token', None)
        if token is None:
            print('Could not retrieve token for jenkinsbot')
            return

        self.client = SlackClient(token)

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
                        self.make_query(TL_QUERY, params, [channel], 'testleaderboard.txt')
                    elif 'build-range' in text:
                        args = text.split(' ')
                        builds = args[2]
                        params = {
                            'job': args[1],
                            'build_low': builds.split('-')[0],
                            'build_high': builds.split('-')[1]
                        }
                        self.make_query(BR_QUERY, params, [channel], 'buildrange.txt')
                    elif 'test-on-master' in text:
                        params = {
                            'test': text.split(' ')[1]
                        }
                        self.make_query(TOM_QUERY, params, [channel], 'testonmaster.txt')
                time.sleep(1)
            except (KeyboardInterrupt, SystemExit):
                # Turning off the bot
                self.cursor.close()
                database.close()
                break
            except MySQLError as error:
                print(error)
                self.post_message(channel, 'Something went wrong with the query. Please try again.')
            except Exception as error:
                # Something unexpected went wrong
                print(error)


    def make_query(self, query, params, channels, filename):
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
