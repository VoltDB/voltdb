#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.

# Script that runs jenkinsbot for VoltDB Slack

from mysql.connector.errors import Error as MySQLError
from slackclient import SlackClient
from tabulate import tabulate

import mysql.connector
import os
import sys
import time

tl_query = ("""
SELECT t.name AS 'Test name', COUNT(*) AS 'Failures'
FROM `junit-test-failures` AS t
WHERE NOT t.status='FIXED' AND t.build >= %(beginning)s AND t.job=%(job)s
GROUP BY t.name
ORDER BY COUNT(*) DESC
""")

br_query = ("""
SELECT t.name AS 'Test name', COUNT(*) AS 'Number of failures in this build range'
FROM `junit-test-failures` AS t
INNER JOIN `junit-job-results` AS j
ON NOT t.status='FIXED' AND j.name=t.job AND j.build=t.build AND j.name=%(job)s AND %(build_low)s <= j.build AND j.build <= %(build_high)s
GROUP by t.name, t.job
ORDER BY COUNT(*) DESC
""")

tom_query = ("""
SELECT MAX(t.build) AS 'Most recent failure on master', MAX(j.build) AS 'Most recent build on master'
FROM `junit-test-failures` AS t
RIGHT JOIN `junit-job-results` AS j
ON j.name=t.job AND t.name=%(test)s
""")


def jenkins_session():
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

    try:
        sc = SlackClient(token)
        db = mysql.connector.connect(host=os.environ.get('dbhost', None),
                                     user=os.environ.get('dbuser', None),
                                     password=os.environ.get('dbpass', None),
                                     database=os.environ.get('dbdb', None))
        cursor = db.cursor()
    except Exception as e:
        print('Could not connect to database')
        print(e)
        return

    # Connect to real time messaging
    if not sc.rtm_connect():
        print "Connection Failed, invalid token?"
        return

    while True:
        try:
            # Wait for and respond to commands.
            incoming = list(sc.rtm_read())
            if len(incoming) > 0 and incoming[0].get('text', None) is not None and incoming[0].get('bot_id', None) is None:
                text = incoming[0]['text']
                channel = incoming[0]['channel']
                if 'end-session' in text:
                    post_message(sc, channel, 'Leaving...')
                    sys.exit(0)
                elif 'help' in text:
                    post_message(sc, channel, ''.join(help_text))
                elif 'test-leaderboard' in text:
                    params = {
                        'job': text.split(' ')[1],
                        'beginning': text.split(' ')[2]
                    }
                    make_query(cursor, tl_query, params, [channel], 'testleaderboard.txt')
                elif 'build-range' in text:
                    args = text.split(' ')
                    builds = args[2]
                    params = {
                        'job': args[1],
                        'build_low': builds.split('-')[0],
                        'build_high': builds.split('-')[1]
                    }
                    make_query(cursor, br_query, params, [channel], 'buildrange.txt')
                elif 'test-on-master' in text:
                    params = {
                        'test': text.split(' ')[1]
                    }
                    make_query(cursor, tom_query, params, [channel], 'testonmaster.txt')
            time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            # Turning off the bot
            cursor.close()
            db.close()
            break
        except MySQLError as e:
            print(e)
            post_message(sc, channel, 'Something went wrong with the query. Please try again.')
        except Exception as e:
            # Something unexpected went wrong
            print(e)

def make_query(cursor, query, params, channels, filename):
    """
    Make a query then upload a text file with tables to the channels.
    """
    cursor.execute(query, params)
    headers = list(cursor.column_names)
    table = list(cursor.fetchall())
    sc.api_call(
        'files.upload', channels=channels, content=tabulate(table, headers), filetype='text', filename=filename
    )

def post_message(sc, channel, text):
    sc.api_call(
        'chat.postMessage', channel=channel, text=text, as_user=True
    )

if __name__ == '__main__':
    jenkins_session()
