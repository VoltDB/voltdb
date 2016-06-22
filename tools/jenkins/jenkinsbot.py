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
SELECT t.name AS 'Test name', t.job AS 'Job name', COUNT(*) AS 'Number of failures in this build range'
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
    help_text = """
    test-leaderboard <job> <build>
    build-range <job> <build #>-<build #>
    test-on-master <test>
    help
    """

    token = os.environ.get('token', None)
    if token is None:
        print('Could not retrieve token for jenkinsbot')
        return

    sc = SlackClient(token)
    try:
        db = mysql.connector.connect(host='volt2.voltdb.lan', user='oolukoya', password='oolukoya', database='qa')
        cursor = db.cursor()

        # Connect to real time messaging
        if sc.rtm_connect():
            while True:
                # Wait for and respond to commands.
                incoming = list(sc.rtm_read())
                if len(incoming) > 0 and incoming[0].get('text', None) is not None and incoming[0].get('bot_id', None) is None:
                    text = incoming[0]['text']
                    channel = incoming[0]['channel']
                    if 'end-session' in text:
                        print sc.api_call(
                            'chat.postMessage', channel=channel, text='Exiting..', as_user=True
                        )
                        return
                    elif 'help' in text:
                        print sc.api_call(
                            'chat.postMessage', channel=channel, text=help_text, as_user=True
                        )
                    elif 'test-leaderboard' in text:
                        params = {
                            'job': text.split(' ')[1],
                            'beginning': text.split(' ')[2]
                        }
                        cursor.execute(tl_query, params)
                        headers = list(cursor.column_names)
                        table = list(cursor.fetchall())
                        print tabulate(table, headers)
                        print sc.api_call(
                            'chat.postMessage', channel=channel, text=tabulate(table, headers), as_user=True
                        )
                    elif 'build-range' in text:
                        args = text.split(' ')
                        builds = args[2]
                        params = {
                            'job': args[1],
                            'build_low': builds.split('-')[0],
                            'build_high': builds.split('-')[1]
                        }
                        cursor.execute(br_query, params)
                        headers = list(cursor.column_names)
                        table = list(cursor.fetchall())
                        print sc.api_call(
                            'chat.postMessage', channel=channel, text=tabulate(table, headers), as_user=True
                        )
                    elif 'test-on-master' in text:
                        params = {
                            'test': text.split(' ')[1]
                        }
                        cursor.execute(tom_query, params)
                        headers = list(cursor.column_names)
                        table = list(cursor.fetchall())
                        print sc.api_call(
                            'chat.postMessage', channel=channel, text=tabulate(table, headers), as_user=True
                        )
                    else:
                        print sc.api_call(
                            'chat.postMessage', channel=channel, text=help_text, as_user=True
                        )
                time.sleep(1)
        else:
            print "Connection Failed, invalid token?"
    except:
        print(sys.exc_info())
    finally:
        cursor.close()
        db.close()

if __name__ == '__main__':
    jenkins_session()
