#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

import logging
import os
import sys
from jenkinsbot import JenkinsBot
from jira import JIRA
from urllib2 import HTTPError, URLError, urlopen

JIRA_USER = os.environ.get('jirauser', None)
JIRA_PASS = os.environ.get('jirapass', None)
JIRA_PROJECT = os.environ.get('jiraproject', None)

class Issues(object):
    def __init__(self):
        self.jhost = 'http://ci.voltdb.lan'
        logging.basicConfig(stream=sys.stdout)

    def read_url(self, url):
        data = None
        try:
            data = urlopen(url).read()
        except (HTTPError, URLError):
            logging.exception('Could not open data from url: %s. The url may not be formed correctly.' % url)
        except IOError:
            logging.exception('Could not read data from url: %s. The data at the url may not be readable.' % url)
        except:
            logging.exception('Something unexpected went wrong.')
        return data

    def build_description(self, name, url, stats):
        description = name + '\n\n'

        description += \
            'SQL Statements:' \
            + '\n - Valid ' + str(stats['valid_statements']) + ' : ' + stats['valid_percent'] + '%' \
            + '\n - Invalid ' + str(stats['invalid_statements']) + ' : ' + stats['invalid_percent'] + '%' \
            + '\n - Total ' + str(stats['total_statements']) + '\n' \
            + 'Test Failures:' \
            + '\n - Mismatched ' + str(stats['mismatched_statements']) + ' : ' + stats['mismatched_percent'] + '%\n' \
            + 'Exceptions:' \
            + '\n - Total Volt Fatal ' + str(stats['total_volt_fatal_excep']) \
            + '\n - Total Volt Nonfatal ' + str(stats['total_volt_nonfatal_excep']) + '\n' \
            + 'Crashes:' \
            + '\n - Volt ' + str(stats['total_volt_crashes']) \
            + '\n - Diff ' + str(stats['total_diff_crashes']) \
            + '\n - Comparison ' + str(stats['total_cmp_crashes'])

        description += '\n\nSee ' + url + ' for more information.\n\n'
        return description

    def report_issue(self, job, build):
        try:
            jira = JIRA(server='https://issues.voltdb.com/', basic_auth=(JIRA_USER, JIRA_PASS), options=dict(verify=False))
        except:
            logging.exception('Could not connect to Jira')
            return

        base_url = self.jhost + '/job/' + job + '/' + str(build)
        build_report = eval(self.read_url(base_url + '/api/python'))

        build_result = build_report.get('result')
        if build_result == "SUCCESS":
            print 'No new issue created. Build ' + str(build) + 'resulted in: ' + build_result
            return

        description = 'Failure on ' + job + ', build ' + str(build) + '\n\n'
        runs = build_report.get('runs')
        for run in runs:
            config_url = run.get('url')
            config_report = eval(self.read_url(config_url + '/api/python'))
            config_result = config_report.get('result')

            if config_result == 'SUCCESS':
                continue

            config_name = config_report.get('fullDisplayName')
            config_data_url = config_url + '/artifact/obj/release/sqlcoverage'
            config_stats = eval(self.read_url(config_data_url + '/stats.txt'))
            description += self.build_description(config_name, config_data_url, config_stats)

        summary = job + ':' + build
        current_version = str(urlopen('https://raw.githubusercontent.com/VoltDB/voltdb/master/version.txt').read())
        existing = jira.search_issues(summary)
        if len(existing) > 0:
            print 'No new Jira issue created. Build ' + str(build) + ' has already been reported.'
            return

        jenkinsbot = JenkinsBot()
        jenkinsbot.create_bug_issue('somechannel', summary, description, 'Core', current_version, ['coverage'],
                                    JIRA_USER, JIRA_PASS, JIRA_PROJECT, DRY_RUN=True)

if __name__ == '__main__':
    job = os.environ.get('job', None)
    build = os.environ.get('build', None)

    args = sys.argv

    if len(args) > 1:
        job = args[1]

    if len(args) > 2:
        build = args[2]

    issue = Issues()
    issue.report_issue(job, build)
