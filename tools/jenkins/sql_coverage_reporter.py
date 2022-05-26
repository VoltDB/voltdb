#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.

import logging
import os
import sys
from jenkinsbot import JenkinsBot
from jira import JIRA
from sql_grammar_reporter import Issues # using symlink alias to reference file

# Set to true to avoid updating Jira
DRY_RUN = False

class Reporter(Issues):
    def build_description(self, name, url, stats):
        description = name + '\n'

        description += \
            'SQL Statements:\n' \
            + ' : Valid=' + str(stats['valid_statements']) + ' (' + stats['valid_percent'] + '%)' \
            + ' : Invalid=' + str(stats['invalid_statements']) + ' (' + stats['invalid_percent'] + '%)' \
            + ' : Total=' + str(stats['total_statements']) + '\n' \
            + 'Test Failures:\n' \
            + ' : Mismatched=' + str(stats['mismatched_statements']) + ' (' + stats['mismatched_percent'] + '%)\n' \
            + 'Exceptions:\n' \
            + ' : Volt Fatal=' + str(stats['total_volt_fatal_excep']) \
            + ' : Volt Nonfatal=' + str(stats['total_volt_nonfatal_excep']) \
            + ' : ' + str(stats['comparison_database']) + ' Total=' + str(stats['total_cmp_excep']) + '\n' \
            + 'Crashes:\n' \
            + ' : Volt=' + str(stats['total_volt_crashes']) \
            + ' : ' + str(stats['comparison_database']) + '=' + str(stats['total_cmp_crashes']) \
            + ' : Diff=' + str(stats['total_diff_crashes'])

        url += '/index.html'
        description += '\nSee [OVERVIEW|' + url + '] for more information.\n\n'
        return description

    def report(self, job, build):
        try:
            jira = JIRA(server='https://issues.voltdb.com/', basic_auth=(JIRA_USER, JIRA_PASS), options=dict(verify=False))
        except:
            logging.exception('Could not connect to Jira')
            return

        base_url = self.jhost + '/job/' + job + '/' + str(build)
        build_report = eval(self.read_url(base_url + '/api/python'))

        build_result = build_report.get('result')
        if build_result == "SUCCESS":
            logging.info('No new issue created. Build ' + str(build) + ' resulted in: ' + build_result)
            return

        current_version = str(self.read_url('https://raw.githubusercontent.com/VoltDB/voltdb/master/version.txt'))
        description = 'SQLCoverage failure(s) on ' + job + ', build ' + str(build) + '\n\n'
        summary = job + ' : ' + build + ' : sqlcov-internal-err'
        runs = build_report.get('runs')
        for run in runs:
            config_url = run.get('url')
            config_report = eval(self.read_url(config_url + 'api/python'))
            config_result = config_report.get('result')

            if config_result == 'SUCCESS':
                continue

            config_name = config_report.get('fullDisplayName')
            config_data_url = config_url + 'artifact/obj/release/sqlcoverage'
            config_stats = eval(self.read_url(config_data_url + '/stats.txt'))
            description += self.build_description(config_name, config_data_url, config_stats)

        jenkinsbot = JenkinsBot()
        jenkinsbot.create_bug_issue(JUNIT, summary, description, 'Core', current_version, ['sqlcoverage-failure', 'automatic'],
                                    DRY_RUN=DRY_RUN)

if __name__ == '__main__':
    job = os.environ.get('job', None)
    build = os.environ.get('build', None)

    args = sys.argv

    if len(args) > 1:
        job = args[1]

    if len(args) > 2:
        build = args[2]

    reporter = Reporter()
    reporter.report(job, build)
