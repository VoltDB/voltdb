import logging
import os
import sys
from jira import JIRA
from urllib2 import HTTPError, URLError, urlopen
JIRA_USER = os.environ.get('jirauser', None)
JIRA_PASS = os.environ.get('jirapass', None)
JIRA_PROJECT = os.environ.get('jiraproject', None)
job = 'test-nextrelease-sql-grammar-gen'


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

    def report_issue(self, build):
        try:
            jira = JIRA(server='https://issues.voltdb.com/', basic_auth=(JIRA_USER, JIRA_PASS))
        except:
            logging.exception('Could not connect to Jira')
            return

        summary = job + ' failed on build ' + str(build)

        existing = jira.search_issues('summary ~ \'%s\'' % summary)
        if len(existing) > 0:
            return 'Already reported'

        build_report_url = self.jhost + '/job/' + job + '/' + str(build) + '/api/python'
        build_report = eval(self.read_url(build_report_url))
        build_url = build_report.get('url')
        build_result = build_report.get('result')

        if build_result == 'SUCCESS':  # only generate Jira issue if the test fails
            return

        summary_url = self.jhost + '/job/' + job + '/' + str(build) + '/artifact/tests/sqlgrammar/summary.out'
        summary_report = self.read_url(summary_url)

        query_split = summary_report.split('(or it was never started??), after SQL statement:')
        crash_query = query_split[1]

        hash_split = summary_report.split('#', 1)
        hash_split = hash_split[1].split('# See problematic frame for where to report the bug.')
        sigsegv_message = hash_split[0] + '# See problematic frame for where to report the bug.\n#'

        description = job + ' build ' + str(build) + ' : ' + str(build_result) + '\n' \
            + 'Link to Jenkins build: ' + build_url + ' \n \n' \
            + 'SIGSEGV Message: \n' + '#' + sigsegv_message + ' \n \n' \
            + 'Query that Caused the Crash: ' + crash_query

        description = description.replace('#', '\#')

        labels = ['grammar-gen']

        component = 'Core'
        components = jira.project_components(JIRA_PROJECT)
        jira_component = {}
        for c in components:
            if c.name == component:
                jira_component = {
                    'name': c.name,
                    'id': c.id
                }
                break

        current_version = 'V' + \
                          str(self.read_url('https://raw.githubusercontent.com/VoltDB/voltdb/master/version.txt'))
        jira_versions = jira.project_versions(JIRA_PROJECT)
        jira_version = {}
        for v in jira_versions:
            current_version.strip()
            if str(v.name) == current_version.strip():
                jira_version = {
                    'name': v.name,
                    'id': v.id
                }

        issue_dict = {
            'project': JIRA_PROJECT,
            'summary': summary,
            'description': description,
            'issuetype': {'name': 'Bug'},
            'priority': {'name': 'Blocker'},
            'labels': labels,
            'components': [jira_component],
            'versions': [jira_version]
        }

        new_issue = jira.create_issue(fields=issue_dict)
        print 'New issue created for failure on build ' + str(build)

if __name__ == '__main__':
    this_build = os.environ.get('build', None)
    issue = Issues()
    issue.report_issue(this_build)
