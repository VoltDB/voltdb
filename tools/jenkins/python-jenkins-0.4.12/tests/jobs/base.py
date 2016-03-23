import copy
import json

from tests.base import JenkinsTestBase


class JenkinsJobsTestBase(JenkinsTestBase):

    config_xml = """
        <matrix-project>
            <actions/>
            <description>Foo</description>
        </matrix-project>"""


class JenkinsGetJobsTestBase(JenkinsJobsTestBase):

    jobs_in_folder = [
        [
            {'name': 'my_job1'},
            {'name': 'my_folder1', 'jobs': None},
            {'name': 'my_job2'}
        ],
        # my_folder1 jobs
        [
            {'name': 'my_job3'},
            {'name': 'my_job4'}
        ]
    ]

    jobs_in_multiple_folders = copy.deepcopy(jobs_in_folder)
    jobs_in_multiple_folders[1].insert(
        0, {'name': 'my_folder2', 'jobs': None})
    jobs_in_multiple_folders.append(
        # my_folder1/my_folder2 jobs
        [
            {'name': 'my_job1'},
            {'name': 'my_job2'}
        ]
    )


def build_jobs_list_responses(jobs_list, server_url):
    responses = []
    for jobs in jobs_list:
        get_jobs_response = []
        for job in jobs:
            job_json = {
                u'url': u'%s/job/%s' % (server_url.rstrip('/'), job['name']),
                u'name': job['name'],
                u'color': u'blue'
            }
            if 'jobs' in job:
                job_json[u'jobs'] = "null"
            get_jobs_response.append(job_json)

        responses.append(json.dumps({u'jobs': get_jobs_response}))

    return responses
