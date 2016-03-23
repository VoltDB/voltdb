import json
from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsDisableJobTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'Test Job'}),
            json.dumps({'name': 'Test Job'}),
        ]

        self.j.disable_job(u'Test Job')

        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/Test%20Job/disable'))
        self.assertTrue(self.j.job_exists('Test Job'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'Test Job'}),
            json.dumps({'name': 'Test Job'}),
        ]

        self.j.disable_job(u'a Folder/Test Job')

        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/Test%20Job/disable'))
        self.assertTrue(self.j.job_exists('a Folder/Test Job'))
        self._check_requests(jenkins_mock.call_args_list)
