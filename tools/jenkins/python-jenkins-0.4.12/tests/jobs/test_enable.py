import json
from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsEnableJobTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'TestJob'}),
            json.dumps({'name': 'TestJob'}),
        ]

        self.j.enable_job(u'TestJob')

        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/TestJob/enable'))
        self.assertTrue(self.j.job_exists('TestJob'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'TestJob'}),
            json.dumps({'name': 'TestJob'}),
        ]

        self.j.enable_job(u'a Folder/TestJob')

        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/TestJob/enable'))
        self.assertTrue(self.j.job_exists('a Folder/TestJob'))
        self._check_requests(jenkins_mock.call_args_list)
