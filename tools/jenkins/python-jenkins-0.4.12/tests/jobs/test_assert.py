import json
from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsAssertJobExistsTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_job_missing(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.NotFoundException()

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.assert_job_exists('NonExistent')
        self.assertEqual(
            str(context_manager.exception),
            'job[NonExistent] does not exist')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_job_missing_in_folder(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.NotFoundException()

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.assert_job_exists('a Folder/NonExistent')
        self.assertEqual(
            str(context_manager.exception),
            'job[a Folder/NonExistent] does not exist')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_job_exists(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'ExistingJob'}),
        ]
        self.j.assert_job_exists('ExistingJob')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_job_exists_in_folder(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'ExistingJob'}),
        ]
        self.j.assert_job_exists('a Folder/ExistingJob')
        self._check_requests(jenkins_mock.call_args_list)
