import json
from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsGetJobNameTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        job_name_to_return = {u'name': 'Test Job'}
        jenkins_mock.return_value = json.dumps(job_name_to_return)

        job_name = self.j.get_job_name(u'Test Job')

        self.assertEqual(job_name, 'Test Job')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/Test%20Job/api/json?tree=name'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder(self, jenkins_mock):
        job_name_to_return = {u'name': 'Test Job'}
        jenkins_mock.return_value = json.dumps(job_name_to_return)

        job_name = self.j.get_job_name(u'a Folder/Test Job')

        self.assertEqual(job_name, 'Test Job')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/Test%20Job/api/json?tree=name'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_return_none(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.NotFoundException()

        job_name = self.j.get_job_name(u'TestJob')

        self.assertEqual(job_name, None)
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/TestJob/api/json?tree=name'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder_return_none(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.NotFoundException()

        job_name = self.j.get_job_name(u'a Folder/TestJob')

        self.assertEqual(job_name, None)
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/TestJob/api/json?tree=name'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_unexpected_job_name(self, jenkins_mock):
        job_name_to_return = {u'name': 'not the right name'}
        jenkins_mock.return_value = json.dumps(job_name_to_return)

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_job_name(u'TestJob')
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/TestJob/api/json?tree=name'))
        self.assertEqual(
            str(context_manager.exception),
            'Jenkins returned an unexpected job name {0} '
            '(expected: {1})'.format(job_name_to_return['name'], 'TestJob'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder_unexpected_job_name(self, jenkins_mock):
        job_name_to_return = {u'name': 'not the right name'}
        jenkins_mock.return_value = json.dumps(job_name_to_return)

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_job_name(u'a Folder/TestJob')
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/TestJob/api/json?tree=name'))
        self.assertEqual(
            str(context_manager.exception),
            'Jenkins returned an unexpected job name {0} (expected: '
            '{1})'.format(job_name_to_return['name'], 'a Folder/TestJob'))
        self._check_requests(jenkins_mock.call_args_list)
