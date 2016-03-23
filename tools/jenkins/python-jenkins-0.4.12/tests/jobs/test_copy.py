import json
from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsCopyJobTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'Test Job_2'}),
            json.dumps({'name': 'Test Job_2'}),
            json.dumps({'name': 'Test Job_2'}),
        ]

        self.j.copy_job(u'Test Job', u'Test Job_2')

        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('createItem?name=Test%20Job_2&mode=copy&from=Test%20Job'))
        self.assertTrue(self.j.job_exists('Test Job_2'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'Test Job_2'}),
            json.dumps({'name': 'Test Job_2'}),
            json.dumps({'name': 'Test Job_2'}),
        ]

        self.j.copy_job(u'a Folder/Test Job', u'a Folder/Test Job_2')

        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/a%20Folder/createItem?name=Test%20Job_2'
                          '&mode=copy&from=Test%20Job'))
        self.assertTrue(self.j.job_exists('a Folder/Test Job_2'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_failed(self, jenkins_mock):
        jenkins_mock.side_effect = [
            None,
            jenkins.NotFoundException(),
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.copy_job(u'TestJob', u'TestJob_2')
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('createItem?name=TestJob_2&mode=copy&from=TestJob'))
        self.assertEqual(
            str(context_manager.exception),
            'create[TestJob_2] failed')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder_failed(self, jenkins_mock):
        jenkins_mock.side_effect = [
            None,
            jenkins.NotFoundException(),
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.copy_job(u'a Folder/TestJob', u'a Folder/TestJob_2')
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/a%20Folder/createItem?name=TestJob_2&mode=copy'
                          '&from=TestJob'))
        self.assertEqual(
            str(context_manager.exception),
            'create[a Folder/TestJob_2] failed')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_another_folder_failed(self, jenkins_mock):
        jenkins_mock.side_effect = [
            jenkins.JenkinsException()
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.copy_job(u'a Folder/TestJob', u'another Folder/TestJob_2')
        self.assertEqual(
            str(context_manager.exception),
            ('copy[a Folder/TestJob to another Folder/TestJob_2] failed, '
             'source and destination folder must be the same'))
        self._check_requests(jenkins_mock.call_args_list)
