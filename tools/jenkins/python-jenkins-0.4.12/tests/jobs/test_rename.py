import json
from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsRenameJobTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'Test Job_2'}),
            json.dumps({'name': 'Test Job_2'}),
            json.dumps({'name': 'Test Job_2'}),
        ]

        self.j.rename_job(u'Test Job', u'Test Job_2')

        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/Test%20Job/doRename?newName=Test%20Job_2'))
        self.assertTrue(self.j.job_exists('Test Job_2'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'Test Job_2'}),
            json.dumps({'name': 'Test Job_2'}),
            json.dumps({'name': 'Test Job_2'}),
        ]

        self.j.rename_job(u'a Folder/Test Job', u'a Folder/Test Job_2')

        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/Test%20Job/doRename?newName=Test%20Job_2'))
        self.assertTrue(self.j.job_exists('Test Job_2'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_failed(self, jenkins_mock):
        jenkins_mock.side_effect = [
            None,
            jenkins.NotFoundException(),
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.rename_job(u'TestJob', u'TestJob_2')
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/TestJob/doRename?newName=TestJob_2'))
        self.assertEqual(
            str(context_manager.exception),
            'rename[TestJob_2] failed')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder_failed(self, jenkins_mock):
        jenkins_mock.side_effect = [
            None,
            jenkins.NotFoundException(),
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.rename_job(u'a Folder/TestJob', u'a Folder/TestJob_2')
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/TestJob/doRename?newName=TestJob_2'))
        self.assertEqual(
            str(context_manager.exception),
            'rename[a Folder/TestJob_2] failed')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_another_folder_failed(self, jenkins_mock):
        jenkins_mock.side_effect = [
            jenkins.JenkinsException()
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.rename_job(u'a Folder/TestJob', u'another Folder/TestJob_2')
        self.assertEqual(
            str(context_manager.exception),
            ('rename[a Folder/TestJob to another Folder/TestJob_2] failed, '
             'source and destination folder must be the same'))
        self._check_requests(jenkins_mock.call_args_list)
