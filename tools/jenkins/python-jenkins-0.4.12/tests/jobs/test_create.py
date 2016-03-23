import json
from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsCreateJobTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            jenkins.NotFoundException(),
            None,
            json.dumps({'name': 'Test Job'}),
        ]

        self.j.create_job(u'Test Job', self.config_xml)

        self.assertEqual(
            jenkins_mock.call_args_list[1][0][0].get_full_url(),
            self.make_url('createItem?name=Test%20Job'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder(self, jenkins_mock):
        jenkins_mock.side_effect = [
            jenkins.NotFoundException(),
            None,
            json.dumps({'name': 'Test Job'}),
        ]

        self.j.create_job(u'a Folder/Test Job', self.config_xml)

        self.assertEqual(
            jenkins_mock.call_args_list[1][0][0].get_full_url(),
            self.make_url('job/a%20Folder/createItem?name=Test%20Job'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_already_exists(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'TestJob'}),
            None,
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.create_job(u'TestJob', self.config_xml)
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/TestJob/api/json?tree=name'))
        self.assertEqual(
            str(context_manager.exception),
            'job[TestJob] already exists')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_already_exists_in_folder(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'TestJob'}),
            None,
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.create_job(u'a Folder/TestJob', self.config_xml)
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/TestJob/api/json?tree=name'))
        self.assertEqual(
            str(context_manager.exception),
            'job[a Folder/TestJob] already exists')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_failed(self, jenkins_mock):
        jenkins_mock.side_effect = [
            jenkins.NotFoundException(),
            None,
            jenkins.NotFoundException(),
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.create_job(u'TestJob', self.config_xml)
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/TestJob/api/json?tree=name'))
        self.assertEqual(
            jenkins_mock.call_args_list[1][0][0].get_full_url(),
            self.make_url('createItem?name=TestJob'))
        self.assertEqual(
            str(context_manager.exception),
            'create[TestJob] failed')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_failed_in_folder(self, jenkins_mock):
        jenkins_mock.side_effect = [
            jenkins.NotFoundException(),
            None,
            jenkins.NotFoundException(),
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.create_job(u'a Folder/TestJob', self.config_xml)
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/TestJob/api/json?tree=name'))
        self.assertEqual(
            jenkins_mock.call_args_list[1][0][0].get_full_url(),
            self.make_url('job/a%20Folder/createItem?name=TestJob'))
        self.assertEqual(
            str(context_manager.exception),
            'create[a Folder/TestJob] failed')
        self._check_requests(jenkins_mock.call_args_list)
