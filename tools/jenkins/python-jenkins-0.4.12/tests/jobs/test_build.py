from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsBuildJobTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            {'foo': 'bar'},
        ]

        build_info = self.j.build_job(u'Test Job')

        self.assertEqual(jenkins_mock.call_args[0][0].get_full_url(),
                         self.make_url('job/Test%20Job/build'))
        self.assertEqual(build_info, {'foo': 'bar'})
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder(self, jenkins_mock):
        jenkins_mock.side_effect = [
            {'foo': 'bar'},
        ]

        build_info = self.j.build_job(u'a Folder/Test Job')

        self.assertEqual(jenkins_mock.call_args[0][0].get_full_url(),
                         self.make_url('job/a%20Folder/job/Test%20Job/build'))
        self.assertEqual(build_info, {'foo': 'bar'})
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_with_token(self, jenkins_mock):
        jenkins_mock.side_effect = [
            {'foo': 'bar'},
        ]

        build_info = self.j.build_job(u'TestJob', token='some_token')

        self.assertEqual(jenkins_mock.call_args[0][0].get_full_url(),
                         self.make_url('job/TestJob/build?token=some_token'))
        self.assertEqual(build_info, {'foo': 'bar'})
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder_with_token(self, jenkins_mock):
        jenkins_mock.side_effect = [
            {'foo': 'bar'},
        ]

        build_info = self.j.build_job(u'a Folder/TestJob', token='some_token')

        self.assertEqual(jenkins_mock.call_args[0][0].get_full_url(),
                         self.make_url('job/a%20Folder/job/TestJob/build?token=some_token'))
        self.assertEqual(build_info, {'foo': 'bar'})
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_with_parameters_and_token(self, jenkins_mock):
        jenkins_mock.side_effect = [
            {'foo': 'bar'},
        ]

        build_info = self.j.build_job(
            u'TestJob',
            parameters={'when': 'now', 'why': 'because I felt like it'},
            token='some_token')

        self.assertTrue('token=some_token' in jenkins_mock.call_args[0][0].get_full_url())
        self.assertTrue('when=now' in jenkins_mock.call_args[0][0].get_full_url())
        self.assertTrue('why=because+I+felt+like+it' in jenkins_mock.call_args[0][0].get_full_url())
        self.assertEqual(build_info, {'foo': 'bar'})
        self._check_requests(jenkins_mock.call_args_list)
