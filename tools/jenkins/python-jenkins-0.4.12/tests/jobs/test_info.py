import json
from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsGetJobInfoTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        job_info_to_return = {
            u'building': False,
            u'msg': u'test',
            u'revision': 66,
            u'user': u'unknown'
        }
        jenkins_mock.return_value = json.dumps(job_info_to_return)

        job_info = self.j.get_job_info(u'Test Job')

        self.assertEqual(job_info, job_info_to_return)
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/Test%20Job/api/json?depth=0'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder(self, jenkins_mock):
        job_info_to_return = {
            u'building': False,
            u'msg': u'test',
            u'revision': 66,
            u'user': u'unknown'
        }
        jenkins_mock.return_value = json.dumps(job_info_to_return)

        job_info = self.j.get_job_info(u'a Folder/Test Job')

        self.assertEqual(job_info, job_info_to_return)
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/Test%20Job/api/json?depth=0'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_regex(self, jenkins_mock):
        jobs = [
            {u'name': u'my-job-1'},
            {u'name': u'my-job-2'},
            {u'name': u'your-job-1'},
            {u'name': u'Your-Job-1'},
            {u'name': u'my-project-1'},
        ]
        job_info_to_return = {u'jobs': jobs}
        jenkins_mock.return_value = json.dumps(job_info_to_return)

        self.assertEqual(len(self.j.get_job_info_regex('her-job')), 0)
        self.assertEqual(len(self.j.get_job_info_regex('my-job-1')), 1)
        self.assertEqual(len(self.j.get_job_info_regex('my-job')), 2)
        self.assertEqual(len(self.j.get_job_info_regex('job')), 3)
        self.assertEqual(len(self.j.get_job_info_regex('project')), 1)
        self.assertEqual(len(self.j.get_job_info_regex('[Yy]our-[Jj]ob-1')), 2)
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_return_none(self, jenkins_mock):
        jenkins_mock.return_value = None

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_job_info(u'TestJob')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/TestJob/api/json?depth=0'))
        self.assertEqual(
            str(context_manager.exception),
            'job[TestJob] does not exist')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_return_invalid_json(self, jenkins_mock):
        jenkins_mock.return_value = 'Invalid JSON'

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_job_info(u'TestJob')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/TestJob/api/json?depth=0'))
        self.assertEqual(
            str(context_manager.exception),
            'Could not parse JSON info for job[TestJob]')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_raise_HTTPError(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.HTTPError(
            self.make_url('job/TestJob/api/json?depth=0'),
            code=401,
            msg="basic auth failed",
            hdrs=[],
            fp=None)

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_job_info(u'TestJob')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/TestJob/api/json?depth=0'))
        self.assertEqual(
            str(context_manager.exception),
            'job[TestJob] does not exist')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder_raise_HTTPError(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.HTTPError(
            self.make_url('job/a%20Folder/job/TestJob/api/json?depth=0'),
            code=401,
            msg="basic auth failed",
            hdrs=[],
            fp=None)

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_job_info(u'a Folder/TestJob')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/TestJob/api/json?depth=0'))
        self.assertEqual(
            str(context_manager.exception),
            'job[a Folder/TestJob] does not exist')
        self._check_requests(jenkins_mock.call_args_list)
