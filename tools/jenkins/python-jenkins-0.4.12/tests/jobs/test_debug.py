import json
from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsDebugJobInfoTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_debug_job_info(self, jenkins_mock):
        job_info_to_return = {
            u'building': False,
            u'msg': u'test',
            u'revision': 66,
            u'user': u'unknown'
        }
        jenkins_mock.return_value = json.dumps(job_info_to_return)

        self.j.debug_job_info(u'Test Job')

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

        self.j.debug_job_info(u'a Folder/Test Job')

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/a%20Folder/job/Test%20Job/api/json?depth=0'))
        self._check_requests(jenkins_mock.call_args_list)
