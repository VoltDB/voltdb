import json
from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsReconfigJobTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'Test Job'}),
            None,
        ]

        self.j.reconfig_job(u'Test Job', self.config_xml)

        self.assertEqual(jenkins_mock.call_args[0][0].get_full_url(),
                         self.make_url('job/Test%20Job/config.xml'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_in_folder(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'Test Job'}),
            None,
        ]

        self.j.reconfig_job(u'a Folder/Test Job', self.config_xml)

        self.assertEqual(jenkins_mock.call_args[0][0].get_full_url(),
                         self.make_url('job/a%20Folder/job/Test%20Job/config.xml'))
        self._check_requests(jenkins_mock.call_args_list)
