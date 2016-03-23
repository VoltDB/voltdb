from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsSetNextBuildNumberTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        self.j.set_next_build_number('TestJob', 1234)

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/TestJob/nextbuildnumber/submit'))
        self.assertEqual(
            jenkins_mock.call_args[0][0].data,
            b'nextBuildNumber=1234')
        self._check_requests(jenkins_mock.call_args_list)
