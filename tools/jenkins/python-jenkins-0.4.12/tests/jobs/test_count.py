import json
from mock import patch

import jenkins
from tests.jobs.base import JenkinsJobsTestBase


class JenkinsJobsCountTest(JenkinsJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jobs = [
            {u'url': u'http://localhost:8080/job/guava/', u'color': u'notbuilt', u'name': u'guava'},
            {u'url': u'http://localhost:8080/job/kiwi/', u'color': u'blue', u'name': u'kiwi'},
            {u'url': u'http://localhost:8080/job/lemon/', u'color': u'red', u'name': u'lemon'}
        ]
        job_info_to_return = {u'jobs': jobs}
        jenkins_mock.return_value = json.dumps(job_info_to_return)
        self.assertEqual(self.j.jobs_count(), 3)
        self._check_requests(jenkins_mock.call_args_list)
