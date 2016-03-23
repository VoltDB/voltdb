import json
from mock import patch

import jenkins
from tests.base import JenkinsTestBase


class JenkinsCancelQueueTest(JenkinsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        job_name_to_return = {u'name': 'TestJob'}
        jenkins_mock.return_value = json.dumps(job_name_to_return)

        self.j.cancel_queue(52)

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('queue/cancelItem?id=52'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open',
                  side_effect=jenkins.NotFoundException('not found'))
    def test_notfound(self, jenkins_mock):
        job_name_to_return = {u'name': 'TestJob'}
        jenkins_mock.return_value = json.dumps(job_name_to_return)

        self.j.cancel_queue(52)

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('queue/cancelItem?id=52'))
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsQueueInfoTest(JenkinsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        queue_info_to_return = {
            'items': {
                u'task': {
                    u'url': u'http://your_url/job/my_job/',
                    u'color': u'aborted_anime',
                    u'name': u'my_job'
                },
                u'stuck': False,
                u'actions': [
                    {
                        u'causes': [
                            {
                                u'shortDescription': u'Started by timer',
                            },
                        ],
                    },
                ],
                u'buildable': False,
                u'params': u'',
                u'buildableStartMilliseconds': 1315087293316,
                u'why': u'Build #2,532 is already in progress (ETA:10 min)',
                u'blocked': True,
            }
        }
        jenkins_mock.return_value = json.dumps(queue_info_to_return)

        queue_info = self.j.get_queue_info()

        self.assertEqual(queue_info, queue_info_to_return['items'])
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('queue/api/json?depth=0'))
        self._check_requests(jenkins_mock.call_args_list)
