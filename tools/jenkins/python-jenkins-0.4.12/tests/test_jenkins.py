import json
import socket

from mock import patch, Mock
import six
from six.moves.urllib.error import HTTPError

import jenkins
from tests.base import JenkinsTestBase


def get_mock_urlopen_return_value(a_dict=None):
    if a_dict is None:
        a_dict = {}
    return six.BytesIO(json.dumps(a_dict).encode('utf-8'))


class JenkinsConstructorTest(JenkinsTestBase):

    def test_url_with_trailing_slash(self):
        self.assertEqual(self.j.server, self.make_url(''))
        self.assertEqual(self.j.auth, b'Basic dGVzdDp0ZXN0')
        self.assertEqual(self.j.crumb, None)

    def test_url_without_trailing_slash(self):
        j = jenkins.Jenkins(self.base_url, 'test', 'test')
        self.assertEqual(j.server, self.make_url(''))
        self.assertEqual(j.auth, b'Basic dGVzdDp0ZXN0')
        self.assertEqual(j.crumb, None)

    def test_without_user_or_password(self):
        j = jenkins.Jenkins('{0}'.format(self.base_url))
        self.assertEqual(j.server, self.make_url(''))
        self.assertEqual(j.auth, None)
        self.assertEqual(j.crumb, None)

    def test_unicode_password(self):
        j = jenkins.Jenkins('{0}'.format(self.base_url),
                            six.u('nonascii'),
                            six.u('\xe9\u20ac'))
        self.assertEqual(j.server, self.make_url(''))
        self.assertEqual(j.auth, b'Basic bm9uYXNjaWk6w6nigqw=')
        self.assertEqual(j.crumb, None)

    def test_long_user_or_password(self):
        long_str = 'a' * 60
        long_str_b64 = 'YWFh' * 20

        j = jenkins.Jenkins('{0}'.format(self.base_url), long_str, long_str)

        self.assertNotIn(b"\n", j.auth)
        self.assertEqual(j.auth.decode('utf-8'), 'Basic %s' % (
            long_str_b64 + 'Om' + long_str_b64[2:] + 'YQ=='))

    def test_default_timeout(self):
        j = jenkins.Jenkins('{0}'.format(self.base_url))
        self.assertEqual(j.timeout, socket._GLOBAL_DEFAULT_TIMEOUT)

    def test_custom_timeout(self):
        j = jenkins.Jenkins('{0}'.format(self.base_url), timeout=300)
        self.assertEqual(j.timeout, 300)


class JenkinsMaybeAddCrumbTest(JenkinsTestBase):

    @patch('jenkins.urlopen')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.NotFoundException()
        request = jenkins.Request(self.make_url('job/TestJob'))

        self.j.maybe_add_crumb(request)

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('crumbIssuer/api/json'))
        self.assertFalse(self.j.crumb)
        self.assertFalse('.crumb' in request.headers)
        self._check_requests(jenkins_mock.call_args_list)

    @patch('jenkins.urlopen')
    def test_with_data(self, jenkins_mock):
        jenkins_mock.return_value = get_mock_urlopen_return_value(self.crumb_data)
        request = jenkins.Request(self.make_url('job/TestJob'))

        self.j.maybe_add_crumb(request)

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('crumbIssuer/api/json'))
        self.assertEqual(self.j.crumb, self.crumb_data)
        self.assertEqual(request.headers['.crumb'], self.crumb_data['crumb'])
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_return_empty_response(self, jenkins_mock):
        "Don't try to create crumb header from an empty response"
        jenkins_mock.side_effect = jenkins.EmptyResponseException("empty response")
        request = jenkins.Request(self.make_url('job/TestJob'))

        self.j.maybe_add_crumb(request)

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('crumbIssuer/api/json'))
        self.assertFalse(self.j.crumb)
        self.assertFalse('.crumb' in request.headers)
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsOpenTest(JenkinsTestBase):

    @patch('jenkins.urlopen')
    def test_simple(self, jenkins_mock):
        data = {'foo': 'bar'}
        jenkins_mock.side_effect = [
            get_mock_urlopen_return_value(self.crumb_data),
            get_mock_urlopen_return_value(data),
        ]
        request = jenkins.Request(self.make_url('job/TestJob'))

        response = self.j.jenkins_open(request)

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/TestJob'))
        self.assertEqual(response, json.dumps(data))
        self.assertEqual(self.j.crumb, self.crumb_data)
        self.assertEqual(request.headers['.crumb'], self.crumb_data['crumb'])
        self._check_requests(jenkins_mock.call_args_list)

    @patch('jenkins.urlopen')
    def test_response_403(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.HTTPError(
            self.make_url('job/TestJob'),
            code=401,
            msg="basic auth failed",
            hdrs=[],
            fp=None)
        request = jenkins.Request(self.make_url('job/TestJob'))

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.jenkins_open(request, add_crumb=False)
        self.assertEqual(
            str(context_manager.exception),
            'Error in request. Possibly authentication failed [401]: '
            'basic auth failed')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/TestJob'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch('jenkins.urlopen')
    def test_response_404(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.HTTPError(
            self.make_url('job/TestJob'),
            code=404,
            msg="basic auth failed",
            hdrs=[],
            fp=None)
        request = jenkins.Request(self.make_url('job/TestJob'))

        with self.assertRaises(jenkins.NotFoundException) as context_manager:
            self.j.jenkins_open(request, add_crumb=False)
        self.assertEqual(
            str(context_manager.exception),
            'Requested item could not be found')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/TestJob'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch('jenkins.urlopen')
    def test_empty_response(self, jenkins_mock):
        jenkins_mock.return_value = Mock(**{'read.return_value': None})

        request = jenkins.Request(self.make_url('job/TestJob'))

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.jenkins_open(request, False)
        self.assertEqual(
            str(context_manager.exception),
            'Error communicating with server[{0}/]: '
            'empty response'.format(self.base_url))
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/TestJob'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch('jenkins.urlopen')
    def test_response_501(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.HTTPError(
            self.make_url('job/TestJob'),
            code=501,
            msg="Not implemented",
            hdrs=[],
            fp=None)
        request = jenkins.Request(self.make_url('job/TestJob'))

        with self.assertRaises(HTTPError) as context_manager:
            self.j.jenkins_open(request, add_crumb=False)
        self.assertEqual(
            str(context_manager.exception),
            'HTTP Error 501: Not implemented')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/TestJob'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch('jenkins.urlopen')
    def test_timeout(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.URLError(
            reason="timed out")
        j = jenkins.Jenkins(self.make_url(''), 'test', 'test', timeout=1)
        request = jenkins.Request(self.make_url('job/TestJob'))

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            j.jenkins_open(request, add_crumb=False)
        self.assertEqual(
            str(context_manager.exception),
            'Error in request: timed out')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('job/TestJob'))
        self._check_requests(jenkins_mock.call_args_list)
