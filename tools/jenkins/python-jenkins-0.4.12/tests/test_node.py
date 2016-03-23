import json
from mock import patch

import jenkins
from tests.base import JenkinsTestBase


class JenkinsNodesTestBase(JenkinsTestBase):

    def setUp(self):
        super(JenkinsNodesTestBase, self).setUp()
        self.node_info = {
            'displayName': 'test node',
            'totalExecutors': 5,
        }
        self.online_node_info = dict(self.node_info)
        self.online_node_info['offline'] = False
        self.offline_node_info = dict(self.node_info)
        self.offline_node_info['offline'] = True


class JenkinsGetNodesTest(JenkinsNodesTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.return_value = json.dumps({
            "computer": [{
                "displayName": "master",
                "offline": False
            }],
            "busyExecutors": 2})
        self.assertEqual(self.j.get_nodes(),
                         [{'name': 'master', 'offline': False}])
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_return_invalid_json(self, jenkins_mock):
        jenkins_mock.side_effect = [
            'Invalid JSON',
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_nodes()
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('computer/api/json'))
        self.assertEqual(
            str(context_manager.exception),
            'Could not parse JSON info for server[{0}/]'.format(self.base_url))
        self._check_requests(jenkins_mock.call_args_list)

    @patch('jenkins.urlopen')
    def test_raise_BadStatusLine(self, urlopen_mock):
        urlopen_mock.side_effect = jenkins.BadStatusLine('not a valid status line')
        with self.assertRaises(jenkins.BadHTTPException) as context_manager:
            self.j.get_nodes()
        self.assertEqual(
            str(context_manager.exception),
            'Error communicating with server[{0}/]'.format(self.base_url))
        self._check_requests(urlopen_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_raise_HTTPError(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.HTTPError(
            self.make_url('job/TestJob'),
            code=401,
            msg="basic auth failed",
            hdrs=[],
            fp=None)

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_nodes()
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('computer/api/json'))
        self.assertEqual(
            str(context_manager.exception),
            'Error communicating with server[{0}/]'.format(self.base_url))
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsGetNodeInfoTest(JenkinsNodesTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps(self.node_info),
        ]

        self.assertEqual(self.j.get_node_info('test node'), self.node_info)
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('computer/test%20node/api/json?depth=0'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_return_invalid_json(self, jenkins_mock):
        jenkins_mock.side_effect = [
            'Invalid JSON',
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_node_info('test_node')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('computer/test_node/api/json?depth=0'))
        self.assertEqual(
            str(context_manager.exception),
            'Could not parse JSON info for node[test_node]')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_raise_HTTPError(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.HTTPError(
            self.make_url('job/TestJob'),
            code=401,
            msg="basic auth failed",
            hdrs=[],
            fp=None)

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_node_info('test_node')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('computer/test_node/api/json?depth=0'))
        self.assertEqual(
            str(context_manager.exception),
            'node[test_node] does not exist')
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsAssertNodeExistsTest(JenkinsNodesTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_node_missing(self, jenkins_mock):
        jenkins_mock.side_effect = [None]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.assert_node_exists('NonExistentNode')
        self.assertEqual(
            str(context_manager.exception),
            'node[NonExistentNode] does not exist')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_node_exists(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'ExistingNode'})
        ]
        self.j.assert_node_exists('ExistingNode')
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsDeleteNodeTest(JenkinsNodesTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps(self.node_info),
            None,
            None,
            None,
        ]

        self.j.delete_node('test node')

        self.assertEqual(
            jenkins_mock.call_args_list[1][0][0].get_full_url(),
            self.make_url('computer/test%20node/doDelete'))
        self.assertFalse(self.j.node_exists('test node'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_failed(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps(self.node_info),
            None,
            json.dumps(self.node_info),
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.delete_node('test_node')
        self.assertEqual(
            jenkins_mock.call_args_list[1][0][0].get_full_url(),
            self.make_url('computer/test_node/doDelete'))
        self.assertEqual(
            str(context_manager.exception),
            'delete[test_node] failed')
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsCreateNodeTest(JenkinsNodesTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            None,
            None,
            json.dumps(self.node_info),
            json.dumps(self.node_info),
        ]

        self.j.create_node('test node', exclusive=True)

        self.assertEqual(
            jenkins_mock.call_args_list[1][0][0].get_full_url().split('?')[0],
            self.make_url('computer/doCreateItem'))
        self.assertTrue(self.j.node_exists('test node'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_urlencode(self, jenkins_mock):
        jenkins_mock.side_effect = [
            None,
            None,
            json.dumps(self.node_info),
            json.dumps(self.node_info),
        ]
        params = {
            'port': '22',
            'username': 'juser',
            'credentialsId': '10f3a3c8-be35-327e-b60b-a3e5edb0e45f',
            'host': 'my.jenkins.slave1'
        }
        self.j.create_node(
            'slave1',
            nodeDescription='my test slave',
            remoteFS='/home/juser',
            labels='precise',
            exclusive=True,
            launcher=jenkins.LAUNCHER_SSH,
            launcher_params=params)

        actual = jenkins_mock.call_args_list[1][0][0].get_full_url()
        # As python dicts do not guarantee order so the parameters get
        # re-ordered when it gets processed by _get_encoded_params(),
        # verify sections of the URL with self.assertIn() instead of
        # the entire URL
        self.assertIn(u'username%22%3A+%22juser', actual)
        self.assertIn(
            u'stapler-class%22%3A+%22hudson.plugins.sshslaves.SSHLauncher',
            actual)
        self.assertIn(u'host%22%3A+%22my.jenkins.slave1', actual)
        self.assertIn(
            u'credentialsId%22%3A+%2210f3a3c8-be35-327e-b60b-a3e5edb0e45f',
            actual)
        self.assertIn(u'port%22%3A+%2222', actual)
        self.assertIn(u'remoteFS%22%3A+%22%2Fhome%2Fjuser', actual)
        self.assertIn(u'labelString%22%3A+%22precise', actual)
        self.assertIn(u'name%22%3A+%22slave1', actual)
        self.assertIn(
            u'type%22%3A+%22hudson.slaves.DumbSlave%24DescriptorImpl',
            actual)
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_already_exists(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps(self.node_info),
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.create_node('test_node')
        self.assertEqual(
            str(context_manager.exception),
            'node[test_node] already exists')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_failed(self, jenkins_mock):
        jenkins_mock.side_effect = [
            None,
            None,
            None,
            None,
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.create_node('test_node')
        self.assertEqual(
            jenkins_mock.call_args_list[1][0][0].get_full_url().split('?')[0],
            self.make_url('computer/doCreateItem'))
        self.assertEqual(
            str(context_manager.exception),
            'create[test_node] failed')
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsEnableNodeTest(JenkinsNodesTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps(self.offline_node_info),
            None,
        ]

        self.j.enable_node('test node')

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            '{0}/computer/test%20node/'
            'toggleOffline?offlineMessage='.format(self.base_url))

        jenkins_mock.side_effect = [json.dumps(self.online_node_info)]
        node_info = self.j.get_node_info('test node')
        self.assertEqual(node_info, self.online_node_info)
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_offline_false(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps(self.online_node_info),
            None,
        ]

        self.j.enable_node('test_node')

        # Node was not offline; so enable_node skips toggle
        # Last call to jenkins was to check status
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('computer/test_node/api/json?depth=0'))

        jenkins_mock.side_effect = [json.dumps(self.online_node_info)]
        node_info = self.j.get_node_info('test_node')
        self.assertEqual(node_info, self.online_node_info)
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsDisableNodeTest(JenkinsNodesTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps(self.online_node_info),
            None,
        ]

        self.j.disable_node('test node')

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            '{0}/computer/test%20node/'
            'toggleOffline?offlineMessage='.format(self.base_url))

        jenkins_mock.side_effect = [json.dumps(self.offline_node_info)]
        node_info = self.j.get_node_info('test node')
        self.assertEqual(node_info, self.offline_node_info)
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_offline_true(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps(self.offline_node_info),
            None,
        ]

        self.j.disable_node('test_node')

        # Node was already offline; so disable_node skips toggle
        # Last call to jenkins was to check status
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('computer/test_node/api/json?depth=0'))

        jenkins_mock.side_effect = [json.dumps(self.offline_node_info)]
        node_info = self.j.get_node_info('test_node')
        self.assertEqual(node_info, self.offline_node_info)
        self._check_requests(jenkins_mock.call_args_list)
