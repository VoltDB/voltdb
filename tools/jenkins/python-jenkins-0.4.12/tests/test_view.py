import json
from mock import patch

import jenkins
from tests.base import JenkinsTestBase


class JenkinsViewsTestBase(JenkinsTestBase):
    config_xml = """
        <listView>
            <description>Foo</description>
            <jobNames />
        </listView>"""


class JenkinsGetViewNameTest(JenkinsViewsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        view_name_to_return = {u'name': 'Test View'}
        jenkins_mock.return_value = json.dumps(view_name_to_return)

        view_name = self.j.get_view_name(u'Test View')

        self.assertEqual(view_name, 'Test View')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('view/Test%20View/api/json?tree=name'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_return_none(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.NotFoundException()

        view_name = self.j.get_view_name(u'TestView')

        self.assertEqual(view_name, None)
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('view/TestView/api/json?tree=name'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_unexpected_view_name(self, jenkins_mock):
        view_name_to_return = {u'name': 'not the right name'}
        jenkins_mock.return_value = json.dumps(view_name_to_return)

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_view_name(u'TestView')
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('view/TestView/api/json?tree=name'))
        self.assertEqual(
            str(context_manager.exception),
            'Jenkins returned an unexpected view name {0} '
            '(expected: {1})'.format(view_name_to_return['name'], 'TestView'))
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsAssertViewTest(JenkinsViewsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_view_missing(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.NotFoundException()

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.assert_view_exists('NonExistent')
        self.assertEqual(
            str(context_manager.exception),
            'view[NonExistent] does not exist')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_view_exists(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'ExistingView'}),
        ]
        self.j.assert_view_exists('ExistingView')
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsGetViewsTest(JenkinsViewsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        views = {
            u'url': u'http://your_url_here/view/my_view/',
            u'name': u'my_view',
        }
        view_info_to_return = {u'views': views}
        jenkins_mock.return_value = json.dumps(view_info_to_return)

        view_info = self.j.get_views()

        self.assertEqual(view_info, views)
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('api/json'))
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsDeleteViewTest(JenkinsViewsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            None,
            jenkins.NotFoundException(),
        ]

        self.j.delete_view(u'Test View')

        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('view/Test%20View/doDelete'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_failed(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'TestView'}),
            json.dumps({'name': 'TestView'}),
            json.dumps({'name': 'TestView'}),
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.delete_view(u'TestView')
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('view/TestView/doDelete'))
        self.assertEqual(
            str(context_manager.exception),
            'delete[TestView] failed')
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsCreateViewTest(JenkinsViewsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            jenkins.NotFoundException(),
            None,
            json.dumps({'name': 'Test View'}),
        ]

        self.j.create_view(u'Test View', self.config_xml)

        self.assertEqual(
            jenkins_mock.call_args_list[1][0][0].get_full_url(),
            self.make_url('createView?name=Test%20View'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_already_exists(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'TestView'}),
            None,
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.create_view(u'TestView', self.config_xml)
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('view/TestView/api/json?tree=name'))
        self.assertEqual(
            str(context_manager.exception),
            'view[TestView] already exists')
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_failed(self, jenkins_mock):
        jenkins_mock.side_effect = [
            jenkins.NotFoundException(),
            None,
            jenkins.NotFoundException(),
        ]

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.create_view(u'TestView', self.config_xml)
        self.assertEqual(
            jenkins_mock.call_args_list[0][0][0].get_full_url(),
            self.make_url('view/TestView/api/json?tree=name'))
        self.assertEqual(
            jenkins_mock.call_args_list[1][0][0].get_full_url(),
            self.make_url('createView?name=TestView'))
        self.assertEqual(
            str(context_manager.exception),
            'create[TestView] failed')
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsReconfigViewTest(JenkinsViewsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.side_effect = [
            json.dumps({'name': 'Test View'}),
            None,
        ]

        self.j.reconfig_view(u'Test View', self.config_xml)

        self.assertEqual(jenkins_mock.call_args[0][0].get_full_url(),
                         self.make_url('view/Test%20View/config.xml'))
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsGetViewConfigTest(JenkinsViewsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_encodes_view_name(self, jenkins_mock):
        self.j.get_view_config(u'Test View')

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('view/Test%20View/config.xml'))
        self._check_requests(jenkins_mock.call_args_list)
