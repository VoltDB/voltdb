# Software License Agreement (BSD License)
#
# Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above
#    copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided
#    with the distribution.
#  * Neither the name of Willow Garage, Inc. nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# 'AS IS' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.


import json
from mock import patch
from testscenarios.scenarios import multiply_scenarios

import jenkins
from jenkins import plugins
from tests.base import JenkinsTestBase


class JenkinsPluginsBase(JenkinsTestBase):

    plugin_info_json = {
        u"plugins":
        [
            {
                u"active": u'true',
                u"backupVersion": u'null',
                u"bundled": u'true',
                u"deleted": u'false',
                u"dependencies": [],
                u"downgradable": u'false',
                u"enabled": u'true',
                u"hasUpdate": u'true',
                u"longName": u"Jenkins Mailer Plugin",
                u"pinned": u'false',
                u"shortName": u"mailer",
                u"supportsDynamicLoad": u"MAYBE",
                u"url": u"http://wiki.jenkins-ci.org/display/JENKINS/Mailer",
                u"version": u"1.5"
            }
        ]
    }

    updated_plugin_info_json = {
        u"plugins":
        [
            dict(plugin_info_json[u"plugins"][0],
                 **{u"version": u"1.6"})
        ]
    }


class JenkinsPluginsInfoTest(JenkinsPluginsBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jenkins_mock.return_value = json.dumps(self.plugin_info_json)

        # expected to return a list of plugins
        plugins_info = self.j.get_plugins_info()
        self.assertEqual(plugins_info, self.plugin_info_json['plugins'])
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('pluginManager/api/json?depth=2'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_return_none(self, jenkins_mock):
        empty_plugin_info_json = {u"plugins": []}

        jenkins_mock.return_value = json.dumps(empty_plugin_info_json)

        plugins_info = self.j.get_plugins_info()
        self.assertEqual(plugins_info, [])
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_depth(self, jenkins_mock):
        jenkins_mock.return_value = json.dumps(self.plugin_info_json)

        self.j.get_plugins_info(depth=1)
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('pluginManager/api/json?depth=1'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_raise_BadStatusLine(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.BadStatusLine('not a valid status line')

        with self.assertRaises(jenkins.BadHTTPException) as context_manager:
            self.j.get_plugins_info()
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('pluginManager/api/json?depth=2'))
        self.assertEqual(
            str(context_manager.exception),
            'Error communicating with server[{0}/]'.format(self.base_url))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_return_invalid_json(self, jenkins_mock):
        jenkins_mock.return_value = 'not valid JSON'

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_plugins_info()
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('pluginManager/api/json?depth=2'))
        self.assertEqual(
            str(context_manager.exception),
            'Could not parse JSON info for server[{0}/]'.format(self.base_url))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_raise_HTTPError(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.HTTPError(
            self.make_url('job/pluginManager/api/json?depth=2'),
            code=401,
            msg="basic auth failed",
            hdrs=[],
            fp=None)

        with self.assertRaises(jenkins.BadHTTPException) as context_manager:
            self.j.get_plugins_info(depth=52)
        self.assertEqual(
            str(context_manager.exception),
            'Error communicating with server[{0}/]'.format(self.base_url))
        self._check_requests(jenkins_mock.call_args_list)


class JenkinsPluginInfoTest(JenkinsPluginsBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_shortname(self, jenkins_mock):
        jenkins_mock.return_value = json.dumps(self.plugin_info_json)

        # expected to return info on a single plugin
        plugin_info = self.j.get_plugin_info("mailer")
        self.assertEqual(plugin_info, self.plugin_info_json['plugins'][0])
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_longname(self, jenkins_mock):
        jenkins_mock.return_value = json.dumps(self.plugin_info_json)

        # expected to return info on a single plugin
        plugin_info = self.j.get_plugin_info("Jenkins Mailer Plugin")
        self.assertEqual(plugin_info, self.plugin_info_json['plugins'][0])
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_get_plugin_info_updated(self, jenkins_mock):

        jenkins_mock.side_effect = [
            json.dumps(self.plugin_info_json),
            json.dumps(self.updated_plugin_info_json)
        ]
        j = jenkins.Jenkins(self.make_url(''), 'test', 'test')

        plugins_info = j.get_plugins()
        self.assertEqual(plugins_info["mailer"]["version"],
                         self.plugin_info_json['plugins'][0]["version"])

        self.assertNotEqual(
            plugins_info["mailer"]["version"],
            self.updated_plugin_info_json['plugins'][0]["version"])

        plugins_info = j.get_plugins()
        self.assertEqual(
            plugins_info["mailer"]["version"],
            self.updated_plugin_info_json['plugins'][0]["version"])

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_return_none(self, jenkins_mock):
        jenkins_mock.return_value = json.dumps(self.plugin_info_json)

        # expected not to find bogus so should return None
        plugin_info = self.j.get_plugin_info("bogus")
        self.assertEqual(plugin_info, None)
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_depth(self, jenkins_mock):
        jenkins_mock.return_value = json.dumps(self.plugin_info_json)

        self.j.get_plugin_info('test', depth=1)
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('pluginManager/api/json?depth=1'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_raise_BadStatusLine(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.BadStatusLine('not a valid status line')

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_plugin_info('test')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('pluginManager/api/json?depth=2'))
        self.assertEqual(
            str(context_manager.exception),
            'Error communicating with server[{0}/]'.format(self.base_url))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_return_invalid_json(self, jenkins_mock):
        jenkins_mock.return_value = 'not valid JSON'

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_plugin_info('test')
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('pluginManager/api/json?depth=2'))
        self.assertEqual(
            str(context_manager.exception),
            'Could not parse JSON info for server[{0}/]'.format(self.base_url))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_raise_HTTPError(self, jenkins_mock):
        jenkins_mock.side_effect = jenkins.HTTPError(
            self.make_url('job/pluginManager/api/json?depth=2'),
            code=401,
            msg="basic auth failed",
            hdrs=[],
            fp=None)

        with self.assertRaises(jenkins.JenkinsException) as context_manager:
            self.j.get_plugin_info(u'TestPlugin', depth=52)
        self.assertEqual(
            str(context_manager.exception),
            'Error communicating with server[{0}/]'.format(self.base_url))
        self._check_requests(jenkins_mock.call_args_list)


class PluginsTestScenarios(JenkinsPluginsBase):

    scenarios = multiply_scenarios(
        JenkinsPluginsBase.scenarios,
        [
            ('s1', dict(v1='1.0.0', op='__gt__', v2='0.8.0')),
            ('s2', dict(v1='1.0.1alpha', op='__gt__', v2='1.0.0')),
            ('s3', dict(v1='1.0', op='__eq__', v2='1.0.0')),
            ('s4', dict(v1='1.0', op='__eq__', v2='1.0')),
            ('s5', dict(v1='1.0', op='__lt__', v2='1.8.0')),
            ('s6', dict(v1='1.0.1alpha', op='__lt__', v2='1.0.1')),
            ('s7', dict(v1='1.0alpha', op='__lt__', v2='1.0.0')),
            ('s8', dict(v1='1.0-alpha', op='__lt__', v2='1.0.0')),
            ('s9', dict(v1='1.1-alpha', op='__gt__', v2='1.0')),
            ('s10', dict(v1='1.0-SNAPSHOT', op='__lt__', v2='1.0')),
            ('s11', dict(v1='1.0.preview', op='__lt__', v2='1.0')),
            ('s12', dict(v1='1.1-SNAPSHOT', op='__gt__', v2='1.0')),
            ('s13', dict(v1='1.0a-SNAPSHOT', op='__lt__', v2='1.0a')),
        ])

    def setUp(self):
        super(PluginsTestScenarios, self).setUp()

        plugin_info_json = dict(self.plugin_info_json)
        plugin_info_json[u"plugins"][0][u"version"] = self.v1

        patcher = patch.object(jenkins.Jenkins, 'jenkins_open')
        self.jenkins_mock = patcher.start()
        self.addCleanup(patcher.stop)
        self.jenkins_mock.return_value = json.dumps(plugin_info_json)

    def test_plugin_version_comparison(self):
        """Verify that valid versions are ordinally correct.

        That is, for each given scenario, v1.op(v2)==True where 'op' is the
        equality operator defined for the scenario.
        """
        plugin_name = "Jenkins Mailer Plugin"
        j = jenkins.Jenkins(self.base_url, 'test', 'test')
        plugin_info = j.get_plugins()[plugin_name]
        v1 = plugin_info.get("version")

        op = getattr(v1, self.op)

        self.assertTrue(op(self.v2),
                        msg="Unexpectedly found {0} {2} {1} == False "
                            "when comparing versions!"
                            .format(v1, self.v2, self.op))

    def test_plugin_version_object_comparison(self):
        """Verify use of PluginVersion for comparison

        Verify that converting the version to be compared to the same object
        type of PluginVersion before comparing provides the same result.
        """
        plugin_name = "Jenkins Mailer Plugin"
        j = jenkins.Jenkins(self.base_url, 'test', 'test')
        plugin_info = j.get_plugins()[plugin_name]
        v1 = plugin_info.get("version")

        op = getattr(v1, self.op)
        v2 = plugins.PluginVersion(self.v2)

        self.assertTrue(op(v2),
                        msg="Unexpectedly found {0} {2} {1} == False "
                            "when comparing versions!"
                            .format(v1, v2, self.op))


class PluginsTest(JenkinsPluginsBase):

    def test_plugin_equal(self):

        p1 = plugins.Plugin(self.plugin_info_json)
        p2 = plugins.Plugin(self.plugin_info_json)

        self.assertEqual(p1, p2)

    def test_plugin_not_equal(self):

        p1 = plugins.Plugin(self.plugin_info_json)
        p2 = plugins.Plugin(self.plugin_info_json)
        p2[u'version'] = u"1.6"

        self.assertNotEqual(p1, p2)
