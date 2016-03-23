from mock import patch

import jenkins
from tests.base import JenkinsTestBase


class JenkinsScriptTest(JenkinsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_run_script(self, jenkins_mock):
        self.j.run_script(u'println(\"Hello World!\")')

        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('scriptText'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_install_plugin(self, jenkins_mock):
        '''Installation of plugins is done with the run_script method
        '''
        j = jenkins.Jenkins(self.make_url(''), 'test', 'test')
        j.install_plugin("jabber")
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('scriptText'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    @patch.object(jenkins.Jenkins, 'run_script')
    def test_install_plugin_with_dependencies(self, run_script_mock, jenkins_mock):
        '''Verify install plugins with dependencies
        '''
        j = jenkins.Jenkins(self.make_url(''), 'test', 'test')
        j.install_plugin("jabber")
        self.assertEqual(len(run_script_mock.call_args_list), 2)
        self.assertEqual(run_script_mock.call_args_list[0][0][0],
                         ('Jenkins.instance.updateCenter.getPlugin(\"jabber\")'
                          '.getNeededDependencies().each{it.deploy()};Jenkins'
                          '.instance.updateCenter.getPlugin(\"jabber\").deploy();'))
        self.assertEqual(run_script_mock.call_args_list[1][0][0],
                         ('Jenkins.instance.updateCenter'
                          '.isRestartRequiredForCompletion()'))

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    @patch.object(jenkins.Jenkins, 'run_script')
    def test_install_plugin_without_dependencies(self, run_script_mock, jenkins_mock):
        '''Verify install plugins without dependencies
        '''
        j = jenkins.Jenkins(self.make_url(''), 'test', 'test')
        j.install_plugin("jabber", include_dependencies=False)
        self.assertEqual(len(run_script_mock.call_args_list), 2)
        self.assertEqual(run_script_mock.call_args_list[0][0][0],
                         ('Jenkins.instance.updateCenter'
                          '.getPlugin(\"jabber\").deploy();'))
        self.assertEqual(run_script_mock.call_args_list[1][0][0],
                         ('Jenkins.instance.updateCenter'
                          '.isRestartRequiredForCompletion()'))

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    @patch.object(jenkins.Jenkins, 'run_script')
    def test_install_plugin_no_restart(self, run_script_mock, jenkins_mock):
        '''Verify install plugin does not need a restart
        '''
        run_script_mock.return_value = u'Result: false\n'
        j = jenkins.Jenkins(self.make_url(''), 'test', 'test')
        self.assertFalse(j.install_plugin("jabber"))

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    @patch.object(jenkins.Jenkins, 'run_script')
    def test_install_plugin_restart(self, run_script_mock, jenkins_mock):
        '''Verify install plugin needs a restart
        '''
        run_script_mock.return_value = u'Result: true\n'
        j = jenkins.Jenkins(self.make_url(''), 'test', 'test')
        self.assertTrue(j.install_plugin("jabber"))
