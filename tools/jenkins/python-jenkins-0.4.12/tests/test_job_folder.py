from mock import patch

import jenkins
from tests.base import JenkinsTestBase


class JenkinsGetJobFolderTest(JenkinsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        folder, name = self.j._get_job_folder('my job')
        self.assertEqual(folder, '')
        self.assertEqual(name, 'my job')

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_single_level(self, jenkins_mock):
        folder, name = self.j._get_job_folder('my folder/my job')
        self.assertEqual(folder, 'job/my folder/')
        self.assertEqual(name, 'my job')

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_multi_level(self, jenkins_mock):
        folder, name = self.j._get_job_folder('folder1/folder2/my job')
        self.assertEqual(folder, 'job/folder1/job/folder2/')
        self.assertEqual(name, 'my job')
