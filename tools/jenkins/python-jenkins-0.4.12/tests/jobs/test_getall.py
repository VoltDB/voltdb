from mock import patch

import jenkins
from tests.jobs.base import build_jobs_list_responses
from tests.jobs.base import JenkinsGetJobsTestBase


class JenkinsGetAllJobsTest(JenkinsGetJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        response = build_jobs_list_responses(
            self.jobs_in_folder, 'http://example.com/')
        jenkins_mock.side_effect = iter(response)

        jobs_info = self.j.get_all_jobs()

        expected_fullnames = [
            u"my_job1", u"my_job2",
            u"my_folder1/my_job3", u"my_folder1/my_job4"
        ]
        self.assertEqual(len(expected_fullnames), len(jobs_info))
        got_fullnames = [job[u"fullname"] for job in jobs_info]
        self.assertEqual(expected_fullnames, got_fullnames)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_multi_level(self, jenkins_mock):
        response = build_jobs_list_responses(
            self.jobs_in_multiple_folders, 'http://example.com/')
        jenkins_mock.side_effect = iter(response)

        jobs_info = self.j.get_all_jobs()

        expected_fullnames = [
            u"my_job1", u"my_job2",
            u"my_folder1/my_job3", u"my_folder1/my_job4",
            u"my_folder1/my_folder2/my_job1", u"my_folder1/my_folder2/my_job2"
        ]
        self.assertEqual(len(expected_fullnames), len(jobs_info))
        got_fullnames = [job[u"fullname"] for job in jobs_info]
        self.assertEqual(expected_fullnames, got_fullnames)
        # multiple jobs with same name
        self.assertEqual(2, len([True
                                 for job in jobs_info
                                 if job['name'] == u"my_job1"]))

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_folders_depth(self, jenkins_mock):
        response = build_jobs_list_responses(
            self.jobs_in_multiple_folders, 'http://example.com/')
        jenkins_mock.side_effect = iter(response)

        jobs_info = self.j.get_all_jobs(folder_depth=1)

        expected_fullnames = [
            u"my_job1", u"my_job2",
            u"my_folder1/my_job3", u"my_folder1/my_job4"
        ]
        self.assertEqual(len(expected_fullnames), len(jobs_info))
        got_fullnames = [job[u"fullname"] for job in jobs_info]
        self.assertEqual(expected_fullnames, got_fullnames)
