import json
from mock import patch

import jenkins
from tests.jobs.base import build_jobs_list_responses
from tests.jobs.base import JenkinsGetJobsTestBase


class JenkinsGetJobsTest(JenkinsGetJobsTestBase):

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_simple(self, jenkins_mock):
        jobs = {
            u'url': u'http://your_url_here/job/my_job/',
            u'color': u'blue',
            u'name': u'my_job',
        }
        job_info_to_return = {u'jobs': jobs}
        jenkins_mock.return_value = json.dumps(job_info_to_return)

        job_info = self.j.get_jobs()

        jobs[u'fullname'] = jobs[u'name']
        self.assertEqual(job_info, [jobs])
        self.assertEqual(
            jenkins_mock.call_args[0][0].get_full_url(),
            self.make_url('api/json?tree=jobs[url,color,name,jobs]'))
        self._check_requests(jenkins_mock.call_args_list)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_folders_simple(self, jenkins_mock):
        response = build_jobs_list_responses(
            self.jobs_in_folder, self.make_url(''))
        jenkins_mock.side_effect = iter(response)

        jobs_info = self.j.get_jobs()

        expected_fullnames = [
            u"my_job1", u"my_job2"
        ]
        self.assertEqual(len(expected_fullnames), len(jobs_info))
        got_fullnames = [job[u"fullname"] for job in jobs_info]
        self.assertEqual(expected_fullnames, got_fullnames)

    @patch.object(jenkins.Jenkins, 'jenkins_open')
    def test_folders_additional_level(self, jenkins_mock):
        response = build_jobs_list_responses(
            self.jobs_in_folder, self.make_url(''))
        jenkins_mock.side_effect = iter(response)

        jobs_info = self.j.get_jobs(folder_depth=1)

        expected_fullnames = [
            u"my_job1", u"my_job2",
            u"my_folder1/my_job3", u"my_folder1/my_job4"
        ]
        self.assertEqual(len(expected_fullnames), len(jobs_info))
        got_fullnames = [job[u"fullname"] for job in jobs_info]
        self.assertEqual(expected_fullnames, got_fullnames)
