from six.moves import StringIO
import testtools
from testtools.content import text_content

import jenkins
from tests.helper import NullServer
from tests.helper import TestsTimeoutException
from tests.helper import time_limit


class JenkinsRequestTimeoutTests(testtools.TestCase):

    def setUp(self):
        super(JenkinsRequestTimeoutTests, self).setUp()
        self.server = NullServer(("127.0.0.1", 0))
        self.messages = StringIO()
        self.addOnException(self._get_messages)

    def _get_messages(self, exc_info):
        self.addDetail('timeout-tests-messages',
                       text_content(self.messages.getvalue()))

    def test_jenkins_open_timeout(self):
        j = jenkins.Jenkins("http://%s:%s" % self.server.server_address,
                            None, None, timeout=0.1)
        request = jenkins.Request('http://%s:%s/job/TestJob' %
                                  self.server.server_address)

        # assert our request times out when no response
        with testtools.ExpectedException(jenkins.TimeoutException):
            j.jenkins_open(request, add_crumb=False)

    def test_jenkins_open_no_timeout(self):
        j = jenkins.Jenkins("http://%s:%s" % self.server.server_address,
                            None, None)
        request = jenkins.Request('http://%s:%s/job/TestJob' %
                                  self.server.server_address)

        # assert we don't timeout quickly like previous test when
        # no timeout defined.
        with testtools.ExpectedException(TestsTimeoutException):
            time_limit(0.5, self.messages,
                       j.jenkins_open, request, add_crumb=False)
