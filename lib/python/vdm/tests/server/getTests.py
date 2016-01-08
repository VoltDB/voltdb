# This file is part of VoltDB.

# Copyright (C) 2008-2016 VoltDB Inc.
#
# This file contains original code and/or modifications of original code.
# Any modifications made by VoltDB Inc. are licensed under the following
# terms and conditions:
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.


"""
    Some basic tests using Apache "requests" module
    (http://docs.python-requests.org/en/latest/index.html) to handle
    simple communications with the server.

    Get "requests": $ easy_install requests or download the module
    and then install.

    In "setUp", make the request and get the response. Then the
    tests are all specific assertions about the result.

    The requests could be in the tests themselves. There are pluses
    and minuses in all the different possible approaches.

    Next steps -- we can consider this approach, consider "pytest"
    instead of "unittest", and review other options.

    I'll convert this to pytest and we can see if that's better or
    worse or just different.
"""

import requests
import json
import unittest

class TestREST( unittest.TestCase ):
    def setUp(self):
        self.localurl = "http://localhost:8000/api/1.0/servers"
        self.response = requests.get(self.localurl)

    def test_1_get_connect(self):
        self.assertEqual(200, self.response.status_code)

    def test_2_get_servers(self):
        jsonContent = self.response.json()
        self.assertTrue("servers" in jsonContent)

    def test_3_get_server_count(self):
        jsonContent = self.response.json()
        print jsonContent["servers"]
        self.assertTrue(len(jsonContent["servers"]) == 3,
                "Expected 3 servers. Found " + str(len(jsonContent["servers"])))

if __name__ == '__main__':
    unittest.main()
