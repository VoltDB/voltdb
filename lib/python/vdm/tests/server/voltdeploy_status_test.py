# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

import unittest
import requests
import xmlrunner
import socket

__host_name__ = socket.gethostname()
__host_or_ip__ = socket.gethostbyname(__host_name__)
__url__ = 'http://%s:8000/api/1.0/voltdeploy/status/' % __host_or_ip__


class voltDeployStatus(unittest.TestCase):
    def test_get_servers(self):
        """
        test to check server status
        """
        try:
            response = requests.get(__url__)
            value = response.json()
            self.assertEqual(value['voltdeploy']['running'], 'true')
            self.assertEqual(response.status_code, 200)
        except Exception, err:
            self.fail("Voltdeploy server is not running.")

if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
