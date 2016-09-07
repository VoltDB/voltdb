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
import socket
import time
import xmlrunner
import json

__host_name__ = socket.gethostname()
__host_or_ip__ = socket.gethostbyname(__host_name__)

URL = 'http://%s:8000/api/1.0/databases/1/servers/' % \
(__host_or_ip__)
__db_url__ = 'http://%s:8000/api/1.0/databases/' % \
             (__host_or_ip__)


class Server(unittest.TestCase):
    """
    Test case for adding and deleting servers
    """
    def setUp(self):
        """
        # Create a db
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        db_data = {'name': 'testDB'}
        response = requests.post(__db_url__, json=db_data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)
        # Create a server
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']

            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)

            data = {'description': 'test', 'hostname': __host_or_ip__, 'name': 'test'}
            response = requests.post(url, json=data, headers=headers)
            if response.status_code == 201:
                self.assertEqual(response.status_code, 201)
            else:
                self.assertEqual(response.status_code, 404)
        else:
            print "The database list is empty"

    def tearDown(self):
        """
        Delete the server
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                print "ServerId to be deleted is " + str(last_server_id)
                url += str(last_server_id)
                response = requests.delete(url)
                self.assertEqual(response.status_code, 204)
                # Delete database
                db_url = __db_url__ + str(last_db_id)
                response = requests.delete(db_url)
                self.assertEqual(response.status_code, 204)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"


class Cluster(unittest.TestCase):
    """
    Test case for adding and deleting servers with ports other than the default ports
    """
    def setUp(self):
        """
        # Create a db
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        db_data = {'name': 'testDB1'}
        response = requests.post(__db_url__, json=db_data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)
        # Create a server
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']

            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)

            data = {'description': 'test', 'hostname': __host_or_ip__, 'name': 'test', 'client-listener': '21214', 'admin-listener': '12345'}
            response = requests.post(url, json=data, headers=headers)
            if response.status_code == 201:
                self.assertEqual(response.status_code, 201)
            else:
                self.assertEqual(response.status_code, 404)
        else:
            print "The database list is empty"

    def tearDown(self):
        """
        Delete the server
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                print "ServerId to be deleted is " + str(last_server_id)
                url += str(last_server_id)
                response = requests.delete(url)
                self.assertEqual(response.status_code, 204)
                # Delete database
                db_url = __db_url__ + str(last_db_id)
                response = requests.delete(db_url)
                self.assertEqual(response.status_code, 204)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"


class Default_01_StartServer(Server):
    """
    Create Server
    """

    def test_start_stop_server(self):
        """
        ensure Start and stop server is working properly
        """
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']

            url = 'http://%s:8000/api/1.0/databases/%u/start' % \
                (__host_or_ip__,last_db_id)
            print "Starting..."
            response = requests.put(url)
            value = response.json()
            if not value['statusString']:
                print "The Server list is empty"
            elif "Start request sent successfully to servers" in value['statusString']:
                self.assertEqual(response.status_code, 200)
                time.sleep(20)
                CheckServerStatus(self, last_db_id, 'running')
                time.sleep(10)
                print "Stopping Cluster...."
                url_stop = 'http://%s:8000/api/1.0/databases/%u/stop' % \
                (__host_or_ip__,last_db_id)
                response = requests.put(url_stop)
                value = response.json()
                if "Server shutdown successfully." in value['statusString']:
                    self.assertEqual(response.status_code, 200)
                    time.sleep(15)
                    CheckServerStatus(self, last_db_id, 'stopped')
            elif response.status_code == 500:
                self.assertEqual(response.status_code, 500)

    def test_start_stop_server_pause_admin_mode(self):
        """
        ensure Start and stop server is working properly
        """
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']

            url = 'http://%s:8000/api/1.0/databases/%u/start?pause=true' % \
                (__host_or_ip__,last_db_id)
            print "Starting with pause enabled..."
            response = requests.put(url)
            value = response.json()
            if not value['statusString']:
                print "The Server list is empty"
            elif "Start request sent successfully to servers" in value['statusString']:
                self.assertEqual(response.status_code, 200)
                time.sleep(20)
                CheckServerStatus(self, last_db_id, 'running')
                time.sleep(10)
                print "Stopping Cluster...."
                url_stop = 'http://%s:8000/api/1.0/databases/%u/stop' % \
                (__host_or_ip__,last_db_id)
                response = requests.put(url_stop)
                value = response.json()
                if "Server shutdown successfully." in value['statusString']:
                    self.assertEqual(response.status_code, 200)
                    time.sleep(15)
                    CheckServerStatus(self, last_db_id, 'stopped')
            elif response.status_code == 500:
                self.assertEqual(response.status_code, 500)

    def test_start_stop_server_force_create_new_db(self):
        """
        ensure Start and stop server is working properly
        """
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']

            url = 'http://%s:8000/api/1.0/databases/%u/start?force=true' % \
                (__host_or_ip__,last_db_id)
            print "Starting with force enabled to create a new database..."
            response = requests.put(url)
            value = response.json()
            if not value['statusString']:
                print "The Server list is empty"
            elif "Start request sent successfully to servers" in value['statusString']:
                self.assertEqual(response.status_code, 200)
                time.sleep(20)
                CheckServerStatus(self, last_db_id, 'running')
                time.sleep(10)
                print "Stopping Cluster...."
                url_stop = 'http://%s:8000/api/1.0/databases/%u/stop' % \
                (__host_or_ip__,last_db_id)
                response = requests.put(url_stop)
                value = response.json()
                if "Server shutdown successfully." in value['statusString']:
                    self.assertEqual(response.status_code, 200)
                    time.sleep(15)
                    CheckServerStatus(self, last_db_id, 'stopped')
            elif response.status_code == 500:
                self.assertEqual(response.status_code, 500)


class StartServer(Cluster):
    """
    Start Server in different port
    """

    def test_start_stop_server_with_different_ports(self):
        """
        ensure Start and stop server is working properly using ports other than default ports
        """
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']

            url = 'http://%s:8000/api/1.0/databases/%u/start' % \
                (__host_or_ip__,last_db_id)

            response = requests.put(url)
            print "Starting...."
            value = response.json()
            if not value['statusString']:
                print "error"
            elif "Start request sent successfully to servers" in value['statusString']:
                self.assertEqual(response.status_code, 200)
                time.sleep(20)
                CheckServerStatus(self, last_db_id, 'running')
                time.sleep(10)
                print "Stopping...."
                url_stop = 'http://%s:8000/api/1.0/databases/%u/stop' % \
                (__host_or_ip__,last_db_id)
                response = requests.put(url_stop)
                value = response.json()
                if "Server shutdown successfully." in value['statusString']:
                    self.assertEqual(response.status_code, 200)
                    time.sleep(15)
                    CheckServerStatus(self, last_db_id, 'stopped')
            elif response.status_code == 500:
                self.assertEqual(response.status_code, 500)


class Default_02_RecoverServer(Server):
    """
    Recover
    """
    def test_recover_stop_server(self):
        """
        Ensure Start and stop server is working properly
        """
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            url = 'http://%s:8000/api/1.0/databases/%u/recover' % \
                (__host_or_ip__,last_db_id)
            print "Recovering..."
            response = requests.put(url)
            value = response.json()

            if not value['statusString']:
                print "Error"
            elif "FATAL: VoltDB Community Edition" in value['statusString']:
                print "Voltdb recover is only supported in Enterprise Edition"
            elif "Start request sent successfully to servers" in value['statusString']:
                self.assertEqual(response.status_code, 200)
                time.sleep(20)
                CheckServerStatus(self, last_db_id, 'running')
                time.sleep(10)
                print "Stopping Cluster...."
                url_stop = 'http://%s:8000/api/1.0/databases/%u/stop' % \
                            (__host_or_ip__, last_db_id)

                response = requests.put(url_stop)
                value = response.json()
                if "Server shutdown successfully." in value['statusString']:
                    self.assertEqual(response.status_code, 200)
                    time.sleep(15)
                    CheckServerStatus(self, last_db_id, 'stopped')
            elif response.status_code == 500:
                self.assertEqual(response.status_code, 500)

    def test_recover_stop_server_pause_admin_mode(self):
        """
        ensure Start and stop server is working properly
        """
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']

            url = 'http://%s:8000/api/1.0/databases/%u/recover' % \
                (__host_or_ip__,last_db_id)
            print "Recovering with pause enabled..."
            response = requests.put(url)
            value = response.json()
            if not value['statusString']:
                print "Error"
            elif "FATAL: VoltDB Community Edition" in value['statusString']:
                print "Voltdb recover is only supported in Enterprise Edition"
            elif "Start request sent successfully to servers" in value['statusString']:
                self.assertEqual(response.status_code, 200)
                time.sleep(20)
                CheckServerStatus(self, last_db_id, 'running')
                time.sleep(10)
                print "Stopping Cluster...."
                url_stop = 'http://%s:8000/api/1.0/databases/%u/stop' % \
                (__host_or_ip__, last_db_id)
                response = requests.put(url_stop)
                value = response.json()
                if "Server shutdown successfully." in value['statusString']:
                    self.assertEqual(response.status_code, 200)
                    time.sleep(15)
                    CheckServerStatus(self, last_db_id, 'stopped')
            elif response.status_code == 500:
                self.assertEqual(response.status_code, 500)


def CheckServerStatus(self, last_db_id, status):
    status_url = 'http://%s:8000/api/1.0/databases/%u/status/' % \
    (__host_or_ip__,last_db_id)
    print "Checking status..."
    response = requests.get(status_url)
    value = response.json()
    if value['status'] and value['dbStatus']['status']:
        print "Status: " + value['dbStatus']['status']
        self.assertEqual(value['dbStatus']['status'], status)
        self.assertEqual(value['dbStatus']['serverStatus'][0][__host_or_ip__]['status'], status)
    else:
        assert False

if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
