"""
This file is part of VoltDB.

Copyright (C) 2008-2016 VoltDB Inc.

This file contains original code and/or modifications of original code.
Any modifications made by VoltDB Inc. are licensed under the following
terms and conditions:

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
"""

import unittest
import requests
import xmlrunner
import socket
import time
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
                self.assertEqual(response.status_code, 200)
                # Delete database
                db_url = __db_url__ + str(last_db_id)
                response = requests.delete(db_url)
                self.assertEqual(response.status_code, 200)
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
                self.assertEqual(response.status_code, 200)
                # Delete database
                db_url = __db_url__ + str(last_db_id)
                response = requests.delete(db_url)
                self.assertEqual(response.status_code, 200)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"


class DefaultStartServer(Server):
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
            if not value['statusstring']:
                print "The Server list is empty"
            elif "Start request sent successfully to servers" in value['statusstring']:
                self.assertEqual(response.status_code, 200)
                time.sleep(20)
                CheckServerStatus(self, last_db_id, 'running')
                time.sleep(10)
                print "Stopping Cluster...."
                url_stop = 'http://%s:8000/api/1.0/databases/%u/stop' % \
                (__host_or_ip__,last_db_id)
                response = requests.put(url_stop)
                value = response.json()
                if "Server shutdown successfully." in value['statusstring']:
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
            if not value['statusstring']:
                print "error"
            elif "Start request sent successfully to servers" in value['statusstring']:
                self.assertEqual(response.status_code, 200)
                time.sleep(20)
                CheckServerStatus(self, last_db_id, 'running')
                time.sleep(10)
                print "Stopping...."
                url_stop = 'http://%s:8000/api/1.0/databases/%u/stop' % \
                (__host_or_ip__,last_db_id)
                response = requests.put(url_stop)
                value = response.json()
                if "Server shutdown successfully." in value['statusstring']:
                    self.assertEqual(response.status_code, 200)
                    time.sleep(15)
                    CheckServerStatus(self, last_db_id, 'stopped')
            elif response.status_code == 500:
                self.assertEqual(response.status_code, 500)

class DefaultRecoverServer(Server):
    """
    Create Server
    """

    def test_recover_stop_server(self):
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
            print "Recovering..."
            response = requests.put(url)
            value = response.json()

            if not value['statusstring']:
                print "Error"
            elif "FATAL: VoltDB Community Edition" in value['statusstring']:
                print "Voltdb recover is only supported in Enterprise Edition"
            elif "Start request sent successfully to servers" in value['statusstring']:
                self.assertEqual(response.status_code, 200)
                time.sleep(20)
                CheckServerStatus(self, last_db_id, 'running')
                time.sleep(10)
                print "Stopping Cluster...."
                url_stop = 'http://%s:8000/api/1.0/databases/%u/stop' % \
                (__host_or_ip__,last_db_id)
                response = requests.put(url_stop)
                value = response.json()
                if "Connection broken" in value['statusstring']:
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
    if value['status'] and value['status'][0]['status']:
        print "Status: " + value['status'][0]['status']
        self.assertEqual(value['status'][0]['status'], status)
        self.assertEqual(value['serverDetails'][0][__host_or_ip__]['status'], status)
    else:
        assert False

if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
    unittest.main()
