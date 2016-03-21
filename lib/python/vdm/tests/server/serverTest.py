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


__host_name__ = socket.gethostname()
__host_or_ip__ = socket.gethostbyname(__host_name__)

URL = 'http://%s:8000/api/1.0/databases/1/servers/' % \
(__host_or_ip__)
__db_url__ = 'http://%s:8000/api/1.0/databases/' % \
             (__host_or_ip__)


class Server(unittest.TestCase):
    """
    test cases for Server
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
                # Delete database
                db_url = __db_url__ + str(last_db_id)
                response = requests.delete(db_url)
                self.assertEqual(response.status_code, 200)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"


class CreateServer(Server):
    """
    Create Server
    """

    def test_get_servers(self):
        """
        ensure GET server list
        """
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']

            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)

            response = requests.get(url)
            value = response.json()
            if not value:
                print "The Server list is empty"
            self.assertEqual(response.status_code, 200)

    def test_validate_host_name(self):
        """
        ensure server name is not empty
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']

        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        # url = URL + str(last_db_id)
        data = {'description': 'test', 'hostname': '', 'name': 'test'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['errors'][0], 'Host name is required.')
        self.assertEqual(response.status_code, 200)

    def test_validate_port(self):
        """
        Validate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': 'test12345', 'admin-listener': '88888'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'][0], 'Port must be greater than 1 and less than 65535')

    def test_validate_ip_address(self):
        """
        Validate IP Address
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': 'test12345', 'internal-interface': '127.0.0.12345'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'][0], 'Invalid IP address.')

    def test_validate_duplicate_port(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': 'test12345', 'admin-listener': '88', 'client-listener': '88'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Duplicate port')

    def test_validate_duplicate_http_port(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Duplicate port')

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345', 'http-listener': '8080'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 8080 for the same host is already used by server test123 for '
                                              'http-listener')

    def test_validate_duplicate_admin_port(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Duplicate port')

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345', 'admin-listener': '21211', 'http-listener': '34'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 21211 for the same host is already used by server test123 for '
                                              'admin-listener')

    def test_validate_duplicate_internal_port(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Duplicate port')

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345', 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '3021'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 3021 for the same host is already used by server test123 '
                                              'for internal-listener')

    def test_validate_duplicate_zookeeper_port(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Duplicate port')

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345', 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                'zookeeper-listener': '7181', 'replication-listener': '567'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 7181 for the same host is already used by server test123 for '
                                              'zookeeper-listener')

    def test_validate_duplicate_client_port(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Duplicate port')

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': __host_or_ip__, 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                'zookeeper-listener': '71', 'replication-listener': '55', 'client-listener': '21212'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 21212 for the same host is already used by server test123 for '
                                              'client-listener')

    def test_validate_duplicate_replication_port(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Duplicate port')

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': __host_or_ip__, 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                'zookeeper-listener': '71', 'replication-listener': '5555'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 5555 for the same host is already used by server test123 for '
                                              'replication-listener')


class UpdateServer(Server):
    """
    Update server
    """

    def test_validate_hostname(self):
        """
        ensure host name is not empty
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
             (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': '', 'name': 'test'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['errors'][0], 'Host name is required.')
        self.assertEqual(response.status_code, 200)

    def test_update_servers(self):
        """
        Ensure update server is working
        """

        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            db_data = {'dbId': last_db_id}
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                print "ServerId to be updated is " + str(last_server_id)
                url += str(last_server_id)
                db_data = {'description': 'test123'}
                response = requests.put(url, db_data)
                self.assertEqual(response.status_code, 200)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"

    def test_validate_invalid_server(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/1' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': 'test12345', 'admin-listener': '88', 'client-listener': '88'}
        response = requests.put(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['statusstring'], 'Given server with id 1 doesn\'t belong to database with id 2.')

    def test_validate_duplicate_port_update(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/2/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': 'test12345', 'admin-listener': '88', 'client-listener': '88'}
        response = requests.put(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Duplicate port')

    def test_validate_duplicate_http_port_update(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)

        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/2/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345', 'http-listener': '8080'}
        response = requests.put(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 8080 for the same host is already used by server test123 for '
                                              'http-listener')

    def test_validate_duplicate_admin_port_update(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/2/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345', 'admin-listener': '21211', 'http-listener': '34'}
        response = requests.put(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 21211 for the same host is already used by server test123 for '
                                              'admin-listener')

    def test_validate_duplicate_internal_port_update(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/2/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345', 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '3021'}
        response = requests.put(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 3021 for the same host is already used by server test123 '
                                              'for internal-listener')

    def test_validate_duplicate_zookeeper_port_update(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/2/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345', 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                'zookeeper-listener': '7181', 'replication-listener': '567'}
        response = requests.put(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 7181 for the same host is already used by server test123 for '
                                              'zookeeper-listener')

    def test_validate_duplicate_client_port_update(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__, last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/2/' % \
            (__host_or_ip__, last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': __host_or_ip__, 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                'zookeeper-listener': '71', 'replication-listener': '55', 'client-listener': '21212'}
        response = requests.put(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 21212 for the same host is already used by server test123 for '
                                              'client-listener')

    def test_validate_duplicate_replication_port_update(self):
        """
        Validate duplicate the port
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)

        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = 'http://%s:8000/api/1.0/databases/%u/servers/2/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': "test123",
                'name': __host_or_ip__, 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                'zookeeper-listener': '71', 'replication-listener': '5555'}
        response = requests.put(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'], 'Port 5555 for the same host is already used by server test123 for '
                                              'replication-listener')


class DeleteServer(unittest.TestCase):
    """
    Delete server
    """
    def test_delete_server(self):
        """
        server delete test
        """

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
                url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
                url += str(last_server_id)
                response = requests.delete(url)
                value = response.json()
                if response.status_code == 403:
                    print value['statusstring']
                    self.assertEqual(value['statusstring'], 'Cannot delete a running server')
                else:
                    self.assertEqual(response.status_code, 200)

                    db_url = __db_url__ + str(last_db_id)
                    response = requests.delete(db_url)
                    self.assertEqual(response.status_code, 200)
            else:
                print "The Server list is empty"

        # # Create a db
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        db_data = {'name': 'Database'}
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
            data = {'hostname': __host_or_ip__, 'name':__host_name__}
            response = requests.post(url, json=data, headers=headers)
            if response.status_code == 201:
                self.assertEqual(response.status_code, 201)
            else:
                self.assertEqual(response.status_code, 404)



if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
    unittest.main()
