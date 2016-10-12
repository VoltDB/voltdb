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
import xmlrunner

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
            self.assertEqual(response.status_code, 400)
        # Create a server
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']

            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__, last_db_id)

            data = {'description': 'test', 'hostname': __host_or_ip__, 'name': 'test',
                    'admin-listener': '21211', 'client-listener': '21212', 'http-listener': '8080',
                    'internal-listener': '3021', 'replication-listener': '5555',
                    'zookeeper-listener': '7181'}
            response = requests.post(url, json=data, headers=headers)
            value = response.json()
            if value['status'] == 201:
                self.assertEqual(value['statusString'], 'OK')
                self.assertEqual(value['status'], 201)
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


class CreateServer(Server):
    """
    Create Server
    """

    def test_get_servers(self):
        """
        ensure GET server list
        """
        print "Running test to check get server list API."
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
            self.assertEqual(value['statusString'], 'OK')
            self.assertEqual(value['status'], 200)
        print "Completed test to check get server list API."

    def test_request_with_id(self):
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
        data = {'description': 'test', 'hostname': '', 'name': 'test', 'id':3}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['statusString'], 'You cannot specify \'Id\' while creating server.')
        self.assertEqual(response.status_code, 404)

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
        data = {'description': 'test', 'hostname': '', 'name': 'test'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['statusString'][0], 'Host name is required.')
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
            self.assertEqual(value['statusString'][0], 'Port must be greater than 1 and less than 65535')

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
            self.assertEqual(value['statusString'][0], 'Invalid IP address.')

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
            self.assertEqual(value['statusString'], 'Duplicate port')

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
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': 'test12345', 'http-listener': '8080'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['statusString'], 'Port 8080 for the same host is already used by server %s for '
                                              'http-listener.' % __host_or_ip__)

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
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': 'test12345', 'admin-listener': '21211', 'http-listener': '34'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['statusString'], 'Port 21211 for the same host is already used by server %s for '
                                              'admin-listener.' % __host_or_ip__)

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
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': 'test12345', 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '3021'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['statusString'], 'Port 3021 for the same host is already used by server %s '
                                              'for internal-listener.' % __host_or_ip__)

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
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': 'test12345', 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                'zookeeper-listener': '7181', 'replication-listener': '567'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['statusString'], 'Port 7181 for the same host is already used by server %s for '
                                              'zookeeper-listener.' % __host_or_ip__)

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
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': __host_or_ip__, 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                'zookeeper-listener': '71', 'replication-listener': '555', 'client-listener': '21212'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['statusString'], 'Port 21212 for the same host is already used by server %s for '
                                              'client-listener.' % __host_or_ip__)

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
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': __host_or_ip__, 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                'zookeeper-listener': '71', 'replication-listener': '5555'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['statusString'], 'Port 5555 for the same host is already used by server %s for '
                                              'replication-listener.' % __host_or_ip__)

    def test_validate_invalid_field_values(self):
        """
        ensure server name is not empty
        """
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            # test invalid admin-listener value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'admin-listener': True}
            check_invalid_value(self, 'admin-listener', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'admin-listener': 111}
            check_invalid_value(self, 'admin-listener', data, url, 'POST')

            # test invalid zookeeper-listener value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'zookeeper-listener': True}
            check_invalid_value(self, 'zookeeper-listener', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'zookeeper-listener': 111}
            check_invalid_value(self, 'zookeeper-listener', data, url, 'POST')

            # test invalid replication-listener value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'replication-listener': True}
            check_invalid_value(self, 'replication-listener', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'replication-listener': 111}
            check_invalid_value(self, 'replication-listener', data, url, 'POST')

            # test invalid client-listener value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'client-listener': True}
            check_invalid_value(self, 'client-listener', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'client-listener': 111}
            check_invalid_value(self, 'client-listener', data, url, 'POST')

            # test invalid internal-interface value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'internal-interface': True}
            check_invalid_value(self, 'internal-interface', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'internal-interface': 111}
            check_invalid_value(self, 'internal-interface', data, url, 'POST')

            # test invalid external-interface value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'external-interface': True}
            check_invalid_value(self, 'external-interface', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'external-interface': 111}
            check_invalid_value(self, 'external-interface', data, url, 'POST')

            # test invalid public-interface value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'public-interface': True}
            check_invalid_value(self, 'public-interface', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'public-interface': 111}
            check_invalid_value(self, 'public-interface', data, url, 'POST')

            # test invalid internal-listener value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'internal-listener': True}
            check_invalid_value(self, 'internal-listener', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'internal-listener': 111}
            check_invalid_value(self, 'internal-listener', data, url, 'POST')

            #  invalid http-listener value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'http-listener': True}
            check_invalid_value(self, 'http-listener', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'http-listener': 111}
            check_invalid_value(self, 'http-listener', data, url, 'POST')

            # test invalid name value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'name': True}
            check_invalid_value(self, 'name', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'name': 111}
            check_invalid_value(self, 'name', data, url, 'POST')

            # test invalid description value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'description': True}
            check_invalid_value(self, 'description', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'description': 111}
            check_invalid_value(self, 'description', data, url, 'POST')

            # test invalid placement-group value
            data = {'description': 'test', 'hostname': __host_or_ip__, 'placement-group': True}
            check_invalid_value(self, 'placement-group', data, url, 'POST')
            data = {'description': 'test', 'hostname': __host_or_ip__, 'placement-group': 111}
            check_invalid_value(self, 'placement-group', data, url, 'POST')

        else:
            print "Database is empty."


class UpdateServer(Server):
    """
    Update server
    """
    def test_request_with_id(self):
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
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                print "ServerId to be updated is " + str(last_server_id)
                url += str(last_server_id) + '/'
                data = {'description': 'test123', 'hostname': __host_or_ip__,
                        'name': 'test12345', 'id': 33333}
                response = requests.put(url, json=data, headers=headers)
                value = response.json()
                self.assertEqual(value['statusString'], 'Server Id mentioned in the payload and url doesn\'t match.')
                self.assertEqual(response.status_code, 404)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"

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
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                url = 'http://%s:8000/api/1.0/databases/%u/servers/%u/' % \
                     (__host_or_ip__,last_db_id,last_server_id)
                data = {'description': 'test', 'hostname': '', 'name': 'test'}
                response = requests.put(url, json=data, headers=headers)
                value = response.json()
                self.assertEqual(value['statusString'][0], 'Host name is required.')
                self.assertEqual(response.status_code, 200)
            else:
                print "The Server list is empty"

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
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                print "ServerId to be updated is " + str(last_server_id)
                url += str(last_server_id) + '/'
                data = {'description': 'test123'}
                response = requests.put(url, json=data, headers=headers)
                self.assertEqual(response.status_code, 200)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"

    def test_update_voltdbroot_directories(self):
        """
        Ensure update server is working
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
                print "ServerId to be updated is " + str(last_server_id)
                url += str(last_server_id) + '/'
                data = {'voltdbroot': 'test_voltdbroot'}
                response = requests.put(url, json=data, headers=headers)
                self.assertEqual(response.status_code, 200)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"

    def test_update_snapshots_directories(self):
        """
        Ensure update server is working
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
                print "ServerId to be updated is " + str(last_server_id)
                url += str(last_server_id) + '/'
                data = {'snapshots': 'test_snapshot'}
                response = requests.put(url, json=data, headers=headers)
                self.assertEqual(response.status_code, 200)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"

    def test_update_commandlogsnapshot_directories(self):
        """
        Ensure update server is working
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
                print "ServerId to be updated is " + str(last_server_id)
                url += str(last_server_id) + '/'
                data = {'commandlogsnapshot': 'test_commandlogsnapshot'}
                response = requests.put(url, json=data, headers=headers)
                self.assertEqual(response.status_code, 200)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"

    def test_update_export_overflow_directories(self):
        """
        Ensure update server is working
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
                print "ServerId to be updated is " + str(last_server_id)
                url += str(last_server_id) + '/'
                data = {'exportoverflow': 'test_overflow'}
                response = requests.put(url, json=data, headers=headers)
                self.assertEqual(response.status_code, 200)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"

    def test_update_dr_overflow_directories(self):
        """
        Ensure update server is working
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
                print "ServerId to be updated is " + str(last_server_id)
                url += str(last_server_id) + '/'
                data = {'droverflow': 'test_dr_overflow'}
                response = requests.put(url, json=data, headers=headers)
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
        url = 'http://%s:8000/api/1.0/databases/%u/servers/1/' % \
            (__host_or_ip__,last_db_id)
        data = {'description': 'test', 'hostname': __host_or_ip__,
                'name': 'test12345', 'admin-listener': '88', 'client-listener': '88'}
        response = requests.put(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['statusString'], 'Given server with id 1 doesn\'t belong to database with id %u.' %last_db_id)

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
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                url = 'http://%s:8000/api/1.0/databases/%u/servers/%u/' % \
                    (__host_or_ip__,last_db_id, last_server_id)
                data = {'description': 'test', 'hostname': __host_or_ip__,
                        'name': 'test12345', 'admin-listener': '88', 'client-listener': '88'}
                response = requests.put(url, json=data, headers=headers)
                value = response.json()
                if response.status_code == 201:
                    self.assertEqual(response.status_code, 201)
                else:
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(value['statusString'], 'Duplicate port')
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"

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
        data = {'description': 'test', 'hostname': __host_or_ip__, 'name': 'test12345',
                'admin-listener': '11', 'client-listener': '22', 'http-listener': '33',
                'internal-listener': '44', 'replication-listener': '55',
                'zookeeper-listener': '66'}
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
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                url = 'http://%s:8000/api/1.0/databases/%u/servers/%u/' % \
                    (__host_or_ip__, last_db_id, last_server_id)
                data = {'description': 'test', 'hostname': __host_or_ip__,
                        'name': 'test12345', 'http-listener': '8080'}
                response = requests.put(url, json=data, headers=headers)
                value = response.json()
                if response.status_code == 201:
                    self.assertEqual(response.status_code, 201)
                else:
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(value['statusString'], 'Port 8080 for the same host is already used by server %s for '
                                                      'http-listener.' % __host_or_ip__)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"

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
            data = {'description': 'test', 'hostname': __host_or_ip__, 'name': 'test12345',
                    'admin-listener': '21111', 'client-listener': '22121', 'http-listener': '8801',
                    'internal-listener': '30211', 'replication-listener': '55551',
                    'zookeeper-listener': '7111'}
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
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                url = 'http://%s:8000/api/1.0/databases/%u/servers/%u/' % \
                    (__host_or_ip__, last_db_id, last_server_id)
                data = {'description': 'test', 'hostname': __host_or_ip__,
                        'name': 'test12345', 'admin-listener': '21211', 'http-listener': '34'}
                response = requests.put(url, json=data, headers=headers)
                value = response.json()
                if response.status_code == 201:
                    self.assertEqual(response.status_code, 201)
                else:
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(value['statusString'], 'Port 21211 for the same host is already used by server %s for '
                                                      'admin-listener.' % __host_or_ip__)

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
        data = {'description': 'test', 'hostname': __host_or_ip__, 'name': 'test12345',
                'admin-listener': '11', 'client-listener': '22', 'http-listener': '33',
                'internal-listener': '44', 'replication-listener': '55',
                'zookeeper-listener': '66'}
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
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                url = 'http://%s:8000/api/1.0/databases/%u/servers/%u/' % \
                    (__host_or_ip__, last_db_id, last_server_id)
                data = {'description': 'test', 'hostname': __host_or_ip__,
                        'name': 'test12345', 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '3021'}
                response = requests.put(url, json=data, headers=headers)
                value = response.json()
                if response.status_code == 201:
                    self.assertEqual(response.status_code, 201)
                else:
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(value['statusString'], 'Port 3021 for the same host is already used by server %s '
                                                      'for internal-listener.' % __host_or_ip__)

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
        data = {'description': 'test', 'hostname': __host_or_ip__, 'name': 'test12345',
                'admin-listener': '11', 'client-listener': '22', 'http-listener': '33',
                'internal-listener': '44', 'replication-listener': '55',
                'zookeeper-listener': '66'}
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
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                url = 'http://%s:8000/api/1.0/databases/%u/servers/%u/' % \
                    (__host_or_ip__, last_db_id, last_server_id)
                data = {'description': 'test', 'hostname': __host_or_ip__,
                        'name': 'test12345', 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                        'zookeeper-listener': '7181', 'replication-listener': '567'}
                response = requests.put(url, json=data, headers=headers)
                value = response.json()
                if response.status_code == 201:
                    self.assertEqual(response.status_code, 201)
                else:
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(value['statusString'], 'Port 7181 for the same host is already used by server %s for '
                                                      'zookeeper-listener.' % __host_or_ip__)

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
        data = {'description': 'test', 'hostname': __host_or_ip__, 'name': 'test12345',
                'admin-listener': '11', 'client-listener': '22', 'http-listener': '33',
                'internal-listener': '44', 'replication-listener': '55',
                'zookeeper-listener': '66'}
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
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                url = 'http://%s:8000/api/1.0/databases/%u/servers/%u/' % \
                    (__host_or_ip__, last_db_id, last_server_id)
                data = {'description': 'test', 'hostname': __host_or_ip__,
                        'name': __host_or_ip__, 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                        'zookeeper-listener': '71', 'replication-listener': '4444', 'client-listener': '21212'}
                response = requests.put(url, json=data, headers=headers)
                value = response.json()
                if response.status_code == 201:
                    self.assertEqual(response.status_code, 201)
                else:
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(value['statusString'], 'Port 21212 for the same host is already used by server %s for '
                                                      'client-listener.' % __host_or_ip__)

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
        data = {'description': 'test', 'hostname': __host_or_ip__, 'name': 'test12345',
                'admin-listener': '11', 'client-listener': '22', 'http-listener': '33',
                'internal-listener': '44', 'replication-listener': '55',
                'zookeeper-listener': '66'}
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
            url = 'http://%s:8000/api/1.0/databases/%u/servers/' % \
                (__host_or_ip__,last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                url = 'http://%s:8000/api/1.0/databases/%u/servers/%u/' % \
                    (__host_or_ip__, last_db_id, last_server_id)
                data = {'description': 'test', 'hostname': __host_or_ip__,
                        'name': __host_or_ip__, 'admin-listener': '456', 'http-listener': '34', 'internal-listener': '63',
                        'zookeeper-listener': '71', 'replication-listener': '5555'}
                response = requests.put(url, json=data, headers=headers)
                value = response.json()
                if response.status_code == 201:
                    self.assertEqual(response.status_code, 201)
                else:
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(value['statusString'], 'Port 5555 for the same host is already used by server %s for '
                                                      'replication-listener.' % __host_or_ip__)

    def test_validate_invalid_field_values(self):
        """
        ensure server fields are of correct data type
        """
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
                (__host_or_ip__, last_db_id)
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length-1]['id']
                url = 'http://%s:8000/api/1.0/databases/%u/servers/%u/' % \
                      (__host_or_ip__,last_db_id, last_server_id)
                # test invalid hostname value
                data = {'description': 'test', 'hostname': 11, 'name': 'test'}
                check_invalid_value(self, 'hostname', data, url, 'PUT')
                data = {'description': 'test', 'hostname': True, 'name': 'test'}
                check_invalid_value(self, 'hostname', data, url, 'PUT')

                # test invalid description value
                data = {'description': True, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'description', data, url, 'PUT')
                data = {'description': 11, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'description', data, url, 'PUT')

                # test invalid enabled value
                data = {'enabled': 'test', 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'enabled', data, url, 'PUT')
                data = {'enabled': 11, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'enabled', data, url, 'PUT')

                # test invalid admin-listener value
                data = {'hostname': __host_or_ip__, 'admin-listener': True}
                check_invalid_value(self, 'admin-listener', data, url, 'PUT')
                data = {'hostname': __host_or_ip__, 'admin-listener': 11}
                check_invalid_value(self, 'admin-listener', data, url, 'PUT')

                # test invalid internal-listener value
                data = {'internal-listener': True, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'internal-listener', data, url, 'PUT')
                data = {'internal-listener': 11, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'internal-listener', data, url, 'PUT')

                # test invalid http-listener value
                data = {'http-listener': True, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'http-listener', data, url, 'PUT')
                data = {'http-listener': 11, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'http-listener', data, url, 'PUT')

                # test invalid zookeeper-listener value
                data = {'zookeeper-listener': True, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'zookeeper-listener', data, url, 'PUT')
                data = {'zookeeper-listener': 11, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'zookeeper-listener', data, url, 'PUT')

                # test invalid replication-listener value
                data = {'replication-listener': True, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'replication-listener', data, url, 'PUT')
                data = {'replication-listener': 11, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'replication-listener', data, url, 'PUT')

                # test invalid client-listener value
                data = {'client-listener': True, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'client-listener', data, url, 'PUT')
                data = {'client-listener': 11, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'client-listener', data, url, 'PUT')

                # test invalid internal-interface value
                data = {'internal-interface': True, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'internal-interface', data, url, 'PUT')
                data = {'internal-interface': 11, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'internal-interface', data, url, 'PUT')

                # test invalid external-interface value
                data = {'external-interface': True, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'external-interface', data, url, 'PUT')
                data = {'external-interface': 11, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'external-interface', data, url, 'PUT')

                # test invalid public-interface value
                data = {'public-interface': True, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'public-interface', data, url, 'PUT')
                data = {'public-interface': 11, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'public-interface', data, url, 'PUT')

                # test invalid placement-group value
                data = {'placement-group': True, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'placement-group', data, url, 'PUT')
                data = {'placement-group': 11, 'hostname': __host_or_ip__, 'name': 'test'}
                check_invalid_value(self, 'placement-group', data, url, 'PUT')
            else:
                print "The Server list is empty"


class ServerDelete(unittest.TestCase):
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
                (__host_or_ip__, last_db_id)

            data = {'description': 'test', 'hostname': __host_or_ip__, 'name': 'test'}
            response = requests.post(url, json=data, headers=headers)
            if response.status_code == 201:
                self.assertEqual(response.status_code, 201)
            else:
                self.assertEqual(response.status_code, 404)
        else:
            print "The database list is empty"


class DeleteServer(ServerDelete):
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
                url_non_existing_server =url + str(3)
                response_non_existing_server = requests.delete(url_non_existing_server)
                value_non_existing_server = response_non_existing_server.json()
                print value_non_existing_server['statusstring']
                self.assertEqual(value_non_existing_server['statusstring'], 'No server found for id: 3 in database 2')
                self.assertEqual(response_non_existing_server.status_code, 404)

                url_non_existing_database = 'http://%s:8000/api/1.0/databases/3/servers/' % \
                (__host_or_ip__)
                url_non_existing_database += str(last_server_id)
                response_non_existing_database = requests.delete(url_non_existing_database)
                value_non_existing_database = response_non_existing_database.json()
                print value_non_existing_database['statusstring']
                self.assertEqual(str(value_non_existing_database['statusstring']), 'No database found for id: 3')
                self.assertEqual(response_non_existing_database.status_code, 404)

                url += str(last_server_id)
                response = requests.delete(url)
                if response.status_code == 403:
                    print value['statusstring']
                    self.assertEqual(value['statusstring'], 'Cannot delete a running server')
                else:
                    self.assertEqual(response.status_code, 204)

                    db_url = __db_url__ + str(last_db_id)
                    response = requests.delete(db_url)
                    self.assertEqual(response.status_code, 204)
            else:
                print "The Server list is empty"


def check_invalid_value(self, field, data, url, request_type):
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    if request_type == 'POST':
        response = requests.post(url, json=data, headers=headers)
    else:
        response = requests.put(url, json=data, headers=headers)
    value = response.json()
    self.assertEqual(value['statusString'], 'Invalid value for field ' + field + '.')
    self.assertEqual(response.status_code, 200)

if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
