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
    This test requires installation of flask-Testing from http://pythonhosted.org/Flask-Testing/
    To install flask-Testing use command:
    pip install Flask-Testing


    This test also requires installation of requests library https://pypi.python.org/pypi/requests/
    To install latest requests version 2.8.1 use command:

    sudo pip install requests --upgrade
"""

from flask import Flask
import unittest
import requests
import xmlrunner
import socket


URL = 'http://localhost:8000/api/1.0/servers/'
__db_url__ = 'http://localhost:8000/api/1.0/databases/'


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
            url = URL + str(last_db_id)
            data = {'description': 'test', 'hostname': 'test', 'name': 'test'}
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
            db_data = {'dbId': last_db_id}
            response = requests.get(URL)
            value = response.json()
            if value:
                server_length = len(value['servers'])
                last_server_id = value['servers'][server_length-1]['id']
                print "ServerId to be deleted is " + str(last_server_id)
                url = URL + str(last_server_id)
                response = requests.delete(url, json=db_data, headers=headers)
                self.assertEqual(response.status_code, 200)
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
    def create_app(self):
        """
        Create app
        """
        app = Flask(__name__)
        app.config['TESTING'] = True
        return app

    def test_get_servers(self):
        """
        ensure GET server list
        """
        response = requests.get(URL)
        value = response.json()
        if not value:
            print "The Server list is empty"
        self.assertEqual(response.status_code, 200)

    def test_validate_server_name(self):
        """
        ensure server name is not empty
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = URL + str(last_db_id)
        data = {'description': 'test', 'hostname': 'test', 'name': ''}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['errors'][0], 'Server name is required.')
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
        url = URL + str(last_db_id)
        data = {'description': 'test', 'hostname': '', 'name': 'test'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['errors'][0], 'Host name is required.')
        self.assertEqual(response.status_code, 200)

    def test_validate_duplicate_server(self):
        """
        ensure Duplicate Server name is not added
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = URL + str(last_db_id)
        data = {'description': 'test', 'hostname': 'test12345', 'name': 'test'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            print "new server created"
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)
            self.assertEqual(value['error'], 'Server name already exists')
            print value['error']

    def test_validate_duplicate_host(self):
        """
        ensure Duplicate Host name is not added
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = URL + str(last_db_id)
        data = {'description': 'test', 'hostname': 'test', 'name': 'test12345'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)
            self.assertEqual(value['error'], 'Host name already exists')
            print value['error']

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
        url = URL + str(last_db_id)
        data = {'description': 'test', 'hostname': 'test4567',
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
        url = URL + str(last_db_id)
        data = {'description': 'test', 'hostname': 'test4567',
                'name': 'test12345', 'internal-interface': '127.0.0.12345'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(value['errors'][0], 'Invalid IP address.')


class UpdateServer(Server):
    """
    Update server
    """
    def test_get_servers(self):
        """
        ensure GET server list
        """
        response = requests.get(URL)
        value = response.json()
        if not value:
            print "The Server list is empty"
        self.assertEqual(response.status_code, 200)

    def test_validate_server_name(self):
        """
        ensure server name is not empty
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = URL + str(last_db_id)
        data = {'description': 'test', 'hostname': 'test', 'name': ''}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['errors'][0], 'Server name is required.')
        self.assertEqual(response.status_code, 200)

    def test_validate_hostname(self):
        """
        ensure server name is not empty
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
        url = URL + str(last_db_id)
        data = {'description': 'test', 'hostname': '', 'name': 'test'}
        response = requests.post(url, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['errors'][0], 'Host name is required.')
        self.assertEqual(response.status_code, 200)

    def test_update_servers(self):
        """
        get a serverId
        """
        response = requests.get(URL)
        value = response.json()

        if value:
            server_length = len(value['servers'])
            last_server_id = value['servers'][server_length-1]['id']
            print "ServerId to be updated is " + str(last_server_id)
            url = URL + str(last_server_id)
            response = requests.put(url, json={'description': 'test123'})
            self.assertEqual(response.status_code, 200)
        else:
            print "The Server list is empty"

    def test_validate_duplicate_server(self):
        """
        ensure Duplicate Server name is not added
        """
        response = requests.get(URL)
        value = response.json()

        if value:
            server_length = len(value['servers'])
            last_server_id = value['servers'][server_length-1]['id']
            print "ServerId to be updated is " + str(last_server_id)
            url = URL + str(last_server_id)
            hostname = socket.gethostname()

        response = requests.put(url, json={"description": "test",
                                           "hostname": "test12345", "name": hostname})
        value = response.json()
        if response.status_code == 200:
            self.assertEqual(response.status_code, 200)
        else:
            self.assertEqual(response.status_code, 404)
            self.assertEqual(value['error'], 'Server name already exists')

    def test_validate_duplicate_host(self):
        """
        ensure Duplicate Host name is not added
        """
        # Get a serverId
        response = requests.get(URL)
        value = response.json()

        if value:
            server_length = len(value['servers'])
            last_server_id = value['servers'][server_length-1]['id']
            print "ServerId to be updated is " + str(last_server_id)
            url = URL + str(last_server_id)
            hostname = socket.gethostname()
            hostname_ip = socket.gethostbyname(hostname)
        response = requests.put(url, json={'description': 'test',
                                           'hostname': hostname_ip, 'name': 'test12345'})
        value = response.json()
        if response.status_code == 200:
            self.assertEqual(response.status_code, 200)
        else:
            self.assertEqual(response.status_code, 404)
            self.assertEqual(value['error'], 'Host name already exists')


class DeleteServer(unittest.TestCase):
    """
    Delete server
    """
    def test_delete_server(self):
        """
        server delete test
        """
        # Create a db
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
            url = URL + str(last_db_id)
            data = {'description': 'test', 'hostname': 'test', 'name': 'test'}
            response = requests.post(url, json=data, headers=headers)
            if response.status_code == 201:
                self.assertEqual(response.status_code, 201)
            else:
                self.assertEqual(response.status_code, 404)

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            db_data = {'dbId': last_db_id}
            response = requests.get(URL)
            value = response.json()
            if value:
                server_length = len(value['servers'])
                last_server_id = value['servers'][server_length-1]['id']
                print "ServerId to be deleted is " + str(last_server_id)
                url = URL + str(last_server_id)
                response = requests.delete(url, json=db_data, headers=headers)
                self.assertEqual(response.status_code, 200)

                db_url = __db_url__ + str(last_db_id)
                response = requests.delete(db_url)
                self.assertEqual(response.status_code, 200)
            else:
                print "The Server list is empty"

if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
    unittest.main()
