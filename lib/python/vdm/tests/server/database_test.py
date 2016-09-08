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

"""
    This test requires installation of flask-Testing from http://pythonhosted.org/Flask-Testing/
    To install flask-Testing use command:
    pip install Flask-Testing


    This test also requires installation of requests library https://pypi.python.org/pypi/requests/
    To install latest requests version 2.8.1 use command:

    sudo pip install requests --upgrade
"""

import unittest
import requests
import socket
import xmlrunner

__host_name__ = socket.gethostname()
__host_or_ip__ = socket.gethostbyname(__host_name__)

__url__ = 'http://'+__host_or_ip__+':8000/api/1.0/databases/'


class Database(unittest.TestCase):
    """
    test case for database
    """

    def setUp(self):
        # Create a db
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        db_data = {'name': 'testDB'}
        response = requests.post(__url__, json=db_data, headers=headers)
        self.assertEqual(response.status_code, 201)

    def tearDown(self):
        response = requests.get(__url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            # Delete database
            db_url = __url__ + str(last_db_id)
            response = requests.delete(db_url)
            self.assertEqual(response.status_code, 204)
        else:
            print "The database list is empty"


class CreateDatabase(Database):
    """
    test case for database create and validation related to it
    """

    def test_request_with_id_member(self):
        """
        ensure id and members are not allowed in payload
        """
        error_msg = 'You cannot specify \'Id\' or \'Members\' while creating database.'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        data = {'name': 'test', 'id': 11}
        response = requests.post(__url__, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['error'], error_msg)
        self.assertEqual(response.status_code, 404)

        data = {'name': 'test', 'members': [2]}
        response = requests.post(__url__, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['error'], error_msg)
        self.assertEqual(response.status_code, 404)

    def test_get_db(self):
        """
        ensure GET server list
        """
        response = requests.get(__url__)
        value = response.json()
        if not value:
            print "Database list is empty."
        self.assertEqual(response.status_code, 200)
        self.assertEqual(value['statusString'], 'OK')
        self.assertEqual(value['status'], 200)

    def test_validate_db_name_empty(self):
        """
        ensure database name is not empty
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        data = {'name': ''}
        response = requests.post(__url__, json=data, headers=headers)
        value = response.json()

        self.assertEqual(value['statusString'][0], 'Database name is required.')
        self.assertEqual(response.status_code, 200)

    def test_validate_duplicate_db(self):
        """
        ensure database name is not duplicate
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        data = {'name': 'testDB'}
        response = requests.post(__url__, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['statusString'], 'database name already exists')
        self.assertEqual(response.status_code, 400)

    def test_validate_db_name_invalid(self):
        """
        ensure database name is valid
        """
        headers = {'Content-type': 'application/json; charset=utf-8'}
        data = {'name': '@#$'}
        response = requests.post(__url__, json=data, headers=headers)
        value =  response.json()

        self.assertEqual(value['statusString'][0], 'Only alphabets, numbers, _ and . are allowed.')
        self.assertEqual(response.status_code, 200)

    def test_validate_db_name_invalid_datatype(self):
        """
        ensure database name is of valid datatype
        """
        headers = {'Content-type': 'application/json; charset=utf-8'}
        # When name is bool value
        data = {'name': True}
        response = requests.post(__url__, json=data, headers=headers)
        value = response.json()

        self.assertEqual(value['statusString'], 'Invalid datatype for field database name.')
        self.assertEqual(response.status_code, 200)

        # When name is integer value
        data = {'name': 11}
        response = requests.post(__url__, json=data, headers=headers)
        value = response.json()

        self.assertEqual(value['statusString'], 'Invalid datatype for field database name.')
        self.assertEqual(response.status_code, 200)



class UpdateDatabase(Database):
    """
    test case for database update and validation related to it
    """

    def test_request_with_id_member(self):
        """
        ensure id and members are not allowed in payload
        """
        response = requests.get(__url__)
        value = response.json()

        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            print 'Database id to be updated is ' + str(last_db_id)
            url = __url__ + str(last_db_id)

        response = requests.put(url, json={'name': 'test', 'members': [3]})
        value = response.json()
        self.assertEqual(value['statusString'], 'You cannot specify \'Members\' while updating database.')
        self.assertEqual(response.status_code, 404)

        response = requests.put(url, json={'name': 'test', 'id': 33333})
        value = response.json()
        self.assertEqual(value['statusString'], 'Database Id mentioned in the payload and url doesn\'t match.')
        self.assertEqual(response.status_code, 404)

        response = requests.put(url, json={'name': 'test123', 'id': last_db_id})
        value = response.json()
        self.assertEqual(value['status'], 200)
        self.assertEqual(response.status_code, 200)

    def test_get_db(self):
        """
        ensure GET database list
        """
        response = requests.get(__url__)
        value = response.json()
        if not value:
            print "Database list is empty."
        self.assertEqual(response.status_code, 200)

    def test_validate_db_name_empty(self):
        """
        ensure database name is not empty
        """
        response = requests.get(__url__)
        value = response.json()

        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            print 'Database id to be updated is ' + str(last_db_id)
            url = __url__ + str(last_db_id)

        response = requests.put(url, json={'name': ''})
        value = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(value['statusString'][0], 'Database name is required.')

    def test_validate_db_name(self):
        """
        ensure database name is valid
        """
        response = requests.get(__url__)
        value = response.json()

        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            print 'Database id to be updated is ' + str(last_db_id)
            url = __url__ + str(last_db_id)

        response = requests.put(url, json={'name': '@@@@'})
        value = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(value['statusString'][0], 'Only alphabets, numbers, _ and . are allowed.')

    def test_validate_update_db(self):
        """
        ensure database is updated
        """
        response = requests.get(__url__)
        value = response.json()

        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            print 'Database id to be updated is ' + str(last_db_id)
            url = __url__ + str(last_db_id)

            response = requests.put(url, json={'deployment': 'test deployment'})
            self.assertEqual(response.status_code, 200)
        else:
            print "Database list is empty."

    def test_validate_db_name_invalid(self):
        """
        ensure database name is valid
        """
        response = requests.get(__url__)
        value = response.json()

        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            url = __url__ + str(last_db_id)
            response = requests.put(url, json={'name': '@#@#@'})
            value = response.json()
            self.assertEqual(value['statusString'][0], 'Only alphabets, numbers, _ and . are allowed.')
            self.assertEqual(response.status_code, 200)
        else:
            print "Database list is empty."

    def test_validate_db_name_invalid_datatype(self):
        """
        ensure database name is of valid datatype
        """
        response = requests.get(__url__)
        value = response.json()

        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            url = __url__ + str(last_db_id)
            # When name is bool value
            response = requests.put(url, json={'name': True})
            value = response.json()
            self.assertEqual(value['statusString'], 'Invalid datatype for field database name.')
            self.assertEqual(response.status_code, 200)
            # When name is integer value
            response = requests.put(url, json={'name': 11})
            value = response.json()
            self.assertEqual(value['statusString'], 'Invalid datatype for field database name.')
            self.assertEqual(response.status_code, 200)
        else:
            print "Database list is empty."

class DeleteDatabase(unittest.TestCase):
    """
    test case for database delete
    """

    def test_delete_db(self):
        """
        Create database and then delete it
        """
        # Create db
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        db_data = {'name': 'testDB'}
        response = requests.post(__url__, json=db_data, headers=headers)
        self.assertEqual(response.status_code, 201)
        # Delete db
        response = requests.get(__url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            # Delete database
            db_url = __url__ + str(last_db_id)
            response = requests.delete(db_url)
            self.assertEqual(response.status_code, 204)
        else:
            print "The database list is empty"

if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
