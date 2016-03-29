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

import unittest
import requests
import xmlrunner
import socket

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
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)

    def tearDown(self):
        response = requests.get(__url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            # Delete database
            db_url = __url__ + str(last_db_id)
            response = requests.delete(db_url)
            self.assertEqual(response.status_code, 200)
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


    def test_validate_db_name_empty(self):
        """
        ensure database name is not empty
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        data = {'name': ''}
        response = requests.post(__url__, json=data, headers=headers)
        value = response.json()

        self.assertEqual(value['errors'][0], 'Database name is required.')
        self.assertEqual(response.status_code, 200)

    def test_validate_duplicate_db(self):
        """
        ensure database name is not duplicate
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        data = {'name': 'testDB'}
        response = requests.post(__url__, json=data, headers=headers)
        value = response.json()
        self.assertEqual(value['error'], 'database name already exists')
        self.assertEqual(response.status_code, 404)


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
        if response.status_code == 200:
            self.assertEqual(response.status_code, 200)
        else:
            self.assertEqual(value['error'], 'You cannot specify \'Members\' while updating database.')
            self.assertEqual(response.status_code, 404)

        response = requests.put(url, json={'name': 'test', 'id': 33333})
        value = response.json()
        if response.status_code == 200:
            self.assertEqual(response.status_code, 200)
        else:
            self.assertEqual(value['error'], 'Database Id mentioned in the payload and url doesn\'t match.')
            self.assertEqual(response.status_code, 404)

        response = requests.put(url, json={'name': 'test123', 'id': last_db_id})
        value = response.json()
        if response.status_code == 200:
            self.assertEqual(value['status'], 1)
            self.assertEqual(response.status_code, 200)
        else:
            self.assertEqual(response.status_code, 404)

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
        if response.status_code == 200:
            self.assertEqual(response.status_code, 200)
        else:
            self.assertEqual(value['error'], 'Database name is required')
            self.assertEqual(response.status_code, 404)


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
        if response.status_code == 200:
            self.assertEqual(response.status_code, 200)
        else:
            self.assertEqual(value['error'], 'Bad request')
            self.assertEqual(response.status_code, 404)

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
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)
        # Delete db
        response = requests.get(__url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            # Delete database
            db_url = __url__ + str(last_db_id)
            response = requests.delete(db_url)
            self.assertEqual(response.status_code, 200)
        else:
            print "The database list is empty"

if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
    unittest.main()
