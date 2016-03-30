"""
This file is part of VoltDB.

Copyright (C) 2008-2015 VoltDB Inc.

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

__db_url__ = 'http://%s:8000/api/1.0/databases/' % \
             __host_or_ip__


class DeploymentUser(unittest.TestCase):
    """
    test case for database
    """

    def setUp(self):
        """Create a deployment user"""
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        db_data = {'name': 'testDB'}
        response = requests.post(__db_url__, json=db_data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)

        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            db_data = {"name": "test", "password": "voltdb", "plaintext": True, "roles": "Administrator,Test", "databaseid": 1}
            db_url = '%s%u/users/' % (__db_url__, last_db_id)
            response = requests.post(db_url, json=db_data, headers=headers)

            if response.status_code == 200:
                self.assertEqual(response.status_code, 200)
            else:
                self.assertEqual(response.status_code, 404)

    def tearDown(self):
        """Delete a deployment user"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            user_url = '%s%u/users/' % (__db_url__, last_db_id)
            response = requests.get(user_url)
            value = response.json()
            if value:
                user_length = len(value['deployment'])
                last_user_id = value['deployment'][user_length-1]['userid']
                user_delete_url = '%s%u/users/%u/' % (__db_url__, last_db_id, last_user_id)
                response = requests.delete(user_delete_url)
                self.assertEqual(response.status_code, 200)
                db_url = __db_url__ + str(last_db_id)
                response = requests.delete(db_url)
                self.assertEqual(response.status_code, 204)


class UpdateDeploymentUser(DeploymentUser):
    def test_validate_duplicate_username(self):
        """Validate duplicate username"""
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        last_db_id = GetLastDbId()
        if last_db_id != -1:
            user_url = '%s%u/users/' % (__db_url__, last_db_id)
            db_data = {"name": "test", "password": "voltdb", "plaintext": True, "roles": "Administrator", "databaseid": 1}
            response = requests.post(user_url, json=db_data, headers=headers)
            value = response.json()
            self.assertEqual(value['error'], u'user name already exists')

            self.assertEqual(response.status_code, 404)
        else:
            print "The database list is empty"

    def test_validate_username_empty(self):
        """ensure username value is not empty"""

        db_data = {"password": "voltdb", "plaintext": True, "roles": "Administrator", "databaseid": 1}
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        last_db_id = GetLastDbId()
        if last_db_id != -1:
            user_url = '%s%u/users/' % (__db_url__, last_db_id)
            response = requests.post(user_url,
                                    json=db_data, headers=headers)
            value = response.json()
            self.assertEqual(value['errors'][0], "'name' is a required property")
            self.assertEqual(response.status_code, 200)
        else:
            print "The database list is empty"

    def test_validate_password_empty(self):
        """ensure password value is not empty"""

        db_data = {"name": "voltdb", "plaintext": True, "roles": "Administrator", "databaseid": 1}
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        last_db_id = GetLastDbId()
        if last_db_id != -1:
            user_url = '%s%u/users/' % (__db_url__, last_db_id)
            response = requests.post(user_url,
                                    json=db_data, headers=headers)
            value = response.json()
            self.assertEqual(value['errors'][0], "'password' is a required property")
            self.assertEqual(response.status_code, 200)
        else:
            print "The database list is empty"

    def test_validate_roles_empty(self):
        """ensure roles value is not empty"""

        db_data = {"name": "voltdb", "password": "test", "plaintext": True, "databaseid": 1}
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        last_db_id = GetLastDbId()
        if last_db_id != -1:
            user_url = '%s%u/users/' % (__db_url__, last_db_id)
            response = requests.post(user_url,
                                    json=db_data, headers=headers)
            value = response.json()
            self.assertEqual(value['errors'][0], "'roles' is a required property")
            self.assertEqual(response.status_code, 200)
        else:
            print "The database list is empty"

    def test_update_deployment_user(self):
        """ensure deployment user is updated"""

        db_data = {"name": "test", "password": "admin", "plaintext": True, "roles": "Administrator,Test1", "databaseid": 1}
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        last_db_id = GetLastDbId()
        if last_db_id != -1:
            user_url = '%s%u/users/' % (__db_url__, last_db_id)
            response = requests.get(user_url)
            value = response.json()
            if value:
                user_length = len(value['deployment'])
                last_user_id = value['deployment'][user_length-1]['userid']
                user_update_url = '%s%u/' % (user_url, last_user_id)
                response = requests.put(user_update_url,
                                         json=db_data, headers=headers)
                value = response.json()
                self.assertEqual(value['statusstring'], "User Updated")
                self.assertEqual(response.status_code, 200)


def GetLastDbId():
    last_db_id = -1
    response = requests.get(__db_url__)
    value = response.json()
    if value:
        db_length = len(value['databases'])
        last_db_id = value['databases'][db_length-1]['id']
    return last_db_id

if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
    unittest.main()
