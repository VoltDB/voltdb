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
        self.assertEqual(response.status_code, 201)
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            db_data = {"name": "test", "password": "voltdb", "plaintext": True, "roles": "Administrator,Test", "databaseid": 1}
            db_url = '%s%u/users/' % (__db_url__, last_db_id)
            response = requests.post(db_url, json=db_data, headers=headers)
            self.assertEqual(response.status_code, 200)

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
            self.assertEqual(value['statusString'], u'user name already exists')

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
            self.assertEqual(value['statusString'][0], "'name' is a required property")
            self.assertEqual(response.status_code, 200)
        else:
            print "The database list is empty"

    def test_validate_invalid_username(self):
        db_data = {"name":"@@@@", "password": "voltdb", "plaintext": True, "roles": "Administrator", "databaseid": 1}
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        last_db_id = GetLastDbId()
        if last_db_id != -1:
            user_url = '%s%u/users/' % (__db_url__, last_db_id)
            response = requests.post(user_url,
                                    json=db_data, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'@@@@' does not match '^[a-zA-Z0-9_.]+$'")
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
            self.assertEqual(value['statusString'][0], "'password' is a required property")
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
            self.assertEqual(value['statusString'][0], "'roles' is a required property")
            self.assertEqual(response.status_code, 200)
        else:
            print "The database list is empty"

    def test_validate_invalid_role(self):
        db_data = {"name": "voltdb", "password": "test", "plaintext": True,"roles":"@@@@", "databaseid": 1}
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        last_db_id = GetLastDbId()
        if last_db_id != -1:
            user_url = '%s%u/users/' % (__db_url__, last_db_id)
            response = requests.post(user_url,
                                    json=db_data, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'@@@@' does not match '^[a-zA-Z0-9_.,-]+$'")
            self.assertEqual(response.status_code, 200)

            db_data = {"name": "voltdb", "password": "test", "plaintext": True,"roles":",", "databaseid": 1}
            response = requests.post(user_url,
                                    json=db_data, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'], "Invalid user roles.")
            self.assertEqual(response.status_code, 200)
        else:
            print "The database list is empty"

    def test_ensure_no_duplicate_role(self):
        """ensure no duplicate roles are inserted"""
        db_data = {"name": "test", "password": "admin", "plaintext": True, "roles": "Test1,Test1", "databaseid": 1}
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
                self.assertEqual(value['user']['roles'], "Test1")
                self.assertEqual(response.status_code, 200)

    def test_update_deployment_user(self):
        """ensure deployment user is updated"""

        db_data = {"name": "test", "password": "admin", "plaintext": True, "roles": "Administrator,Test2", "databaseid": 1}
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

    def test_get_user_with_non_existing_id(self):
        __user_url = 'http://%s:8000/api/1.0/databases/3/users/' % \
                    __host_or_ip__
        response = requests.get(__user_url)
        value = response.json()
        self.assertEqual(value['statusString'], "No database found for id: 3")
        self.assertEqual(response.status_code, 404)

    def test_add_user_with_non_existing_id(self):
        db_data = {"name":"user", "password": "voltdb", "plaintext": True, "roles": "Administrator", "databaseid": 1}
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        last_db_id = 3
        if last_db_id != -1:
            user_url = '%s%u/users/' % (__db_url__, last_db_id)
            response = requests.post(user_url,
                                    json=db_data, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'], "No database found for id: 3")
            self.assertEqual(response.status_code, 404)
        else:
            print "The database list is empty"

    def test_delete_user_with_non_existing_databaseid(self):
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_data = {"name":"user", "password": "voltdb", "plaintext": True, "roles": "Administrator", "databaseid": 1}
            headers = {'Content-Type': 'application/json; charset=utf-8'}
            last_db_id = GetLastDbId()
            if last_db_id != -1:
                user_url = '%s%u/users/' % (__db_url__, last_db_id)
                response = requests.post(user_url,
                                        json=db_data, headers=headers)
                value = response.json()
                self.assertEqual(response.status_code, 200)

                last_db_id = 3
                last_user_id = 1
                user_delete_url = '%s%u/users/%u/' % (__db_url__, last_db_id, last_user_id)
                response = requests.delete(user_delete_url)
                value1 = response.json()
                self.assertEqual(response.status_code, 404)
                self.assertEqual(value1['statusString'], "No database found for id: 3")
        else:
            print "The database list is empty"



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
