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

__url__ = 'http://%s:8000/api/1.0/databases/' % __host_or_ip__
__db_url__ = 'http://%s:8000/api/1.0/databases/' % __host_or_ip__


class Deployment(unittest.TestCase):
    """
    test case for database
    """

    def setUp(self):
        """Create a db"""
        url = __db_url__
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        db_data = {'name': 'testDB'}
        response = requests.post(url, json=db_data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)

    def tearDown(self):
        """Delte a db"""
        url = __db_url__
        response = requests.get(url)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            # Delete database
            db_url = url + str(last_db_id)
            response = requests.delete(db_url)
            self.assertEqual(response.status_code, 204)
        else:
            print "The database list is empty"


class DefaultDeployment(unittest.TestCase):
    """
    test case for default deployment
    """

    def test_get_default_deployment(self):
        """
        ensure GET Deployment
        """
        headers = {'Accept': 'application/json'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            response = requests.get(dep_url, headers=headers)
            value = response.json()
            if not value:
                print "Cannot get Deployment information"
            self.assertEqual(response.status_code, 200)


class UpdateDeployment(Deployment):
    """test case for update deployment and validation related to it"""

    def test_get_deployment(self):
        """
        ensure GET Deployment
        """
        headers = {'Accept': 'application/json'}
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            response = requests.get(__db_url__ + str(last_db_id) + '/deployment/', headers=headers)
            value = response.json()
            if not value:
                print "Cannot get Deployment information"
            self.assertEqual(response.status_code, 200)

    def test_validate_sitesperhost_empty(self):
        """ensure sites per host is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'sitesperhost': ''}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_validate_sitesperhost_negative(self):
        """ensure sites per host is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'sitesperhost': -1}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "-1 is less than the minimum of 0")
            self.assertEqual(response.status_code, 200)

    def test_validate_sitesperhost_maximum(self):
        """ensure sites per host is not greater than 15"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            maximum_value = 16
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'sitesperhost': maximum_value}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], str(maximum_value) + " is greater than the maximum of 15")
            self.assertEqual(response.status_code, 200)

    def test_validate_ksafety_empty(self):
        """ensure ksafety is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'kfactor': '', 'sitesperhost': 1}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_validate_ksafety_negative(self):
        """ensure ksafety is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'kfactor': -1, 'sitesperhost': 1}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "-1 is less than the minimum of 0")
            self.assertEqual(response.status_code, 200)

    def test_validate_ksafety_maximum(self):
        """ensure ksafety is not greater than 2"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            maximum_value = 3
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'kfactor': maximum_value,
                                                               'sitesperhost': 1}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], str(maximum_value) + " is greater than "
                                                                      "the maximum of 2")
            self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_time_empty(self):
        """ensure commandlog frequency time is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                                   'commandlog': {'frequency': {'time': ''}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_time_negative(self):
        """ensure commandlog frequency time is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                                   'commandlog': {'frequency': {'time': -1}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "-1 is less than the minimum of 0")
            self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_time_maximum(self):
        """ensure commandlog frequency time is greater than 5000"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            maximum_value = 5001
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                                   'commandlog': {'frequency': {'time': maximum_value}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], str(maximum_value) + " is greater than "
                                                                      "the maximum of 1000")
            self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_transaction_empty(self):
        """ensure commandlog frequency transacation is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                                   'commandlog': {'frequency': {'transactions': ''}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_transaction_negative(self):
        """ensure commandlog frequency transaction is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                                   'commandlog': {'frequency': {'transactions': -1}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "-1 is less than the minimum of 1")
            self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_transaction_maximum(self):
        """ensure commanlog frequency transaction is not greater than 2147483647"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            maximum_value = 2147483648
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                                   'commandlog': {'frequency': {'transactions': maximum_value}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], str(maximum_value) + " is greater than the "
                                                                      "maximum of 2147483647")
            self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_log_size_empty(self):
        """ensure commandlog log size is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                          'commandlog': {'logsize': ''}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_log_size_negative(self):
        """ensure commandlog log size is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                          'commandlog': {'logsize': -1}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "-1 is less than the minimum of 3")
            self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_log_size_maximum(self):
        """ensure commandlog log size is not greater than 102400"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            maximum_value = 102401
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                          'commandlog': {'logsize': maximum_value}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], str(maximum_value) + " is greater "
                                                                      "than the maximum of 3000")
            self.assertEqual(response.status_code, 200)

    def test_validate_heart_beat_timeout_empty(self):
        """ensure heart beat timeout is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                          'heartbeat': {'timeout': ''}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_validate_heart_beat_timeout_negative(self):
        """ensure heart beat timeout is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                          'heartbeat': {'timeout': -1}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "-1 is less than the minimum of 1")
            self.assertEqual(response.status_code, 200)

    def test_validate_heart_beat_timeout_maximum(self):
        """ensure heart beat timeout is not greater than 35"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            maximum_value = 2147483648
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                          'heartbeat': {'timeout': maximum_value}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], str(maximum_value) + " is greater "
                                                                      "than the maximum of 2147483647")
            self.assertEqual(response.status_code, 200)

    def test_validate_query_timeout_empty(self):
        """ensure query timeout is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                          'systemsettings': {'query': {'timeout': ''}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_validate_query_timeout_negative(self):
        """ensure query timeout is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                          'systemsettings': {'query': {'timeout': -1}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "-1 is less than the minimum of 0")
            self.assertEqual(response.status_code, 200)

    def test_validate_query_timeout_maximum(self):
        """ensure query timeout is not greater than 2147483647"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            maximum_value = 2147483648
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                          'systemsettings': {'query': {'timeout': maximum_value}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], str(maximum_value) + " is greater than the maximum "
                                                                      "of 2147483647")
            self.assertEqual(response.status_code, 200)

    def test_validate_temp_table_size_empty(self):
        """ensure temp table size is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                          'systemsettings': {'temptables': {'maxsize': ''}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_validate_temp_table_size_negative(self):
        """ensure temp table size is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                          'systemsettings': {'temptables': {'maxsize': -1}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "-1 is less than the minimum of 1")
            self.assertEqual(response.status_code, 200)

    def test_validate_snapshot_priority_empty(self):
        """ensure snapshot priority is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                          'systemsettings': {'snapshot': {'priority': ''}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_validate_snapshot_priority_negative(self):
        """ensure snapshot priority is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                          'systemsettings': {'snapshot': {'priority': -1}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "-1 is less than the minimum of 0")
            self.assertEqual(response.status_code, 200)

    def test_validate_snapshot_priority_maximum(self):
        """ensure snapshot priority is not greater than 35"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            maximum_value = 11
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                          'systemsettings': {'snapshot': {'priority': maximum_value}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], str(maximum_value) + " is greater than the "
                                                                      "maximum of 10")
            self.assertEqual(response.status_code, 200)

    def test_validate_memory_limit_empty(self):
        """ensure max java memory limit is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                          'systemsettings': {'resourcemonitor': {'memorylimit': {'size': ''}}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_validate_memory_limit_negative(self):
        """ensure max java memory limit is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                          'systemsettings': {'resourcemonitor': {'memorylimit': {'size': '-1'}}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'], "memorylimit value must be between 1 and 2147483647.")
            self.assertEqual(response.status_code, 200)

    def test_validate_memory_limit_negative_percent(self):
        """ensure max java memory limit is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                          'systemsettings': {'resourcemonitor': {'memorylimit': {'size': '-1%'}}}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'], "memorylimit percent value must be between 1 and 99.")
            self.assertEqual(response.status_code, 200)

    def test_validate_export_invalid_export_type(self):
        """ensure invalid type is not saved in export"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'export':{'configuration':[{'exportconnectorclass':'',
                                                                      'enabled':False,'stream':'test',
                                                                      'type':'test','property':[{'name':'test','value':'test'}]}]}}, headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(response.status_code, 200)

    def test_validate_export_stream_empty(self):
        """ensure export stream is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'export':{'configuration':[{'exportconnectorclass':'',
                                                                      'enabled':False,
                                                                      'type':'KAFKA','property':[{'name':'test','value':'test'}]}]}}, headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(response.status_code, 200)

    def test_validate_export_stream_invalid(self):
        """ensure export stream is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={"export": {"configuration": [{"stream":"e e","enabled": True,
                                                                        "property": [{"name": "endpoint",
                                                                                      "value": "1"}],
                                                                        "type": "elasticsearch"}]}},
                                    headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(response.status_code, 200)

    def test_validate_export_property_value_empty(self):
        """ensure property value is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={"export": {"configuration": [{"enabled":False,"type":"KAFKA",
                                                                      "exportconnectorclass":"test",
                                                                      "property":[{"name":"test"}]}]}}, headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(response.status_code, 200)

    def test_validate_export_extra_property(self):
        """ensure property value is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={"export": {"configuration": [{"enabled":False,"type": "KAFKA", "extra": "extra",
                                                                        "exportconnectorclass": "test",
                                                                        "property":[{"name": "metadata.broker.list",
                                                                                     "value": "1"}]}]}}, headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(response.status_code, 200)

    def test_validate_export_invalid_property(self):
        """ensure property value is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={"export": {"configuration": [{"enabled":True,
                                                                      "property":[{"name":"test", "value": "2"}],
                                                                        "stream": "test", "type": "elasticsearch"}]}},
                                    headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(value['statusString'], 'Export: Default property(endpoint) of elasticsearch not present.')
            self.assertEqual(response.status_code, 200)

    def test_validate_export_duplicate_property(self):
        """ensure property value is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={"export": {"configuration": [{"enabled": True,
                                                                        "property": [{"name": "endpoint", "value": "2"},
                                                                                     {"name": "endpoint", "value": "2"}
                                                                                     ],
                                                                        "stream": "test", "type": "elasticsearch"},
                                                                       ]}},
                                    headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(value['statusString'], 'Export: Duplicate properties are not allowed.')
            self.assertEqual(response.status_code, 200)

    def test_validate_import_invalid_export_type(self):
        """ensure invalid type is not saved in export"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'import':{'configuration':[{'format':'',
                                                                      'enabled':False,'module':'test',
                                                                      'type':'test','property':[{'name':'test','value':'test'}]}]}}, headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(response.status_code, 200)

    def test_validate_import_module_empty(self):
        """ensure export stream is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'import':{'configuration':[{'format':'',
                                                                      'enabled':False,
                                                                      'type':'KAFKA','property':[{'name':'test','value':'test'}]}]}}, headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(response.status_code, 200)

    def test_validate_import_property_value_empty(self):
        """ensure property value is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={"import": {"configuration": [{"enabled":False,"type":"KAFKA",
                                                                      "format":"test",
                                                                      "property":[{"name":"test"}]}]}}, headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(response.status_code, 200)

    def test_validate_import_extra_property(self):
        """ensure property value is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={"import": {"configuration": [{"enabled":False,"type":"KAFKA",
                                                                      "format":"test", "extra": "extra",
                                                                      "property":[{"name":"metadata.broker.list",
                                                                                   "value": "2"}
                                                                                  ]}]}}, headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(response.status_code, 200)

    def test_validate_import_property(self):
        """ensure property value is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={"import":{"configuration":
                                                        [{"enabled": True,
                                                          "format": "2",
                                                          "module": "",
                                                          "property":
                                                              [{"name": "metadata.brokerd.list","value": "2"}],
                                                          "type": "kafka"}]}}, headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 401)
            self.assertEqual(value['statusString'], 'Import: Default property(metadata.broker.list) of kafka not present.')
            self.assertEqual(response.status_code, 200)

    def test_validate_dr_no_additional_property(self):
        """ensure no additional properties are inserted"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'dr': {'id': 2, "extraproperty":"extra"}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "Additional properties are not allowed (u'extraproperty' was unexpected)")
            self.assertEqual(response.status_code, 200)

    def test_validate_dr_id_empty(self):
        """ensure dr id  is not empty"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'dr': {'id': ''}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "u'' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_validate_dr_no_id(self):
        """ensure id is required field"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'dr': {'port': 22}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'], "DR id is required.")
            self.assertEqual(response.status_code, 200)

    def test_validate_dr_id_negative(self):
        """ensure dr id is not negative"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'dr': {'id': -1}}, headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], "-1 is less than the minimum of 0")
            self.assertEqual(response.status_code, 200)

    def test_validate_dr_id_greater_than_limit(self):
        """ensure dr id is not greater than given limit"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json={'dr': {'id': 5555555555555555}},headers=headers)
            value = response.json()
            self.assertEqual(value['statusString'][0], u'5555555555555555 is greater than the maximum of 2147483647')
            self.assertEqual(response.status_code, 200)

    def test_validate_dr_enabled_empty_and_boolean(self):
        """ensure dr enabled  is not empty and is boolean"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url, json={'dr': {'enabled': ''}}, headers=headers)
            value = response.json()
            # FIXME
            #self.assertEqual(value['statusString'][0], "u'' is not of type 'boolean'")
            self.assertEqual(response.status_code, 200)

    def test_update_deployment(self):
        """ensure update deployment is working properly"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            json_data = {
                "cluster": {"hostcount": 1, "sitesperhost": 8, "kfactor": 0, "elastic": "enabled",
                            "schema": "DDL"},
                "paths": {"voltdbroot": {"path": "voltdbroot"}, "snapshots": {"path": "snapshots"},
                          "exportoverflow":
                              {"path": "export_overflow"}, "commandlog": {"path": "command_log"},
                          "commandlogsnapshot": {"path": "command_log_snapshot"}},
                "partition-detection": {"snapshot": {"prefix": "voltdb_partition_detection"},
                                       "enabled": True},
                "admin-mode": {"port": 21211, "adminstartup": False}, "heartbeat": {"timeout": 90},
                "httpd": {"jsonapi": {"enabled": True}, "port": 8080, "enabled": True},
                "snapshot": {"frequency": "24h", "retain": 2,
                             "prefix": "AUTOSNAP", "enabled": False},
                "commandlog": {"frequency": {"time": 200, "transactions": 2147483647},
                               "synchronous": False, "enabled": False, "logsize": 1024},
                "systemsettings": {"temptables": {"maxsize": 100}, "snapshot": {"priority": 6},
                                   "elastic": {"duration": 50, "throughput": 2},
                                   "query": {"timeout": 0},
                                   "resourcemonitor": {"memorylimit": {"size": "1"},
                                                       "disklimit": {"feature": [
                                                           {"name": "snapshots", "size": "2"},
                                                           {"name": "commandlog", "size": "2"}],
                                                           "size": "10"},
                                                       "frequency": 5}},
                "security": {"enabled": False, "provider": "HASH"},
                "export":{"configuration":[{"enabled":False,
                                            "type": "kafka", "exportconnectorclass":"test",
                                            "stream": "test", "property":[{"name": "metadata.broker.list",
                                                                           "value": "test"}]}]},
                "import": {"configuration": [{"enabled":False,"type":"kafka", "module": "test", "format": "test",
                                                                      "property":[{"name":"metadata.broker.list","value":"test"}]}]},
                # "dr": {"id": 33, "type": "Master", "enabled": True, "connection": {"source": "testttt", "servers": []}}
            }
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json=json_data, headers=headers)
            value = response.json()
            self.assertEqual(value['status'], 200)
            self.assertEqual(response.status_code, 200)

    def test_invalid_string(self):
        """ensure invalid string is validated properly"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            json_data = {"dr": {"id": "1", "listen": True, "port": 112}
            }
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json=json_data, headers=headers)
            value = response.json()
            self.assertEqual(str(value['statusString'][0]), "u'1' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_invalid_boolean(self):
        """ensure invalid boolean is validated properly"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            json_data = {"dr": {"id": 1, "listen": "true", "port": 112}}
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json=json_data, headers=headers)
            value = response.json()
            self.assertEqual(str(value['statusString'][0]), "u'true' is not of type 'boolean'")
            self.assertEqual(response.status_code, 200)

    def test_invalid_number(self):
        """ensure invalid number is validated properly"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            json_data = {"dr": {"id": 1, "listen": True, "port": "112"}}
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json=json_data, headers=headers)
            value = response.json()
            self.assertEqual(str(value['statusString'][0]), "u'112' is not of type 'integer'")
            self.assertEqual(response.status_code, 200)

    def test_additional_properties(self):
        """ensure additional properties is validated properly"""
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            dep_url = __db_url__ + str(last_db_id) + '/deployment/'
            json_data = {"dr": {"id": 1, "liste": True, "port": 112}}
            headers = {'Content-Type': 'application/json; charset=UTF-8'}
            response = requests.put(dep_url,
                                    json=json_data, headers=headers)
            value = response.json()
            self.assertEqual(str(value['statusString'][0]), "Additional properties are not allowed (u'liste' was unexpected)")
            self.assertEqual(response.status_code, 200)

if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
