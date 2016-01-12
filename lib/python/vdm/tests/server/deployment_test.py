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

__url__ = 'http://localhost:8000/api/1.0/deployment/1'


class Deployment(unittest.TestCase):
    """
    test case for database
    """

    def setUp(self):
        """Create a db"""
        url = 'http://localhost:8000/api/1.0/databases/'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        db_data = {'name': 'testDB'}
        response = requests.post(url, json=db_data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)

    def tearDown(self):
        """Delte a db"""
        url = 'http://localhost:8000/api/1.0/databases/'
        response = requests.get(url)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length - 1]['id']
            # Delete database
            db_url = url + str(last_db_id)
            response = requests.delete(db_url)
            self.assertEqual(response.status_code, 200)
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
        response = requests.get(__url__)
        value = response.json()
        if not value:
            print "Cannot get Deployment information"
        self.assertEqual(response.status_code, 200)


class UpdateDeployment(Deployment):
    """test case for update deployment and validation related to it"""
    __url__ = "http://localhost:8000/api/1.0/deployment/2"

    def test_get_deployment(self):
        """
        ensure GET Deployment
        """
        response = requests.get(__url__)
        value = response.json()
        if not value:
            print "Cannot get Deployment information"
        self.assertEqual(response.status_code, 200)

    def test_validate_sitesperhost_empty(self):
        """ensure sites per host is not empty"""
        response = requests.put(__url__, json={'cluster': {'sitesperhost': ''}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'integer'")
        self.assertEqual(response.status_code, 200)

    def test_validate_sitesperhost_negative(self):
        """ensure sites per host is not negative"""
        response = requests.put(__url__, json={'cluster': {'sitesperhost': -1}})
        value = response.json()
        self.assertEqual(value['errors'][0], "-1 is less than the minimum of 0")
        self.assertEqual(response.status_code, 200)

    def test_validate_sitesperhost_maximum(self):
        """ensure sites per host is not greater than 15"""
        maximum_value = 16
        response = requests.put(__url__, json={'cluster': {'sitesperhost': maximum_value}})
        value = response.json()
        self.assertEqual(value['errors'][0], str(maximum_value) + " is greater than the maximum of 15")
        self.assertEqual(response.status_code, 200)

    def test_validate_ksafety_empty(self):
        """ensure ksafety is not empty"""
        response = requests.put(__url__, json={'cluster': {'kfactor': '', 'sitesperhost': 1}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'integer'")
        self.assertEqual(response.status_code, 200)

    def test_validate_ksafety_negative(self):
        """ensure ksafety is not negative"""
        response = requests.put(__url__, json={'cluster': {'kfactor': -1, 'sitesperhost': 1}})
        value = response.json()
        self.assertEqual(value['errors'][0], "-1 is less than the minimum of 0")
        self.assertEqual(response.status_code, 200)

    def test_validate_ksafety_maximum(self):
        """ensure ksafety is not greater than 2"""
        maximum_value = 3
        response = requests.put(__url__, json={'cluster': {'kfactor': maximum_value,
                                                           'sitesperhost': 1}})
        value = response.json()
        self.assertEqual(value['errors'][0], str(maximum_value) + " is greater than "
                                                                  "the maximum of 2")
        self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_time_empty(self):
        """ensure commandlog frequency time is not empty"""
        response = requests.put(__url__, json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                               'commandlog': {'frequency': {'time': ''}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'integer'")
        self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_time_negative(self):
        """ensure commandlog frequency time is not negative"""
        response = requests.put(__url__, json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                               'commandlog': {'frequency': {'time': -1}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "-1 is less than the minimum of 0")
        self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_time_maximum(self):
        """ensure commandlog frequency time is greater than 1000"""
        maximum_value = 1001
        response = requests.put(__url__, json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                               'commandlog': {'frequency': {'time': maximum_value}}})
        value = response.json()
        self.assertEqual(value['errors'][0], str(maximum_value) + " is greater than "
                                                                  "the maximum of 1000")
        self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_transaction_empty(self):
        """ensure commandlog frequency transacation is not empty"""
        response = requests.put(__url__, json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                               'commandlog': {'frequency': {'transactions': ''}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'integer'")
        self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_transaction_negative(self):
        """ensure commandlog frequency transaction is not negative"""
        response = requests.put(__url__, json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                               'commandlog': {'frequency': {'transactions': -1}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "-1 is less than the minimum of 0")
        self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_frequency_transaction_maximum(self):
        """ensure commanlog frequency transaction is not greater than 2147483647"""
        maximum_value = 2147483648
        response = requests.put(__url__, json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                               'commandlog': {'frequency': {'transactions': maximum_value}}})
        value = response.json()
        self.assertEqual(value['errors'][0], str(maximum_value) + " is greater than the "
                                                                  "maximum of 2147483647")
        self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_log_size_empty(self):
        """ensure commandlog log size is not empty"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                      'commandlog': {'logsize': ''}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'integer'")
        self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_log_size_negative(self):
        """ensure commandlog log size is not negative"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                      'commandlog': {'logsize': -1}})
        value = response.json()
        self.assertEqual(value['errors'][0], "-1 is less than the minimum of 3")
        self.assertEqual(response.status_code, 200)

    def test_validate_commandlog_log_size_maximum(self):
        """ensure commandlog log size is not greater than 4000"""
        maximum_value = 4001
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                      'commandlog': {'logsize': maximum_value}})
        value = response.json()
        self.assertEqual(value['errors'][0], str(maximum_value) + " is greater "
                                                                  "than the maximum of 4000")
        self.assertEqual(response.status_code, 200)

    def test_validate_heart_beat_timeout_empty(self):
        """ensure heart beat timeout is not empty"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                      'heartbeat': {'timeout': ''}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'integer'")
        self.assertEqual(response.status_code, 200)

    def test_validate_heart_beat_timeout_negative(self):
        """ensure heart beat timeout is not negative"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                      'heartbeat': {'timeout': -1}})
        value = response.json()
        self.assertEqual(value['errors'][0], "-1 is less than the minimum of 1")
        self.assertEqual(response.status_code, 200)

    def test_validate_heart_beat_timeout_maximum(self):
        """ensure heart beat timeout is not greater than 35"""
        maximum_value = 2147483648
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                      'heartbeat': {'timeout': maximum_value}})
        value = response.json()
        self.assertEqual(value['errors'][0], str(maximum_value) + " is greater "
                                                                  "than the maximum of 2147483647")
        self.assertEqual(response.status_code, 200)

    def test_validate_query_timeout_empty(self):
        """ensure query timeout is not empty"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                      'systemsettings': {'query': {'timeout': ''}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'integer'")
        self.assertEqual(response.status_code, 200)

    def test_validate_query_timeout_negative(self):
        """ensure query timeout is not negative"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                      'systemsettings': {'query': {'timeout': -1}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "-1 is less than the minimum of 0")
        self.assertEqual(response.status_code, 200)

    def test_validate_query_timeout_maximum(self):
        """ensure query timeout is not greater than 2147483647"""
        maximum_value = 2147483648
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                      'systemsettings': {'query': {'timeout': maximum_value}}})
        value = response.json()
        self.assertEqual(value['errors'][0], str(maximum_value) + " is greater than the maximum "
                                                                  "of 2147483647")
        self.assertEqual(response.status_code, 200)

    def test_validate_temp_table_size_empty(self):
        """ensure temp table size is not empty"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                      'systemsettings': {'temptables': {'maxsize': ''}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'integer'")
        self.assertEqual(response.status_code, 200)

    def test_validate_temp_table_size_negative(self):
        """ensure temp table size is not negative"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                      'systemsettings': {'temptables': {'maxsize': -1}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "-1 is less than the minimum of 0")
        self.assertEqual(response.status_code, 200)

    def test_validate_snapshot_priority_empty(self):
        """ensure snapshot priority is not empty"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                      'systemsettings': {'snapshot': {'priority': ''}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'integer'")
        self.assertEqual(response.status_code, 200)

    def test_validate_snapshot_priority_negative(self):
        """ensure snapshot priority is not negative"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                      'systemsettings': {'snapshot': {'priority': -1}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "-1 is less than the minimum of 0")
        self.assertEqual(response.status_code, 200)

    def test_validate_snapshot_priority_maximum(self):
        """ensure snapshot priority is not greater than 35"""
        maximum_value = 11
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                      'systemsettings': {'snapshot': {'priority': maximum_value}}})
        value = response.json()
        self.assertEqual(value['errors'][0], str(maximum_value) + " is greater than the "
                                                                  "maximum of 10")
        self.assertEqual(response.status_code, 200)

    def test_validate_memory_limit_empty(self):
        """ensure max java memory limit is not empty"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': '', 'sitesperhost': 1},
                                      'systemsettings': {'resourcemonitor': {'memorylimit': {'size': ''}}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'integer'")
        self.assertEqual(response.status_code, 200)

    def test_validate_memory_limit_negative(self):
        """ensure max java memory limit is not negative"""
        response = requests.put(__url__,
                                json={'cluster': {'kfactor': 2, 'sitesperhost': 1},
                                      'systemsettings': {'resourcemonitor': {'memorylimit': {'size': -1}}}})
        value = response.json()
        self.assertEqual(value['errors'][0], "-1 is less than the minimum of 0")
        self.assertEqual(response.status_code, 200)

    def test_validate_export_invalid_export_type(self):
        """ensure invalid type is not saved in export"""
        response = requests.put(__url__,
                                json={'export':{'configuration':[{'exportconnectorclass':'',
                                                                  'enabled':False,'stream':'test',
                                                                  'type':'test','property':[{'name':'test','value':'test'}]}]}})
        value = response.json()
        self.assertEqual(value['success'],False)
        self.assertEqual(response.status_code, 200)

    def test_validate_export_stream_empty(self):
        """ensure export stream is not empty"""
        response = requests.put(__url__,
                                json={'export':{'configuration':[{'exportconnectorclass':'',
                                                                  'enabled':False,
                                                                  'type':'KAFKA','property':[{'name':'test','value':'test'}]}]}})
        value = response.json()
        self.assertEqual(value['success'],False)
        self.assertEqual(response.status_code, 200)

    def test_validate_export_property_value_empty(self):
        """ensure property value is not empty"""
        response = requests.put(__url__,
                                json={"export": {"configuration": [{"enabled":False,"type":"KAFKA",
                                                                  "exportconnectorclass":"test",
                                                                  "property":[{"name":"test"}]}]}})
        value = response.json()
        self.assertEqual(value['success'],False)
        self.assertEqual(response.status_code, 200)

    def test_validate_import_invalid_export_type(self):
        """ensure invalid type is not saved in export"""
        response = requests.put(__url__,
                                json={'import':{'configuration':[{'format':'',
                                                                  'enabled':False,'module':'test',
                                                                  'type':'test','property':[{'name':'test','value':'test'}]}]}})
        value = response.json()
        self.assertEqual(value['success'], False)
        self.assertEqual(response.status_code, 200)

    def test_validate_import_module_empty(self):
        """ensure export stream is not empty"""
        response = requests.put(__url__,
                                json={'import':{'configuration':[{'format':'',
                                                                  'enabled':False,
                                                                  'type':'KAFKA','property':[{'name':'test','value':'test'}]}]}})
        value = response.json()
        self.assertEqual(value['success'],False)
        self.assertEqual(response.status_code, 200)

    def test_validate_import_property_value_empty(self):
        """ensure property value is not empty"""
        response = requests.put(__url__,
                                json={"import": {"configuration": [{"enabled":False,"type":"KAFKA",
                                                                  "format":"test",
                                                                  "property":[{"name":"test"}]}]}})
        value = response.json()
        self.assertEqual(value['success'],False)
        self.assertEqual(response.status_code, 200)

    def test_validate_dr_id_empty(self):
        """ensure dr id  is not empty"""
        response = requests.put(__url__, json={'dr': {'id': ''}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'integer'")
        self.assertEqual(response.status_code, 200)

    def test_validate_dr_id_negative(self):
        """ensure dr id is not negative"""
        response = requests.put(__url__,
                                json={'dr': {'id': -1}})
        value = response.json()
        self.assertEqual(value['errors'][0], "-1 is less than the minimum of 0")
        self.assertEqual(response.status_code, 200)

    def test_validate_dr_id_greater_than_limit(self):
        """ensure dr id is not greater than given limit"""
        response = requests.put(__url__,
                                json={'dr': {'id': 5555555555555555}})
        value = response.json()
        self.assertEqual(value['errors'][0], u'5555555555555555 is greater than the maximum of 2147483647')
        self.assertEqual(response.status_code, 200)

    def test_validate_dr_enabled_empty_and_boolean(self):
        """ensure dr enabled  is not empty and is boolean"""
        response = requests.put(__url__, json={'dr': {'enabled': ''}})
        value = response.json()
        self.assertEqual(value['errors'][0], "u'' is not of type 'boolean'")
        self.assertEqual(response.status_code, 200)

    def test_update_deployment(self):
        """ensure update deployment is working properly"""

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
                                                       {"name": "SNAPSHOTS", "size": "2"},
                                                       {"name": "COMMANDLOG", "size": "2"}],
                                                       "size": "10"},
                                                   "frequency": 5}},
            "security": {"enabled": False, "provider": "HASH"},
            "export":{"configuration":[{"enabled":False,
                                        "type": "KAFKA", "exportconnectorclass":"test",
                                        "stream": "test", "property":[{"name": "test",
                                                                       "value": "test"}]}]},
            "import": {"configuration": [{"enabled":False,"type":"KAFKA", "module": "test",
                                                                  "property":[{"name":"test","value":"test"}]}]},
            "dr": {"id": 33, "type": "Master", "enabled": True, "connection": {"source": "testttt", "servers": []}}
        }

        response = requests.put(__url__,
                                json=json_data)
        value = response.json()
        self.assertEqual(value['status'], 1)
        self.assertEqual(response.status_code, 200)


if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
    unittest.main()
