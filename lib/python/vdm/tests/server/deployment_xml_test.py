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


import unittest
import requests
import xmlrunner
from xml.etree import ElementTree

__url__ = 'http://localhost:8000/api/1.0/databases/1/deployment/'


class XML(unittest.TestCase):
    """
    test case for database
    """

    def test_valid_xml(self):
        data = requests.get(__url__)
        try:
            doc = ElementTree.fromstring(data.content)
        except:
            raise self.fail('Input is not a valid XML document')

        return doc

    def test_default_xml_attributes(self):

        data = requests.get(__url__)
        tree = ElementTree.fromstring(data.content)

        for child in tree:
            if child.tag == "paths":
                for subnode in child:
                    if subnode.tag == "snapshots":
                        self.assertEqual(subnode.attrib['path'], "snapshots")
                    if subnode.tag == "commandlogsnapshot":
                        self.assertEqual(subnode.attrib['path'], "command_log_snapshot")
                    if subnode.tag == "voltdbroot":
                        self.assertEqual(subnode.attrib['path'], "voltdbroot")
                    if subnode.tag == "exportoverflow":
                        self.assertEqual(subnode.attrib['path'], "export_overflow")
            if child.tag == "snapshots":
                self.assertEqual(child.attrib['enabled'], "true")
                self.assertEqual(child.attrib['frequency'], "24h")
                self.assertEqual(child.attrib['prefix'], "AUTOSHAP")
                self.assertEqual(child.attrib['retain'], "2")
            if child.tag == "httpd":
                self.assertEqual(child.attrib['enabled'], "true")
                self.assertEqual(child.attrib['port'], "8080")
                for subnode in child:
                    if subnode.tag == "jsonapi":
                        self.assertEqual(subnode.attrib['enabled'], "true")
            if child.tag == "systemsettings":
                for subnode in child:
                    if subnode.tag == "query":
                        self.assertEqual(subnode.attrib['timeout'], "10000")
                    if subnode.tag == "temptables":
                        self.assertEqual(subnode.attrib['maxsize'], "100")
                    if subnode.tag == "snapshot":
                        self.assertEqual(subnode.attrib['priority'], "6")
                    if subnode.tag == "elastic":
                        self.assertEqual(subnode.attrib['duration'], "50")
                        self.assertEqual(subnode.attrib['throughput'], "2")
                    if subnode.tag == "resourcemonitor":
                        for supersubnode in subnode:
                            self.assertEqual(supersubnode.attrib['size'], "80%")
            if child.tag == "admin-mode":
                self.assertEqual(child.attrib['adminstartup'], "false")
                self.assertEqual(child.attrib['port'], "21211")
            if child.tag == "cluster":
                self.assertEqual(child.attrib['elastic'], "enabled")
                self.assertEqual(child.attrib['hostcount'], "1")
                self.assertEqual(child.attrib['kfactor'], "0")
                self.assertEqual(child.attrib['schema'], "ddl")
                self.assertEqual(child.attrib['sitesperhost'], "8")
            if child.tag == "snapshot":
                self.assertEqual(child.attrib['enabled'], "false")
                self.assertEqual(child.attrib['frequency'], "24h")
                self.assertEqual(child.attrib['prefix'], "AUTOSNAP")
                self.assertEqual(child.attrib['retain'], "2")
            if child.tag == "security":
                self.assertEqual(child.attrib['enabled'], "false")
                self.assertEqual(child.attrib['provider'], "hash")
            if child.tag == "partition-detection":
                self.assertEqual(child.attrib['enabled'], "true")
                for subnode in child:
                    self.assertEqual(subnode.attrib['prefix'], "voltdb_partition_detection")


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

        url_dep = "http://localhost:8000/api/1.0/deployment/2"
        json_data = {
            "cluster": {"sitesperhost": 8, "kfactor": 0, "elastic": "enabled",
                        "schema": "DDL"},
            "paths": {"voltdbroot": {"path": "voltdbroot"}, "snapshots": {"path": "snapshotstest"},
                      "exportoverflow":
                          {"path": "export_overflowtest"}, "commandlog": {"path": "command_logtest"},
                      "commandlogsnapshot": {"path": "command_log_snapshot"}},
            "partition-detection": {"snapshot": {"prefix": "voltdb_partition_detection"},
                                    "enabled": True},
            "admin-mode": {"port": 21211, "adminstartup": False}, "heartbeat": {"timeout": 90},
            "httpd": {"jsonapi": {"enabled": True}, "port": 8080, "enabled": True},
            "snapshot": {"frequency": "1h", "retain": 1,
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
            "security": {"enabled": True, "provider": "HASH"},
            "export": {"configuration": [{"enabled": False,
                                          "type": "kafka", "exportconnectorclass": "test",
                                          "stream": "test", "property": [{"name": "test",
                                                                          "value": "test"}]}]},
            "import": {"configuration": [{"enabled": False, "type": "kafka", "module": "test", "format": "test",
                                          "property": [{"name": "test", "value": "test"}]}]}
        }

        response = requests.put(url_dep,
                                json=json_data)
        value = response.json()
        self.assertEqual(value['status'], 1)
        self.assertEqual(response.status_code, 200)

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


class UpdateDeployment(Deployment):
    def test_updated_deployment_xml(self):
        """ensure update deployment is working properly"""

        data = requests.get("http://localhost:8000/api/1.0/databases/2/deployment/")
        tree = ElementTree.fromstring(data.content)

        for child in tree:
            if child.tag == "paths":
                for subnode in child:
                    if subnode.tag == "snapshots":
                        self.assertEqual(subnode.attrib['path'], "snapshotstest")
                    if subnode.tag == "commandlogsnapshot":
                        self.assertEqual(subnode.attrib['path'], "command_log_snapshot")
                    if subnode.tag == "voltdbroot":
                        self.assertEqual(subnode.attrib['path'], "voltdbroot")
                    if subnode.tag == "exportoverflow":
                        self.assertEqual(subnode.attrib['path'], "export_overflowtest")
                    if subnode.tag == "commandlog":
                        self.assertEqual(subnode.attrib['path'], "command_logtest")
            if child.tag == "snapshots":
                self.assertEqual(child.attrib['enabled'], "true")
                self.assertEqual(child.attrib['frequency'], "1h")
                self.assertEqual(child.attrib['prefix'], "AUTOSHAP")
                self.assertEqual(child.attrib['retain'], "2")
            if child.tag == "httpd":
                self.assertEqual(child.attrib['enabled'], "true")
                self.assertEqual(child.attrib['port'], "8080")
                for subnode in child:
                    if subnode.tag == "jsonapi":
                        self.assertEqual(subnode.attrib['enabled'], "true")
            if child.tag == "systemsettings":
                for subnode in child:
                    if subnode.tag == "query":
                        self.assertEqual(subnode.attrib['timeout'], "0")
                    if subnode.tag == "temptables":
                        self.assertEqual(subnode.attrib['maxsize'], "100")
                    if subnode.tag == "snapshot":
                        self.assertEqual(subnode.attrib['priority'], "6")
                    if subnode.tag == "elastic":
                        self.assertEqual(subnode.attrib['duration'], "50")
                        self.assertEqual(subnode.attrib['throughput'], "2")
                    if subnode.tag == "resourcemonitor":
                        for supersubnode in subnode:
                            if supersubnode == "memorylimit":
                                self.assertEqual(supersubnode.attrib['size'], "1")
            if child.tag == "admin-mode":
                self.assertEqual(child.attrib['adminstartup'], "false")
                self.assertEqual(child.attrib['port'], "21211")
            if child.tag == "cluster":
                self.assertEqual(child.attrib['elastic'], "enabled")
                self.assertEqual(child.attrib['kfactor'], "0")
                self.assertEqual(child.attrib['schema'], "DDL")
                self.assertEqual(child.attrib['sitesperhost'], "8")
            if child.tag == "security":
                self.assertEqual(child.attrib['enabled'], "true")
                self.assertEqual(child.attrib['provider'], "HASH")
            if child.tag == "partition-detection":
                self.assertEqual(child.attrib['enabled'], "true")
                for subnode in child:
                    self.assertEqual(subnode.attrib['prefix'], "voltdb_partition_detection")


if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
    unittest.main()
