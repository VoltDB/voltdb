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
from xml.etree import ElementTree
import socket

__host_name__ = socket.gethostname()
__host_or_ip__ = socket.gethostbyname(__host_name__)

__url__ = 'http://%s:8000/api/1.0/voltdeploy/' % __host_or_ip__
__db_url__ = 'http://%s:8000/api/1.0/databases/' % __host_or_ip__


def get_last_db_id():
    """
        Get last database id
    """
    response = requests.get(__db_url__)
    value = response.json()
    last_db_id = 0
    if value:
        db_length = len(value['databases'])
        last_db_id = value['databases'][db_length - 1]['id']

    return last_db_id


def get_last_server_id():
    """
        Get last Server Id
    """

    url = __db_url__ + str(get_last_db_id()) + '/servers/'
    response = requests.get(url)
    value = response.json()
    last_server_id = 0
    if value:
        server_length = len(value['members'])
        last_server_id = value['members'][server_length - 1]['id']

    return last_server_id


class Database(unittest.TestCase):
    """
    setup a new database for test
    """

    def setUp(self):
        # Create a db
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        db_data = {'name': 'testDB'}
        response = requests.post(__db_url__, json=db_data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)

    def tearDown(self):
        # Delete database

        last_db_id = get_last_db_id()
        db_url = __db_url__ + str(last_db_id)
        response = requests.delete(db_url)
        self.assertEqual(response.status_code, 204)


class XML(unittest.TestCase):
    """
    test case for testing valid deployment.xml
    """

    def test_valid_xml(self):
        """
            test valid xml
        """
        data = requests.get(__url__)
        try:
            doc = ElementTree.fromstring(data.content)
        except:
            raise self.fail('Input is not a valid XML document')

        return doc


class UpdateDatabase(Database):
    """
    Update Database and check deployment xml in vdm.xml
    """
    def test_update_database_xml(self):
        """update database xml"""
        last_db_id = get_last_db_id()
        data = requests.get(__url__)
        root = ElementTree.fromstring(data.content)

        for database in root.findall('database'):
            if database.attrib['id'] == last_db_id:
                self.assertEqual(database.attrib['deployment'], "")
                self.assertEqual(database.attrib['name'], "testDB")
                self.assertEqual(database.attrib['members'], "[]")
                self.assertEqual(database.attrib['id'], str(last_db_id))
        for deployment in root.findall('deployment'):
            if deployment.attrib['databaseid'] == str(last_db_id):
                for child in deployment:
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
            last_db_id = value['databases'][db_length - 1]['id']
            url = __db_url__ + str(last_db_id) + '/servers/'
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
            last_db_id = value['databases'][db_length - 1]['id']
            db_data = {'dbId': last_db_id}
            url = __db_url__ + str(last_db_id) + '/servers/'
            response = requests.get(url)
            value = response.json()
            if value:
                server_length = len(value['members'])
                last_server_id = value['members'][server_length - 1]['id']
                print "ServerId to be deleted is " + str(last_server_id)
                url = __db_url__ + str(last_db_id) + '/servers/' + str(last_server_id)
                response = requests.delete(url, json=db_data, headers=headers)
                self.assertEqual(response.status_code, 204)
                # Delete database
                db_url = __db_url__ + str(last_db_id)
                response = requests.delete(db_url)
                self.assertEqual(response.status_code, 204)
            else:
                print "The Server list is empty"
        else:
            print "The database list is empty"


class UpdateMember(Server):
    """
        Update member and check in vdm.xml
    """
    def test_update_member_xml(self):
        """
            Update member xml
        """
        last_server_id = get_last_server_id()
        data = requests.get(__url__)
        root = ElementTree.fromstring(data.content)

        for member in root.findall('member'):
            if member.attrib['id'] == last_server_id:
                self.assertEqual(member.attrib['hostname'], "test")
                self.assertEqual(member.attrib['name'], "test")
                self.assertEqual(member.attrib['description'], "test")
                self.assertEqual(member.attrib['id'], str(last_server_id))


class Deployment(unittest.TestCase):
    """
    test case for Updating Deployment
    """

    def setUp(self):
        """Create a db"""
        url = 'http://%s:8000/api/1.0/databases/' % __host_or_ip__
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        db_data = {'name': 'testDB'}
        response = requests.post(url, json=db_data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)

        last_db_id = get_last_db_id()
        url_dep = 'http://%s:8000/api/1.0/databases/%u/deployment/' % (__host_or_ip__, last_db_id)
        json_data = {
            "cluster": {"sitesperhost": 8, "kfactor": 0, "elastic": "enabled",
                        "schema": "DDL"},
            "paths": {"voltdbroot": {"path": "voltdbroottest"}, "snapshots": {"path": "snapshotstest"},
                      "exportoverflow":
                          {"path": "export_overflow"}, "commandlog": {"path": "command_logtest"},
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
                               "query": {"timeout": 10000},
                               "resourcemonitor": {"memorylimit": {"size": "1"},
                                                   "disklimit": {"feature": [
                                                       {"name": "SNAPSHOTS", "size": "2"}
                                                   ],
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
        self.assertEqual(value['status'], 200)
        self.assertEqual(response.status_code, 200)

    def tearDown(self):
        """Delte a db"""
        last_db_id = get_last_db_id()
        # Delete database
        db_url = __db_url__ + str(last_db_id)
        response = requests.delete(db_url)
        self.assertEqual(response.status_code, 204)


class UpdateDatabaseDeployment(Deployment):
    """
        Update Deployment and check in vdm.xml
    """
    def test_updated_deployment_xml(self):
        """Test updated deployment xml in vdm.xml"""
        last_db_id = get_last_db_id()
        data = requests.get(__url__)
        root = ElementTree.fromstring(data.content)

        for database in root.findall('database'):
            if database.attrib['id'] == last_db_id:
                self.assertEqual(database.attrib['deployment'], "")
                self.assertEqual(database.attrib['name'], "testDB")
                self.assertEqual(database.attrib['members'], "[]")
                self.assertEqual(database.attrib['id'], str(last_db_id))
        for deployment in root.findall('deployment'):
            if deployment.attrib['databaseid'] == str(last_db_id):
                for child in deployment:
                    if child.tag == "paths":
                        for subnode in child:
                            if subnode.tag == "snapshots":
                                self.assertEqual(subnode.attrib['path'], "snapshotstest")
                            if subnode.tag == "commandlogsnapshot":
                                self.assertEqual(subnode.attrib['path'], "command_log_snapshot")
                            if subnode.tag == "voltdbroot":
                                self.assertEqual(subnode.attrib['path'], "voltdbroottest")
                            if subnode.tag == "exportoverflow":
                                self.assertEqual(subnode.attrib['path'], "export_overflow")
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
                                    if supersubnode.tag == "memorylimit":
                                        self.assertEqual(supersubnode.attrib['size'], "1")
                                    if supersubnode.tag == "disklimit":
                                        for disklimit in supersubnode:
                                            self.assertEqual(disklimit.attrib['name'], "SNAPSHOTS")
                                            self.assertEqual(disklimit.attrib['size'], "2")

                    if child.tag == "admin-mode":
                        self.assertEqual(child.attrib['adminstartup'], "false")
                        self.assertEqual(child.attrib['port'], "21211")
                    if child.tag == "cluster":
                        self.assertEqual(child.attrib['elastic'], "enabled")
                        self.assertEqual(child.attrib['hostcount'], "1")
                        self.assertEqual(child.attrib['kfactor'], "0")
                        self.assertEqual(child.attrib['schema'], "DDL")
                        self.assertEqual(child.attrib['sitesperhost'], "8")
                    if child.tag == "snapshot":
                        self.assertEqual(child.attrib['enabled'], "false")
                        self.assertEqual(child.attrib['frequency'], "1h")
                        self.assertEqual(child.attrib['prefix'], "AUTOSNAP")
                        self.assertEqual(child.attrib['retain'], "1")
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
