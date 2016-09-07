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
from xml.etree import ElementTree
import socket
import xmlrunner

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


class DatabaseServer(unittest.TestCase):
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


class UpdateDatabase(DatabaseServer):
    """
    Update Database and check deployment xml in vdm.xml
    """
    def test_update_database_xml(self):
        """update database xml"""
        last_db_id = get_last_db_id()
        data = requests.get(__url__)
        root = ElementTree.fromstring(data.content)

        for database in root.findall('databases')[0].findall('database'):
            if int(database.attrib['id']) == last_db_id:
                self.assertEqual(database.attrib['name'], "testDB")
                self.assertEqual(database.attrib['id'], str(last_db_id))
                for member in database.findall('members')[0].findall('member'):
                    self.assertEqual(member.attrib['hostname'], __host_or_ip__)
                    self.assertEqual(member.attrib['description'], 'test')
                    self.assertEqual(member.attrib['name'], 'test')
                    self.assertEqual(member.attrib['admin-listener'], '')
                    self.assertEqual(member.attrib['replication-listener'], '')
                    self.assertEqual(member.attrib['enabled'], 'true')
                    self.assertEqual(member.attrib['public-interface'], '')
                    self.assertEqual(member.attrib['internal-listener'], '')
                    self.assertEqual(member.attrib['http-listener'], '')
                    self.assertEqual(member.attrib['client-listener'], '')
                    self.assertEqual(member.attrib['placement-group'], '')
                    self.assertEqual(member.attrib['external-interface'], '')
                    self.assertEqual(member.attrib['internal-interface'], '')
                    self.assertEqual(member.attrib['id'], str(get_last_server_id()))
                    self.assertEqual(member.attrib['zookeeper-listener'], '')
        for deployment in root.findall('deployments')[0].findall('deployment'):
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


class UpdateMember(DatabaseServer):
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

        if root.findall('members'):
            for member in root.findall('members')[0].findall('member'):
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
                                                       {"name": "snapshots", "size": "2"}
                                                   ],
                                                       "size": "10"},
                                                   "frequency": 5}},
            "security": {"enabled": True, "provider": "HASH"},
            "export": {"configuration": [{"enabled": False,
                                          "type": "kafka", "exportconnectorclass": "test",
                                          "stream": "test", "property": [{"name": "metadata.broker.list",
                                                                          "value": "test"}]}]},
            "import": {"configuration": [{"enabled": False, "type": "kafka", "module": "test", "format": "test",
                                          "property": [{"name": "metadata.broker.list", "value": "test"}]}]}
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

        for database in root.findall('databases')[0].findall('database'):
            if database.attrib['id'] == str(last_db_id):
                self.assertEqual(database.attrib['name'], "testDB")
                self.assertEqual(database.attrib['id'], str(last_db_id))
                self.assertEqual(database.findall('members')[0].findall('member'), [])

        for deployment in root.findall('deployments')[0].findall('deployment'):
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
                                            self.assertEqual(disklimit.attrib['name'], "snapshots")
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
