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

__url__ = 'http://localhost:8000/api/1.0/vdm/'


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

    def test_default_xml_attributes(self):
        """
            test xml default attributes
        """
        data = requests.get(__url__)
        tree = ElementTree.fromstring(data.content)
        databaseFound = False
        membersFound = False
        for child in tree:
            if child.tag == "databases":
                for subnode in child:
                    if subnode.tag == "database":
                        databaseFound = True
                        self.assertEqual(subnode.attrib['deployment'], "default")
                        self.assertEqual(subnode.attrib['id'], "1")
                        self.assertEqual(subnode.attrib['members'], "[1]")
                        self.assertEqual(subnode.attrib['name'], "local")
            if child.tag == "members":
                for subnode in child:
                    if subnode.tag == "member":
                        membersFound = True
                        self.assertEqual(subnode.attrib['admin-listener'], "")
                        self.assertEqual(subnode.attrib['client-listener'], "")
                        self.assertEqual(subnode.attrib['description'], "")
                        self.assertEqual(subnode.attrib['enabled'], "true")
                        self.assertEqual(subnode.attrib['external-interface'], "")
                        # self.assertEqual(subnode.attrib['hostname'], "127.0.1.1")
                        self.assertEqual(subnode.attrib['http-listener'], "")
                        self.assertEqual(subnode.attrib['id'], "1")
                        self.assertEqual(subnode.attrib['internal-interface'], "")
                        self.assertEqual(subnode.attrib['internal-listener'], "")
                        # self.assertEqual(subnode.attrib['name'], "")
                        self.assertEqual(subnode.attrib['placement-group'], "")
                        self.assertEqual(subnode.attrib['public-interface'], "")
                        self.assertEqual(subnode.attrib['replication-listener'], "")
                        self.assertEqual(subnode.attrib['zookeeper-listener'], "")
            if child.tag == "deployments":
                for subnode in child:
                    if subnode.tag == "deployment":
                        for supersubnode in subnode:
                            if supersubnode.tag == "paths":
                                for node in supersubnode:
                                    if supersubnode.tag == "snapshots":
                                        self.assertEqual(node.attrib['path'], "snapshots")
                                    if supersubnode.tag == "commandlog":
                                        self.assertEqual(node.attrib['path'], "command_log")
                                    if supersubnode.tag == "voltdbroot":
                                        self.assertEqual(node.attrib['path'], "voltdbroot")
                                    if supersubnode.tag == "exportoverflow":
                                        self.assertEqual(node.attrib['path'], "export_overflow")
                                    if supersubnode.tag == "droverflow":
                                        self.assertEqual(node.attrib['path'], "dr_overflow")
                            if supersubnode.tag == "httpd":
                                self.assertEqual(supersubnode.attrib['enabled'], "true")
                                self.assertEqual(supersubnode.attrib['port'], "8080")
                                for node in supersubnode:
                                    if supersubnode.tag == "jsonapi":
                                        self.assertEqual(node.attrib['enabled'], "true")
                            if supersubnode.tag == "systemsettings":
                                for node in supersubnode:
                                    if node.tag == "query":
                                        self.assertEqual(node.attrib['timeout'], "10000")
                                    if node.tag == "temptables":
                                        self.assertEqual(node.attrib['maxsize'], "100")
                                    if node.tag == "snapshot":
                                        self.assertEqual(node.attrib['priority'], "6")
                                    if node.tag == "elastic":
                                        self.assertEqual(node.attrib['duration'], "50")
                                        self.assertEqual(node.attrib['throughput'], "2")
                            if supersubnode.tag == "commandlog":
                                self.assertEqual(supersubnode.attrib['enabled'], "false")
                                self.assertEqual(supersubnode.attrib['logsize'], "1024")
                                self.assertEqual(supersubnode.attrib['synchronous'], "false")
                                for node in supersubnode:
                                    if node.tag == "query":
                                        self.assertEqual(node.attrib['timeout'], "10000")

        if databaseFound == False:
            self.fail("Xml must contain databases tag")
        if membersFound == False:
            self.fail("Xml must contain members tag")

if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
    unittest.main()
