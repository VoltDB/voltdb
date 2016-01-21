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
        except :
            raise self.fail('Input is not a valid XML document')

        return doc

    def test_default_xml_attributes(self):

        data = requests.get(__url__)
        tree = ElementTree.fromstring(data.content)

        for child in tree:
            if child.tag == "paths":
                for subnode in child:
                    if subnode.tag == "snapshots":
                        self.assertEqual(subnode.attrib['path'],"snapshots")
                    if subnode.tag == "commandlogsnapshot":
                        self.assertEqual(subnode.attrib['path'],"command_log_snapshot")
                    if subnode.tag == "voltdbroot":
                        self.assertEqual(subnode.attrib['path'],"voltdbroot")
                    if subnode.tag == "exportoverflow":
                        self.assertEqual(subnode.attrib['path'],"export_overflow")
            if child.tag == "snapshots":
                self.assertEqual(child.attrib['enabled'],"true")
                self.assertEqual(child.attrib['frequency'],"24h")
                self.assertEqual(child.attrib['prefix'],"AUTOSHAP")
                self.assertEqual(child.attrib['retain'],"2")
            if child.tag == "httpd":
                self.assertEqual(child.attrib['enabled'],"true")
                self.assertEqual(child.attrib['port'],"8080")
                for subnode in child:
                    if subnode.tag == "jsonapi":
                        self.assertEqual(subnode.attrib['enabled'],"true")
            if child.tag == "systemsettings":
                for subnode in child:
                    if subnode.tag == "query":
                        self.assertEqual(subnode.attrib['timeout'],"10000")
                    if subnode.tag == "temptables":
                        self.assertEqual(subnode.attrib['maxsize'],"100")
                    if subnode.tag == "snapshot":
                        self.assertEqual(subnode.attrib['priority'],"6")
                    if subnode.tag == "elastic":
                        self.assertEqual(subnode.attrib['duration'],"50")
                        self.assertEqual(subnode.attrib['throughput'],"2")
                    if subnode.tag == "resourcemonitor":
                        for supersubnode in subnode:
                            self.assertEqual(supersubnode.attrib['size'],"80%")
            if child.tag == "admin-mode":
                self.assertEqual(child.attrib['adminstartup'],"false")
                self.assertEqual(child.attrib['port'],"21211")
            if child.tag == "cluster":
                self.assertEqual(child.attrib['elastic'],"enabled")
                self.assertEqual(child.attrib['hostcount'],"1")
                self.assertEqual(child.attrib['kfactor'],"0")
                self.assertEqual(child.attrib['schema'],"ddl")
                self.assertEqual(child.attrib['sitesperhost'],"8")
            if child.tag == "snapshot":
                self.assertEqual(child.attrib['enabled'],"false")
                self.assertEqual(child.attrib['frequency'],"24h")
                self.assertEqual(child.attrib['prefix'],"AUTOSNAP")
                self.assertEqual(child.attrib['retain'],"2")
            if child.tag == "security":
                self.assertEqual(child.attrib['enabled'],"false")
                self.assertEqual(child.attrib['provider'],"hash")
            if child.tag == "partition-detection":
                self.assertEqual(child.attrib['enabled'],"true")
                for subnode in child:
                    self.assertEqual(subnode.attrib['prefix'],"voltdb_partition_detection")

if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
    unittest.main()
