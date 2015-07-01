#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
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
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import cStringIO
import time
import unittest
import xml.dom.minidom

import runcppunit
import valleak_test

class ParseStupidUnitTest(unittest.TestCase):
    def testSimple(self):
        output = """[{ "class_name": "MessageRegistryTest", "name": "BadAdd" },
{ "class_name": "MessageRegistryTest", "name": "AddAlreadyRegistered", "failure": "foo" }]\n"""
        results = runcppunit.parseStupidUnit(output)

        self.assertEquals(2, len(results))
        self.assertEquals("MessageRegistryTest", results[0].class_name)
        self.assertEquals("BadAdd", results[0].name)
        assert results[0].failure is None

        self.assertEquals("MessageRegistryTest", results[1].class_name)
        self.assertEquals("AddAlreadyRegistered", results[1].name)
        self.assertEquals("foo", results[1].failure)

    def testEmpty(self):
        output = ""
        results = runcppunit.parseStupidUnit(output)
        self.assertEquals(0, len(results))

    def testUnfinished(self):
        # If the test crashes in the middle, it might be unfinished
        output = output = '[{ "class_name": "MessageRegistryTest", "name": "BadAdd" },\n'
        results = runcppunit.parseStupidUnit(output)
        self.assertEquals(1, len(results))

    def testUnfinishedNoOutput(self):
        # If the test crashes in the middle, it might be unfinished
        output = output = '['
        results = runcppunit.parseStupidUnit(output)
        self.assertEquals(0, len(results))


class WriteJUnitTest(unittest.TestCase):
    def testEmpty(self):
        #~ results = [runcppunit.Result("Foo", "Bar", True), runcppunit.Result("Foo", "Bar", False)]

        text = '<?xml version="1.0"?><hello>world&bar</hello>'
        out = cStringIO.StringIO()
        runcppunit.writeJUnitXml(out, "foo_test", 0.5, text, text, [])

        dom = xml.dom.minidom.parseString(out.getvalue())

        self.assertEquals(dom.firstChild.getAttribute("name"), "foo_test")
        self.assertEquals(dom.firstChild.getAttribute("tests"), "0")
        self.assertEquals(dom.firstChild.getAttribute("failures"), "0")
        self.assertEquals(dom.firstChild.getAttribute("time"), "0.500")
        timestamp = dom.firstChild.getAttribute("timestamp")
        tuple_time = time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")

        stdout = dom.getElementsByTagName("system-out")[0].firstChild.data
        stderr = dom.getElementsByTagName("system-err")[0].firstChild.data
        self.assertEquals(text, stderr)

    def testNonEmpty(self):
        error = "<error>&</error>"
        results = [runcppunit.Result("Foo", "Bar", None), runcppunit.Result("Foo", "Baz", error)]
        out = cStringIO.StringIO()
        runcppunit.writeJUnitXml(out, "foo_test", 0.5, "", "", results)

        dom = xml.dom.minidom.parseString(out.getvalue())

        self.assertEquals(dom.firstChild.getAttribute("tests"), "2")
        self.assertEquals(dom.firstChild.getAttribute("failures"), "1")

        tests = dom.getElementsByTagName("testcase")
        self.assertEquals(2, len(tests))

        self.assertEquals("foo_test.Foo", tests[0].getAttribute("classname"))
        self.assertEquals("Bar", tests[0].getAttribute("name"))
        self.assertEquals("0.000", tests[0].getAttribute("time"))
        self.assertEquals(0, tests[0].childNodes.length)

        self.assertEquals("foo_test.Foo", tests[1].getAttribute("classname"))
        self.assertEquals("Baz", tests[1].getAttribute("name"))
        self.assertEquals("0.000", tests[1].getAttribute("time"))
        self.assertEquals(1, tests[1].childNodes.length)
        self.assertEquals("failure", tests[1].firstChild.tagName)
        self.assertEquals("null", tests[1].firstChild.getAttribute("message"))
        self.assertEquals("junit.framework.AssertionFailedError", tests[1].firstChild.getAttribute("type"))
        self.assertEquals(error, tests[1].firstChild.firstChild.data)


class RunTestsTest(unittest.TestCase):
    def testNoStupidUnitSuccess(self):
        exe = valleak_test.compile(self, "int main() { return 0; }\n")
        runcppunit.runtests([exe], self.tempdir.name)
        output_name = self.tempdir.name + "/TEST-" + exe.replace("/", "_") + ".xml"
        f = open(output_name)
        data = f.read()
        assert 'errors="0"' in data
        assert 'failures="0"' in data
        f.close()

    def testNoStupidUnitFailure(self):
        exe = valleak_test.compile(self, "int main() { return 1; }\n")
        runcppunit.runtests([exe], self.tempdir.name)
        output_name = self.tempdir.name + "/TEST-" + exe.replace("/", "_") + ".xml"
        f = open(output_name)
        data = f.read()
        assert 'errors="0"' in data
        assert 'failures="1"' in data
        f.close()


if __name__ == "__main__":
    unittest.main()
