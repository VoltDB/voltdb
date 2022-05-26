#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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

"""Runs C++ unit tests until an error is found."""

import optparse
import os
import sys
import tempfile
import time

import simplejson
import valleak


class Result(object):
    __slots__ = ('class_name', 'name', 'failure')

    def __init__(self, class_name, name, failure):
        assert len(class_name) > 0
        self.class_name = class_name
        assert len(name) > 0
        self.name = name
        assert failure is None or len(failure) > 0
        self.failure = failure


def parseStupidUnit(json_data):
    """Parses json_data from a stupidunit test suite into Result objects."""

    if len(json_data) == 0:
        return []

    if not json_data.endswith("]\n"):
        if json_data.endswith(",\n"):
            json_data = json_data[:-2] + "]\n"
        else:
            assert json_data == "["
            json_data = "[]"

    objects = simplejson.loads(json_data)
    out = []
    for data in objects:
        out.append(Result(data["class_name"], data["name"], data.get("failure", None)))
    return out


def lameXmlEscape(input):
    input = input.replace('&', '&amp;')
    return input.replace('<', '&lt;')


def writeJUnitXml(fileobj, suite_name, elapsed_time, stdout, stderr, results):
    """Writes a JUnit test report in XML format to fileobj from results."""

    timestamp = time.time()
    iso_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(timestamp))

    tests = 0
    failures = 0
    for result in results:
        if result.failure is not None:
            failures += 1
        tests += 1

    fileobj.write('<?xml version="1.0" encoding="utf-8"?>\n')
    fileobj.write('<testsuite errors="0" failures="%d" name="%s" tests="%d" time="%.3f" timestamp="%s">\n' %
            (failures, suite_name, tests, elapsed_time, iso_time))

    for result in results:
        fileobj.write('<testcase classname="%s" name="%s" time="0.000">' %
                (suite_name + "." + result.class_name, result.name))
        if result.failure is not None:
            fileobj.write('<failure message="null" type="junit.framework.AssertionFailedError">')
            fileobj.write(lameXmlEscape(result.failure))
            fileobj.write('</failure>')
        fileobj.write('</testcase>\n')

    fileobj.write("<system-out>")
    fileobj.write(lameXmlEscape(stdout))
    fileobj.write("</system-out>\n")

    fileobj.write("<system-err>")
    fileobj.write(lameXmlEscape(stderr))
    fileobj.write("</system-err>\n")

    fileobj.write("</testsuite>\n")


def runtests(tests, output_dir):
    stupidunit_out = None
    if output_dir is not None:
        # Create a temporary file for stupidunit output
        fileobj, stupidunit_out = tempfile.mkstemp()
        os.close(fileobj)
        os.environ["STUPIDUNIT_OUTPUT"] = stupidunit_out
        os.unlink(stupidunit_out)

    try:
        failures = []
        for test in tests:
            start = time.time()
            error, stdout, stderr = valleak.valleak(test)
            end = time.time()

            if error:
                failures.append(test)
                sys.stdout.write("F")
            else:
                sys.stdout.write(".")
            sys.stdout.flush()

            if output_dir is not None:
                if os.path.exists(stupidunit_out):
                    # Read the stupidunit output
                    input = open(stupidunit_out)
                    json = input.read()
                    input.close()
                    os.unlink(stupidunit_out)
                    results = parseStupidUnit(json)
                else:
                    results = []

                if error:
                    # Process exited with an error code: ensure there is a failure recorded
                    results.append(Result(
                            "Process", "warning", "Process exited with error code %d" % error))
                test_name = test.replace("/", "_")
                out = open("%s/TEST-%s.xml" % (output_dir, test_name), "w")
                writeJUnitXml(out, test_name, end - start, stdout, stderr, results)
                out.close()

        print "\n%d/%d failed" % (len(failures), len(tests))
        for failure in failures:
            print failure
        return len(failures)
    finally:
        if stupidunit_out is not None and os.path.exists(stupidunit_out):
            os.unlink(stupidunit_out)


if __name__ == "__main__":
    options = optparse.OptionParser()
    options.add_option("-o", "--output_dir", dest="output_dir", help="Directory to place output files")
    options, remaining_args = options.parse_args()

    if len(remaining_args) == 1:
        sys.stderr.write("runcppunit.py [test names]\n")
        sys.exit(1)

    errors = runtests(remaining_args, options.output_dir)
    sys.exit(errors)
