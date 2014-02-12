#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2014 VoltDB Inc.
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

import sys
import cgi
import os
import cPickle
import decimal
from distutils.util import strtobool
from optparse import OptionParser
from voltdbclient import VoltColumn, VoltTable, FastSerializer

__quiet = True

def highlight(s, flag = True):
    if not isinstance(s, basestring):
        s = str(s)
    return flag and "<span style=\"color: red\">%s</span>" % (s) or s

def generate_table_str(res):
    tablestr = {"jni": "", "hsqldb": ""}

    for k in tablestr.iterkeys():
        tmp = []
        if "Result" in res[k] and res[k]["Result"] != None:
            for i in xrange(len(res[k]["Result"])):
                result = []

                result.append("column count: %d" %
                              (len(res[k]["Result"][i].columns)))
                result.append("row count: %d" %
                              (len(res[k]["Result"][i].tuples)))
                result.append("cols: " +
                              ", ".join(map(lambda x: str(x),
                                            res[k]["Result"][i].columns)))
                result.append("rows -")
                result.extend(map(lambda x: str(x), res[k]["Result"][i].tuples))

                for j in xrange(len(result)):
                    if "highlight" in res and \
                            isinstance(res["highlight"][i], list) and \
                            j in res["highlight"][i]:
                        result[j] = highlight(result[j])
                tmp.append("<br />".join(result))
            tablestr[k] = "<br />".join(tmp)

    return tablestr

def generate_detail(name, item, output_dir):
    filename = "%s.html" % (item["id"])
    if output_dir == None:
        return

    tablestr = generate_table_str(item)

    details = """
<html>
<head>
<title>Detail of "%s"</title>
<style>
td {width: 50%%}
</style>
</head>

<body>
<h2>%s</h2>
<table cellpadding=3 cellspacing=1 border=1>
<tr>
<th>VoltDB Response</th>
<th>HSQLDB Response</th>
</tr>
<tr>
<td>%s</td>
<td>%s</td>
</tr>
<tr>
<td>%s</td>
<td>%s</td>
</tr>
<tr>
<td>%s</td>
<td>%s</td>
</tr>
</table>
</body>

</html>
""" % (cgi.escape(item["SQL"]),
       cgi.escape(item["SQL"]),
       highlight(item["jni"]["Status"],
                 "highlight" in item and "Status" in item["highlight"]),
       highlight(item["hsqldb"]["Status"],
                 "highlight" in item and "Status" in item["highlight"]),
       "Info" in item["jni"] and item["jni"]["Info"] or "",
       "Info" in item["hsqldb"] and item["hsqldb"]["Info"] or "",
       tablestr["jni"],
       tablestr["hsqldb"])

    fd = open(os.path.join(output_dir, filename), "w")
    fd.write(details.encode("utf-8"))
    fd.close()

    return filename

def safe_print(s):
    if not __quiet:
        print s

def print_section(name, mismatches, output_dir):
    result = """
<h2>%s: %d</h2>
<table cellpadding=3 cellspacing=1 border=1>
<tr>
<th>ID</th>
<th>SQL Statement</th>
<th>VoltDB Status</th>
<th>HSQLDB Status</th>
</tr>
""" % (name, len(mismatches))

    for i in mismatches:
        safe_print(i["SQL"])
        detail_page = generate_detail(name, i, output_dir)
        jniStatus = i["jni"]["Status"]
        if jniStatus < 0:
            jniStatus = "Error: " + `jniStatus`
        hsqldbStatus = i["hsqldb"]["Status"]
        if hsqldbStatus < 0:
            hsqldbStatus = "Error: " + `hsqldbStatus`
        result += """
<tr>
<td>%s</td>
<td><a href="%s">%s</a></td>
<td>%s</td>
<td>%s</td>
</tr>""" % (i["id"],
            detail_page,
            cgi.escape(i["SQL"]),
            jniStatus,
            hsqldbStatus)

    result += """
</table>
"""

    return result

def is_different(x, cntonly):
    """Marks the attributes that are different. Since the whole table will be
    printed out as a single string.
    the first line is column count,
    the second line is row count,
    the third line is column names and types,
    and then followed by rows.
    """

    # JNI returns a variety of negative error result values that we
    # can't easily match with the HSQL backend.  Reject only pairs of
    # status values where one of them wasn't an error
    if x["jni"]["Status"] != x["hsqldb"]["Status"]:
        if int(x["jni"]["Status"]) > 0 or int(x["hsqldb"]["Status"]) > 0:
            x["highlight"] = ["Status"]
            # print "DEBUG is_different -- just different errors (0 or less)"
            return True

    if "Result" in x["jni"] and "Result" in x["hsqldb"]:
        x["highlight"] = []
        jniResult = x["jni"]["Result"]
        hsqldbResult = x["hsqldb"]["Result"]
        if jniResult == None or hsqldbResult == None:
            # print "DEBUG is_different -- got a Result of None"
            return True
        if len(jniResult) != len(hsqldbResult):
            # print "DEBUG is_different -- got different result lengths"
            return True
        for i in xrange(len(jniResult)):
            x["highlight"].append([])
            # Disable column type checking for now because Volt and HSQL don't
            # promote int types in the same way.
            # if jniResult[i].columns != hsqldbResult[i].columns:
            #     x["highlight"][i].append(2) # Column names and types
            if len(jniResult[i].tuples) != len(hsqldbResult[i].tuples):
                x["highlight"][i].append(1) # Tuple count
                # print "DEBUG is_different -- got different numbers of tuples?"
                return True
            if cntonly != True:
                # print "DEBUG is_different -- count only got FALSE return"
                return False # The results are close enough to pass a count-only check
                for j in xrange(len(jniResult[i].tuples)):
                    # print "DEBUG is_different -- comparing highlight? ", i, j, jniResult[i].tuples[j], hsqldbResult[i].tuples[j]
                    if jniResult[i].tuples[j] != hsqldbResult[i].tuples[j]:
                        # Work around any false value differences caused by default type differences.
                        # These differences are "properly" ignored by the Decimal/float != implementation post-python 2.6.
                        if (jniResult[i].columns[j].type == FastSerializer.VOLTTYPE_FLOAT and
                            hsqldbResult[i].columns[j].type == FastSerializer.VOLTTYPE_DECIMAL and
                            decimal.Decimal(str(jniResult[i].tuples[j])) == hsqldbResult[i].tuples[j]):
                            continue
                        # print "DEBUG is_different -- appending highlight? ", i, j
                        x["highlight"][i].append(j + 4) # Offset the mismatched row to skip the hard-coded values used above.
        if cntonly != True:
            for i in x["highlight"]:
                # print "DEBUG is_different -- different highlight: ", i
                return True
    # print "DEBUG is_different -- got FALSE return"
    return False

def usage(prog_name):
    print """
Usage:
\t%s report [-o output_dir] [-f true/false] [-a]

Generates HTML reports based on the given report files. The generated reports
contain the SQL statements which caused different responses on both backends.
""" % (prog_name)

def generate_html_reports(suite, seed, statements_path, hsql_path, jni_path,
                          output_dir, report_all, cntonly = False, is_matching = False):
    if output_dir != None and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    statements_file = open(statements_path, "rb")
    hsql_file = open(hsql_path, "rb")
    jni_file = open(jni_path, "rb")
    failures = 0
    count = 0
    mismatches = []
    all_results = []

    while True:
        try:
            statement = cPickle.load(statements_file)
            # print "DEBUG loaded statement ", statement
        except EOFError:
            break

        try:
            jni = cPickle.load(jni_file)
            hsql = cPickle.load(hsql_file)
        except EOFError as e:
            raise IOError("Not enough results for generated statements: %s" %
                          (str(e)))

        count += 1
        if int(jni["Status"]) != 1:
            failures += 1

        statement["hsqldb"] = hsql
        statement["jni"] = jni
        if is_matching:
            if not is_different(statement, cntonly):
                mismatches.append(statement)
        else:
            if is_different(statement, cntonly):
                mismatches.append(statement)
        if report_all:
            all_results.append(statement)

    statements_file.close()
    hsql_file.close()
    jni_file.close()

    topLine = getTopSummaryLine()
    keyStats = createSummaryInHTML(count, failures, len(mismatches), seed)
    report = """
<html>
<head>
<title>SQL Coverage Test Report</title>
<style>
h2 {text-transform: uppercase}
</style>
</head>

<body>
    <h2>Test Suite Name: %s</h2>
    <h4>Random Seed: <b>%d</b></h4>
    <table border=1><tr>%s</tr>
""" % (suite, seed, topLine)

    report += """
<tr>%s</tr>
</table>
""" % (keyStats)

    def key(x):
        return int(x["id"])
    if(len(mismatches) > 0):
        sorted(mismatches, cmp=cmp, key=key)
        report += print_section("Mismatched Statements", mismatches, output_dir)

    if report_all:
        report += print_section("Total Statements", all_results, output_dir)

    report += """
</body>
</html>
"""

    if output_dir != None:
        summary = open(os.path.join(output_dir, "index.html"), "w")
        summary.write(report.encode("utf-8"))
        summary.close()

    results = {}
    results["mis"] = len(mismatches)
    results["keyStats"] = keyStats
    return results

def getTopSummaryLine():
    topLine = """
<td>Valid</td><td>Valid %</td>
<td>Invalid</td><td>Invalid %</td>
<td>Total</td>
<td>Mismatched</td><td>Mismatched %</td>
"""
    return topLine

def createSummaryInHTML(count, failures, misses, seed):
    passed = count - (failures + misses)
    passed_ps = fail_ps = cell4misPct = cell4misCnt = color = None
    if(failures == 0):
        fail_ps = "0.00%"
    else:
        fail_ps = str("{0:.2f}".format((failures/float(count)) * 100)) + "%"
    if(misses == 0):
        cell4misPct = "<td align=right>0.00%</td>"
        cell4misCnt = "<td align=right>0</td>"
    else:
        color = "#FF0000" # red
        mis_ps = "{0:.2f}".format((misses/float(count)) * 100)
        cell4misPct = "<td align=right bgcolor=" + color + ">" + mis_ps + "%</td>"
        cell4misCnt = "<td align=right bgcolor=" + color + ">" + str(misses) + "</td>"
    misRow = cell4misCnt + cell4misPct

    if(passed == count):
        passed_ps = "100.00%"
    else:
        passed_ps = str("{0:.2f}".format((passed/float(count)) * 100)) + "%"
    stats = """
<td align=right>%d</td>
<td align=right>%s</td>
<td align=right>%d</td>
<td align=right>%s</td>
<td align=right>%d</td>%s</tr>
""" % (passed, passed_ps, failures, fail_ps, count, misRow)

    return stats

def generate_summary(output_dir, statistics):
    fd = open(os.path.join(output_dir, "index.html"), "w")
    topLine = getTopSummaryLine()
    content = """
<html>
<head>
<title>SQL Coverage Test Report</title>
<style>
h2 {text-transform: uppercase}
</style>
</head>

<body>
<h2>SQL Coverage Test Summary Grouped By Suites:</h2>
<h3>Random Seed: %d</h3>
<table border=1>
<tr>
<td rowspan=2 align=center>Test Suite</td>
<td colspan=5 align=center>Statements</td><td colspan=2 align=center>Test Failures</td></tr>
<tr>%s</tr>
""" % (statistics["seed"], topLine)

    def bullets(name, stats):
        return "<tr><td><a href=\"%s/index.html\">%s</a></td>%s</tr>" % \
            (name, name, stats)

    for suiteName in sorted(statistics.iterkeys()):
        if(suiteName != "seed"):
            content += bullets(suiteName, statistics[suiteName])
    content += """
</table>
</body>
</html>
"""

    fd.write(content)
    fd.close()

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-o", "--output", dest="output_dir",
                      help="The directory to put all the output HTML files.")
    parser.add_option("-f", "--flag", dest="flag",
                      help="true to print out matching statements, "
                      "false to print out mismatching statements")
    parser.add_option("-a", action="store_true", dest="all", default=False,
                      help="Whether or not to report all statements")
    (options, args) = parser.parse_args()

    if len(args) != 1:
        usage(sys.argv[0])
        exit(-1)

    is_matching = False

    fd = open(args[0], "rb")
    data = fd.read()
    fd.close()

    if options.flag != None:
        __quiet = False
        is_matching = strtobool(options.flag)

    generate_html_reports("suite name", data, options.output_dir, options.all, is_matching)
