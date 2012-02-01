#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2012 VoltDB Inc.
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
from distutils.util import strtobool
from optparse import OptionParser
from voltdbclient import VoltColumn, VoltTable

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

        result += """
<tr>
<td>%s</td>
<td><a href="%s">%s</a></td>
<td>%s</td>
<td>%s</td>
</tr>""" % (i["id"],
            detail_page,
            cgi.escape(i["SQL"]),
            i["jni"]["Status"],
            i["hsqldb"]["Status"])

    result += """
</table>
"""

    return result

def is_different(x):
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
            return True

    if "Result" in x["jni"] and "Result" in x["hsqldb"]:
        x["highlight"] = []
        if (x["jni"]["Result"] == None or x["hsqldb"]["Result"] == None
            or len(x["jni"]["Result"]) != len(x["hsqldb"]["Result"])):
            return True
        for i in xrange(len(x["jni"]["Result"])):
            x["highlight"].append([])
            # Disable column type checking for now because Volt and HSQL don't
            # promote int types in the same way.
            # if x["jni"]["Result"][i].columns != x["hsqldb"]["Result"][i].columns:
            #     x["highlight"][i].append(2) # Column names and types
            if len(x["jni"]["Result"][i].tuples) != \
                    len(x["hsqldb"]["Result"][i].tuples):
                x["highlight"][i].append(1) # Tuple count
                return True
            for j in xrange(len(x["jni"]["Result"][i].tuples)):
                if x["jni"]["Result"][i].tuples[j] != \
                        x["hsqldb"]["Result"][i].tuples[j]:
                    x["highlight"][i].append(j + 4) # Offset to the correct row
        for i in x["highlight"]:
            if i:
                return True

    return False

def usage(prog_name):
    print """
Usage:
\t%s report [-o output_dir] [-f true/false] [-a]

Generates HTML reports based on the given report files. The generated reports
contain the SQL statements which caused different responses on both backends.
""" % (prog_name)

def generate_html_reports(seed, statements_path, hsql_path, jni_path,
                          output_dir, report_all, is_matching = False):
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
            if not is_different(statement):
                mismatches.append(statement)
        else:
            if is_different(statement):
                mismatches.append(statement)
        if report_all:
            all_results.append(statement)

    statements_file.close()
    hsql_file.close()
    jni_file.close()

    report = """
<html>
<head>
<title>SQL Coverage Test Report</title>
<style>
h2 {text-transform: uppercase}
</style>
</head>

<body>
Random seed: %s
<br/>
Total statements: %d
<br/>
Failed (*not* necessarily mismatched) statements: %d
""" % (seed, count, failures)

    def key(x):
        return int(x["id"])
    sorted(mismatches, cmp=cmp, key=key)

    report += print_section("Mismatched Statements",
                            mismatches, output_dir)

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

    return (failures, len(mismatches))

def generate_summary(output_dir, statistics):
    fd = open(os.path.join(output_dir, "index.html"), "w")
    content = """
<html>
<head>
<title>SQL Coverage Test Report</title>
<style>
h2 {text-transform: uppercase}
</style>
</head>

<body>
Summary:
<br/>
<ul>
"""

    def bullets(name, failures, mismatches):
        return '<li><a href="%s/index.html">%s</a>: ' \
            '%d failures, %d mismatches</li>' % \
            (name, name, failures, mismatches)

    for k, v in statistics.iteritems():
        content += bullets(k, v[0], v[1])

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

    generate_html_reports(data, options.output_dir, options.all, is_matching)
