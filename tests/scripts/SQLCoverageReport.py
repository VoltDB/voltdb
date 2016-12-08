#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
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
import codecs
import cPickle
import decimal
import datetime
import os
import traceback
from distutils.util import strtobool
from optparse import OptionParser
from voltdbclient import VoltColumn, VoltTable, FastSerializer

__quiet = True

def highlight(s, flag):
    if not isinstance(s, basestring):
        s = str(s)
    return flag and "<span style=\"color: red\">%s</span>" % (s) or s

def as_html_unicode_string(s):
    if isinstance(s, list):
        return '[' + ", ".join(as_html_unicode_string(x) for x in s) + ']'
    elif isinstance(s, basestring):
        return "'" + s.encode('ascii', 'xmlcharrefreplace') + "'"
    else:
        return str(s)

def generate_table_str(res, key):

    source = res[key].get("Result")
    if not source:
        return ""
    highlights = res.get("highlight")
    if isinstance(highlights, list):
        key_highlights = highlights
    else:
        key_highlights = res.get("highlight_" + key)

    result = []
    result.append(highlight("column count: %d" % (len(source.columns)), "Columns" == highlights))
    result.append(highlight("row count: %d" % (len(source.tuples)), "Tuples" == highlights))
    result.append("cols: " + ", ".join(map(lambda x: str(x), source.columns)))
    result.append("rows -")
    if isinstance(key_highlights, list):
        for j in xrange(len(source.tuples)):
            result.append(highlight(as_html_unicode_string(source.tuples[j]), j in key_highlights))
    else:
        result.extend(map(lambda x: as_html_unicode_string(x), source.tuples))
    tablestr = "<br />".join(result)
    return tablestr

def generate_modified_query(cmpdb, sql, modified_sql):
    result = ''
    mod_sql = modified_sql.get(sql, None)
    if mod_sql:
        result = '<p>Modified SQL query, as sent to ' + str(cmpdb) + ':</p><h2>' + str(mod_sql) + '</h2>'
    return result

def generate_detail(name, item, output_dir, cmpdb, modified_sql):
    if output_dir == None:
        return

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
%s
<table cellpadding=3 cellspacing=1 border=1>
<tr>
<th>VoltDB Response</th>
<th>%s Response</th>
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
""" % (cgi.escape(item["SQL"]).encode('ascii', 'xmlcharrefreplace'),
       cgi.escape(item["SQL"]).encode('ascii', 'xmlcharrefreplace'),
       generate_modified_query(cmpdb, cgi.escape(item["SQL"]).encode('ascii', 'xmlcharrefreplace'), modified_sql),
       cmpdb,
       highlight(item["jni"]["Status"], "Status" == item.get("highlight")),
       highlight(item["cmp"]["Status"], "Status" == item.get("highlight")),
       item["jni"].get("Info") or "",
       item["cmp"].get("Info") or "",
       generate_table_str(item, "jni"),
       generate_table_str(item, "cmp") )

    filename = "%s.html" % (item["id"])
    fd = open(os.path.join(output_dir, filename), "w")
    fd.write(details.encode("utf-8"))
    fd.close()

    return filename

def safe_print(s):
    if not __quiet:
        print s

def print_section(name, mismatches, output_dir, cmpdb, modified_sql):
    result = """
<h2>%s: %d</h2>
<table cellpadding=3 cellspacing=1 border=1>
<tr>
<th>ID</th>
<th>SQL Statement</th>
<th>VoltDB Status</th>
<th>%s Status</th>
</tr>
""" % (name, len(mismatches), cmpdb)

    temp = []
    for i in mismatches:
        safe_print(i["SQL"])
        detail_page = generate_detail(name, i, output_dir, cmpdb, modified_sql)
        jniStatus = i["jni"]["Status"]
        if jniStatus < 0:
            jniStatus = "Error: " + `jniStatus`
        cmpdbStatus = i["cmp"]["Status"]
        if cmpdbStatus < 0:
            cmpdbStatus = "Error: " + `cmpdbStatus`
        temp.append("""
<tr>
<td>%s</td>
<td><a href="%s">%s</a></td>
<td>%s</td>
<td>%s</td>
</tr>""" % (i["id"],
            detail_page,
            cgi.escape(i["SQL"]).encode('ascii', 'xmlcharrefreplace'),
            jniStatus,
            cmpdbStatus))

    result += ''.join(temp)

    result += """
</table>
"""

    return result

def is_different(x, cntonly):
    """Notes the attributes that are different. Since the whole table will be
    printed out as a single string.
    the first line is column count,
    the second line is row count,
    the third line is column names and types,
    followed by rows.
    """

    jni = x["jni"]
    cmp = x["cmp"]
    # JNI returns a variety of negative error result values that we
    # can't easily match with the HSqlDB backend.  Reject only pairs
    # of status values where one of them wasn't an error.
    if jni["Status"] != cmp["Status"]:
        if int(jni["Status"]) > 0 or int(cmp["Status"]) > 0:
            x["highlight"] = "Status"
            # print "DEBUG is_different -- one error (0 or less)"
            return True
        # print "DEBUG is_different -- just different errors (0 or less)"
        return False;
    if int(jni["Status"]) <= 0:
        # print "DEBUG is_different -- same error (0 or less)"
        return False;

    # print "DEBUG is_different -- same non-error Status? : ", jni["Status"]

    jniResult = jni["Result"]
    cmpResult = cmp["Result"]
    if (not jniResult) or (not cmpResult):
        x["highlight"] = "Result"
        # print "DEBUG is_different -- lacked expected result(s)"
        return True

    # Disable column type checking for now because VoltDB and HSqlDB don't
    # promote int types in the same way.
    # if jniResult.columns != cmpResult.columns:
    #     x["highlight"] = "Columns"
    #     return True

    jniColumns  = jniResult.columns
    cmpColumns = cmpResult.columns
    nColumns = len(jniColumns)
    if nColumns != len(cmpColumns):
        x["highlight"] = "Columns"
        return True;
    # print "DEBUG is_different -- got same column lengths? ", nColumns

    jniTuples = jniResult.tuples
    cmpTuples = cmpResult.tuples

    if len(jniTuples) != len(cmpTuples):
        x["highlight"] = "Tuples"
        x["highlight_jni"] = []
        x["highlight_cmp"] = []
        # print "DEBUG is_different -- got different numbers of tuples?"
        for ii in xrange(len(jniTuples)):
            if jniTuples[ii] not in cmpTuples:
                x["highlight_jni"].append(ii)
        for ii in xrange(len(cmpTuples)):
            if cmpTuples[ii] not in jniTuples:
                x["highlight_cmp"].append(ii)
        return True
    # print "DEBUG is_different -- got same numbers of tuples?", len(jniTuples), "namely ", jniTuples

    if cntonly:
        # print "DEBUG is_different -- count only got FALSE return"
        return False # The results are close enough to pass a count-only check

    for ii in xrange(len(jniTuples)):
        if jniTuples[ii] == cmpTuples[ii]:
            continue
        # Work around any false value differences caused by default type differences.
        # These differences are "properly" ignored by the
        # Decimal/float != implementation post-python 2.6.
        column_problem = False # hope for the best.
        for jj in xrange(nColumns):
            if jniTuples[ii][jj] == cmpTuples[ii][jj]:
                continue
            if (jniColumns[jj].type == FastSerializer.VOLTTYPE_FLOAT and
                    cmpColumns[jj].type == FastSerializer.VOLTTYPE_DECIMAL):
                if decimal.Decimal(str(jniTuples[ii][jj])) == cmpTuples[ii][jj]:
                    ### print "INFO is_different -- Needed float-to-decimal help"
                    continue
                print "INFO is_different -- float-to-decimal conversion did not help convert between values:" , \
                        "jni:(" , jniTuples[ii][jj] , ") and cmp:(" , cmpTuples[ii][jj] , ")."
                print "INFO is_different -- float-to-decimal conversion stages:" , \
                        " from jniTuples[ii][jj] of type:" , type(jniTuples[ii][jj]) , \
                        " to cmpTuples[ii][jj] of type:" , type(cmpTuples[ii][jj]) , \
                        " via str(jniTuples[ii][jj]):" , str(jniTuples[ii][jj]) , " of type: " , type(str(jniTuples[ii][jj])) , \
                        " via decimal.Decimal(str(jniTuples[ii][jj])):" , decimal.Decimal(str(jniTuples[ii][jj])) , " of type: " , type(decimal.Decimal(str(jniTuples[ii][jj])))
            column_problem = True
        if column_problem:
            # print "DEBUG is_different -- appending difference highlight? ", ii
            if not x.get("highlight"):
                x["highlight"] = []
            x["highlight"].append(ii)
    if x.get("highlight"):
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

def generate_html_reports(suite, seed, statements_path, cmpdb_path, jni_path,
                          output_dir, report_invalid, report_all, extra_stats='',
                          cmpdb='HSqlDB', modified_sql_path=None, max_mismatches=0,
                          cntonly=False):
    if output_dir != None and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    statements_file = open(statements_path, "rb")
    cmpdb_file = open(cmpdb_path, "rb")
    jni_file = open(jni_path, "rb")
    if modified_sql_path:
        modified_sql_file = codecs.open(modified_sql_path, encoding='utf-8')
    else:
        modified_sql_file = None
    modified_sql = {}
    failures = 0
    count = 0
    mismatches = []
    crashed = []
    voltdb_npes = []
    cmpdb_npes  = []
    invalid     = []
    all_results = []

    try:
        while True:
            try:
                statement = cPickle.load(statements_file)
                # print "DEBUG loaded statement ", statement
            except EOFError:
                break

            notFound = False
            try:
                jni = cPickle.load(jni_file)
            except EOFError as e:
                notFound = True
                jni = {'Status': -99, 'Exception': 'None', 'Result': None,
                       'Info': '<p style="color:red">RESULT NOT FOUND! Probably due to a VoltDB crash!</p>'}
            try:
                cdb = cPickle.load(cmpdb_file)
            except EOFError as e:
                notFound = True
                cdb = {'Status': -98, 'Exception': 'None', 'Result': None,
                       'Info': '<p style="color:red">RESULT NOT FOUND! Probably due to a ' + cmpdb + ' backend crash!</p>'}

            count += 1
            if int(jni["Status"]) != 1:
                failures += 1
                if report_invalid:
                    invalid.append(statement)

            statement["jni"] = jni
            statement["cmp"] = cdb

            if notFound:
                crashed.append(statement)
            elif is_different(statement, cntonly):
                mismatches.append(statement)
            if ('NullPointerException' in str(jni)):
                voltdb_npes.append(statement)
            if ('NullPointerException' in str(cdb)):
                cmpdb_npes.append(statement)
            if report_all:
                all_results.append(statement)

    except EOFError as e:
        raise IOError("Not enough results for generated statements: %s" % str(e))

    statements_file.close()
    cmpdb_file.close()
    jni_file.close()

    if modified_sql_file:
        try:
            while True:
                try:
                    orig_sql = modified_sql_file.readline().rstrip('\n').replace('original SQL: ', '')
                    mod_sql  = modified_sql_file.readline().rstrip('\n').replace('modified SQL: ', '')
                    if orig_sql and mod_sql:
                        modified_sql[cgi.escape(orig_sql).encode('ascii', 'xmlcharrefreplace')] \
                                    = cgi.escape(mod_sql).encode('ascii', 'xmlcharrefreplace')
                    else:
                        break
                except EOFError as e:
                    break
            modified_sql_file.close()
        except Exception as e:
            traceback.print_exc()
            raise IOError("Unable to read modified SQL file: %s\n  %s" % (modified_sql_path, str(e)))

    topLines = getTopSummaryLines(cmpdb, False)
    currentTime = datetime.datetime.now().strftime("%A, %B %d, %I:%M:%S %p")
    keyStats = createSummaryInHTML(count, failures, len(mismatches), len(voltdb_npes),
                                   len(cmpdb_npes), extra_stats, seed, max_mismatches)
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
    <p>This report was generated on <b>%s</b></p>
    <table border=1>
    %s
""" % (suite, seed, currentTime, topLines)

    report += """
<tr>%s</tr>
</table>
""" % (keyStats)

    def key(x):
        return int(x["id"])
    if(len(mismatches) > 0):
        sorted(mismatches, cmp=cmp, key=key)
        report += print_section("Mismatched Statements", mismatches, output_dir, cmpdb, modified_sql)

    if(len(crashed) > 0):
        sorted(crashed, cmp=cmp, key=key)
        report += print_section("Statements Missing Results, due to a Crash<br>(the first one probably caused the crash)", crashed, output_dir, cmpdb, modified_sql)

    if(len(voltdb_npes) > 0):
        sorted(voltdb_npes, cmp=cmp, key=key)
        report += print_section("Statements That Cause a NullPointerException (NPE) in VoltDB", voltdb_npes, output_dir, cmpdb, modified_sql)

    if(len(cmpdb_npes) > 0):
        sorted(cmpdb_npes, cmp=cmp, key=key)
        report += print_section("Statements That Cause a NullPointerException (NPE) in " + cmpdb, cmpdb_npes, output_dir, cmpdb, modified_sql)

    if report_invalid and (len(invalid) > 0):
        report += print_section("Invalid Statements", invalid, output_dir, cmpdb, modified_sql)

    if report_all:
        report += print_section("Total Statements", all_results, output_dir, cmpdb, modified_sql)

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

def getTopSummaryLines(cmpdb, includeAll=True):
    topLines = "<tr>"
    if includeAll:
        topLines += "<td rowspan=2 align=center>Test Suite</td>"
    topLines += """
<td colspan=5 align=center>SQL Statements</td>
<td colspan=5 align=center>Test Failures</td>
<td colspan=5 align=center>SQL Statements per Pattern</td>
<td colspan=5 align=center>Time (min:sec)</td>
</tr><tr>
<td>Valid</td><td>Valid %%</td>
<td>Invalid</td><td>Invalid %%</td>
<td>Total</td>
<td>Mismatched</td><td>Mismatched %%</td>
<td>NPE's(V)</td><td>NPE's(%s)</td><td>Crashes</td>
<td>Minimum</td><td>Maximum</td><td># Inserts</td><td># Patterns</td><td># Unresolved</td>
<td>Generating SQL</td><td>VoltDB</td><td>%s</td>
""" % (cmpdb[:1], cmpdb)
    if includeAll:
        topLines += "<td>Comparing</td><td>Total</td>"
    topLines += "</tr>"
    return topLines

def createSummaryInHTML(count, failures, misses, voltdb_npes, cmpdb_npes,
                        extra_stats, seed, max_misses=0):
    passed = count - (failures + misses)
    passed_ps = fail_ps = cell4misPct = cell4misCnt = color = None
    count_color = fail_color = ""

    if (count < 1):
        count_color = " bgcolor=#FFA500" # orange

    if (failures == 0):
        fail_ps = "0.00%"
    else:
        percent = (failures/float(max(count, 1))) * 100
        fail_ps = str("{0:.2f}".format(percent)) + "%"
        if (percent > 50):
            fail_color = " bgcolor=#FFFF00" # yellow

    if (misses == 0):
        cell4misPct = "<td align=right>0.00%</td>"
        cell4misCnt = "<td align=right>0</td>"
    else:
        color = "#FF0000" # red
        if misses <= max_misses:
            color = "#FFA500" # orange
        mis_ps = "{0:.2f}".format((misses/float(max(count, 1))) * 100)
        cell4misPct = "<td align=right bgcolor=" + color + ">" + mis_ps + "%</td>"
        cell4misCnt = "<td align=right bgcolor=" + color + ">" + str(misses) + "</td>"
    misRow = cell4misCnt + cell4misPct

    if (voltdb_npes > 0):
        color = "#FF0000" # red
        voltNpeRow = "<td align=right bgcolor=" + color + ">" + str(voltdb_npes) + "</td>"
    else:
        voltNpeRow = "<td align=right>0</td>"
    if (cmpdb_npes > 0):
        color = "#FFA500" # orange
        cmpNpeRow = "<td align=right bgcolor=" + color + ">" + str(cmpdb_npes) + "</td>"
    else:
        cmpNpeRow = "<td align=right>0</td>"

    if (passed == count and passed > 0):
        passed_ps = "100.00%"
    else:
        passed_ps = str("{0:.2f}".format((passed/float(max(count, 1))) * 100)) + "%"
    stats = """
<td align=right>%d</td>
<td align=right>%s</td>
<td align=right>%d</td>
<td align=right%s>%s</td>
<td align=right%s>%d</td>
%s%s%s%s</tr>
""" % (passed, passed_ps, failures, fail_color, fail_ps, count_color, count, misRow, voltNpeRow, cmpNpeRow, extra_stats)

    return stats

def generate_summary(output_dir, statistics, cmpdb='HSqlDB'):
    fd = open(os.path.join(output_dir, "index.html"), "w")
    topLines = getTopSummaryLines(cmpdb)
    content = """
<html>
<head>
<title>SQL Coverage Test Summary</title>
<style>
h2 {text-transform: uppercase}
</style>
</head>

<body>
<h2>SQL Coverage Test Summary Grouped By Suites:</h2>
<h3>Random Seed: %d</h3>
<table border=1>
%s
""" % (statistics["seed"], topLines)

    def bullets(name, stats):
        return "<tr><td><a href=\"%s/index.html\">%s</a></td>%s</tr>" % \
            (name, name, stats)

    for suiteName in sorted(statistics.iterkeys()):
        if(suiteName != "seed" and suiteName != "totals"):
            content += bullets(suiteName, statistics[suiteName])
    content += "<tr><td>Totals</td>%s</tr>\n</table>" % statistics["totals"]
    content += """
<table border=0><tr><td>Key:</td></tr>
<tr><td align=right bgcolor=#FF0000>Red</td><td>table elements indicate a test failure(s), due to a mismatch between VoltDB and %s results, a crash,
                                                   or an NPE in VoltDB (or, an <i>extremely</i> slow test suite).</td></tr>
<tr><td align=right bgcolor=#FFA500>Orange</td><td>table elements indicate a strong warning, for something that should be looked into (e.g. a pattern
                                                   that generated no SQL queries, an NPE in %s, or a <i>very</i> slow test suite), but no test failures
                                                   (or only "known" failures).</td></tr>
<tr><td align=right bgcolor=#FFFF00>Yellow</td><td>table elements indicate a mild warning, for something you might want to improve (e.g. a pattern
                                                   that generated a very large number of SQL queries, or a somewhat slow test suite).</td></tr>
<tr><td align=right bgcolor=#D3D3D3>Gray</td><td>table elements indicate data that was not computed, due to a crash.</td></tr>
<tr><td colspan=2>NPE's(V): number of NullPointerExceptions while running against VoltDB.</td></tr>
<tr><td colspan=2>NPE's(%s): number of NullPointerExceptions while running against %s (likely in VoltDB's %s backend code).</td></tr>
</table>
</body>
</html>
""" % (cmpdb, cmpdb, cmpdb[:1], cmpdb, cmpdb)

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
