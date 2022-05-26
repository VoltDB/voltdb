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

import sys
import cgi
import codecs
import cPickle
import decimal
import os
import traceback
from datetime import datetime
from distutils.util import strtobool
from optparse import OptionParser
from os.path import basename, isfile
from shutil import copyfile
from time import mktime
from voltdbclientpy2 import VoltColumn, VoltTable, FastSerializer

__quiet = True

# One way of representing Enums in earlier versions of Python,
# per Stack Overflow
def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    reverse = dict((value, key) for key, value in enums.iteritems())
    enums['reverse_mapping'] = reverse
    return type('Enum', (), enums)

# Options for which SQL statement types to include when generating Steps to
# Reproduce a failure (or crash, exception, etc.)
Reproduce = enum('NONE', 'DDL', 'DML', 'CTE', 'ALL')

# Define which SQL statement types are included in each of the above options
ReproduceStatementTypes = {}
ReproduceStatementTypes[Reproduce.NONE] = []
ReproduceStatementTypes[Reproduce.DDL]  = ['CREATE', 'DROP', 'ALTER', 'PARTITION', 'DR ']
ReproduceStatementTypes[Reproduce.DML]  = ['INSERT', 'UPSERT', 'UPDATE', 'DELETE', 'TRUNCATE']
ReproduceStatementTypes[Reproduce.DML].extend(ReproduceStatementTypes[Reproduce.DDL])
ReproduceStatementTypes[Reproduce.CTE]  = ['WITH']
ReproduceStatementTypes[Reproduce.CTE].extend(ReproduceStatementTypes[Reproduce.DML])
ReproduceStatementTypes[Reproduce.ALL]  = ['']    # any statement, including SELECT
### print "DEBUG: ReproduceStatementTypes:", str(ReproduceStatementTypes)

# Define which Exceptions (or other error messages) should be flagged as 'fatal'
# errors (meaning that SqlCoverage should fail); and which as 'non-fatal' errors
# (meaning that SqlCoverage should NOT fail)
FatalExceptionTypes = ['NullPointerException',
                       'IndexOutOfBoundsException',
                       'AssertionError',
                       'unexpected internal error occurred',
                       'Please contact VoltDB at support'
                       ]
# The first 4 of these are experimental; we have yet to see any of them:
NonfatalExceptionTypes = ['ClassCastException',
                          'SAXParseException',
                          'VoltTypeException',
                          'ERROR: IN: NodeSchema',
                          # This is sometimes returned by PostgreSQL:
                          'timeout: procedure call took longer than 5 seconds',
                          # This is sometimes returned by HSqlDB:
                          'unsupported internal operation'
                          ]
# Rejected idea (happens frequently in advanced-scalar-set-subquery):
#                        'VOLTDB ERROR: UNEXPECTED FAILURE',
# More rejected ideas (for now):
#                           'is already defined',
#                           'is not defined',
#                           'Could not parse reader',
#                           'unexpected token: EXEC',
#                           'Failed to plan for statement',
#                           'Resource limit exceeded',
#                           "Attribute 'enabled' is not allowed to appear in element 'export'",
#                           'PartitionInfo specifies invalid parameter index for procedure',
#                           'Unexpected error in the SQL parser for statement'
AllExceptionTypes = []
AllExceptionTypes.extend(FatalExceptionTypes)
AllExceptionTypes.extend(NonfatalExceptionTypes)

def highlight(s, flag):
    if not isinstance(s, basestring):
        s = str(s)
    return flag and '<span style="color:red">%s**</span>' % (s) or s

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

def generate_ddl(output_dir, ddl_path):
    filename = basename(ddl_path) + '.html'
    local_ddl_path = output_dir + '/' + filename
    if not isfile(local_ddl_path):
        ### print 'DEBUG: copying %s to %s' % (ddl_path, local_ddl_path)
        read_ddl_file = open(ddl_path, 'rb')
        copy_ddl_file = open(local_ddl_path, 'w')
        copy_ddl_file.write('<html>\n<head>\n<title>DDL File</title>\n</head>\n<body>\n<pre>\n')
        while True:
            line = read_ddl_file.readline()
            if not line:
                break
            copy_ddl_file.write(line)
        read_ddl_file.close()
        copy_ddl_file.write('</pre>\n</body>\n</html>')
        copy_ddl_file.close()
    return '<p>First, run: <a href="%s">%s</a></p>' % (filename, 'the DDL file')

def generate_reproducer(item, output_dir, reproducer, ddl_file):
    result = ''
    if item.get("reproducer"):
        reproducer_html = """
<html>
<head>
<title>Reproduce Query #%s</title>
<style>
td {width: 50%%}
</style>
</head>
<body>
<h2>Steps to Reproduce Query #%s</h2>
<p>%s</p>
<p>Then, run the following (includes %s statements):</p>
<p>%s</p>
</body>
</html>
""" % (item["id"], item["id"],
       generate_ddl(output_dir, ddl_file),
       Reproduce.reverse_mapping[reproducer],
       item["reproducer"] )

        filename = "reproduce%s.html" % (item["id"])
        fd = open(os.path.join(output_dir, filename), "w")
        fd.write(reproducer_html.encode("utf-8"))
        fd.close()
        result = '<p><a href="%s">%s</a></p>' % (filename, 'For Steps to Reproduce, Click Here')
    return result

def generate_modified_query(cmpdb, sql, modified_sql):
    result = ''
    mod_sql = modified_sql.get(sql.rstrip(';'), None)
    if mod_sql:
        result = '<p>Modified SQL query, as sent to ' + str(cmpdb) + ':</p><h2>' + str(mod_sql) + '</h2>'
    return result

def generate_detail(name, item, output_dir, cmpdb, modified_sql,
                    reproducer, ddl_file, create_detail_files=True):
    if output_dir == None:
        return

    filename = "%s.html" % (item["id"])
    if not create_detail_files:
        return filename

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
       generate_reproducer(item, output_dir, reproducer, ddl_file),
       cmpdb,
       highlight(item["jni"]["Status"], "Status" == item.get("highlight")),
       highlight(item["cmp"]["Status"], "Status" == item.get("highlight")),
       item["jni"].get("Info") or "",
       item["cmp"].get("Info") or "",
       generate_table_str(item, "jni"),
       generate_table_str(item, "cmp") )

    fd = open(os.path.join(output_dir, filename), "w")
    fd.write(details.encode("utf-8"))
    fd.close()

    return filename

def safe_print(s):
    if not __quiet:
        print s

def print_section(name, mismatches, output_dir, cmpdb, modified_sql,
                  reproducer, ddl_file, max_num_detail_files=-1):
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

    max_num_detail_files_int = int(max_num_detail_files)
    # print "DEBUG: In SQLCoverageReport.py, print_section:"
    # print "DEBUG:   max_num_detail_files    :", str(max_num_detail_files),     str(type(max_num_detail_files))
    # print "DEBUG:   max_num_detail_files_int:", str(max_num_detail_files_int), str(type(max_num_detail_files_int))
    # print "DEBUG:   len(mismatches)         :", str(len(mismatches))

    if max_num_detail_files_int < 0 or len(mismatches) <= max_num_detail_files_int:
        create_detail_files = mismatches
    else:
        n = max_num_detail_files_int / 2.0
        # create only the first n and the last n detail files (for a total of
        # max_num_detail_files, with the extra in the first group if it's odd)
        create_detail_files = []
        for j in range(0, int(round(n+0.1))):
            create_detail_files.append(mismatches[j])
        for j in range(len(mismatches) - int(round(n-0.1)), len(mismatches)):
            create_detail_files.append(mismatches[j])

    # print "DEBUG:   len(create_detail_files):", str(len(create_detail_files))

    temp = []
    for i in mismatches:
        safe_print(i["SQL"])
        detail_page = generate_detail(name, i, output_dir, cmpdb, modified_sql,
                                      reproducer, ddl_file, (i in create_detail_files) )
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

def time_diff_close_enough(time1, time2, within_minutes):
    """Test whether two datetimes (TIMESTAMP's) are:
    1. within a specified number of minutes of each other; and
    2. within a specified number (the same number) of minutes of right now.
    If both are true, then they are deemed to be "close enough", on the
    assumption that they were each set to NOW() or CURRENT_TIMESTAMP(), and
    the difference is because VoltDB and its comparison database (HSQL or
    PostgreSQL) called that function at slightly different times.
    """
    time_diff_in_minutes = (time1 - time2).total_seconds() / 60
    if abs(time_diff_in_minutes) > within_minutes:
        return False
    time_diff_in_minutes = (time2 - datetime.now()).total_seconds() / 60
    if abs(time_diff_in_minutes) > within_minutes:
        return False
    return True

def is_different(x, cntonly, within_minutes):
    """Notes the attributes that are different. Since the whole table will be
    printed out as a single string.
    the first line is column count,
    the second line is row count,
    the third line is column names and types,
    followed by rows.
    """

    jni = x["jni"]
    cmp = x["cmp"]
    # JNI returns a variety of negative error result values that we can't
    # easily match with the HSqlDB (or PostgreSQL) backend.  Reject only pairs
    # of status values where one of them wasn't an error.
    if jni["Status"] != cmp["Status"]:
        if int(jni["Status"]) > 0 or int(cmp["Status"]) > 0:
            x["highlight"] = "Status"
            # HSqlDB now sometimes returns an error where VoltDB does not:
            # in that case, do not fail the test.  This will be caught later
            # as a 'cmpdb_excep' - an exception in the comparison DB - and
            # reported as such in the 'SQL Coverage Test Summary', but without
            # a test failure (see ENG-19845 & ENG-19702)
            if (int(jni["Status"]) > 0 and int(cmp["Status"]) < 0 and
                    any(nfet in str(cmp) for nfet in NonfatalExceptionTypes)):
                return False
            # print "DEBUG is_different -- one error (0 or less)"
            return True
        # print "DEBUG is_different -- just different errors (0 or less)"
        return False
    if int(jni["Status"]) <= 0:
        # print "DEBUG is_different -- same error (0 or less)"
        return False

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
        return True
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
            if (within_minutes and jniColumns[jj].type == FastSerializer.VOLTTYPE_TIMESTAMP and
                    time_diff_close_enough(jniTuples[ii][jj], cmpTuples[ii][jj], within_minutes)):
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

def get_reproducer(reproduce_results, sql):
    result = ";<br>\n".join(reproduce_results)
    if reproduce_results:
        result += ";<br>\n"
    if not reproduce_results or sql != reproduce_results[len(reproduce_results)-1]:
        result += sql + ";\n"
    return result

def generate_html_reports(suite, seed, statements_path, cmpdb_path, jni_path,
                          output_dir, report_invalid, report_all, extra_stats='',
                          cmpdb='HSqlDB', modified_sql_path=None,
                          max_expected_mismatches=0, max_detail_pages=-1,
                          within_minutes=0, reproducer=Reproduce.NONE, ddl_file=None,
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
    volt_fatal_excep = []
    volt_nonfatal_excep = []
    cmpdb_excep = []
    invalid     = []
    all_results = []
    reproduce_results = []
    volt_fatal_excep_types    = set([])
    volt_nonfatal_excep_types = set([])
    cmpdb_excep_types         = set([])

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

        cdb_info = ''
        while True:
            try:
                cdb = cPickle.load(cmpdb_file)
            except EOFError as e:
                notFound = True
                cdb = {'Status': -98, 'Exception': 'None', 'Result': None,
                       'Info': '<p style="color:red">RESULT NOT FOUND! Probably due to a ' + cmpdb + ' backend crash!</p>'}

            if 'timeout: procedure call took longer than 5 seconds' in (cdb.get('Info') or ''):
                if cdb_info:
                    cdb_info += ';\n' + cdb.get('Info')
                else:
                    cdb_info = cdb.get('Info')
                print "WARNING: found %s error '%s'; ignoring Status '%s'" % (cmpdb, cdb.get('Info'), cdb.get('Status'))
            else:
                if cdb_info:
                    if cdb.get('Info'):
                        cdb['Info'] = cdb_info + ';\n' + cdb.get('Info')
                    else:
                        cdb['Info'] = cdb_info
                break

        # VoltDB usually produces this error message, for the first couple
        # of queries after a crash, before results disappear completely
        if ('Connection broken' in (jni.get('Info') or '') or
            'Connection broken' in (cdb.get('Info') or '') ):
            notFound = True

        count += 1
        if int(jni["Status"]) != 1:
            failures += 1
            if report_invalid:
                invalid.append(statement)

        statement["jni"] = jni
        statement["cmp"] = cdb

        if reproducer and (
            any(str(statement["SQL"].lstrip()[:12]).upper().startswith(statementType)
                for statementType in ReproduceStatementTypes[reproducer]) ):
            reproduce_results.append(statement["SQL"])

        if notFound:
            if reproducer:
                statement["reproducer"] = get_reproducer(reproduce_results, statement["SQL"])
                # No point in adding additional steps to reproduce, after a crash
                reproducer = Reproduce.NONE
            crashed.append(statement)
        elif is_different(statement, cntonly, within_minutes):
            if reproducer:
                statement["reproducer"] = get_reproducer(reproduce_results, statement["SQL"])
            mismatches.append(statement)

        if any(exceptionType in str(jni) for exceptionType in FatalExceptionTypes):
            if reproducer:
                statement["reproducer"] = get_reproducer(reproduce_results, statement["SQL"])
            volt_fatal_excep.append(statement)
            [volt_fatal_excep_types.add(et) if et in str(jni) else None for et in FatalExceptionTypes]
        if any(exceptionType in str(jni) for exceptionType in NonfatalExceptionTypes):
            if reproducer:
                statement["reproducer"] = get_reproducer(reproduce_results, statement["SQL"])
            volt_nonfatal_excep.append(statement)
            [volt_nonfatal_excep_types.add(et) if et in str(jni) else None for et in NonfatalExceptionTypes]
        if any(exceptionType in str(cdb) for exceptionType in AllExceptionTypes):
            if reproducer:
                statement["reproducer"] = get_reproducer(reproduce_results, statement["SQL"])
            cmpdb_excep.append(statement)
            [cmpdb_excep_types.add(et) if et in str(cdb) else None for et in AllExceptionTypes]

        if report_all:
            all_results.append(statement)

    statements_file.close()
    cmpdb_file.close()
    jni_file.close()

    # Print the Exception/Error types that were found, if any
    if cmpdb_excep_types:
        print "All Exception/Error types found in %s, for test suite '%s':" % (cmpdb, suite)
        print '\n'.join(['    %s' % et for et in cmpdb_excep_types])
    if volt_nonfatal_excep_types:
        print "'Non-fatal' Exception/Error types found in VoltDB, for test suite '%s':" % suite
        print '\n'.join(['    %s' % et for et in volt_nonfatal_excep_types])
    if volt_fatal_excep_types:
        print "'Fatal' Exception/Error types found in VoltDB, for test suite '%s':" % suite
        print '\n'.join(['    %s' % et for et in volt_fatal_excep_types])

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
    currentTime = datetime.now().strftime("%A, %B %d, %I:%M:%S %p")
    keyStats = createSummaryInHTML(count, failures, len(mismatches), len(volt_fatal_excep), len(volt_nonfatal_excep),
                                   len(cmpdb_excep), extra_stats, seed, max_expected_mismatches)
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
        report += print_section("Mismatched Statements", mismatches,
                                output_dir, cmpdb, modified_sql, reproducer, ddl_file, max_detail_pages)

    if(len(crashed) > 0):
        sorted(crashed, cmp=cmp, key=key)
        report += print_section("Statements Missing Results, due to a Crash<br>(the first one probably caused the crash)", crashed,
                                output_dir, cmpdb, modified_sql, reproducer, ddl_file, max_detail_pages)

    if(len(volt_fatal_excep) > 0):
        sorted(volt_fatal_excep, cmp=cmp, key=key)
        report += print_section("Statements That Cause a 'Fatal' Exception in VoltDB", volt_fatal_excep,
                                output_dir, cmpdb, modified_sql, reproducer, ddl_file, max_detail_pages)

    if(len(volt_nonfatal_excep) > 0):
        sorted(volt_nonfatal_excep, cmp=cmp, key=key)
        report += print_section("Statements That Cause a 'Non-fatal' Exception in VoltDB", volt_nonfatal_excep,
                                output_dir, cmpdb, modified_sql, reproducer, ddl_file, max_detail_pages)

    if(len(cmpdb_excep) > 0):
        sorted(cmpdb_excep, cmp=cmp, key=key)
        report += print_section("Statements That Cause (any) Exception in " + cmpdb, cmpdb_excep,
                                output_dir, cmpdb, modified_sql, reproducer, ddl_file, max_detail_pages)

    if report_invalid and (len(invalid) > 0):
        report += print_section("Invalid Statements", invalid,
                                output_dir, cmpdb, modified_sql, reproducer, ddl_file, max_detail_pages)

    if report_all:
        report += print_section("Total Statements", all_results,
                                output_dir, cmpdb, modified_sql, reproducer, ddl_file)

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
<td colspan=2 align=center>Test Failures</td>
<td colspan=3 align=center>Exceptions</td>
<td colspan=3 align=center>Crashes</td>
<td colspan=5 align=center>SQL Statements per Pattern</td>
<td colspan=5 align=center>Time (min:sec)</td>
</tr><tr>
<td>Valid</td><td>Valid %%</td>
<td>Invalid</td><td>Invalid %%</td>
<td>Total</td>
<td>Mismatched</td><td>Mismatched %%</td>
<td>VF</td><td>VN</td><td>%s</td>
<td>V</td><td>%s</td><td>C</td>
<td>Minimum</td><td>Maximum</td><td># Inserts</td><td># Patterns</td><td># Unresolved</td>
<td>Generating SQL</td><td>VoltDB</td><td>%s</td>
""" % (cmpdb[:1], cmpdb[:1], cmpdb)
    if includeAll:
        topLines += "<td>Comparing</td><td>Total</td>"
    topLines += "</tr>"
    return topLines

def createSummaryInHTML(count, failures, misses, volt_fatal_excep, volt_nonfatal_excep,
                        cmpdb_excep, extra_stats, seed, max_misses=0):
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

    if (volt_fatal_excep > 0):
        color = "#FF0000" # red
        voltFatalExcepRow = "<td align=right bgcolor=" + color + ">" + str(volt_fatal_excep) + "</td>"
    else:
        voltFatalExcepRow = "<td align=right>0</td>"
    if (volt_nonfatal_excep > 0):
        color = "#FFA500" # orange
        voltNonfatalExcepRow = "<td align=right bgcolor=" + color + ">" + str(volt_nonfatal_excep) + "</td>"
    else:
        voltNonfatalExcepRow = "<td align=right>0</td>"
    if (cmpdb_excep > 0):
        color = "#FFA500" # orange
        cmpExcepRow = "<td align=right bgcolor=" + color + ">" + str(cmpdb_excep) + "</td>"
    else:
        cmpExcepRow = "<td align=right>0</td>"

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
%s%s%s%s%s</tr>
""" % (passed, passed_ps, failures, fail_color, fail_ps, count_color, count,
       misRow, voltFatalExcepRow, voltNonfatalExcepRow, cmpExcepRow, extra_stats)

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
                                                   or a 'Fatal' Exception in VoltDB (or, an <i>extremely</i> slow test suite).</td></tr>
<tr><td align=right bgcolor=#FFA500>Orange</td><td>table elements indicate a strong warning, for something that should be looked into (e.g. a pattern
                                                   that generated no SQL queries, an Exception in %s, or a <i>very</i> slow test suite), but no test
                                                   failures (or only "known" failures).</td></tr>
<tr><td align=right bgcolor=#FFFF00>Yellow</td><td>table elements indicate a mild warning, for something you might want to improve (e.g. a pattern
                                                   that generated a very large number of SQL queries, or a somewhat slow test suite).</td></tr>
<tr><td align=right bgcolor=#D3D3D3>Gray</td><td>table elements indicate data that was not computed, due to a crash.</td></tr>
<tr><td colspan=2>Exceptions/VF: number of 'Fatal' Exceptions (those deemed worth failing the test, e.g., NullPointerException's) while running
                                                   against VoltDB.</td></tr>
<tr><td colspan=2>Exceptions/VN: number of 'Non-fatal' Exceptions (those deemed NOT worth failing the test, e.g., VoltTypeException's) while running
                                                   against VoltDB.</td></tr>
<tr><td colspan=2>Exceptions/%s: number of (any) Exceptions (or timeouts) while running against %s (likely in VoltDB's %s backend code).</td></tr>
<tr><td colspan=2>Crashes/V: number of VoltDB crashes.</td></tr>
<tr><td colspan=2>Crashes/%s: number of %s crashes.</td></tr>
<tr><td colspan=2>Crashes/C: number of crashes while comparing VoltDB and %s results.</td></tr>
</table>
</body>
</html>
""" % (cmpdb, cmpdb, cmpdb[:1], cmpdb, cmpdb, cmpdb[:1], cmpdb, cmpdb)

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
