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

import decimal
import math
import re
import types

from SQLCoverageReport import generate_html_reports

# A compare function which can handle datetime to None comparisons
# -- where the standard cmp gets a TypeError.
def safecmp(x, y):
    # iterate over lists -- just like cmp does,
    # but compare in a way that avoids a TypeError
    # when a None value and datetime are corresponding members of the list.
    for (xn, yn) in zip(x, y):
        if xn is None:
            if yn is None:
                continue
            return -1
        if yn is None:
            return 1
        rn = cmp(xn, yn)
        if rn:
            return rn  # return first difference
    # With all elements the same, return 0
    # unless one list is longer, but even that is not an expected local
    # use case
    return cmp(len(x), len(y))

# lame, but it matches at least up to 6 ORDER BY columns
__EXPR = re.compile(r"ORDER BY\s(\w+\.(?P<column_1>\w+)(\s+\w+)?)"
                    r"(,\s+\w+\.(?P<column_2>\w+)(\s+\w+)?)?"
                    r"(,\s+\w+\.(?P<column_3>\w+)(\s+\w+)?)?"
                    r"(,\s+\w+\.(?P<column_4>\w+)(\s+\w+)?)?"
                    r"(,\s+\w+\.(?P<column_5>\w+)(\s+\w+)?)?"
                    r"(,\s+\w+\.(?P<column_6>\w+)(\s+\w+)?)?")

VOLTTYPE_NULL = 1
VOLTTYPE_TINYINT = 3  # int8
VOLTTYPE_SMALLINT = 4 # int16
VOLTTYPE_INTEGER = 5  # int32
VOLTTYPE_BIGINT = 6   # int64
VOLTTYPE_FLOAT = 8    # float64
VOLTTYPE_STRING = 9
VOLTTYPE_TIMESTAMP = 11 # 8 byte long
VOLTTYPE_MONEY = 20     # 8 byte long
VOLTTYPE_DECIMAL = 22  # 9 byte long

__NULL = {VOLTTYPE_TINYINT: -128,
          VOLTTYPE_SMALLINT: -32768,
          VOLTTYPE_INTEGER: -2147483648,
          VOLTTYPE_BIGINT: -9223372036854775808,
          VOLTTYPE_FLOAT: -1.7E+308}

RELIABLE_DECIMAL_DIGITS = 12

def normalize_value(v, vtype):
    global __NULL
    if v == None:
        return v
    if vtype in __NULL and v == __NULL[vtype]:
        return None
    elif vtype == VOLTTYPE_BIGINT:
        # Adapt (with rounding?) to the way HSQL returns some BIGINT results as floats
        ### print "DEBUG in fuzzynormalizer's normalize_value, v was " , v , " of type " , type(v)
        v = float(v)
        ### print "DEBUG in fuzzynormalizer's normalize_value, v now " , v
        vtype = VOLTTYPE_FLOAT
    elif vtype == VOLTTYPE_DECIMAL:
        # Adapt (with rounding?) to the way HSQL returns some DECIMAL results
        ### print "DEBUG in fuzzynormalizer's normalize_value, v was " , v , " of type " , type(v)
        v = float(str(v))
        ### print "DEBUG in fuzzynormalizer's normalize_value, v now " , v
        vtype = VOLTTYPE_FLOAT
    if vtype == VOLTTYPE_FLOAT:
        if v != 0.0:
            # round to the desired number of decimal places -- including any digits before the decimal
            decimal_places = RELIABLE_DECIMAL_DIGITS - 1 - int(math.floor(math.log10(abs(v))))
            ### print "DEBUG in fuzzynormalizer's normalize_value, v finally " , round(v, decimal_places) , \
            ###        " of type " , type(round(v, decimal_places))
            return round(v, decimal_places)
    return v

def normalize_values(tuples, columns):
    # 'c' here is a voltdbclient.VoltColumn and
    # I assume t is a voltdbclient.VoltTable.
    if hasattr(tuples, "__iter__"):
        for i in xrange(len(tuples)):
            if hasattr(tuples[i], "__iter__"):
                normalize_values(tuples[i], columns)
            else:
                tuples[i] = normalize_value(tuples[i], columns[i].type)

def project_sorted(row, sorted_cols):
    """Extract the values in the ORDER BY columns from a row.
    """
    return [ row[col] for col in sorted_cols ]

def project_unsorted(row, sorted_cols):
    """Extract the values in the non-ORDERBY columns from a row.
    """
    return [ row[col] for col in xrange(len(row)) if col not in sorted_cols ]

def sort(rows, sorted_cols):
    """Two steps:
        1. find the subset of rows which have the same values in the ORDER BY
        columns.
        2. sort them on the rest of the columns.
    """
    if not sorted_cols:
        rows.sort(cmp=safecmp)
        return

    begin = 0
    prev = None
    unsorteds = lambda row: project_unsorted(row, sorted_cols)

    for i in xrange(len(rows)):
        tmp = project_sorted(rows[i], sorted_cols)
        if prev != tmp:
            if prev:
                # sort a complete "group" with matching ORDER BY values
                rows[begin:i] = sorted(rows[begin:i], cmp=safecmp, key=unsorteds)
            prev = tmp
            begin = i

    # sort final "group"
    rows[begin:] = sorted(rows[begin:], cmp=safecmp, key=unsorteds)

def parse_sql(x):
    """Finds if the SQL statement contains ORDER BY command.
    """

    global __EXPR

    result = __EXPR.search(x)
    if result:
        return filter(lambda x: x, result.groupdict().values())
    else:
        return None

def normalize(table, sql):
    """Normalizes the result tuples of ORDER BY statements.
    """
    normalize_values(table.tuples, table.columns)

    sort_cols = parse_sql(sql)
    indices = []
    if sort_cols:
        for i in xrange(len(table.columns)):
            if table.columns[i].name in sort_cols:
                indices.append(i)

    # Make sure if there is an ORDER BY clause, the order by columns appear in
    # the result table. Otherwise all the columns will be sorted by the
    # normalizer.
    sort(table.tuples, indices)

    return table

def compare_results(suite, seed, statements_path, hsql_path, jni_path,
                    output_dir, report_all, extra_stats, comparison_database):
    return generate_html_reports(suite, seed, statements_path, hsql_path, jni_path,
                                 output_dir, report_all, extra_stats, comparison_database)
