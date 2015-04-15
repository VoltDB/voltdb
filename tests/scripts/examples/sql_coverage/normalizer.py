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

import sys
# For spot testing, add the path to SQLCoverageReport, just based on knowing
# where we are now
sys.path.append('../../')
sys.path.append('../../../../lib/python')

import decimal
import math
import re
import types

from voltdbclient import FastSerializer

from SQLCoverageReport import generate_html_reports

# A compare function which can handle datetime to None comparisons
# -- where the standard cmp gets a TypeError.
def safecmp(x, y):
    # It's easiest to bypass the datetime to None comparison by
    # explicitly filtering out None arguments,
    # even though cmp can handle MOST of these cases
    # and will only get a TypeError if the other value is a datetime.
    if x is None:
        if y is None:
            return 0
        return -1
    if y is None:
        return 1
    # recurse on lists -- just like cmp does,
    # again, to avoid a TypeError,
    # this is for the most common local use case,
    # when the None value and datetime are members of lists.
    # cmp also recurses into Tuples, but, so far, that is not required, locally.
    # cmp also supports comparison of lists with scalars, treating the scalar
    # as a singleton list, but, so far, that is not required, either.
    if isinstance(x, types.ListType) and isinstance(y, types.ListType):
        for (xn, yn) in zip(x, y):
            rn = safecmp(xn, yn)
            if rn:
                return rn  # return first difference
        # With all elements the same, return 0
        # unless one list is longer, but even that is not an expected local
        # use case
        return cmp(len(x), len(y))
    return cmp(x, y)

# lame, but it matches at least up to 6 ORDER BY columns
__EXPR = re.compile(r"ORDER BY\s(\w+\.(?P<column_1>\w+)(\s+\w+)?)"
                    r"(,\s+\w+\.(?P<column_2>\w+)(\s+\w+)?)?"
                    r"(,\s+\w+\.(?P<column_3>\w+)(\s+\w+)?)?"
                    r"(,\s+\w+\.(?P<column_4>\w+)(\s+\w+)?)?"
                    r"(,\s+\w+\.(?P<column_5>\w+)(\s+\w+)?)?"
                    r"(,\s+\w+\.(?P<column_6>\w+)(\s+\w+)?)?")

# This appears to be a weak knock-off of FastSerializer.NullCheck
# TODO: There's probably a way to use the actual FastSerializer.NullCheck
__NULL = {FastSerializer.VOLTTYPE_TINYINT: FastSerializer.NULL_TINYINT_INDICATOR,
          FastSerializer.VOLTTYPE_SMALLINT: FastSerializer.NULL_SMALLINT_INDICATOR,
          FastSerializer.VOLTTYPE_INTEGER: FastSerializer.NULL_INTEGER_INDICATOR,
          FastSerializer.VOLTTYPE_BIGINT: FastSerializer.NULL_BIGINT_INDICATOR,
          FastSerializer.VOLTTYPE_FLOAT: FastSerializer.NULL_FLOAT_INDICATOR,
          FastSerializer.VOLTTYPE_STRING: FastSerializer.NULL_STRING_INDICATOR}

SIGNIFICANT_DIGITS = 12

def normalize_value(v, vtype):
    global __NULL
    if not v:
        return None
    if vtype in __NULL and v == __NULL[vtype]:
        return None
    elif vtype == FastSerializer.VOLTTYPE_FLOAT:
        # round to the desired number of decimal places -- accounting for significant digits before the decimal
        decimal_places = SIGNIFICANT_DIGITS
        abs_v = abs(float(v))
        if abs_v >= 1.0:
            # round to the total number of significant digits, including the integer part
            decimal_places = SIGNIFICANT_DIGITS - 1 - int(math.floor(math.log10(abs_v)))
        # print "DEBUG normalized float:(", round(v, decimal_places), ")"
        return round(v, decimal_places)
    elif vtype == FastSerializer.VOLTTYPE_DECIMAL:
        # print "DEBUG normalized_to:(", decimal.Decimal(v)._rescale(-12, "ROUND_HALF_EVEN"), ")"
        return decimal.Decimal(v)._rescale(-12, "ROUND_HALF_EVEN")
    else:
        # print "DEBUG normalized pass-through:(", v, ")"
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
    unsorteds = lambda x: project_unsorted(sorted_cols, x)

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

def compare_results(suite, seed, statements_path, hsql_path, jni_path, output_dir, report_all):
    return generate_html_reports(suite, seed, statements_path, hsql_path,
            jni_path, output_dir, report_all)
