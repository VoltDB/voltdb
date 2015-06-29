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

from SortNulls import SortNulls
from SQLCoverageReport import generate_html_reports
from voltdbclient import FastSerializer

# lame, but it matches at least up to 6 ORDER BY columns
__EXPR = re.compile(r"ORDER BY\s+((\w+\.)?(?P<column_1>\w+)(\s+\w+)?)"
                    r"((\s+)?,\s+(\w+\.)?(?P<column_2>\w+)(\s+\w+)?)?"
                    r"((\s+)?,\s+(\w+\.)?(?P<column_3>\w+)(\s+\w+)?)?"
                    r"((\s+)?,\s+(\w+\.)?(?P<column_4>\w+)(\s+\w+)?)?"
                    r"((\s+)?,\s+(\w+\.)?(?P<column_5>\w+)(\s+\w+)?)?"
                    r"((\s+)?,\s+(\w+\.)?(?P<column_6>\w+)(\s+\w+)?)?")

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
    if v is None:
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

def move_rows(rows, start, end, moveto):
    """Given a list of rows, moves the rows beginning with index 'start'
       (inclusive) and ending with index 'end' (exclusive) to a position
       just before index 'moveto', and returns the result.
    """
    if start < 0 or end < 0 or start > len(rows) or end > len(rows) or start > end:
        raise ValueError('Illegal value of start (%d) or end (%d): negative, greater than len(rows) (%d), or start > end' % (start, end, len(rows)))
    elif moveto < 0 or (moveto > start and moveto < end) or moveto > len(rows):
        raise ValueError('Illegal value of moveto (%d): negative, greater than len(rows) (%d), or between start (%d) and end (%d)' % (moveto, len(rows), start, end))
    elif moveto == start or moveto == end:
        return

    newrows = []
    newrows.extend(rows[0:min(moveto, start)])
    if moveto < start:
        newrows.extend(rows[start:end])
        newrows.extend(rows[moveto:start])
    elif moveto > end:
        newrows.extend(rows[end:moveto])
        newrows.extend(rows[start:end])
    newrows.extend(rows[max(moveto, end):len(rows)])
    rows[0:len(newrows)] = newrows[0:len(newrows)]

def sort(rows, sorted_cols, desc, sort_nulls=SortNulls.never):
    """Three steps:
        1. Find the subset of rows which have the same values in the ORDER BY
        columns.
        2. Sort them on the rest of the columns.
        3. Optionally, re-sort SQL NULL (Python None) values in the ORDER BY
        columns to be first or last (or first when ASC, last when DESC, or
        vice versa), as specified.
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
                # Sort a complete "group", with matching ORDER BY column values
                rows[begin:i] = sorted(rows[begin:i], cmp=safecmp, key=unsorteds)
            prev = tmp
            begin = i

    # Sort the final "group" (of rows with matching ORDER BY column values)
    rows[begin:] = sorted(rows[begin:], cmp=safecmp, key=unsorteds)

    # Sort SQL NULL (Python None) values, in ORDER BY columns, in the
    # specified order (if any)
    if sort_nulls != SortNulls.never:
        for i in xrange(len(sorted_cols)):
            begin = 0
            prev = None
            for j in xrange(len(rows) + 1):
                tmp = []
                if j < len(rows):
                    tmp = project_sorted(rows[j], sorted_cols[0:i + 1])
                if prev != tmp:
                    if prev and prev[i] is None:
                        if (sort_nulls == SortNulls.first or
                              (sort_nulls == SortNulls.highest and desc[i]) or
                              (sort_nulls == SortNulls.lowest  and not desc[i])):
                            if i == 0:  # first ORDER BY column, so don't need to check others
                                move_rows(rows, begin, j, 0)
                                break
                            ref = project_sorted(rows[begin], sorted_cols[0:i])
                            for k in range(begin - 1, -2, -1):
                                if (k < 0 or ref != project_sorted(rows[k], sorted_cols[0:i])):
                                    move_rows(rows, begin, j, k + 1)
                                    break
                        elif (sort_nulls == SortNulls.last or
                              (sort_nulls == SortNulls.lowest  and desc[i]) or
                              (sort_nulls == SortNulls.highest and not desc[i])):
                            if i == 0:  # first ORDER BY column, so don't need to check others
                                move_rows(rows, begin, j, len(rows))
                                break
                            ref = project_sorted(rows[begin], sorted_cols[0:i])
                            for k in range(j, len(rows) + 1):
                                if (k >= len(rows) or ref != project_sorted(rows[k], sorted_cols[0:i])):
                                    move_rows(rows, begin, j, k)
                                    break
                    prev = tmp
                    begin = j

def parse_sql(x):
    """Finds if the SQL statement contains an ORDER BY command, and returns
       the names of the ORDER BY columns.
    """

    global __EXPR

    result = __EXPR.search(x)
    if result:
        # returns the ORDER BY column names, in the order used in that clause
        getcol = lambda i: result.groupdict()['column_' + str(i)]
        return map(lambda i: getcol(i), filter(lambda i: getcol(i), range(1, 7)))
    else:
        return None

def parse_for_order_by_desc(sql, col_name):
    """Checks whether the SQL statement contains an ORDER BY clause for the
       specified column name, followed by DESC (meaning descending order);
       returns True if so, otherwise False.
    """
    desc_expr = re.compile(r"ORDER BY([\s\w.]*,)*\s+(\w+\.)?" + col_name.upper() + "\s+DESC")
    result = desc_expr.search(sql)
    if result:
        return True
    else:
        return False

class StandardNormalizer:
    """This class contains a standard normalizer ('normalize' static method)
       that normalizes the result tuples of ORDER BY statements, sorting SQL
       NULL (Python None) values in the ORDER BY columns in the specified
       manner; and a standard comparison ('safecmp' static method) that
       compares values (including lists, None values and datetimes), to see
       if they are equal. By making this a class, it can be easily called
       from other modules; it could also serve as the base class for other
       normalizers.
    """

    # A compare function which can handle datetime to None comparisons
    # -- where the standard cmp gets a TypeError.
    @staticmethod
    def safecmp(x, y):
        """A safe comparison (static) method, which performs a comparison
           similar to cmp, including iterating over lists, but two None values
           are considered equal, and a TypeError is avoided when a None value
           and a datetime are corresponding members of a list. Like cmp,
           returns 0 if the two objects are equal, negative if the first
           object is less, positive if it is greater.
        """
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
        # With all elements the same, return 0 unless one list is longer,
        # though that is not an expected local use case
        return cmp(len(x), len(y))

    @staticmethod
    def normalize(table, sql, sort_nulls=SortNulls.never):
        """Normalizes the result tuples of ORDER BY statements, sorting SQL
           NULL (Python None) values in the ORDER BY columns in the specified
           manner (by default, not at all).
        """
        normalize_values(table.tuples, table.columns)

        sql_upper = sql.upper()
        # Ignore ORDER BY clause only in a sub-query
        last_paren_index = sql_upper.rfind(')')  # will often be -1
        sort_cols = parse_sql(sql_upper[last_paren_index+1:])
        indices = []
        desc = []
        if sort_cols:
            # gets the ORDER BY column indices, in the order used in that clause
            for i in xrange(len(sort_cols)):
                # Find the table column name that matches the ORDER BY column name
                for j in xrange(len(table.columns)):
                    if sort_cols[i] == table.columns[j].name:
                        indices.append(j)
                        desc.append(parse_for_order_by_desc(sql_upper[last_paren_index+1:], sort_cols[i]))
                        break

        # Make sure if there is an ORDER BY clause, the order by columns appear in
        # the result table. Otherwise all the columns will be sorted by the
        # normalizer.
        sort(table.tuples, indices, desc, sort_nulls)

        return table

def safecmp(x, y):
    """Simply calls StandardNormalizer.safecmp(x, y).
    """
    return StandardNormalizer.safecmp(x, y)
