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
# For spot testing, add the path to SQLCoverageReport, just based on knowing
# where we are now
sys.path.append('../../')
sys.path.append('../../../../lib/python')

import decimal
import math
import re
import types
import array
from binascii import hexlify
from NotANormalizer import NotANormalizer
from SortNulls import SortNulls
from SQLCoverageReport import generate_html_reports
from voltdbclientpy2 import FastSerializer

# lame, but it matches at least up to 6 ORDER BY columns
__EXPR = re.compile(r"ORDER BY\s+((\w+\.)?(?P<column_1>\w+)(\s+\w+)?)"
                    r"((\s+)?,\s+(\w+\.)?(?P<column_2>\w+)(\s+\w+)?)?"
                    r"((\s+)?,\s+(\w+\.)?(?P<column_3>\w+)(\s+\w+)?)?"
                    r"((\s+)?,\s+(\w+\.)?(?P<column_4>\w+)(\s+\w+)?)?"
                    r"((\s+)?,\s+(\w+\.)?(?P<column_5>\w+)(\s+\w+)?)?"
                    r"((\s+)?,\s+(\w+\.)?(?P<column_6>\w+)(\s+\w+)?)?")

# matches a string (VARCHAR) column representing a GEOGRAPHY_POINT (point),
# of roughly this form: 'POINT (-12.3456 -65.4321)', but the initial space
# between 'POINT' and '(', the minus signs, and the decimal points are optional
__NUMBER = '-?\d+\.?\d*'
__LONG_LAT_NUMS   =     __NUMBER+ '\s+' +__NUMBER
__LONG_LAT_GROUPS = '('+__NUMBER+')\s+('+__NUMBER+')'
__POINT = re.compile(r"^POINT\s?\("+__LONG_LAT_GROUPS+"\)$")

# matches a string (VARCHAR) column representing a GEOGRAPHY (polygon), of roughly this form:
# 'POLYGON ((-1.1 -1.1, -4.4 -1.1, -4.4 -4.4, -1.1 -4.4, -1.1 -1.1), (-2.2 -2.2, -3.3 -2.2, -2.2 -3.3, -2.2 -2.2))',
# but the initial space between 'POINT' and '(', the spaces following the commas, the minus
# signs, and the decimal points are all optional; and of course the number of loops (2 in this
# example), the number of vertices within each loop, the number of digits before or after the
# decimal point, and the actual numbers, can all vary.
__LOOP_GROUP = '(\('+__LONG_LAT_NUMS+'(?:,\s?'+__LONG_LAT_NUMS+')+\))'
__POLYGON = re.compile(r"^POLYGON\s?\("+__LOOP_GROUP+"(?:,\s?"+__LOOP_GROUP+")*\)$")
# used to match individual loops within a in a GEOGRAPHY (polygon), and vertices (longitude
# & latitude) within a loop
__LOOP = re.compile(r"\("+__LONG_LAT_NUMS+"(?:,\s?"+__LONG_LAT_NUMS+")+\)")
__LONG_LAT = re.compile(r"^\s?"+__LONG_LAT_GROUPS+"$")

# This appears to be a weak knock-off of FastSerializer.NullCheck
# TODO: There's probably a way to use the actual FastSerializer.NullCheck
__NULL = {FastSerializer.VOLTTYPE_TINYINT: FastSerializer.NULL_TINYINT_INDICATOR,
          FastSerializer.VOLTTYPE_SMALLINT: FastSerializer.NULL_SMALLINT_INDICATOR,
          FastSerializer.VOLTTYPE_INTEGER: FastSerializer.NULL_INTEGER_INDICATOR,
          FastSerializer.VOLTTYPE_BIGINT: FastSerializer.NULL_BIGINT_INDICATOR,
          FastSerializer.VOLTTYPE_FLOAT: FastSerializer.NULL_FLOAT_INDICATOR,
          FastSerializer.VOLTTYPE_STRING: FastSerializer.NULL_STRING_INDICATOR}

def round_to_num_digits(v, num_digits):
    # round to the specified total number of significant digits,
    # regardless of where the decimal point occurs
    f = float(v)
    if f == 0.0:
        return f
    decimal_places = num_digits - int(math.floor(math.log10(abs(f)))) - 1
    return round(f, decimal_places)

def normalize_value(v, vtype, num_digits):
    global __NULL
    if v is None:
        return None
    if vtype in __NULL and v == __NULL[vtype]:
        return None
    elif vtype == FastSerializer.VOLTTYPE_FLOAT:
        # round to the desired number of decimal places -- accounting for significant digits before the decimal
        return round_to_num_digits(v, num_digits)
    elif vtype == FastSerializer.VOLTTYPE_DECIMAL:
        # round to the desired number of decimal places -- accounting for significant digits before the decimal
        return round_to_num_digits(v, num_digits)
    elif vtype == FastSerializer.VOLTTYPE_STRING and v:
        # for strings (VARCHAR) representing a point or polygon (GEOGRAPHY_POINT
        # or GEOGRAPHY), round off the longitude and latitude values to the desired
        # number of significant digits (regardless of where the decimal point is);
        # but leave other strings alone
        r = v
        point = __POINT.search(v)
        if point:
            r = 'POINT (' + str(round_to_num_digits(point.group(1), num_digits)) + ' ' \
                          + str(round_to_num_digits(point.group(2), num_digits)) + ')'
        polygon = __POLYGON.search(v)
        if polygon:
            r = "POLYGON ("
            loops = re.findall(__LOOP, v)
            for i in range(len(loops)):
                loop = loops[i]
                if loop:
                    if i > 0:
                        r += ", "
                    r += '('
                    points = loop[1:-1].split(',')
                    for j in range(len(points)):
                        if j > 0:
                            r += ", "
                        longlat = __LONG_LAT.search(points[j])
                        r += str(round_to_num_digits(longlat.group(1), num_digits)) + ' ' \
                           + str(round_to_num_digits(longlat.group(2), num_digits))
                    r += ')'
            r += ')'
        #if r != v:
        #    print "DEBUG: normalizing:", v
        #    print "DEBUG: modified to:", r
        return r
    elif vtype == FastSerializer.VOLTTYPE_VARBINARY and v:
        # output VARBINARY columns in a format similar to that in which they
        # are inserted into VoltDB, e.g. x'abcd1234', and not in Python's crazy
        # default format, e.g. array('c', '\xab\xcd\x124'); and no, that is not
        # a typo: Python (without calling hexlify) actually changes '34' to '4'
        # because 34 is the (hex) ASCII value of the character '4'!
        return "x'" + hexlify(v)
    else:
        # print "DEBUG normalized pass-through:(", v, ")"
        return v

def normalize_values(tuples, columns, num_digits):
    # 'c' here is a voltdbclient.VoltColumn and
    # I assume t is a voltdbclient.VoltTable.
    if hasattr(tuples, "__iter__"):
        for i in xrange(len(tuples)):
            # varbinary is array.array type and has __iter__ defined, but should be considered
            # as a single value to be compared.
            if hasattr(tuples[i], "__iter__") and type(tuples[i]) is not array.array:
                normalize_values(tuples[i], columns, num_digits)
            else:
                tuples[i] = normalize_value(tuples[i], columns[i].type, num_digits)

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
       just before index 'moveto', and modifies the rows arg accordingly.
    """
    newrows = []
    if start < 0 or end < 0 or start > len(rows) or end > len(rows) or start > end:
        raise ValueError('Illegal value of start (%d) or end (%d): negative, greater than len(rows) (%d), or start > end' % (start, end, len(rows)))
    elif moveto < 0 or moveto > len(rows) or (moveto > start and moveto < end):
        raise ValueError('Illegal value of moveto (%d): negative, greater than len(rows) (%d), or between start (%d) and end (%d)' % (moveto, len(rows), start, end))
    elif moveto == start or moveto == end:
        return
    elif moveto < start:
        newrows.extend(rows[0:moveto])
        newrows.extend(rows[start:end])
        newrows.extend(rows[moveto:start])
        newrows.extend(rows[end:len(rows)])
    else:  # moveto > end: (only remaining possibility here)
        newrows.extend(rows[0:start])
        newrows.extend(rows[end:moveto])
        newrows.extend(rows[start:end])
        newrows.extend(rows[moveto:len(rows)])
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
        rows.sort(cmp=StandardNormalizer.safecmp)
        return

    #print "sort_nulls : " + str(sort_nulls)
    #print "sorted_cols: " + str(sorted_cols)
    #print "desc       : " + str(desc)
    #print "rows 0:\n"     + str(rows)

    begin = 0
    prev = None
    unsorteds = lambda row: project_unsorted(row, sorted_cols)

    for i in xrange(len(rows)):
        tmp = project_sorted(rows[i], sorted_cols)
        if prev != tmp:
            if prev:
                # Sort a complete "group", with matching ORDER BY column values
                rows[begin:i] = sorted(rows[begin:i], cmp=StandardNormalizer.safecmp, key=unsorteds)
            prev = tmp
            begin = i

    # Sort the final "group" (of rows with matching ORDER BY column values)
    rows[begin:] = sorted(rows[begin:], cmp=StandardNormalizer.safecmp, key=unsorteds)

    #print "rows 1:\n" + str(rows)

    # Sort SQL NULL (Python None) values, in ORDER BY columns
    # (i.e. sorted_cols), in the specified order (if any)
    if sort_nulls != SortNulls.never:
        sorted_col_values = lambda row, last_sorted_col : project_sorted(rows[row], sorted_cols[0:last_sorted_col])
        # Look for NULL values in each the of the ORDER BY columns
        for i in xrange(len(sorted_cols)):
            begin = 0
            prev = None
            # Loop through each row, plus one extra, so you can grab
            # the final block of identical values
            for j in xrange(len(rows) + 1):
                tmp = []  # for j = len(rows) case
                if j < len(rows):
                    # For the current row, we're interested in the values of
                    # ORDER BY columns, including the current column, and all
                    # the ones before it (but not after)
                    tmp = sorted_col_values(j, i+1)
                if prev != tmp:
                    # We've reached the end of a block of rows with identical
                    # values of the current set of ORDER BY columns
                    if prev and prev[i] is None:
                        # ... and the current column is NULL, in that block
                        if (sort_nulls == SortNulls.first or
                              (sort_nulls == SortNulls.highest and desc[i]) or
                              (sort_nulls == SortNulls.lowest  and not desc[i])):
                            if i == 0:  # first ORDER BY column, so don't need to check others
                                # Move the current block of rows to the very beginning
                                move_rows(rows, begin, j, 0)
                                break
                            # Get the values of the ORDER BY columns before the
                            # current one, for the current block of rows
                            ref = sorted_col_values(begin, i)
                            # Look for a block of rows before the current block,
                            # which has the identical values of the ORDER BY
                            # columns before the current column, but non-null
                            # values of the current column; start k as the row
                            # before the current block, and go backwards: if
                            # you find a non-identical row, or go beyond the
                            # first row, you're done
                            for k in range(begin - 1, -2, -1):
                                if (k < 0 or ref != sorted_col_values(k, i)):
                                    # If you found such a block of rows (possibly
                                    # empty), move the current block before it
                                    move_rows(rows, begin, j, k + 1)
                                    break
                        # remaining possibilities: sort_nulls == SortNulls.last or
                        #     (sort_nulls == SortNulls.lowest  and desc[i]) or
                        #     (sort_nulls == SortNulls.highest and not desc[i])):
                        else:
                            if i == 0:  # first ORDER BY column, so don't need to check others
                                # Move the current block of rows to the very end
                                move_rows(rows, begin, j, len(rows))
                                break
                            # Get the values of the ORDER BY columns before the
                            # current one, for the current block of rows
                            ref = sorted_col_values(begin, i)
                            # Look for a block of rows after the current block,
                            # which has the identical values of the ORDER BY
                            # columns before the current column, but non-null
                            # values of the current column; start k as the row
                            # after the current block, and go forward: if you
                            # find a non-identical row, or go beyond the last
                            # row, you're done
                            for k in range(j, len(rows) + 1):
                                if (k >= len(rows) or ref != sorted_col_values(k, i)):
                                    # If you found such a block of rows (possibly
                                    # empty), move the current block before it
                                    move_rows(rows, begin, j, k)
                                    break
                    prev = tmp
                    begin = j

    #print "rows 2:\n" + str(rows)

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

class StandardNormalizer(NotANormalizer):
    """This class contains a standard normalizer ('normalize' static method)
       that normalizes the result tuples of ORDER BY statements, sorting SQL
       NULL (Python None) values in the ORDER BY columns in the specified
       manner; and a standard comparison ('safecmp' static method) that
       compares values (including lists, None values and datetimes), to see
       if they are equal. By making this a class, it can be easily called
       from other modules; it could also serve as the base class for other
       normalizers.
    """

    @staticmethod
    def normalize(table, sql, num_digits=12, sort_nulls=SortNulls.never):
        """Normalizes the result tuples of ORDER BY statements, sorting SQL
           NULL (Python None) values in the ORDER BY columns in the specified
           manner (by default, not at all).
        """
        normalize_values(table.tuples, table.columns, num_digits)

        sql_upper = sql.upper()
        # Ignore ORDER BY clause only in a sub-query
        last_paren_index = sql_upper.rfind(')')  # will often be -1
        sort_cols = parse_sql(sql_upper[last_paren_index+1:])
        indices = []
        desc = []
        #print "last_paren_index: " + str(last_paren_index)
        #print "sort_cols: " + str(sort_cols)
        if sort_cols:
            # gets the ORDER BY column indices, in the order used in that clause
            for i in xrange(len(sort_cols)):
                sort_col_i_upper = sort_cols[i].upper()
                # Find the table column name or index that matches the ORDER BY column name
                for j in xrange(len(table.columns)):
                    #print "i, j+1; sort_cols[i], table.columns[j].name: " + str(i) + ", " + str(j+1) + ", " + str(sort_cols[i]) + ", " + str(table.columns[j].name)
                    if sort_col_i_upper == table.columns[j].name.upper() or sort_cols[i] == str(j+1):
                        #print "  appending j: " + str(j)
                        indices.append(j)
                        desc.append(parse_for_order_by_desc(sql_upper[last_paren_index+1:], sort_cols[i]))
                        break
        #print "indices: " + str(indices)
        #print "desc   : " + str(desc)

        # Make sure if there is an ORDER BY clause, the order by columns appear in
        # the result table. Otherwise all the columns will be sorted by the
        # normalizer.
        sort(table.tuples, indices, desc, sort_nulls)

        return table
