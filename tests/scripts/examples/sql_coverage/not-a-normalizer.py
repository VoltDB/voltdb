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

from SQLCoverageReport import generate_html_reports

import types

# This bit of funtionality is required of all normalizers just to pass basic validation.
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

def normalize(table, sql):
    """Do nothing other than returning the table
    """
    return table

def compare_results(suite, seed, statements_path, hsql_path, jni_path, output_dir, report_all):
    return generate_html_reports(suite, seed, statements_path, hsql_path,
            jni_path, output_dir, report_all, True)
