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

from SortNulls import SortNulls
from SQLCoverageReport import generate_html_reports
from StandardNormalizer import StandardNormalizer

def normalize(table, sql):
    """Normalizes the result tuples of ORDER BY statements, sorting SQL NULL
       (Python None) values last (whether using ORDER BY ASC or DESC).
    """
    return StandardNormalizer.normalize(table, sql, SortNulls.last)

def safecmp(x, y):
    """Calls the 'standard' safecmp function, which performs a comparison
       similar to cmp, including iterating over lists, but two None values
       are considered equal, and a TypeError is avoided when a None value
       and a datetime are corresponding members of a list.
    """
    return StandardNormalizer.safecmp(x,y)

def compare_results(suite, seed, statements_path, hsql_path, jni_path,
                    output_dir, report_all, extra_stats, comparison_database):
    """Just calls SQLCoverageReport.generate_html_reports(...).
    """
    return generate_html_reports(suite, seed, statements_path, hsql_path, jni_path,
                                 output_dir, report_all, extra_stats, comparison_database)
