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

from SQLCoverageReport import generate_html_reports

import types

class NotANormalizer:
    """The simplest possible Normalizer, whose normalize function merely
    returns the original table without making any changes; also offers a
    safecmp function that can compare two lists that may contain None, and
    avoid a TypeError when using datetime.
    """

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
    def normalize(table, sql):
        """Do nothing other than returning the table
        """
        return table
