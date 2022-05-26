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

class SortNulls:
    """Simple class to enumerate the options for when and how to sort SQL NULL
       (Python None) values, found in the ORDER BY columns: never, first, last,
       lowest or highest.
    """
    never = 0  # do not re-sort SQL NULL (Python None) values
    first = 1  # always sort SQL NULL (Python None) values first
    last  = 2  # always sort SQL NULL (Python None) values last
    lowest  = 3  # sort SQL NULL (Python None) values first with ORDER BY ASC, last with ORDER BY DESC
    highest = 4  # sort SQL NULL (Python None) values last with ORDER BY ASC, first with ORDER BY DESC
