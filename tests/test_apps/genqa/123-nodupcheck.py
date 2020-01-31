#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2019 VoltDB Inc.
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

import fileinput

metas = [0] * 6
rows = 0
keyCounts = {}
for line in fileinput.input():
    fields = line.split(',')
    if len(fields) != 29:
        print "Row contains %s fields: %s" % (len(fields), line)
    metas[int( fields[5].replace('"', ''))] += 1
    rows += 1

print "Inserts: %s\nDeletes: %s\nUpdates/Before: %s\nUpdates/After: %s" % \
    (metas[1], metas[2], metas[3], metas[4])
print "Metadata-total: %s" % rows
