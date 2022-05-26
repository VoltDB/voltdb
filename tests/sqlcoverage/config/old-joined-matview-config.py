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

{
    # Tests of Materialized Views defined using Joins
    "joined-matview-default-full": {"schema": "joined-matview-schema.py",
                                    "ddl": "joined-matview-DDL.sql",
                                    "template": "joined-matview-default-full.sql",
                                    "normalizer": "normalizer.py"},
    "joined-matview-int": {"schema": "joined-matview-int-schema.py",
                           "ddl": "joined-matview-int-DDL.sql",
                           "template": "joined-matview-int.sql",
                           "normalizer": "nulls-lowest-normalizer.py"},
    "joined-matview-string": {"schema": "joined-matview-string-schema.py",
                              "ddl": "joined-matview-string-DDL.sql",
                              "template": "joined-matview-string.sql",
                              "normalizer": "normalizer.py"},
    "joined-matview-timestamp": {"schema": "joined-matview-timestamp-schema.py",
                                 "ddl": "joined-matview-timestamp-DDL.sql",
                                 "template": "joined-matview-timestamp.sql",
                                 "normalizer": "normalizer.py"},
}
