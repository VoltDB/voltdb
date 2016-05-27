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

# This SQL coverage configuration set represents tests that are only intended
# to run against PostgreSQL, not against HSQL. Originally, this was limited
# to the Geospatial features (added in version 6.0), but it now includes other
# features, such as UPSERT statements. Hence, to run it, use
# something like the following:
#     ant -Dpostgis=1 -Dsql_coverage_suite=geo-config.py sqlcoverage

{
    # Geospatial Tests
    "geo-basic": {"schema": "geo-schema.py",
                  "ddl": "geo-DDL.sql",
                  "template": "geo-basic.sql",
                  "normalizer": "geo-normalizer-4digits.py"},
    "geo-basic-point": {"schema": "geo-schema.py",
                        "ddl": "geo-DDL.sql",
                        "template": "geo-basic-point.sql",
                        "normalizer": "geo-normalizer.py"},
    "geo-basic-polygon": {"schema": "geo-schema.py",
                          "ddl": "geo-DDL.sql",
                          "template": "geo-basic-polygon.sql",
                          "normalizer": "geo-normalizer.py"},
    "geo-advanced-point": {"schema": "geo-schema.py",
                           "ddl": "geo-DDL.sql",
                           "template": "geo-advanced-point.sql",
                           "normalizer": "geo-normalizer.py"},
    "geo-advanced-polygon": {"schema": "geo-schema.py",
                             "ddl": "geo-DDL.sql",
                             "template": "geo-advanced-polygon.sql",
                             "normalizer": "geo-normalizer.py"},
    # UPSERT tests
    "upsert": {"schema": "schema.py",
               "ddl": "DDL.sql",
               "template": "upsert.sql",
               "normalizer": "nulls-lowest-normalizer.py"},
    "upsert-ints": {"schema": "int-schema.py",
                    "ddl": "int-DDL-null.sql",
                    "template": "upsert-ints.sql",
                    "normalizer": "nulls-lowest-normalizer.py"},
    "upsert-strings": {"schema": "strings-schema.py",
                       "ddl": "strings-DDL-null.sql",
                       "template": "upsert-strings.sql",
                       "normalizer": "nulls-lowest-normalizer.py"},
}
