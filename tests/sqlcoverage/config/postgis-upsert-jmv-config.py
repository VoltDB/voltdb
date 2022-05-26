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


# This file contains the "upsert" tests (and one quick joined materialized
# view test suite), which are a subset of those run by the "postgis"
# sqlcoverage Jenkins job.

# These tests only work when run against PostgreSQL, not HSQL, and some require
# PostGIS. Hence, to run them on the command line, use something like the following:
#     ant -Dpostgis=1 -Dsql_coverage_suite=postgis-upsert-jmv-config.py sqlcoverage
{
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
    "upsert-decimal": {"schema": "decimal-schema.py",
                       "ddl": "DDL-null.sql",
                       "template": "upsert-decimal.sql",
                       "normalizer": "nulls-lowest-normalizer.py"},
    "upsert-timestamp": {"schema": "timestamp-schema.py",
                         "ddl": "DDL-null.sql",
                         "template": "upsert-timestamp.sql",
                         "normalizer": "nulls-lowest-normalizer.py"},
    "upsert-geo-point": {"schema": "geo-point-schema.py",
                         "ddl": "geo-DDL.sql",
                         "template": "upsert-geo-point.sql",
                         "normalizer": "normalizer.py",
                         "precision": "8"},
    "upsert-geo-polygon": {"schema": "geo-polygon-schema.py",
                           "ddl": "geo-DDL.sql",
                           "template": "upsert-geo-polygon.sql",
                           "normalizer": "normalizer.py",
                           "precision": "8"},

    # Quick tests of Materialized Views defined using Joins; lengthier
    # tests are included in separate config files, matview-config.py
    # and matview-int-config.py
    "joined-matview-default-quick": {"schema": "joined-matview-schema.py",
                                     "ddl": "joined-matview-DDL.sql",
                                     "template": "joined-matview-default-quick.sql",
                                     "normalizer": "normalizer.py"},
}
