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

# This SQL coverage configuration set represents tests that are only intended
# to run against PostgreSQL, not against HSQL. Originally, this was limited
# to the Geospatial features (added in version 6.0), but it now includes other
# features, such as UPSERT statements. Hence, to run it, use
# something like the following:
#     ant -Dpostgis=1 -Dsql_coverage_suite=geo-config.py sqlcoverage

{
    # Geospatial (point & polygon) Tests
    "geo-basic": {"schema": "geo-point-schema.py",
                  "ddl": "geo-DDL.sql",
                  "template": "geo-basic.sql",
                  "normalizer": "normalizer.py",
                  "precision": "4"},
    "geo-basic-point": {"schema": "geo-point-schema.py",
                        "ddl": "geo-DDL.sql",
                        "template": "geo-basic-point.sql",
                        "normalizer": "normalizer.py",
                        "precision": "8"},
    "geo-basic-polygon": {"schema": "geo-polygon-schema.py",
                          "ddl": "geo-DDL.sql",
                          "template": "geo-basic-polygon.sql",
                          "normalizer": "normalizer.py",
                          "precision": "8"},
    "geo-advanced-point": {"schema": "geo-point-schema.py",
                           "ddl": "geo-DDL.sql",
                           "template": "geo-advanced-point.sql",
                           "normalizer": "normalizer.py",
                           "precision": "8"},
    "geo-advanced-polygon": {"schema": "geo-polygon-schema.py",
                             "ddl": "geo-DDL.sql",
                             "template": "geo-advanced-polygon.sql",
                             "normalizer": "normalizer.py",
                             "precision": "8"},
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

    # Tests of SQL Analytic Functions (e.g. RANK)
    "analytic": {"schema": "schema.py",
                 "ddl": "DDL.sql",
                 "template": "analytic.sql",
                 "normalizer": "normalizer.py"},
    "analytic-ints": {"schema": "int-schema.py",
                      "ddl": "int-DDL-null.sql",
                      "template": "analytic-ints.sql",
                      "normalizer": "normalizer.py"},
    "analytic-strings": {"schema": "strings-schema.py",
                         "ddl": "strings-DDL-null.sql",
                         "template": "analytic-strings.sql",
                         "normalizer": "normalizer.py"},
    "analytic-timestamp": {"schema": "timestamp-schema.py",
                           "ddl": "DDL-null.sql",
                           "template": "analytic-timestamp.sql",
                           "normalizer": "normalizer.py"},
    "analytic-decimal": {"schema": "decimal-schema.py",
                         "ddl": "DDL-null.sql",
                         "template": "analytic-decimal.sql",
                         "normalizer": "normalizer.py"},
    "analytic-geo-point": {"schema": "geo-point-schema.py",
                           "ddl": "geo-DDL.sql",
                           "template": "analytic-geo-point.sql",
                           "normalizer": "normalizer.py"},
    "analytic-geo-polygon": {"schema": "geo-polygon-schema.py",
                             "ddl": "geo-DDL.sql",
                             "template": "analytic-geo-polygon.sql",
                             "normalizer": "normalizer.py"},

    # May want to add something like these, someday:
#     "analytic-float": {"schema": "float-schema.py",
#                        "ddl": "float-DDL.sql",
#                        "template": "analytic-float.sql",
#                        "normalizer": "normalizer.py"},
#     "analytic-varbinary": {"schema": "int-schema.py",
#                            "ddl": "index-DDL.sql",
#                            "template": "analytic-varbinary.sql",
#                            "normalizer": "normalizer.py"},

    # Tests of Materialized Views defined using Joins
    "joined-matview-default-quick": {"schema": "joined-matview-schema.py",
                                     "ddl": "joined-matview-DDL.sql",
                                     "template": "joined-matview-default-quick.sql",
                                     "normalizer": "normalizer.py"},
}
