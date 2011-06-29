#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2011 VoltDB Inc.
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

# This SQL coverage configuration set represents hopefully the largest
# set of statements that should always pass.  Some of the template sets
# result in repeated failures due to non-functional issues with HSQL and/or
# the SQL coverage tool; these sets have been replaced with an input
# file that is all of the successful statements from a run of the set, and
# are named regression-<template set name>.sql
#
# To regenerate the regression-*-.sql file for a set, run the SQLGenerateReport.py
# tool on the report.xml file generated for that set, using the -f true switch,
# which will cause the successful statements to be written to stdout.
{
# THESE ALL SUCCEED, USE THE TEMPLATE INPUT
    "basic": {"schema": "schema.py",
              "ddl": "DDL.sql",
              "template": "basic.sql",
              "normalizer": "normalizer.py"},
# THESE ALL SUCCEED, USE THE TEMPLATE INPUT
    "basic-index": {"schema": "schema.py",
                    "ddl": "index-DDL.sql",
                    "template": "basic.sql",
                    "normalizer": "normalizer.py"},
# THESE ALL SUCCEED, USE THE TEMPLATE INPUT
    "basic-strings": {"schema": "strings-schema.py",
                      "ddl": "strings-DDL.sql",
                      "template": "basic-strings.sql",
                      "normalizer": "normalizer.py"},
# BIGINT OVERFLOW CAUSES FAILURES IN THIS SUITE, USE REGRESSION INPUT
    "basic-ints": {"schema": "int-schema.py",
                   "ddl": "int-DDL.sql",
                   "template": "regression-basic-ints.sql",
                   "normalizer": "normalizer.py"},
# HSQL HAS BAD DEFAULT PRECISION, USE REGRESSION INPUT
    "basic-decimal": {"schema": "decimal-schema.py",
                      "ddl": "decimal-DDL.sql",
                      "template": "regression-basic-decimal.sql",
                      "normalizer": "normalizer.py"},
# FLOATING POINT ROUNDING ISSUES BETWEEN VOLT AND HSQL, USE REGRESSION INPUT
    "basic-timestamp": {"schema": "timestamp-schema.py",
                        "ddl": "timestamp-DDL.sql",
                        "template": "regression-basic-timestamp.sql",
                        "template-hsqldb": "regression-basic-timestamp-hsql.sql",
                        "normalizer": "normalizer.py"},
# BIGINT OVERFLOW CAUSES FAILURES IN THIS SUITE, USE REGRESSION INPUT
    "basic-matview": {"schema": "matview-schema.py",
                      "ddl": "matview-DDL.sql",
                      "template": "regression-basic-matview.sql",
                      "normalizer": "normalizer.py"},
# THESE ALL SUCCEED, USE TEMPLATE INPUT
    "advanced": {"schema": "schema.py",
                 "ddl": "DDL.sql",
                 "template": "advanced.sql",
                 "normalizer": "normalizer.py"},
# THESE ALL SUCCEED, USE TEMPLATE INPUT
    "advanced-index": {"schema": "schema.py",
                       "ddl": "index-DDL.sql",
                       "template": "advanced.sql",
                       "normalizer": "normalizer.py"},
# THESE ALL SUCCEED, USE TEMPLATE INPUT
    "advanced-strings": {"schema": "strings-schema.py",
                         "ddl": "strings-DDL.sql",
                         "template": "advanced-strings.sql",
                         "normalizer": "normalizer.py"},
# BIGINT OVERFLOW CAUSES FAILURES IN THIS SUITE, USE REGRESSION INPUT
    "advanced-ints": {"schema": "int-schema.py",
                      "ddl": "int-DDL.sql",
                      "template": "regression-advanced-ints.sql",
                      "normalizer": "normalizer.py"},
# This suite written to test push-down of aggregates and limits in combination
# with indexes, projections and order-by.
    "pushdown": {"schema": "pushdown-schema.py",
        "ddl": "pushdown-DDL.sql",
        "template": "pushdown.sql",
        "normalizer": "normalizer.py"},

# HSQL SEEMS TO HAVE A BAD DEFAULT PRECISION, THE
# REGRESSION SET IS NEXT TO USELESS, SKIPPING IT
#    "advanced-decimal": {"schema": "decimal-schema.py",
#                         "ddl": "decimal-DDL.sql",
#                         "template": "regression-advanced-decimal.sql",
#                         "normalizer": "normalizer.py"},
}


