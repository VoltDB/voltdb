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


# This file contains roughly half (in execution time) of the "advanced"
# tests (specifically, those for materialized views and scalar subqueries),
# which are a subset of those run by the main, or "default", sqlcoverage
# Jenkins job.
{
    # To test advanced scalar subqueries
    "advanced-scalar-subquery": {"schema": "schema.py",
                                 "ddl": "DDL.sql",
                                 "template": "advanced-scalar-subquery.sql",
                                 "normalizer": "normalizer.py"},
    # To test advanced scalar subqueries containing set operators
    "advanced-scalar-set-subquery": {"schema": "schema.py",
                                     "ddl": "DDL.sql",
                                     "template": "advanced-scalar-set-subquery.sql",
                                     "normalizer": "normalizer.py"},
    # ADVANCED MATERIALIZED VIEW TESTING, INCLUDING COMPLEX GROUP BY AND AGGREGATIONS.
    "advanced-matview-nonjoin": {"schema": "matview-advanced-nonjoin-schema.py",
                                 "ddl": "matview-DDL.sql",
                                 "template": "advanced-matview-nonjoin.sql",
                                 "normalizer": "nulls-lowest-normalizer.py"},
    "advanced-matview-join": {"schema": "matview-advanced-join-schema.py",
                              "ddl": "matview-DDL.sql",
                              "template": "advanced-matview-join.sql",
                              "normalizer": "nulls-lowest-normalizer.py"},
}
