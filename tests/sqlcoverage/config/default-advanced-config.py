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
# tests (specifically, those NOT involving materialized views or scalar
# subqueries), which are a subset of those run by the main, or "default",
# sqlcoverage Jenkins job.
{
    "advanced": {"schema": "schema.py",
                 "ddl": "DDL.sql",
                 "template": "advanced.sql",
                 "normalizer": "nulls-lowest-normalizer.py"},
    "advanced-ints": {"schema": "int-schema.py",
                      "ddl": "int-DDL.sql",
                      "template": "advanced-ints.sql",
                      "normalizer": "normalizer.py"},
    "advanced-strings": {"schema": "strings-schema.py",
                         "ddl": "strings-DDL-null.sql",
                         "template": "advanced-strings.sql",
                         "normalizer": "nulls-lowest-normalizer.py"},
    "advanced-timestamp": {"schema": "timestamp-schema.py",
                           "ddl": "DDL.sql",
                           "template": "advanced-timestamp.sql",
                           "normalizer": "normalizer.py"},
    "advanced-index": {"schema": "schema.py",
                       "ddl": "index-DDL.sql",
                       "template": "advanced.sql",
                       "normalizer": "nulls-lowest-normalizer.py"},
    "advanced-compoundex": {"schema": "schema.py",
                            "ddl": "compoundex-DDL.sql",
                            "template": "advanced.sql",
                            "normalizer": "nulls-lowest-normalizer.py"},
    # To test advanced IN/EXISTS
    "advanced-inexists": {"schema": "schema.py",
                          "ddl": "DDL.sql",
                          "template": "advanced-inexists.sql",
                          "normalizer": "normalizer.py"},
# TODO: we may want to add, or resurrect, an advanced-decimal test suite
# at some point, but see the old note below:
# HSQL SEEMS TO HAVE A BAD DEFAULT PRECISION
# AND VoltDB gives VOLTDB ERROR: Type DECIMAL can't be cast as FLOAT, so keep it disabled, for now.
# If the only problem were HSQL HAS BAD DEFAULT PRECISION, we could USE FUZZY MATCHING.
# Enable this test to investigate the "DECIMAL can't be cast as FLOAT" issue
#    "advanced-decimal-fuzzy": {"schema": "decimal-schema.py",
#                               "ddl": "DDL.sql",
#                               "template": "advanced-decimal.sql",
#                               "normalizer": "fuzzynormalizer.py"},

    # This logically belongs in default-advanced-mv-subq-config.py, but is
    # placed here to balance execution time.
    # "ncs" is No Count Star, i.e., Materialized Views without an explicit COUNT(*)
    "advanced-matview-ncs-join": {"schema": "matview-advanced-ncs-join-schema.py",
                                  "ddl": "matview-DDL.sql",
                                  "template": "advanced-matview-join.sql",
                                  "normalizer": "nulls-lowest-normalizer.py"},
}
