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


# This file contains the "basic" tests, which are a subset of those run
# by the main, or "default", sqlcoverage Jenkins job.
{
    "basic": {"schema": "schema.py",
              "ddl": "DDL.sql",
              "template": "basic.sql",
              "normalizer": "nulls-lowest-normalizer.py"},
    "basic-joins": {"schema": "schema.py",
                    "ddl": "DDL.sql",
                    "template": "basic-joins.sql",
                    "normalizer": "normalizer.py"},
    "basic-int-joins": {"schema": "int-schema.py",
                    "ddl": "int-DDL.sql",
                    "template": "basic-int-joins.sql",
                    "normalizer": "normalizer.py"},
    "basic-unions": {"schema": "union-schema.py",
                     "ddl": "DDL.sql",
                     "template": "basic-unions.sql",
                     "normalizer": "normalizer.py"},
    "basic-index": {"schema": "schema.py",
                    "ddl": "index-DDL.sql",
                    "template": "basic.sql",
                    "normalizer": "nulls-lowest-normalizer.py"},
    "basic-strings": {"schema": "strings-schema.py",
                      "ddl": "strings-DDL.sql",
                      "template": "basic-strings.sql",
                      "normalizer": "nulls-lowest-normalizer.py"},
    "basic-timestamp": {"schema": "timestamp-schema.py",
                        "ddl": "DDL.sql",
                        "template": "basic-timestamp.sql",
                        "normalizer": "normalizer.py"},
# TODO: we may want to add, or resurrect, a basic-decimal test suite
# at some point, but see the old note below:
# If the ONLY problem was that HSQL HAS BAD DEFAULT PRECISION, we could use the original template input
# and FUZZY MATCHING, instead.
# Enable this test to investigate the "DECIMAL can't be cast as FLOAT" and/or "Backend DML Error" issues
# without being thrown off by HSQL HAS BAD DEFAULT PRECISION issues.
#    "basic-decimal-fuzzy": {"schema": "decimal-schema.py",
#                            "ddl": "DDL.sql",
#                            "template": "basic-decimal.sql",
#                            "normalizer": "fuzzynormalizer.py"},
#
}
