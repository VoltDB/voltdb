#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2010 VoltDB L.L.C.
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
    "basic": {"schema": "schema.py",
              "ddl": "DDL.sql",
              "template": "basic.sql",
              "normalizer": "normalizer.py"},
    "basic-index": {"schema": "schema.py",
                    "ddl": "index-DDL.sql",
                    "template": "basic.sql",
                    "normalizer": "normalizer.py"},
    "basic-strings": {"schema": "strings-schema.py",
                      "ddl": "strings-DDL.sql",
                      "template": "basic-strings.sql",
                      "normalizer": "normalizer.py"},
    "basic-ints": {"schema": "int-schema.py",
                   "ddl": "int-DDL.sql",
                   "template": "basic-ints.sql",
                   "normalizer": "normalizer.py"},
    "basic-decimal": {"schema": "decimal-schema.py",
                      "ddl": "decimal-DDL.sql",
                      "template": "basic-decimal.sql",
                      "normalizer": "normalizer.py"},
    "basic-timestamp": {"schema": "timestamp-schema.py",
                        "ddl": "timestamp-DDL.sql",
                        "template": "basic-timestamp.sql",
                        "normalizer": "normalizer.py"},
#     "basic-joins": {"schema": "schema.py",
#                    "ddl": "DDL.sql",
#                     "template": "basic-joins.sql",
#                     "normalizer": "normalizer.py"},
#     "basic-index-joins": {"schema": "schema.py",
#                           "ddl": "index-DDL.sql",
#                           "template": "basic-joins.sql",
#                           "normalizer": "normalizer.py"},
    "advanced": {"schema": "schema.py",
                 "ddl": "DDL.sql",
                 "template": "advanced.sql",
                 "normalizer": "normalizer.py"},
    "advanced-index": {"schema": "schema.py",
                       "ddl": "index-DDL.sql",
                       "template": "advanced.sql",
                       "normalizer": "normalizer.py"},
    "advanced-strings": {"schema": "strings-schema.py",
                         "ddl": "strings-DDL.sql",
                         "template": "advanced-strings.sql",
                         "normalizer": "normalizer.py"},
    "advanced-ints": {"schema": "int-schema.py",
                      "ddl": "int-DDL.sql",
                      "template": "advanced-ints.sql",
                      "normalizer": "normalizer.py"},
    "advanced-decimal": {"schema": "decimal-schema.py",
                         "ddl": "decimal-DDL.sql",
                         "template": "advanced-decimal.sql",
                         "normalizer": "normalizer.py"},
#    "advanced-joins": {"schema": "schema.py",
#                       "ddl": "DDL.sql",
#                       "template": "advanced-joins.sql",
#                       "normalizer": "normalizer.py"},
#    "advanced-index-joins": {"schema": "schema.py",
#                             "ddl": "index-DDL.sql",
#                             "template": "advanced-joins.sql",
#                             "normalizer": "normalizer.py"},
}
