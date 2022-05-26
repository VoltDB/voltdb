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


# This file contains the miscellaneous tests (neither "basic" nor "advanced"),
# including those labeled "regression", which are a subset of those run by the
# main, or "default", sqlcoverage Jenkins job.


# TODO: the old note below (some of which may no longer be accurate) explains
# how the "regression" test suites were generated, and how they could be
# regenerated. We may wish to improve this process, at some point.

# HACK:
# Some of the template files
# generate statements that result in repeated failures due to functional
# differences between HSQL and VoltDB backends. In such cases, we work around
# these errors by generating a fixed sample query file and, after culling out
# any statements that cause mismatches, using it to replace the original
# template file name in the configuration list below.
# In this way, the sample file gets used as a trivial template file that passes
# through the generator untouched. It also has an unfortunate side-effect of
# causing any future improvements to the template to be ignored unless/until
# the sample file is manually re-generated from it and re-edited to eliminate
# mismatches.
#
# Actually, in the specific case of templates that generate random integer
# constant timestamp values, the template must be replaced by TWO separate
# generated sample files -- one generated for hsql with millisecond constants
# and one for VoltDB with microsecond constants (always 1000 X the hsql values).
# The hsql version of the sample file gets associated with the optional
# "template-hsqldb" key in the configuration. Otherwise, both hsql and VoltDB
# use the same input file associated with the "template" key.
#
# The generated sample files follow a naming convention of starting with
# "regression". The hsql variants end in "-hsql.sql".
# It is NOT advisable to try to edit these sample files directly.
# It is better to edit the original template, re-generate the sample(s),
# and re-cull the resulting "mismatches" -- being careful to cull ONLY
# mismatches that are NOT accountable to the known backend differences that
# we are working around in this way. It helps to have comments, below,
# to describe which specific issues each "regression" file is intended to
# work around.
#
# To regenerate the regression-*.sql file for a configuration, run the SQLGenerateReport.py
# tool on the report.xml file generated for that configuration, using the -f true switch,
# which will cause the successful statements to be written to stdout.
{
# THESE ALL SUCCEED, USE TEMPLATE INPUT
    "mixed-unions": {"schema": "union-schema.py",
                     "ddl": "DDL.sql",
                     "template": "mixed-unions.sql",
                     "normalizer": "normalizer.py"},
# THESE ALL SUCCEED, USE TEMPLATE INPUT
    "partial-covering": {"schema": "partial-covering-schema.py",
                         "ddl": "partial-covering-DDL.sql",
                         "template": "partial-covering.sql",
                         "normalizer": "normalizer.py"},
# BIGINT OVERFLOW CAUSES FAILURES IN THIS SUITE, USE REGRESSION INPUT
    "regression-basic-ints": {"schema": "int-schema.py",
                              "ddl": "int-DDL.sql",
                              "template": "regression-basic-ints.sql",
                              "normalizer": "normalizer.py"},
# HSQL HAS BAD DEFAULT PRECISION
# AND VoltDB gives VOLTDB ERROR: Type DECIMAL can't be cast as FLOAT
# AND HSQLDB backend gives the likes of:
#   VOLTDB ERROR: UNEXPECTED FAILURE: org.voltdb.ExpectedProcedureException:
#   HSQLDB Backend DML Error (Scale of 56.11063569750000000000000000000000 is 32 and the max is 12)
# USE REGRESSION INPUT
    "regression-basic-decimal": {"schema": "decimal-schema.py",
                                 "ddl": "DDL.sql",
                                 "template": "regression-basic-decimal.sql",
                                 "normalizer": "normalizer.py"},
# BIGINT OVERFLOW CAUSES FAILURES IN THIS SUITE, USE REGRESSION INPUT
    "regression-basic-matview": {"schema": "matview-basic-schema.py",
                                 "ddl": "matview-DDL.sql",
                                 "template": "regression-basic-matview.sql",
                                 "normalizer": "normalizer.py"},
# BIGINT OVERFLOW CAUSES FAILURES IN THIS SUITE, USE REGRESSION INPUT
    "regression-advanced-ints-cntonly": {"schema": "int-schema.py",
                                         "ddl": "int-DDL.sql",
                                         "template": "regression-advanced-ints-cntonly.sql",
                                         "normalizer": "not-a-normalizer.py"},
# To test index count
    "index-count1": {"schema": "index-count1-schema.py",
                     "ddl": "DDL.sql",
                     "template": "index-count1.sql",
                     "normalizer": "normalizer.py"},

# To test index scan: forward scan, reverse scan
    "index-scan": {"schema": "index-scan-schema.py",
                   "ddl": "index-DDL.sql",
                   "template": "index-scan.sql",
                   "normalizer": "nulls-lowest-normalizer.py"},

# To test index with varbinary
    "index-varbinary": {"schema": "index-varbinary-schema.py",
                        "ddl": "index-DDL.sql",
                        "template": "index-varbinary.sql",
                        "normalizer": "normalizer.py"},

# This suite written to test push-down of aggregates and limits in combination
# with indexes, projections and order-by.
    "pushdown": {"schema": "pushdown-schema.py",
                 "ddl": "DDL.sql",
                 "template": "pushdown.sql",
                 "normalizer": "normalizer.py"},

# To test materialized view correctness, even with query optimization
# (see ENG-2878)
    "matview-query-opt": {"schema": "matview-query-opt-schema.py",
                          "ddl": "matview-DDL.sql",
                          "template": "matview-query-opt.sql",
                          "normalizer": "nulls-lowest-normalizer.py"},

    # This logically belongs in default-advanced-mv-subq-config.py, but is
    # placed here to balance execution time.
    # "ncs" is No Count Star, i.e., Materialized Views without an explicit COUNT(*)
    "advanced-matview-ncs-nonjoin": {"schema": "matview-advanced-ncs-nonjoin-schema.py",
                                     "ddl": "matview-DDL.sql",
                                     "template": "advanced-matview-nonjoin.sql",
                                     "normalizer": "nulls-lowest-normalizer.py"},
}
