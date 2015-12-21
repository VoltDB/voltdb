#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
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

# HACK:
# This SQL coverage configuration set represents hopefully the largest
# set of statements that should always pass.  Some of the template files
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
# THESE ALL SUCCEED, USE THE TEMPLATE INPUT
    "geo-basic": {"schema": "geo-schema.py",
                  "ddl": "geo-DDL.sql",
                  "template": "geo-basic.sql",
                  "normalizer": "fuzzynormalizer.py"},

}


