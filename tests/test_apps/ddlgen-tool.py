#!/usr/bin/env python2.7
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

import shlex
import string
import sys

from ddlgenerator import DDLGenerator

"""
A simple script to read in a DDL file and replace
    %%TABLE ...
    + %%INDEX ...
    + %%STREAM ...
    + %%TOPIC ...
with generated SQL
"""


# TODO every parameter needs an equal sign.  check and raise a specific error
# TODO validate each key is valid?
def process_args(line, lineno):
    line = string.strip(line)
    splitter = shlex.shlex(line, posix=True)
    splitter.whitespace = ","
    splitter.whitespace_split = True

    map = {}
    for param in list(splitter):
        try:
            k, v = param.split("=", 1)
            k = string.strip(k)
            v = string.strip(v)
            map[k] = v
        except Exception as e:
            msg = "line %d: '%s': %s\n" % (lineno, param, e)
            raise SyntaxError(msg)

    return map


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("usage: python ddlgen-tool FILE")
        sys.exit(1)

    generator = DDLGenerator()

    with open(sys.argv[1]) as fp:
        for lineno, line in enumerate(fp):
            line = line.rstrip()
            line2 = line.lstrip()
            if line2.startswith("%%TABLE"):
                print("-- %s" % line2)
                params = process_args(line2[7:], lineno)
                print(generator.gen_table(**params))

            elif line2.startswith("%%STREAM"):
                print("-- %s" % line2)
                params = process_args(line2[8:], lineno)
                print(generator.gen_stream(**params))

            elif line2.startswith("%%TOPIC"):
                print("-- %s" % line2)
                params = process_args(line2[8:], lineno)
                print(generator.gen_topic(**params))

            else:
                print(line)
