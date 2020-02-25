#!/usr/bin/env python2.7

import shlex
import string
import sys

from ddlgenerator import DDLGenerator

"""
A simple script to read in a DDL file and replace
    %%TABLE ...
    + %%INDEX ...
    + %%STREAM ...
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

            else:
                print(line)
