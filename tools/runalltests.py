#!/usr/bin/env python

import sys
import argparse

parser = argparse.ArgumentParser(description="Run All The Tests.")
parser.add_argument("--valgrind",
                    default=False,
                    action='store_true',
                    help='Use valgrind on tests.')
parser.add_argument("tests",
                    nargs='*',
                    help='Tests in dir/test_name form.')

args=parser.parse_args()
for test in args.tests:
    valgrind = "";
    if args.valgrind:
        valgrind = "valgrind "
    print("%s%s" % (valgrind, test))
