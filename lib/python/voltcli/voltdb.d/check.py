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

import multiprocessing
import platform
from voltcli import checkconfig

output = {
            "Hostname" : ["", "Unable to gather information"],
            "OS" : ["", "Unable to gather information"],
            "OS release" : ["", "Unable to gather information"],
            "ThreadCount" : ["", "Unable to gather information"],
            "64 bit" : ["", "Unable to gather information"],
            "Memory" : ["", "Unable to gather information"]
          }

def displayResults():
    fails = 0
    warns = 0
    for key,val in sorted(output.items()):
        if val[0] == "FAIL":
            fails += 1
        elif val[0] == "WARN":
            warns += 1
        print "Stats: %-25s %-8s %-9s" % ( key, val[0], val[1] )
    if fails > 0:
        print "\nCheck FAILED. Please review."
    elif warns > 0:
        print "\nCheck completed with " + str(warns) + " WARNINGS."
    else:
        print "\nCheck completed successfully."

@VOLT.Command(
    description = 'Check system properties.'
)
def check(runner):
    systemCheck()

def systemCheck():
    output['Hostname'] = ["", platform.node()]
    output['ThreadCount'] = ["", str(multiprocessing.cpu_count())]
    checkconfig.test_full_config(output)
    displayResults()
