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


import shlex
import sys
import testsupport
from datetime import datetime

manifest = open("testrunner/manifest.txt", 'r').readlines()
lineno = 1

failures = 0
testsrun = 0
duration = 0.0

for line in manifest:
    lineno += 1
    line = line.strip()

    if line[0] == '#':
        continue
    if len(line) == 0:
        continue

    line = shlex.split(line)

    timestamp = datetime.now().strftime("%Y.%m.%d-%H.%M.%S")

    # assume each line has
    # NAME RUNNER SPECIFICINFO SAFETY CORES
    if len(line) != 5:
        print "Line %d doesn't have five values. This is an error."
        sys.exit(-1)

    name = line[0]
    runner = line[1]
    task = line[2]
    safety = line[3]
    cores = int(line[4])
    timeout = 400

    if runner == "junit":
        runner = testsupport.JUnit(name, timestamp, timeout, task, safety, cores)
        runner.run()
        failures += runner.failures
        testsrun += runner.testsrun
        duration += runner.duration

        msg = ""
        if runner.didtimeout:
            msg = msg =  "TIMEOUT for test %s (took %g seconds)\n" % (task, timeout)
        elif runner.failures > 0:
            msg =  "Failures in test %s (took %g seconds)\n" % (task, runner.duration)
        else:
            msg =  "Completed test %s (took %g seconds)\n" % (task, runner.duration)
        msg += "  %d failures / %d tests run " % (runner.failures, runner.testsrun)
        msg += "/ %d out of %d cumulative failures in %g cumulative seconds" % \
                (failures, testsrun, duration)

        print msg

