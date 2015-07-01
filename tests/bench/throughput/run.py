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

from subprocess import *

def run(procs, threadsPerProc, workload):
    results = []
    totalThroughput = 0.0

    output = Popen(['./throughput', str(procs), str(threadsPerProc), workload], stdout=PIPE).communicate()[0]

    lines = output.split('\n')
    for line in lines:
        if line.startswith('RESULT: '):
            print line
            line = line.split(' ')[1]
            parts = line.split(',')
            results += [float(parts[2])]

    for r in results:
        totalThroughput += r

    print "--"
    print "PER THREAD AVG:   " + str(totalThroughput / (procs * threadsPerProc))
    print "PER PROC AVG:     " + str(totalThroughput / procs)
    print "TOTAL THROUGHPUT: " + str(totalThroughput)
    print "--"

run(1, 1, 'r')
run(1, 2, 'r')
run(1, 3, 'r')
run(1, 4, 'r')
