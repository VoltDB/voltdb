#!/usr/bin/env python
# This file is part of VoltDB.
# Copyright (C) 2008-2013 VoltDB Inc.
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

import sys
import os
import glob
import re
import shutil
import time
import signal

RE_SNAP = re.compile('and took ([0-9.]+)')
RE_THROUGHPUT = re.compile('Average throughput:\s*([0-9,]+)')
RE_LATENCY = re.compile('Average latency:\s*([0-9.]+)')

class Results(object):
    def __init__(self, runname):
        self.runname = runname
        self.snapshot_count = 0
        self.snapshot_total_duration = 0.0
        self.throughput = 0
        self.latency = 0.0
    def __str__(self):
        def format(label, value):
            return '%20s: %s' % (label, str(value))
        if self.snapshot_count == 0:
            duration = 0.0
        else:
            duration = self.snapshot_total_duration / self.snapshot_count
        return '\n'.join([
            '===== Results (%s) =====' % self.runname,
            format('Throughput', self.throughput),
            format('Latency', self.latency),
            format('Snapshots', self.snapshot_count),
            format('Snapshot duration', '%0.2f' % duration),
        ])

def delta(old, new):
    try:
        return (new - old) / float(old)
    except ZeroDivisionError:
        return 0.0

def delta_attr(res1, res2, name):
    v1 = getattr(res1, name)
    v2 = getattr(res2, name)
    return delta(v1, v2)

def format_delta(value):
    return '%+0.2f%%' % (value * 100.0)

def analyze(outroot, runname):
    outdir = '%(outroot)s/%(runname)s' % locals()
    logfile = '%(outdir)s/volt.log' % locals()
    runfile = '%(outdir)s/run.txt' % locals()
    results = Results(runname)
    f = open(logfile)
    for line in f:
        m = RE_SNAP.search(line)
        if m:
            results.snapshot_count += 1
            results.snapshot_total_duration += float(m.group(1))
    f.close()
    f = open(runfile)
    for line in f:
        m = RE_THROUGHPUT.search(line)
        if m:
            results.throughput = int(m.group(1).replace(',', ''))
        else:
            m = RE_LATENCY.search(line)
            if m:
                results.latency = float(m.group(1))
    f.close()
    return results

if __name__ == '__main__':
    outroot = sys.argv[1]
    editions = ('old', 'new')
    tests = ('nosnap', 'snap')
    metrics = ('throughput', 'latency')
    results = []
    for edition in editions:
        for test in tests:
            results.append(analyze(outroot, '%s-%s' % (edition, test)))
    print '\n\n'.join([str(result) for result in results])
    deltas = []
    for metric in metrics:
        for i in range(len(editions)):
            deltas.append(delta_attr(results[i*len(editions)+0], results[i*len(editions)+1], metric))
    print '\n===== Snapshot performance impact ====='
    for i in range(len(deltas)):
        print '   %10s (%d): %s' % (metrics[i/len(metrics)], i % len(metrics) + 1, format_delta(deltas[i]))
    print '\n===== Change in snapshot performance impact ====='
    for i in range(len(metrics)):
        print '   %10s: %s' % (metrics[i], format_delta(delta(deltas[i*len(editions)+0], deltas[i*len(editions)+1])))
