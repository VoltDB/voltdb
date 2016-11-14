#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
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

import argparse
import json
import collections
import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(sys.path[0]),"lib","python"))
from voltdbclient import *

parser = argparse.ArgumentParser(description="This script is used to monitor current performance metrics.")
parser.add_argument('-s', '--server', help='Hostname or IP of VoltDB server', default='localhost')
parser.add_argument('-p', '--port', help='Port number of VoltDB server', default=21211)
parser.add_argument('-u', '--username', help='User name (if security is enabled)', default='')
parser.add_argument('-pw', '--password', help='Password (if security is enabled)', default='')
parser.add_argument('-f', '--frequency', help='Frequency of gathering statistics in seconds (default = 5 seconds)', default=5)
parser.add_argument('-d', '--duration', help='Duration of gathering statistics in minutes (default = 30)', default=30)
args = parser.parse_args()

client = FastSerializer(args.server, args.port, args.username, args.password)

# procedure call response error handling
def check_response(response):
    status = response.status
    if status != 1:
        status_codes = {
            -1: "User Abort",
            -2: "Graceful Failure",
            -3: "Unexpected Failure",
            -4: "Connection Lost",
            -5: "Server Unavailable",
            -6: "Connection Timeout",
            -7: "Response Unkonwn",
            -8: "Transaction Restart",
            -9: "Operational Failure"
        }
        print status_codes.get(status, "No Status code was returned") + ": " + response.statusString
        ex = response.exception
        if ex is not None:
            print ex.typestr + ": " + ex.message
        exit(-1)

# define procedure calls
proc_stats = VoltProcedure( client, "@Statistics", [FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_INTEGER] )
proc_catalog = VoltProcedure( client, "@SystemCatalog", [FastSerializer.VOLTTYPE_STRING] )

# Statistics gathering functions
def get_cpu():
    response = proc_stats.call(["CPU",0])
    check_response(response)
    table = response.tables[0]
    cpu_perc = table.tuples[0][3]
    return cpu_perc

def get_pp():
    response = proc_stats.call(["PROCEDUREPROFILE",1])
    check_response(response)
    table = response.tables[0]
    return table.tuples

def get_latencies():
    response = proc_stats.call(["INITIATOR",1])
    check_response(response)
    table = response.tables[0]
    latencies = dict()
    for row in table.tuples:
        procname = row[6]
        avg_millis = row[8]
        latencies[procname] = avg_millis
    return latencies

def get_proc_labels():
    response = proc_catalog.call(["PROCEDURES"])
    check_response(response)
    table = response.tables[0]
    labels = dict()
    for row in table.tuples:
        procname = row[2]
        remarks = row[6]
        remarks_json = json.loads(remarks)
        if remarks_json.get("singlePartition") == True:
            label_p = "SP"
        else:
            label_p = "MP"
        if remarks_json.get("readOnly") == True:
            label_rw = "RO"
        else:
            label_rw = "RW"
        label = label_p + "-" + label_rw
        labels[procname] = label
    return labels

# before monitoring
proc_labels = get_proc_labels()
lasttime = dict()

# print headers
print "    time cpu                                procedure label pct  invocations   avgnanos     tps lat_millis     c"
print "-------- --- ---------------------------------------- ----- --- ------------ ---------- ------- ---------- -----"

# begin monitoring every (frequency) seconds for (duration) minutes
start_time = time.time()
end_time = start_time + args.duration * 60
while end_time > time.time():

    now = time.strftime('%X')

    # gather cpu and latency metrics
    cpu = get_cpu()
    latencies = get_latencies()

    # gather and iterate through procedureprofile statistics, calculating and printing output for each procedure executed
    for row in get_pp():

        # get columns as variables
        epochmillis = row[0]
        procname = row[1]
        perc = row[2]
        invs = row[3]
        avgnanos = row[4]

        # fix procname
        if '.' in procname:
            # get short name
            shortname = procname[procname.rindex('.')+1:] # everything after the right-most '.'
            if shortname not in ['delete','insert','select','update','upsert']:
                procname = shortname

        # get label
        label = proc_labels.get(procname)

        # compute timestamp difference
        elapsedmillis = 0
        if procname in lasttime:
            elapsedmillis = epochmillis - lasttime[procname]
        lasttime[procname] = epochmillis

        # compute tps, c_svrs
        tps = 0
        c_svrs = 0.0
        if (elapsedmillis > 0):
            tps = invs *1000 / elapsedmillis
            c_svrs = float(avgnanos) * invs / (1000000 * elapsedmillis)

        print '%8s %3d %40s %5s %3d %12d %10d %7d %10d %5.1f' % (now, cpu, procname, label, perc, invs, avgnanos, tps, latencies.get(procname,0), c_svrs)

    time.sleep(args.frequency)
