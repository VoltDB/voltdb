#!/usr/bin/env python3

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

import argparse
import json
import collections
import sys
import time

sys.path.append("../lib/python")
from voltdbclient import *

def print_usage():
    # replaces the standard argparse-generated usage to include definitions of the output columns
    return '''watch_performance.py

Output column definitions:
  utc_time:      current UTC time
  procedure:     name of each procedure that was executed in this interval
  label:         SP (single partition), MP (multi-partition), RO (read-only), RW (read-write)
  exec_pct:      percentage of the overall procedure execution workload during this interval
  invocations:   # of executed transactions in the last interval
  txn/sec:       rate of transactions in the last interval
  exec_ms:       average execution time in fractional milliseconds
                  (since the last schema change, not just for this interval)
  lat_ms:        servers-side latency in milliseconds, including wait time
                  (from the last of possibly multiple hosts that initiated this procedure)
  work:          total partition execution time / elapsed time
  cpu:           percentage CPU usage
  partitions:    fraction of partitions that executed this procedure during this interval
  skew:          coefficient of variance for the # of invocations executed by each partition
                  (0 is exactly even, > 1 is very skewed)
  inMB/s:        MB/s passed in as procedure invocation parameters
  outMB/s:       MB/s returned as results of procedure invocations
'''


parser = argparse.ArgumentParser(description="This script is used to monitor current performance metrics.", usage=print_usage())
parser.add_argument('-s', '--server', help='Hostname or IP of VoltDB server', default='localhost')
parser.add_argument('-p', '--port', help='Port number of VoltDB server', type=int, default=21211)
parser.add_argument('-u', '--username', help='User name (if security is enabled)', default='')
parser.add_argument('-pw', '--password', help='Password (if security is enabled)', default='')
parser.add_argument('-f', '--frequency', help='Frequency of gathering statistics in seconds (default = 5 seconds)', type=int, default=5)
parser.add_argument('-d', '--duration', help='Duration of gathering statistics in minutes (default = 30)', type=int, default=30)
args = parser.parse_args()

client = FastSerializer(args.server, args.port, False, args.username, args.password)

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
        print (status_codes.get(status, "No Status code was returned") + ": " + response.statusString)
        ex = response.exception
        if ex is not None:
            print (ex.typestr + ": " + ex.message)
        exit(-1)

# define procedure calls
proc_stats = VoltProcedure( client, "@Statistics", [FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_INTEGER] )
proc_catalog = VoltProcedure( client, "@SystemCatalog", [FastSerializer.VOLTTYPE_STRING] )

# function to get short name of procedure
def get_proc_name(procname):
    if '.' in procname:
        shortname = procname[procname.rindex('.')+1:] # everything after the right-most '.'
        if shortname not in ['delete','insert','select','update','upsert']:
            procname = shortname
    return procname

def get_partition_count():
    response = proc_stats.call(["PARTITIONCOUNT",0])
    check_response(response)
    table = response.tables[0]
    return table.tuples[0][3]

def get_proc_labels():
    response = proc_catalog.call(["PROCEDURES"])
    check_response(response)
    table = response.tables[0]
    labels = dict()
    for row in table.tuples:
        procname = row[2] # uses short name, so no need to call get_proc_name()
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




# Statistics gathering functions
def get_proc_stats():
    response = proc_stats.call(["PROCEDURE",0])
    check_response(response)
    table = response.tables[0]
    return table.tuples

def get_cpu():
    response = proc_stats.call(["CPU",0])
    check_response(response)
    table = response.tables[0]
    cpu_perc = table.tuples[0][3]
    return cpu_perc

def get_latencies():
    response = proc_stats.call(["INITIATOR",0])
    check_response(response)
    table = response.tables[0]
    latencies = dict()
    for row in table.tuples:
        procname = row[6]
        avg_millis = row[8]
        latencies[procname] = avg_millis
    return latencies


# Statistics calculation functions
def mean(data):
    """Return the sample arithmetic mean of data."""
    n = len(data)
    if n < 1:
        raise ValueError('mean requires at least one data point')
    return sum(data)/n

def _ss(data):
    """Return sum of square deviations of sequence data."""
    c = mean(data)
    ss = sum((x-c)**2 for x in data)
    return ss

def pstdev(data):
    """Calculates the population standard deviation."""
    n = len(data)
    if n < 2:
        raise ValueError('variance requires at least two data points')
    ss = _ss(data)
    pvar = ss/n # the population variance
    return pvar**0.5

def _min(data):
    min_value = None
    for value in data:
        if not min_value:
            min_value = value
        elif value < min_value:
            min_value = value
    return min_value

def _max(data):
    max_value = None
    for value in data:
        if not max_value:
            max_value = value
        elif value > max_value:
            max_value = value
    return max_value

# before monitoring
last_stats = dict()
partition_proc_stats = dict()
partition_stats = dict()
procedure_stats = dict()
partition_count = get_partition_count()

print ("           utc_time                                procedure label exec_pct invocations txn/sec    exec_ms  lat_ms  work cpu partitions   skew   inMB/s  outMB/s")
print ("------------------- ---------------------------------------- ----- -------- ----------- ------- ---------- ------- ----- --- ---------- ------ -------- --------")

# begin monitoring every (frequency) seconds for (duration) minutes
start_time = time.time()
end_time = start_time + args.duration * 60
proc_labels = get_proc_labels()

while end_time > time.time():

    partition_proc_stats.clear()
    partition_stats.clear()
    procedure_stats.clear()
    utc_datetime = datetime.datetime.utcnow()
    utc_now = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # gather cpu and latency metrics
    cpu = get_cpu()
    latencies = get_latencies()

    total_exec_millis = 0.0

    # gather and iterate through procedureprofile statistics, calculating and printing output for each procedure executed
    for row in get_proc_stats():

        # get columns as variables
        epochmillis = row[0]
        host_id = row[1]
        partition_id = row[4]
        procname = get_proc_name(row[5]) # convert long names to short names
        invs = row[6]
        avgnanos = row[10]
        avgbytesout = row[13]
        avgbytesin = row[16]

        # compute incremental stats
        incr_millis = 0
        incr_invs = 0
        if (host_id, partition_id, procname) in last_stats:
            prev_stats = last_stats[(host_id, partition_id, procname)]
            incr_millis = epochmillis - prev_stats[0]
            incr_invs = invs - prev_stats[1]
        last_stats[(host_id, partition_id, procname)] = (epochmillis, invs)


        # compute tps, work
        tps = 0
        exec_millis = 0
        work = 0.0
        mbin = 0.0
        mbout = 0.0
        if (incr_millis > 0):
            tps = incr_invs *1000 / incr_millis
            exec_millis = float(avgnanos) * incr_invs / 1000000
            work = exec_millis / incr_millis
            mbin = float(avgbytesin*incr_invs)/(incr_millis*1000)
            mbout = float(avgbytesout*incr_invs)/(incr_millis*1000)


        new_values = (incr_invs, tps, exec_millis, work, mbin, mbout)

        if (incr_invs > 0 and exec_millis > 0):
            if (procname, partition_id) in partition_proc_stats:
                pass # do nothing
            else:
                total_exec_millis += exec_millis
                partition_proc_stats[(procname, partition_id)] = new_values
                if procname in procedure_stats:
                    procedure_stats[procname] = tuple(sum(x) for x in zip(procedure_stats[procname],new_values))
                else:
                    procedure_stats[procname] = new_values
                if partition_id in partition_stats:
                    partition_stats[partition_id] = tuple(sum(x) for x in zip(partition_stats[partition_id],new_values))
                else:
                    partition_stats[partition_id] = new_values

    procs_sort = list(procedure_stats.items())
    procs_sort.sort(key=lambda x:x[1][3], reverse=True) # sort procedures by exec_millis (highest first)
    for row in procs_sort:
        procname = row[0]
        invs, tps, exec_millis, work, mbin, mbout = row[1]
        avgms = exec_millis/invs
        exec_pct = 100 * exec_millis / total_exec_millis
        label = proc_labels.get(procname)
        latency = latencies.get(procname,0)

        # calculate skew
        tps_list = []
        min_tps = None
        max_tps = None
        partitions_used = 0
        for p in range(partition_count):
            if (procname, p) in partition_proc_stats:
                tps_list.append(partition_proc_stats[(procname,p)][1])
                partitions_used += 1
            else:
                tps_list.append(0)
        tps_stdev = pstdev(tps_list)
        tps_mean = mean(tps_list)
        tps_coeff_var = 0 # skew of distribution across partitions
        if tps_mean > 0:
            tps_coeff_var = tps_stdev / tps_mean

        partitions_used_string = str(partitions_used) + "/" + str(partition_count)

        print ('%19s %40s %5s %8.1f %11d %7d %10.3f %7d %5.2f %3d %10s %6.3f %8.3f %8.3f' % (utc_now, procname, label, exec_pct, invs, tps, avgms, latency, work, cpu, partitions_used_string, tps_coeff_var, mbin, mbout))

    time.sleep(args.frequency)
