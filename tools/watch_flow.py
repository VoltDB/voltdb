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
import datetime
import socket

sys.path.append("../lib/python")
from voltdbclient import *

class ProcedureCaller:
    '''Creates a client and has methods to call procedures and check responses.'''
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

    def __init__(self, args):
        try:
            self.client = FastSerializer(args.server, args.port, False, args.username, args.password)
        except socket.error:
            print ("Can't connect to " + args.server + ":" + str(args.port))
            exit(-1)

        self.stats_caller = VoltProcedure( self.client, "@Statistics", [FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_INTEGER] )
        self.querystats_caller = VoltProcedure( self.client, "@QueryStats", [FastSerializer.VOLTTYPE_STRING] )
        print ("Connected to VoltDB server: " + args.server + ":" + str(args.port))

    def check_response(self,response):
        '''procedure call response error handling'''
        status = response.status
        ex = response.exception
        if status != 1:
            print (status_codes.get(status, "No Status code was returned") + ": " + response.statusString)
        if ex is not None:
            print (ex.typestr + ": " + ex.message)
            exit(-1)

    def call_stats(self,component):
        '''Call @Statistics and check response'''
        response = self.stats_caller.call([component,0])
        self.check_response(response)
        return response.tables

    def call_querystats(self,query):
        '''Call @QueryStats and check response'''
        response = self.querystats_caller.call([query])
        self.check_response(response)
        return response.tables


class ImporterStatsTracker:
    '''Process importer stats'''

    def __init__(self):
        self.last_stats = dict()
        self.by_procedure_stats = dict()
        self.agg_stats = dict()

    def process(self,table):
        self.by_procedure_stats.clear()
        self.agg_stats.clear()


        for row in table.tuples:

            # get columns as variables
            epochmillis = row[0]
            host_id = row[1]
            host_name = row[2]
            site_id = row[3]
            importer_name = row[4]
            procname = row[5]
            successes = row[6]
            failures = row[7]
            outstanding = row[8]
            retries = row[9]

            # compute increments from last values
            incr_successes = 0
            incr_failures = 0
            incr_retries = 0
            if (host_id, site_id, procname) in self.last_stats:
                prev_stats = self.last_stats[(host_id, site_id, procname)]
                incr_successes = successes - prev_stats[0]
                incr_failures = failures - prev_stats[1]
                incr_retries = retries - prev_stats[2]
            self.last_stats[(host_id, site_id, procname)] = (successes, failures, retries)

            new_values = (incr_successes, incr_failures, outstanding, incr_retries)

            # aggregate metrics
            key = "importer"
            if key in self.agg_stats:
                self.agg_stats[key] = tuple(sum(x) for x in zip(self.agg_stats[key],new_values))
            else:
                self.agg_stats[key] = new_values

        return self.agg_stats

class CounterDiffTracker:
    '''class to calculate diff from previous counter value'''

    def __init__(self):
        self.last_val = 0

    def calc(self,new_value):
        diff = new_value - self.last_val
        self.last_val = new_value
        return diff



class TableStatsTracker:
    '''Process TABLE stats'''

    def __init__(self):
        self.last_stats = dict()
        self.dedup_stats = dict()
        self.agg_stats = dict()

    def process(self,table):
        self.agg_stats.clear()
        self.dedup_stats.clear()

        new_tuples = 0
        new_streamed = 0
        stream_buffered = 0

        for row in table.tuples:

            # get columns as variables
            epochmillis = row[0]
            host_id = row[1]
            host_name = row[2]
            site_id = row[3]
            partition_id = row[4]
            table_name = row[5]
            table_type = row[6]
            tuple_count = row[7]
            tuple_allocated_memory = row[8]
            tuple_data_memory = row[9]
            string_data_memory = row[10]
            tuple_limit = row[11]
            percent_full = row[12]

            # compute increments from last values
            diff_tuples = 0
            key = (host_id, site_id, table_name)
            if key in self.last_stats:
                prev_stats = self.last_stats[key]
                diff_tuples = tuple_count - prev_stats[0]
            self.last_stats[key] = (tuple_count,0)

            # add up stream_buffered_kb for every site
            if (table_type == "StreamedTable"):
                stream_buffered += tuple_allocated_memory

            # de-dup
            key = (partition_id, table_name)
            if key in self.dedup_stats:
                pass # do nothing
            else:
                self.dedup_stats[key] = 1

                # add non-duplicate metrics to totals
                if (table_type == "StreamedTable"):
                    new_streamed += diff_tuples
                else:
                    new_tuples += diff_tuples

        self.agg_stats["TABLE"] = (new_tuples, new_streamed, stream_buffered)
        return self.agg_stats

class ProcedureStatsTracker:
    '''Process PROCEDURE stats'''

    def __init__(self):
        self.last_stats = dict()
        self.dedup_stats = dict()
        self.agg_stats = dict()

    def process(self,table):
        self.agg_stats.clear()
        self.dedup_stats.clear()

        for row in table.tuples:

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
            key = (host_id, partition_id, procname)
            if key in self.last_stats:
                prev_stats = self.last_stats[key]
                incr_millis = epochmillis - prev_stats[0]
                incr_invs = invs - prev_stats[1]
            self.last_stats[key] = (epochmillis, invs)

            # de-dup
            key = (procname,partition_id)
            value = (incr_millis,incr_invs)
            if key in self.dedup_stats:
                pass # do nothing
            else:
                self.dedup_stats[key] = value
                # calculate
                tps = 0
                exec_millis = 0
                c_svrs = 0.0
                mbin = 0.0
                mbout = 0.0
                if (incr_millis > 0):
                    tps = incr_invs *1000 / incr_millis
                    exec_millis = float(avgnanos) * incr_invs / 1000000
                    c_svrs = exec_millis / incr_millis
                    mbin = float(avgbytesin*incr_invs)/(incr_millis*1000)
                    mbout = float(avgbytesout*incr_invs)/(incr_millis*1000)

                # aggregate
                k = "PROCEDURE"
                new_values = (incr_invs, tps, exec_millis, c_svrs, mbin, mbout)
                if k in self.agg_stats:
                    self.agg_stats[k] = tuple(sum(x) for x in zip(self.agg_stats[k],new_values))
                else:
                    self.agg_stats[k] = new_values

        return self.agg_stats



def get_proc_name(procname):
    if '.' in procname:
        shortname = procname[procname.rindex('.')+1:] # everything after the right-most '.'
        if shortname not in ['delete','insert','select','update','upsert']:
            procname = shortname
    return procname


def agg_avg_cpu(table):
    cpu_level = 0
    cnt = 0
    for row in table.tuples:
        cpu_level += row[3]
        cnt += 1
    return cpu_level/cnt

def agg_sum_liveclients(table):
    outstanding_tx = 0
    connections = 0
    for row in table.tuples:
        outstanding_tx += row[8]
        connections += 1
    return connections, outstanding_tx

def print_metrics(data):
    # get variables from data dictionary (separate entries from different sources)
    incr_successes, incr_failures, outstanding, incr_retries = data.get("importer",(0,0,0,0))
    cpu = data["CPU"]
    new_tuples, streamrows, buffered = data.get("TABLE",(0,0,0))
    invs, tps, exec_millis, c_svrs, mbin, mbout = data.get("PROCEDURE",(0,0,0,0,0,0))
    connections, outstanding_tx = data.get("LIVECLIENTS",(0,0))
    streamrows = data["EXPORT_INSERTS"]
    buffered = data["EXPORT_PENDING"]

    if (invs >= 0):
        print ('%19s %3d %10d %10d %10d %7d %10d %11d %7d %5.2f %10d %10d %10d %8.3f %8.3f' % (
            utc_now, cpu, incr_successes, incr_failures, outstanding, connections, outstanding_tx, invs, tps, c_svrs, new_tuples, streamrows, buffered, mbin, mbout))

def print_header():
    print ("                       |----------- Importer -----------|                                                       |------ Exporter -----|")
    print ("           utc_time cpu    records   failures    pending clients   requests invocations txn/sec  work new_tuples    inserts    pending   inMB/s  outMB/s")
    print ("------------------- --- ---------- ---------- ---------- ------- ---------- ----------- ------- ----- ---------- ---------- ---------- -------- --------")

def print_usage():
    # replaces the standard argparse-generated usage to include definitions of the output columns
    return '''watch_flow.py

Output column definitions:
  utc_time:      current UTC time
  cpu:           percentage CPU usage
  Importer:
   records:      # of records imported successfully (committed) in the last interval
   failures:     # of importer failures in the last interval (rollback + fail to invoke procedure)
   pending:      # of outstanding procedure calls for all importers
  clients:       # of client connections
  requests:      # of outstanding requests from clients
  invocations:   # of executed transactions in the last interval
  txn/sec:       rate of transactions in the last interval
  work:          total partition execution time / elapsed time
  new_tuples:    net change to # of records in all tables
  Exporter:
   inserts:      # of records inserted into streams
   pending:      # of records pending export output
  inMB/s:        MB/s passed in as procedure invocation parameters
  outMB/s:       MB/s returned as results of procedure invocations
'''

parser = argparse.ArgumentParser(description="This script outputs a periodic aggregation of statistics to show the changing levels of database activity over time.  It outputs to STDOUT, which you can redirect or tee to a file.", usage=print_usage())
parser.add_argument('-s', '--server', help='Hostname or IP of VoltDB server (default=%(default)s)', default='localhost')
parser.add_argument('-p', '--port', help='Port number of VoltDB server (default=%(default)s)', type=int, default=21211)
parser.add_argument('-u', '--username', help='User name (if security is enabled)', default='')
parser.add_argument('-pw', '--password', help='Password (if security is enabled)', default='')
parser.add_argument('-f', '--frequency', help='Frequency of gathering statistics in seconds (default=%(default)s)', type=int, default=10)
parser.add_argument('-d', '--duration', help='Duration of gathering statistics in minutes (default=%(default)s)', type=int, default=50000)
args = parser.parse_args()

caller = ProcedureCaller(args)
imp_tracker = ImporterStatsTracker()
table_tracker = TableStatsTracker()
proc_tracker = ProcedureStatsTracker()
export_rows_tracker = CounterDiffTracker()
print_header()

# begin monitoring every (frequency) seconds for (duration) minutes
start_time = time.time()
end_time = start_time + args.duration * 60
lines_output = 0
while end_time > time.time():

    utc_datetime = datetime.datetime.utcnow()
    utc_now = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # Get Stats
    imp_tables = caller.call_stats("IMPORTER")
    cpu_tables = caller.call_stats("CPU")
    table_tables = caller.call_stats("TABLE")
    proc_tables = caller.call_stats("PROCEDURE")
    liveclients_tables = caller.call_stats("LIVECLIENTS")
    export_tables = caller.call_querystats("select coalesce(sum(tuple_count),0) as tuples, coalesce(sum(tuple_pending),0) as pending from statistics(export,0) where status='ACTIVE'")

    # Process stats
    imp_data = imp_tracker.process(imp_tables[0])
    table_data = table_tracker.process(table_tables[0])
    proc_data = proc_tracker.process(proc_tables[0])
    cpu_level = agg_avg_cpu(cpu_tables[0])
    client_data = agg_sum_liveclients(liveclients_tables[0])
    export_inserts = export_rows_tracker.calc(export_tables[0].tuples[0][0])

    # Add processing results to "data" for output
    data = dict()
    data.update(imp_data)
    data.update(table_data)
    data.update(proc_data)
    data["CPU"] = cpu_level
    data["LIVECLIENTS"] = client_data
    data["EXPORT_INSERTS"] = export_inserts
    data["EXPORT_PENDING"] = export_tables[0].tuples[0][1]

    # skip output on first round, stats need a baseline
    if (lines_output > 0):
        print_metrics(data)
    lines_output += 1

    # sleep until next round
    time.sleep(args.frequency)
