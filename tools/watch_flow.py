#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2017 VoltDB Inc.
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
import datetime

sys.path.append(os.path.join(os.path.dirname(sys.path[0]),"lib","python"))
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
            self.client = FastSerializer(args.server, args.port, args.username, args.password)
        except socket.error,e:
            print "Can't connect to " + args.server + ":" + str(args.port)
            print str(e)
            exit(-1)

        self.stats_caller = VoltProcedure( self.client, "@Statistics", [FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_INTEGER] )
        print "Connected to VoltDB server: " + args.server + ":" + str(args.port)

    def check_response(self,response):
        '''procedure call response error handling'''
        status = response.status
        ex = response.exception
        if status != 1:
            print status_codes.get(status, "No Status code was returned") + ": " + response.statusString
        if ex is not None:
            print ex.typestr + ": " + ex.message
            exit(-1)

    def call_stats(self,component):
        '''Call @Statistics and check response'''
        response = self.stats_caller.call([component,0])
        self.check_response(response)
        return response.tables

class ImporterStatsKeeper:
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


class TableStatsKeeper:
    '''Process TABLE stats'''

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
            diff_allocated_memory = 0
            key = (host_id, site_id, table_name)
            if key in self.last_stats:
                prev_stats = self.last_stats[key]
                diff_tuples = tuple_count - prev_stats[0]
                diff_allocated_memory = tuple_allocated_memory - prev_stats[1]
            self.last_stats[key] = (tuple_count, tuple_allocated_memory)

            # de-dup
            key = (partition_id, table_name)
            value = (diff_tuples, diff_allocated_memory)
            if (table_type == "StreamedTable"):
                value = (diff_tuples, tuple_allocated_memory) # for streams, allocated memory is a gauge, not a counter
            if key in self.dedup_stats:
                pass # do nothing
            else:
                self.dedup_stats[key] = value
                # aggregate metrics
                k = table_type
                if k in self.agg_stats:
                    self.agg_stats[k] = tuple(sum(x) for x in zip(self.agg_stats[k],value))
                else:
                    self.agg_stats[k] = value

        return self.agg_stats

class ProcedureStatsKeeper:
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


def agg_cpu(table):
    cpu_level = 0
    cnt = 0
    for row in table.tuples:
        cpu_level += row[3]
        cnt += 1
    return cpu_level/cnt

def agg_liveclients(table):
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
    new_tuples, new_alloc = data.get("PersistentTable",(0,0))
    streamrows, buffered = data.get("StreamedTable",(0,0))
    invs, tps, exec_millis, c_svrs, mbin, mbout = data["PROCEDURE"]
    connections, outstanding_tx = data["LIVECLIENTS"]

    if (invs >= 0):
        print '%19s %3d %10d %10d %10d %7d %10d %11d %7d %5.2f %10d %10d %10d %8.3f %8.3f' % (
            utc_now, cpu, incr_successes, incr_failures, outstanding, connections, outstanding_tx, invs, tps, c_svrs, new_tuples, streamrows, buffered, mbin, mbout)

def print_header():
    print "           utc_time cpu   imported   failures im pending clients cl pending invocations txn/sec     c new_tuples   streamed   buffered   inMB/s  outMB/s"
    print "------------------- --- ---------- ---------- ---------- ------- ---------- ----------- ------- ----- ---------- ---------- ---------- -------- --------"
    #      2017-03-03 15:54:51


parser = argparse.ArgumentParser(description="This script is used to monitor current performance metrics.")
parser.add_argument('-s', '--server', help='Hostname or IP of VoltDB server', default='localhost')
parser.add_argument('-p', '--port', help='Port number of VoltDB server', type=int, default=21211)
parser.add_argument('-u', '--username', help='User name (if security is enabled)', default='')
parser.add_argument('-pw', '--password', help='Password (if security is enabled)', default='')
parser.add_argument('-f', '--frequency', help='Frequency of gathering statistics in seconds (default = 10 seconds)', type=int, default=10)
parser.add_argument('-d', '--duration', help='Duration of gathering statistics in minutes (default = 50000)', type=int, default=50000)
args = parser.parse_args()

caller = ProcedureCaller(args)
imp_keeper = ImporterStatsKeeper()
table_keeper = TableStatsKeeper()
proc_keeper = ProcedureStatsKeeper()
print_header()

# begin monitoring every (frequency) seconds for (duration) minutes
start_time = time.time()
end_time = start_time + args.duration * 60
while end_time > time.time():

    utc_datetime = datetime.datetime.utcnow()
    utc_now = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")

    imp_tables = caller.call_stats("IMPORTER")
    cpu_tables = caller.call_stats("CPU")
    table_tables = caller.call_stats("TABLE")
    proc_tables = caller.call_stats("PROCEDURE")
    liveclients_tables = caller.call_stats("LIVECLIENTS")

    data = dict()
    imp_data = imp_keeper.process(imp_tables[0])
    data.update(imp_data)

    table_data = table_keeper.process(table_tables[0])
    data.update(table_data)

    proc_data = proc_keeper.process(proc_tables[0])
    data.update(proc_data)

    cpu_level = agg_cpu(cpu_tables[0])
    data["CPU"] = cpu_level

    client_data = agg_liveclients(liveclients_tables[0])
    data["LIVECLIENTS"] = client_data

    #print data
    print_metrics(data)

    time.sleep(args.frequency)
