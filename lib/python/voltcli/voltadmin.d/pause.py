# This file is part of VoltDB.

# Copyright (C) 2008-2016 VoltDB Inc.
#
# This file contains original code and/or modifications of original code.
# Any modifications made by VoltDB Inc. are licensed under the following
# terms and conditions:
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

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
import time

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Pause the VoltDB cluster and switch it to admin mode.',
    options = (
        VOLT.BooleanOption('-w', '--wait', 'waiting',
                           'wait for all DR and Export transactions to be externally processed',
                           default = False)
    )
)
def pause(runner):
    # Check the STATUS column. runner.call_proc() detects and aborts on errors.
    status = runner.call_proc('@Pause', [], []).table(0).tuple(0).column_integer(0)
    if status <> 0:
        runner.error('The cluster has failed to pause with status: %d' % status)
        return
    runner.info('The cluster is paused.')
    if runner.opts.waiting:
        status = runner.call_proc('@Quiesce', [], []).table(0).tuple(0).column_integer(0)
        if status <> 0:
            runner.error('The cluster has failed to quiesce with status: %d' % status)
            return
        runner.info('The cluster is quiesced.')
        # check the dr stats
        partition_min_host = dict()
        partition_min = dict()
        partition_max = dict()
        check_dr(runner, partition_min_host, partition_min, partition_max)
        # check the export stats twice because they are periodic
        export_tables_with_data = dict()
        check_dr(runner, partition_min_host, partition_min, partition_max)
        last_table_stat_time = 0
        last_table_stat_time = check_export(runner, export_tables_with_data, last_table_stat_time)
        if not partition_min and last_table_stat_time == 1:
            # there are no outstanding export or dr transactions
            runner.info('All export and DR transactions have been processed.')
            return
        # after 10 seconds notify admin of what transactions have not drained
        notifyInterval = 10
        # have to get two samples of table stats because the cached value could be from before Quiesce
        while True:
            time.sleep(1)
            if partition_min:
                check_dr(runner, partition_min_host, partition_min, partition_max)
            if last_table_stat_time > 1:
                curr_table_stat_time = check_export(runner, export_tables_with_data, last_table_stat_time)
            if last_table_stat_time == 1 or curr_table_stat_time > last_table_stat_time:
                # have a new sample from table stat cache or there are no tables
                if not export_tables_with_data and not partition_min:
                    runner.info('All export and DR transactions have been processed.')
                    return
            notifyInterval -= 1
            if notifyInterval == 0:
                notifyInterval = 10
                if last_table_stat_time > 1 and export_tables_with_data:
                    print_export_pending(runner, export_tables_with_data)
                if partition_min:
                    print_dr_pending(runner, partition_min_host, partition_min, partition_max)

def get_stats(runner, component):
    retry = 5
    while True:
        response = runner.call_proc('@Statistics', [VOLT.FastSerializer.VOLTTYPE_STRING,
                                    VOLT.FastSerializer.VOLTTYPE_INTEGER], [component, 0])
        status = response.status()
        if status <> 1 and "timeout" in response.statusString:
            if retry == 0:
                runner.error('Unable to collect DR or export statistics from the cluster')
            else:
                sleep(1)
                retry -= 1
                continue
        if status <> 1:
            runner.error("Unexpected response to @Statistics %s: %s" % (component, resp))
        return response

def check_dr(runner, partition_min_host, partition_min, partition_max):
    resp = get_stats(runner, 'DRPRODUCER')
    partition_data = resp.table(0)
    for pid in partition_min:
        # reset all min values to find the new min
        if pid in partition_max:
            partition_min[pid] = partition_max[pid]
    for r in partition_data.tuples():
        pid = r[3]
        hostname = str(r[2])
        if str(r[8]) == 'None':
            last_queued = -1
        else:
            last_queued = r[8]
        if str(r[9]) == 'None':
            last_acked = -1
        else:
            last_acked = r[9]
        # check TOTALBYTES
        if r[5] > 0:
            # track the highest seen drId for each partition
            # use last queued to get the upper bound
            if pid in partition_max:
                partition_max[pid] = max(last_queued, partition_max[pid])
            else:
                partition_max[pid] = last_queued
            if pid in partition_min:
                if last_acked < partition_min[pid]:
                    # this replica is farther behind
                    partition_min[pid] = last_acked
            else:
                partition_min_host[pid] = set()
                partition_min[pid] = last_acked
            partition_min_host[pid].add(hostname)
        else:
            # this hostname's partition has an empty InvocationBufferQueue
            if pid in partition_min:
                # it was not empty on a previous call
                partition_min_host[pid].discard(hostname)
                if not partition_min_host[pid]:
                    del partition_min_host[pid]
                    del partition_min[pid]
            if pid in partition_max:
                if partition_max[pid] > last_acked:
                    runner.warning("DR Producer reports no data for partition %i on host %s but last acked drId (%i) does not match other hosts last acked drId (%s)" % (pid, hostname, last_acked, partition_max[pid]))
                partition_max[pid] = max(last_acked, partition_max[pid])
            else:
                partition_max[pid] = last_acked

def print_dr_pending(runner, partition_min_host, partition_min, partition_max):
    runner.info('The following partitions have pending DR transactions that the consumer cluster has not processed:')
    summaryline = "    Partition %i needs acknowledgements for drIds %i to %i on hosts: %s."
    for pid in partition_min_host:
        runner.info(summaryline % (pid, partition_min[pid]+1, partition_max[pid], ', '.join(partition_min_host[pid])))

def check_export(runner, export_tables_with_data, last_collection_time):
    resp = get_stats(runner, 'TABLE')
    export_tables = 0
    collection_time = 0
    if not resp.table_count() > 0:
        # this is an empty database and we don't need to wait for export to drain
        return 1
    else:
        tablestats = resp.table(0)
        firsttuple = tablestats.tuple(0)
        if firsttuple.column(0) == last_collection_time:
            # this statistic is the same cached set as the last call
            return last_collection_time
        else:
            collection_time = firsttuple.column(0)
    for r in tablestats.tuples():
        # first look for streaming (export) tables
        if str(r[6]) == 'StreamedTable':
            pendingData = r[8]
            tablename = str(r[5])
            pid = r[4]
            hostname = str(r[2])
            if pendingData > 0:
                if not tablename in export_tables_with_data:
                    export_tables_with_data[tablename] = dict()
                tabledata = export_tables_with_data[tablename]
                if not hostname in tabledata:
                    tabledata[hostname] = set()
                tabledata[hostname].add(pid)
            else:
                if tablename in export_tables_with_data:
                    tabledata = export_tables_with_data[tablename]
                    if hostname in tabledata:
                        tabledata[hostname].discard(pid)
                        if not tabledata[hostname]:
                            del tabledata[hostname]
                            if not export_tables_with_data[tablename]:
                                del export_tables_with_data[tablename]
    return collection_time

def print_export_pending(runner, export_tables_with_data):
    runner.info('The following export tables have unacknowledged transactions:')
    summaryline = "    %s needs acknowledgements on host(s) %s for partition(s) %s."
    for table in export_tables_with_data:
        pidlist = set()
        hostlist = list(export_tables_with_data[table].keys())
        for host in hostlist:
            pidlist = pidlist | export_tables_with_data[table][host]
        partlist = reduce(lambda a,x: a+","+str(x), list(pidlist), "")[1:]
        runner.info(summaryline % (table, ', '.join(hostlist), partlist))

