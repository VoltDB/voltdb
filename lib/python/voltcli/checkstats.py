# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

import sys
import time
import voltdbclient

from functools import reduce

def check_exporter(runner):
    runner.info('Completing outstanding exporter transactions...')
    last_table_stat_time = 0
    export_tables_with_data = dict()
    last_table_stat_time = check_export_stats(runner, export_tables_with_data, last_table_stat_time)
    if last_table_stat_time == 1:
        # there are no outstanding export transactions
        runner.info('All exporter transactions have been processed.')
        return
    # after 10 seconds notify admin of what transactions have not drained
    notifyInterval = 10
    # have to get two samples of table stats because the cached value could be from before Quiesce
    last_export_tables_with_data = dict()
    lastUpdatedTime = time.time()
    while True:
        time.sleep(1)
        if last_table_stat_time > 1:
            curr_table_stat_time = check_export_stats(runner, export_tables_with_data, last_table_stat_time)
            if last_table_stat_time == 1 or curr_table_stat_time == 1 or curr_table_stat_time > last_table_stat_time:
                last_table_stat_time = curr_table_stat_time
                # have a new sample from table stat cache or there are no tables
                if not export_tables_with_data:
                    runner.info('All exporter transactions have been processed.')
                    return
            notifyInterval -= 1
            if notifyInterval == 0:
                notifyInterval = 10
                if last_table_stat_time > 1 and export_tables_with_data:
                    print_export_pending(runner, export_tables_with_data)
        lastUpdatedTime = monitorStatisticsProgress(last_export_tables_with_data, export_tables_with_data, lastUpdatedTime, runner, 'Exporter')
        last_export_tables_with_data = export_tables_with_data.copy()


def check_dr_producer(runner):
    runner.info('Completing outstanding DR producer transactions...')
    partition_min_host = dict()
    partition_min = dict()
    partition_max = dict()
    partition_gap_min = dict()
    last_partition_min = dict()
    last_partition_max = dict()
    lastUpdatedTime = time.time()
    dr_producer_stats(runner, partition_min_host, partition_min, partition_max, partition_gap_min)
    if not partition_min:
        # there are no outstanding export or dr transactions
        runner.info('All DR producer transactions have been processed.')
        return
    # after 10 seconds notify admin of what transactions have not drained
    notifyInterval = 10
    # have to get two samples of table stats because the cached value could be from before Quiesce
    while True:
        time.sleep(1)
        if partition_min:
            dr_producer_stats(runner, partition_min_host, partition_min, partition_max, partition_gap_min)
            if not partition_min:
                runner.info('All DR producer transactions have been processed.')
                return
            notifyInterval -= 1
            if notifyInterval == 0:
                notifyInterval = 10
                if partition_min:
                    print_dr_pending(runner, partition_min_host, partition_min, partition_max, partition_gap_min)
        lastUpdatedTime = monitorDRProducerStatisticsProgress(last_partition_min, last_partition_max, partition_min, partition_max, lastUpdatedTime, runner)
        last_partition_min = partition_min.copy()
        last_partition_max = partition_max.copy()


def monitorDRProducerStatisticsProgress(lastPartitionMin, lastPartitionMax, currentPartitionMin,
                             currentPartitionMax, lastUpdatedTime, runner):
    currentTime = time.time()
    timeout = runner.opts.timeout
    # any stats progress?
    partitionMinProgressed = (lastPartitionMin != currentPartitionMin)
    partitionMaxProgressed = (lastPartitionMax != currentPartitionMax)
    # stats moved
    if partitionMinProgressed or partitionMaxProgressed:
        return currentTime

    timeSinceLastUpdate = currentTime - lastUpdatedTime
    # stats timeout
    if timeSinceLastUpdate > timeout:
         msg = "The cluster has not drained any transactions for DRPRODUCER in last %d seconds. There are outstanding transactions."
         raise StatisticsProcedureException(msg % (timeout), 1)
    # stats has not been moved but not timeout yet
    return lastUpdatedTime


def get_stats(runner, component):
    retry = 5
    while retry > 0:
        retry -= 1
        resp = runner.call_proc('@Statistics', [voltdbclient.FastSerializer.VOLTTYPE_STRING,
                                    voltdbclient.FastSerializer.VOLTTYPE_INTEGER], [component, 0], False)
        status = resp.status()
        if status == 1:
            return resp
        # procedure timeout, retry
        if status == -6:
            time.sleep(1)
        else:
            raise StatisticsProcedureException("Unexpected errors to collect statistics for %s: %s." % (component, resp.response.statusString), 1, False)
        if retry == 0:
            raise StatisticsProcedureException("Unable to collect statistics for %s after 5 attempts." % component, 1, False)


def dr_producer_stats(runner, partition_min_host, partition_min, partition_max, partition_gap_min):
    resp = get_stats(runner, 'DRPRODUCER')
    partition_data = resp.table(0)
    for pid in partition_min:
        # reset all min values to find the new min
        if pid in partition_max:
            partition_min[pid] = partition_max[pid]
    if len(partition_data.tuples()) == 0:
        return
    for row in partition_data.tuples():
        pid = row[5]
        hostname = str(row[2])
        if str(row[10]) == 'None':
            last_queued = -1
        else:
            last_queued = row[10]
        if str(row[11]) == 'None':
            last_acked = -1
        else:
            last_acked = row[11]
        if str(row[16]) == 'None':
            queue_gap = 0
        else:
            queue_gap = row[16]

        # Initial state, no transactions are queued and acknowledged.
        if last_queued == -1 and last_acked == -1:
            continue

        # check TOTALBYTES
        if row[7] > 0 or queue_gap != 0:
            # track the highest seen drId for each partition. use last queued to get the upper bound
            if pid in partition_max:
                partition_max[pid] = max(last_queued, partition_max[pid])
            else:
                partition_max[pid] = last_queued
            if pid in partition_gap_min:
                # if queue_gap == 0 and last_acked == -1 do nothing because without a real ack the gap value is meaningless
                if queue_gap != 0 or last_acked != -1:
                    partition_gap_min[pid] = min(queue_gap, partition_gap_min[pid])
            else:
                partition_gap_min[pid] = queue_gap
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
            # set last queued equal to last acked
            if pid in partition_max:
                if partition_max[pid] > last_acked:
                    runner.warning("DR Producer reports no data for partition %i on host %s but last acked drId (%i) does not match other hosts last acked drId (%s)" % (pid, hostname, last_acked, partition_max[pid]))
                partition_max[pid] = max(last_acked, partition_max[pid])
            else:
                partition_max[pid] = last_acked


def print_dr_pending(runner, partition_min_host, partition_min, partition_max, partition_gap_min):
    runner.info('The following partitions have pending DR transactions that the consumer cluster has not processed:')
    summarylineUnacked = "    Partition %i needs acknowledgement for drIds %i to %i on hosts: %s."
    summarylineGap = "    Partition %i has a gap of %i missing drIds that exist on a host that is currently offline."
    for pid in partition_min_host:
        if pid in partition_gap_min and partition_gap_min[pid] > 0:
            runner.info(summarylineGap % (pid, partition_gap_min[pid]))
        else:
            runner.info(summarylineUnacked % (pid, partition_min[pid] + 1, partition_max[pid], ', '.join(partition_min_host[pid])))


def check_export_stats(runner, export_tables_with_data, last_collection_time):
    resp = get_stats(runner, 'EXPORT')
    export_tables = 0
    collection_time = 0
    if not resp.table_count() > 0:
        # this is an empty database and we don't need to wait for export to drain
        export_tables_with_data.clear()
        return 1
    else:
        tablestats = resp.table(0)
        if len(tablestats.tuples()) == 0:
            export_tables_with_data.clear()
            return 1
        firsttuple = tablestats.tuple(0)
        if firsttuple.column(0) == last_collection_time:
            # this statistic is the same cached set as the last call
            return last_collection_time
        else:
            collection_time = firsttuple.column(0)

    export_tables_with_data.clear()
    for r in tablestats.tuples():
        # TUPLE_PENDING
        pendingData = r[9]
        # SOURCE
        tablename = str(r[5])
        # PARTITION_ID
        pid = r[4]
        # HOSTNAME
        hostname = str(r[2])
        if pendingData > 0:
            if not tablename in export_tables_with_data:
                export_tables_with_data[tablename] = dict()
            tabledata = export_tables_with_data[tablename]
            if not hostname in tabledata:
                tabledata[hostname] = set()
            tabledata[hostname].add(pid)
    return collection_time


def print_export_pending(runner, export_tables_with_data):
    runner.info('\tThe following export tables have unacknowledged transactions:')
    summaryline = "    %s needs acknowledgement on host %s for partition %s."
    for table in export_tables_with_data:
        pidlist = set()
        hostlist = list(export_tables_with_data[table].keys())
        for host in hostlist:
            pidlist = pidlist | export_tables_with_data[table][host]
        partlist = reduce(lambda a, x: a + "," + str(x), list(pidlist), "")[1:]
        runner.info(summaryline % (table, ', '.join(hostlist), partlist))


def check_clients(runner):
     runner.info('Completing outstanding client transactions...')
     lastUpdatedTime = time.time()
     lastValidationParamms = [0, 0, 0]
     notifyInterval = 10
     while True:
        resp = get_stats(runner, 'LIVECLIENTS')
        trans = 0
        bytes = 0
        msgs = 0
        for r in resp.table(0).tuples():
            bytes += r[6]
            msgs += r[7]
            trans += r[8]
            notifyInterval -= 1
            if notifyInterval == 0:
                notifyInterval = 10
                runner.info('\tOutstanding transactions=%d, Outstanding request bytes=%d, Outstanding response messages=%d' % (trans, bytes, msgs))
        if trans == 0 and bytes == 0 and msgs == 0:
            return
        currentValidationParams = [trans, bytes, msgs]
        lastUpdatedTime = monitorStatisticsProgress(lastValidationParamms, currentValidationParams, lastUpdatedTime, runner, 'LIVECLIENTS')
        lastValidationParamms = [trans, bytes, msgs]
        time.sleep(1)


def check_importer(runner):
     runner.info('Completing outstanding importer requests...')
     lastUpdatedTime = time.time()
     lastValidationParamms = [0]
     notifyInterval = 10
     while True:
        resp = get_stats(runner, 'IMPORTER')
        outstanding = 0
        if len(resp.table(0).tuples()) == 0:
            return
        for r in resp.table(0).tuples():
            outstanding += r[8]

        notifyInterval -= 1
        if notifyInterval == 0:
            notifyInterval = 10
            runner.info('\tOutstanding importer requests=%d' % (outstanding))
        if outstanding == 0:
            return
        currentValidationParams = [outstanding]
        lastUpdatedTime = monitorStatisticsProgress(lastValidationParamms, currentValidationParams, lastUpdatedTime, runner, 'IMPORTER')
        lastValidationParamms = [outstanding]
        time.sleep(1)


def check_dr_consumer(runner):
     runner.info('Completing outstanding DR consumer transactions...')
     lastUpdatedTime = time.time()
     lastValidationParamms = dict()
     notifyInterval = 10
     while True:
        resp = get_stats(runner, 'DRCONSUMER')
        outstanding = 0
        if len(resp.table(1).tuples()) == 0:
            return
        # DR consumer stats
        # column 8: The timestamp of the last transaction received from the producer for the partition
        # column 9: The timestamp of the last transaction successfully applied to this partition on the consumer
        # If the two timestamps are the same, all the transactions have been applied for the partition.
        notifyInterval -= 1
        currentValidationParams = dict()
        for r in resp.table(1).tuples():
            if r[8] != r[9]:
                outstanding += 1
                currentValidationParams[str(r[1]) + '-' + str(r[5])] = "%s-%s" % (r[8], r[9])
                if notifyInterval == 0:
                    runner.info('\tPartition %d on host %d has outstanding DR consumer transactions. last received: %s, last applied:%s' % (r[5], r[1], r[8], r[9]))
        if outstanding == 0:
            return
        if notifyInterval == 0:
            notifyInterval = 10
        lastUpdatedTime = monitorStatisticsProgress(lastValidationParamms, currentValidationParams, lastUpdatedTime, runner, 'DRCONSUMER')
        lastValidationParamms = currentValidationParams.copy()
        time.sleep(1)


def check_no_dr_consumer(runner, forDrop=True):
    runner.info('Checking dr consumers...')
    last_node_dispatcher = []
    last_updated_time = time.time()
    notify_interval = 10
    while True:
        resp = get_stats(runner, 'DRCONSUMERNODE')
        node_dispatcher = [row[1:6] for row in resp.table(0).tuples()]
        if not node_dispatcher:
            return True
        elif not forDrop:
            return False
        notify_interval -= 1
        if notify_interval == 0:
            notify_interval = 10
            print_dr_consumer(runner, node_dispatcher)
        last_updated_time = monitorStatisticsProgress(last_node_dispatcher, node_dispatcher, last_updated_time, runner, 'connected clusters',
                                    msg="The cluster has not sent reset to all %s in last %d seconds. ")
        last_node_dispatcher = node_dispatcher[:]
        time.sleep(1)


def print_dr_consumer(runner, node_dispatcher):
    runner.info('The following dispatchers haven\'t successfully received acknowledgment of reset:')
    summaryline = "    Dispatcher on host id % s for remote cluster %i."
    for dispatcher in node_dispatcher:
        runner.info(summaryline % (dispatcher[0], dispatcher[3]))


def check_command_log(runner):
    runner.info('Completing outstanding Command Log transactions...')
    lastUpdatedTime = time.time()
    lastValidationParamms = [0, 0]
    notifyInterval = 10
    while True:
        resp = get_stats(runner, 'COMMANDLOG')
        outstandingByte = 0
        outstandingTxn = 0
        if len(resp.table(0).tuples()) == 0:
            return
        # Command log stats
        # column 3: OUTSTANDING_BYTES for a host
        # column 4: OUTSTANDING_TXNS for a host
        # The sum of both should be zero
        for r in resp.table(0).tuples():
            outstandingByte += r[3]
            outstandingTxn += r[4]

        if outstandingByte == 0 and outstandingTxn == 0:
            return
        if notifyInterval == 0:
            notifyInterval = 10
            runner.info('\tOutstanding command log bytes = %d and transactions = %d.' % (outstandingByte, outstandingTxn))
        currentValidationParams = [outstandingByte, outstandingTxn]
        lastUpdatedTime = monitorStatisticsProgress(lastValidationParamms, currentValidationParams, lastUpdatedTime, runner, 'COMMANDLOG')
        lastValidationParamms = [outstandingByte, outstandingTxn]
        time.sleep(1)


def monitorStatisticsProgress(lastUpdatedParams, currentParams, lastUpdatedTime, runner, component, msg="The cluster has not drained any transactions for %s in last %d seconds. There are outstanding transactions."):
    currentTime = time.time()
    timeout = runner.opts.timeout
    statsProgressed = (lastUpdatedParams != currentParams)

    # stats progressed, update lastUpdatedTime
    if statsProgressed:
        return currentTime

    # stats has not made any progress since last check
    timeSinceLastUpdate = currentTime - lastUpdatedTime

    # stats timeout
    if timeout > 0 and timeSinceLastUpdate > timeout:
         raise StatisticsProcedureException(msg % (component, timeout), 1)

    # not timeout yet
    return lastUpdatedTime

def check_partition_leaders_on_host(runner, hostid):
    lastUpdatedTime = time.time()
    notifyInterval = 10
    lastValidationParamms = [sys.maxsize]
    while True:
        resp = get_stats(runner, 'TOPO')
        if len(resp.table(0).tuples()) == 0:
            return
        # TOPO stats
        # column 0: partition id
        # column 2: partition leader
        leaders = 0;
        for r in resp.table(0).tuples():
            if r[0] == 16383:
                continue
            leader = int(r[2].split(":")[0])
            if leader == hostid:
                leaders +=1

        # all partition leaders have been moved
        if leaders == 0:
            return

        notifyInterval -= 1
        if notifyInterval == 0:
            notifyInterval = 10
            runner.info('\tThe number of partition leaders on the host: %d' % (leaders))

        lastUpdatedTime = monitorStatisticsProgress(lastValidationParamms, [leaders], lastUpdatedTime, runner, str(hostid),
                                                    msg="The cluster has not moved any partition leaders away from host %s in %d seconds.")
        lastValidationParamms = [leaders]
        time.sleep(1)

def check_export_mastership_on_host(runner, hostid):
    lastUpdatedTime = time.time()
    notifyInterval = 10
    lastValidationParamms = [sys.maxsize]
    while True:
        resp = get_stats(runner, 'EXPORT')
        if len(resp.table(0).tuples()) == 0:
            return
        # EXPORT stats
        # column 1: host id
        # column 7: active
        mastershipCount = 0;
        for r in resp.table(0).tuples():
            host = int(r[1])
            active = r[7]
            if host == hostid and active == 'TRUE':
                mastershipCount +=1

        # all partition leaders have been moved
        if mastershipCount == 0:
            return

        notifyInterval -= 1
        if notifyInterval == 0:
            notifyInterval = 10
            runner.info('\tThe number of export masters on the host: %d' % (mastershipCount))

        lastUpdatedTime = monitorStatisticsProgress(lastValidationParamms, [mastershipCount], lastUpdatedTime, runner, str(hostid),
                                                    msg="The cluster has not moved any export masters away from host %s in %d seconds.")
        lastValidationParamms = [mastershipCount]
        time.sleep(1)

class StatisticsProcedureException(Exception):

    def __init__(self, message, exitCode, isTimeout=True):
       self.message = message
       self.exitCode = exitCode
       self.isTimeout = isTimeout
