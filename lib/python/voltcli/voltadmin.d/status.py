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

from voltcli.hostinfo import Host
from voltcli.hostinfo import Hosts
from voltcli import checkstats
from voltcli.clusterinfo import Cluster
from datetime import datetime
import sys
import time
import subprocess
import json
from voltcli.checkstats import StatisticsProcedureException

RELEASE_MAJOR_VERSION = 7
RELEASE_MINOR_VERSION = 2

RESIZE_MAJOR_VERSION = 9
RESIZE_MINOR_VERSION = 1

@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description="Show status of current cluster and remote cluster(s) it connects to",
    options=(
            VOLT.BooleanOption('-c', '--continuous', 'continuous', 'continuous listing', default=False),
            VOLT.BooleanOption('-j', '--json', 'json', 'print out JSON format instead of plain text', default=False),
            VOLT.BooleanOption(None, '--dr', 'dr', 'display DR/XDCR related status', default=False)
    ),
)

def status(runner):
    available_hosts = []
    if runner.opts.continuous:
        try:
            while True:
                # clear screen first
                tmp = subprocess.call('clear', shell=True)
                doStatus(runner, available_hosts)

                time.sleep(5)  # used to be runner.opts.interval, default as 2 seconds
        except KeyboardInterrupt as e:
            pass # don't care
    else:
        doStatus(runner, available_hosts)

def doStatus(runner, available_hosts):
    # the cluster(host) which voltadmin is running on always comes first
    clusterInfo = None
    try:
        if runner.client.host != runner.opts.host.host:
            runner.__voltdb_connect__(available_hosts[0].split(':')[0],
                                      int(available_hosts[0].split(':')[1]),
                                      runner.opts.username,
                                      runner.opts.password,
                                      runner.opts.ssl_config)
        clusterInfo = getClusterInfo(runner, available_hosts, True)
    except:
        for hostname in available_hosts:
            try:
                runner.__voltdb_connect__(hostname.split(':')[0],
                                          int(hostname.split(':')[1]),
                                          runner.opts.username,
                                          runner.opts.password,
                                          runner.opts.ssl_config)
                clusterInfo = getClusterInfo(runner, available_hosts, False)
                if clusterInfo != None:
                    break
            except:
                available_hosts.remove(hostname)
                pass
        if clusterInfo is None:
            runner.abort("Failed to connect any host had previously detected, exiting.")

    if runner.opts.json:
        printJSONSummary(clusterInfo)
    else:
        printPlainSummary(clusterInfo)

    if runner.opts.dr:
        # repeat the process to discover remote cluster.
        for clusterId, remoteCluster in list(clusterInfo.remoteclusters_by_id.items()):
            for remoteHost in remoteCluster.members:
                hostname = remoteHost.split(':')[0]
                try:
                    runner.__voltdb_connect__(hostname,
                                             runner.opts.host.port,
                                             runner.opts.username,
                                             runner.opts.password,
                                             runner.opts.ssl_config)
                    clusterInfo = getClusterInfo(runner, available_hosts, False)
                    if runner.opts.json:
                        printJSONSummary(clusterInfo)
                    else:
                        printPlainSummary(clusterInfo)
                    break
                except Exception as e:
                    pass  # ignore it

def getClusterInfo(runner, available_hosts, clearHostCache):
    # raise execption when failed to connect
    response = runner.call_proc('@SystemInformation',
                                    [VOLT.FastSerializer.VOLTTYPE_STRING],
                                    ['OVERVIEW'],
                                    True, None, True)
    if response.response.status != 1:
        return None

    if clearHostCache:
        available_hosts[:] = []

    # Convert @SystemInformation results to objects.
    hosts = Hosts(runner.abort)
    for tuple in response.table(0).tuples():
        hosts.update(tuple[0], tuple[1], tuple[2])

    for hostId, hostInfo in list(hosts.hosts_by_id.items()):
        if hostInfo.hostname not in available_hosts:
            available_hosts.append(hostInfo.hostname + ":" + str(hostInfo.clientport))

    # get current version and root directory from an arbitrary node
    host = next(iter(hosts.hosts_by_id.values()))

    # ClusterId in @SystemInformation is added in v7.2, so must check the version of target cluster to make it work properly.
    version = host.version
    versionStr = version.split('.')
    majorVersion = int(versionStr[0])
    minorVersion = int(versionStr[1])
    if majorVersion < RELEASE_MAJOR_VERSION or (majorVersion == RELEASE_MAJOR_VERSION and minorVersion < RELEASE_MINOR_VERSION):
        runner.abort("Only v7.2 or higher version of VoltDB supports this command. Target cluster running on v" + version + ".")

    clusterId = host.clusterid
    fullClusterSize = int(host.fullclustersize)
    uptime = host.uptime

    response = runner.call_proc('@SystemInformation',
                                    [VOLT.FastSerializer.VOLTTYPE_STRING],
                                    ['DEPLOYMENT'],
                                    True, None, True)
    for tuple in response.table(0).tuples():
        if tuple[0] == 'kfactor':
            kfactor = tuple[1]
            break


    cluster = Cluster(int(clusterId), version, int(kfactor), int(fullClusterSize), uptime)
    for hostId, hostInfo in list(hosts.hosts_by_id.items()):
        cluster.add_member(hostId, hostInfo.hostname)

    # number of live clients connect to the cluster
    try:
        response = checkstats.get_stats(runner, "LIVECLIENTS")
    except StatisticsProcedureException as e:
        runner.info(e.message)
        sys.exit(e.exitCode)

    liveclients = 0
    for tuple in response.table(0).tuples():
        isAdmin = tuple[5]
        # exclude admin connections
        if isAdmin != 1:
            liveclients += 1
    cluster.update_live_clients(liveclients)

    # check cluster elastic resizing info
    if "license" in host and\
            (majorVersion > RESIZE_MAJOR_VERSION or
             (majorVersion == RESIZE_MAJOR_VERSION and minorVersion >= RESIZE_MINOR_VERSION)):
        result = runner.call_proc('@ElasticRemoveNT',
                                  [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_STRING,
                                   VOLT.FastSerializer.VOLTTYPE_STRING, VOLT.FastSerializer.VOLTTYPE_BIGINT],
                                  [2, '', '', -1]).table(0)
        query_status = result.tuple(0).column_integer(0)
        elastic_status = result.tuple(0).column_string(1)

        if query_status == 0 and elastic_status != "NONE":
            percentage_moved = 0.0
            try:
                response = checkstats.get_stats(runner, "REBALANCE")
            except StatisticsProcedureException as e:
                runner.info(e.message)
                sys.exit(e.exitCode)

            if response.table(0).tuple_count() == 1:
                percentage_moved = float(response.table(0).tuple(0).column(1))

            cluster.set_elastic_status(elastic_status, percentage_moved)


    if runner.opts.dr:
        # Do we have any ongoing DR conversation?
        try:
            response = checkstats.get_stats(runner, "DRROLE")
        except StatisticsProcedureException as e:
            runner.info(e.message)
            sys.exit(e.exitCode)

        for tuple in response.table(0).tuples():
            role = tuple[0]
            status = tuple[1]
            remote_cluster_id = tuple[2]
            if (remote_cluster_id != -1):
                cluster.add_remote_cluster(remote_cluster_id, status, role)

        try:
            response = checkstats.get_stats(runner, "DRPRODUCER")
        except StatisticsProcedureException as e:
            runner.info(e.message)
            sys.exit(e.exitCode)
        for tuple in response.table(0).tuples():
            host_name = tuple[2]
            remote_cluster_id = tuple[4]
            last_queued_drid = tuple[10]
            last_queued_ts = tuple[12]
            last_acked_ts = tuple[13]
            if last_queued_drid == -1:
                delay = 0
            else:
                delay = (last_queued_ts - last_acked_ts).total_seconds()
            cluster.get_remote_cluster(remote_cluster_id).update_producer_latency(host_name, remote_cluster_id, delay)

        # Find remote topology through drconsumer stats
        try:
            response = checkstats.get_stats(runner, "DRCONSUMER")
        except StatisticsProcedureException as e:
            runner.info(e.message)
            sys.exit(e.exitCode)

        for tuple in response.table(1).tuples():
            remote_cluster_id = tuple[4]
            covering_host = tuple[7]
            last_applied_ts = tuple[9]
            if covering_host != '':
                cluster.get_remote_cluster(remote_cluster_id).add_remote_member(covering_host)
    return cluster

def printPlainSummary(cluster):
    header1 = "Cluster {}, version {}, hostcount {}, kfactor {}".format(cluster.id,
                                                                        cluster.version,
                                                                        cluster.hostcount,
                                                                        cluster.kfactor)

    livehost = len(cluster.hosts_by_id)
    missing = cluster.hostcount - livehost
    header2 = " {} live host{}, {} missing host{}, {} live client{}, uptime {}".format(
                livehost, 's' if livehost > 2 else '',
                missing, 's' if missing > 2 else '',
                cluster.liveclients, 's' if cluster.liveclients > 2 else '',
                cluster.uptime)

    # additional elastic info
    if cluster.elastic_status:
        header2 += "\n Cluster {} , progress: {:.2f}% completed".format(cluster.elastic_status, cluster.percentage_moved)

    delimiter = '-' * header1.__len__()

    # print host info
    hostHeader = '{:>8} {:>16}'.format("HostId", "Host Name")
    for clusterId, remoteCluster in list(cluster.remoteclusters_by_id.items()):
        hostHeader += ' {:>20}'.format("Cluster " + str(clusterId) + " (" + remoteCluster.status + ")")

    rows = list()
    for hostId, hostname in list(cluster.hosts_by_id.items()):
        row = "{:>8} {:>16}".format(hostId, hostname)
        for clusterId, remoteCluster in list(cluster.remoteclusters_by_id.items()):
            # use get() to avoid keyError when node is shut down
            row += ' {:>17} s'.format(remoteCluster.producer_max_latency.get(hostname + str(clusterId), ''))
        rows.append(row)

    sys.stdout.write(header1)
    sys.stdout.write('\n')
    sys.stdout.write(header2)
    sys.stdout.write('\n')
    sys.stdout.write(delimiter)
    sys.stdout.write('\n')
    sys.stdout.write(hostHeader)
    sys.stdout.write('\n')
    for row in rows:
        sys.stdout.write(row)
        sys.stdout.write('\n')
    sys.stdout.write('\n')

def printJSONSummary(cluster):

    remoteClusterInfos = []
    for clusterId, remoteCluster in list(cluster.remoteclusters_by_id.items()):
        clusterInfo = {
            "clusterId": clusterId,
            "state": remoteCluster.status,
            "role": remoteCluster.role
        }
        remoteClusterInfos.append(clusterInfo)

    members = []
    for hostId, hostname in list(cluster.hosts_by_id.items()):
        latencies = []
        for clusterId, remoteCluster in list(cluster.remoteclusters_by_id.items()):
            latency = {
                "clusterId": clusterId,
                "delay": remoteCluster.producer_max_latency[hostname + str(clusterId)]
            }
            latencies.append(latency)
        hostInfo = {
            "hostId": hostId,
            "hostname": hostname,
            "replicationLatencies": latencies,
        }
        members.append(hostInfo)

    livehost = len(cluster.hosts_by_id)
    missing = cluster.hostcount - livehost
    body = {
        "clusterId": cluster.id,
        "version": cluster.version,
        "hostcount": cluster.hostcount,
        "kfactor": cluster.kfactor,
        "livehost": livehost,
        "missing": missing,
        "liveclient": cluster.liveclients,
        "uptime": cluster.uptime,
        "remoteClusters": remoteClusterInfos,
        "members": members
    }
    # additional elastic info
    if cluster.elastic_status:
        body["elastic_status"] = cluster.elastic_status
        body["elastic_progress"] = cluster.percentage_moved

    jsonStr = json.dumps(body)
    print(jsonStr)
