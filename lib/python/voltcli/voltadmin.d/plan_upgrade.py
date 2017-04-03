#!/usr/bin/env python
# This file is part of VoltDB.
# Copyright (C) 2008-2017 VoltDB Inc.
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
from xml.etree import ElementTree
from collections import defaultdict
import os.path

# TODO: change to actual release version when in-service upgrade is released
RELEASE_MAJOR_VERSION = 7
RELEASE_MINOR_VERSION = 0

@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description="Generate a checklist before performing in-service upgrade.",
    arguments=(
            VOLT.PathArgument('newKit', 'path to new VoltDB kit directory', absolute=True, optional=False),
            VOLT.PathArgument('newRoot', 'path to the parent of new VoltDB root directory', absolute=True, optional=False),
            VOLT.StringArgument('newNode', 'hostname[:PORT] or IP[:PORT] of the extra node. (default PORT=3021)', optional=True)
    ),
    hideverb=True
)

def plan_upgrade(runner):
    hosts, kfactor = basicCheck(runner)

    # first check the existence of root path on all the existing nodes
    # runner.call_proc('@UpgradeCheck',[VOLT.FastSerializer.VOLTTYPE_STRING], [runner.opts.newKit, runner.opts.newRoot])

    # FIXME: now just assume both newKit and newRoot exist, create newRoot if necessary.
    # In the future we need call a non transactional sysproc to check it.
    if not os.path.exists(runner.opts.newRoot):
        os.makedirs(runner.opts.newRoot)

    # verify the version of new kit is above the feature release version (e.g. 7.3)
    try:
        versionF = open(os.path.join(runner.opts.newKit, 'version.txt'), 'r')
    except IOError:
        runner.abort("Couldn't find version information in new VoltDB kit.")

    version = versionF.read().split(".");
    if len(version) < 2:
        runner.abort("Invalid version information in new VoltDB kit.")
    majorVersion = version[0];
    minorVersion = version[1];
    if (int(majorVersion) < RELEASE_MAJOR_VERSION or
        int(majorVersion) == RELEASE_MAJOR_VERSION and int(minorVersion) < RELEASE_MINOR_VERSION):
        runner.abort("The version of new VoltDB kit is too low. In-service upgrade is supported from V%d.%d"
                     % (RELEASE_MAJOR_VERSION, RELEASE_MINOR_VERSION));

    print 'Pre-upgrade check is passed.'

    generateCommands(runner.opts,
                     hosts,
                     kfactor)

def basicCheck(runner):
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['OVERVIEW'])

    # Convert @SystemInformation results to objects.
    hosts = Hosts(runner.abort)
    for tuple in response.table(0).tuples():
        hosts.update(tuple[0], tuple[1], tuple[2])

    # get current version and root directory from an arbitrary node
    host = hosts.hosts_by_id.itervalues().next();
    fullClusterSize = int(host.fullclustersize)
    if len(hosts.hosts_by_id) < fullClusterSize:
        runner.abort("Current cluster needs %d more node(s) to achieve full K-safety. In-service upgrade is not recommended in partial K-safety cluster."
                     % (fullClusterSize - len(hosts.hosts_by_id)))

    if fullClusterSize % 2 == 1 and runner.opts.newNode is None:
        runner.abort("The cluster has odd number of nodes, plan_upgrade needs an extra node to generate the instructions")

    host = hosts.hosts_by_id.itervalues().next();
    currentVersion = host.version
    currentVoltDBRoot = host.voltdbroot
    currentDeployment = host.deployment
    xmlroot = ElementTree.parse(currentDeployment).getroot()
    cluster = xmlroot.find("./cluster");
    if cluster is None:
        runner.abort("Couldn't find cluster tag in current deployment file")
    kfactor_tag = cluster.get('kfactor')
    if kfactor_tag is None:
        kfactor = 0
    else:
        kfactor = int(kfactor_tag)

    # sanity check, in case of nodes number or K-factor is less than required
    # K = 0, abort with error message
    if kfactor == 0:
        runner.abort("Current cluster doesn't have duplicate partitions to perform in-service upgrade. K-factor: %d" % kfactor)

    # N = 1, abort with error message
    if fullClusterSize == 1:
        runner.abort("Current cluster doesn't have enough node to perform in-service upgrade, at least two nodes are required")

    return hosts, kfactor

def generateCommands(opts, hosts, kfactor):
    halfNodes = len(hosts.hosts_by_id) / 2
    (killSet, surviveSet) = pickNodeToKill(hosts, kfactor, halfNodes)

    # find a survivor node and a killSet node
    survivor = surviveSet[0]
    victim = killSet[0]

    # 0 generate deployment file
    step = 0
    files = {}
    (et, drId) = updateDeployment(survivor,
                                 None,
                                 getHostnameOrIp(victim) + ':' + str(victim.drport))
    cluster_1_deploy = "deployment1.xml"
    et.write(cluster_1_deploy)

    (et, drId) = updateDeployment(victim,
                                 drId,
                                 getHostnameOrIp(survivor) + ':' + str(survivor.drport))
    cluster_2_deploy = "deployment2.xml"
    et.write(cluster_2_deploy)
    # copy deployment file to individual node
    for hostId, hostInfo in hosts.hosts_by_id.items():
        file = open("upgradePlan-%s:%s-%s.txt" % (hostInfo.ipaddress, hostInfo.internalport, hostInfo.hostname), 'w+')
        writeHeader(file)
        files[getKey(hostInfo)] = file
        if hostInfo in killSet:
            writeCommands(file,
                          'Step %d: copy deployment file' % step,
                          '#note# copy %s to %s' % (cluster_2_deploy, opts.newRoot))
        if hostInfo in surviveSet:
            writeCommands(file,
                          'Step %d: copy deployment file' % step,
                          '#note# copy %s and %s to %s' % (cluster_1_deploy, cluster_2_deploy, opts.newRoot))
    if opts.newNode is not None:
        newNodeF = open("upgradePlan-%s.txt" % (opts.newNode), 'w+')
        writeHeader(newNodeF)
        writeCommands(newNodeF,
                      'Step %d: copy deployment file' % step,
                      '#note# copy %s to %s' % (cluster_2_deploy, opts.newRoot))

    # 1 kill half of the cluster
    step += 1
    for hostId, hostInfo in hosts.hosts_by_id.items():
        if hostInfo in killSet:
            writeCommands(files[getKey(hostInfo)],
                          'Step %d: stop node' % step,
                          'voltadmin stop -H %s:%d %s:%d' % (survivor.hostname,
                                                             survivor.adminport,
                                                             getHostnameOrIp(hostInfo),
                                                             hostInfo.internalport))

    # 2 for the new cluster, initialize the new root path
    step += 1
    for hostInfo in killSet:
        writeCommands(files[getKey(hostInfo)],
                      'Step %d: initialize new cluster' % step,
                      '%s init --dir=%s --config=%s' % (os.path.join(opts.newKit, 'bin/voltdb'),
                                                        opts.newRoot,
                                                        os.path.join(opts.newRoot, cluster_2_deploy)))
    if opts.newNode is not None:
        if step == 1:
            step += 1
        writeCommands(newNodeF,
                      'Step %d: initialize new cluster' % step,
                      '%s init --dir=%s --config=%s' % (os.path.join(opts.newKit, 'bin/voltdb'),
                                                        opts.newRoot,
                                                        os.path.join(opts.newRoot, cluster_2_deploy)))

    # 3 start the new cluster
    step += 1
    leadersString = []
    for hostInfo in killSet:
        leadersString.append(getHostnameOrIp(hostInfo) + ':' + str(hostInfo.internalport))
    if opts.newNode is not None:
        leadersString.append(opts.newNode)

    for hostInfo in killSet:
        writeCommands(files[getKey(hostInfo)],
                      'Step %d: start new cluster' % step,
                      "%s start --dir=%s -H %s -c %d --missing=%d" % (os.path.join(opts.newKit, 'bin/voltdb'),
                                                                      opts.newRoot,
                                                                      ','.join(leadersString),
                                                                      len(hosts.hosts_by_id),
                                                                      halfNodes))
    if opts.newNode is not None:
        writeCommands(newNodeF,
                      'Step %d: start new cluster' % step,
                      "%s start --dir=%s -H %s -c %d --missing=%d" % (os.path.join(opts.newKit, 'bin/voltdb'),
                                                                      opts.newRoot,
                                                                      ','.join(leadersString),
                                                                      len(hosts.hosts_by_id),
                                                                      halfNodes))

    # 4 set up XDCR replication between two clusters
    # update old cluster's deployment file
    step += 1
    writeCommands(files[getKey(survivor)],
                  'Step %d: turn XDCR on in the original cluster' % step,
                  'voltadmin update -H %s:%d %s' % (survivor.hostname,
                                                    survivor.adminport,
                                                    os.path.join(opts.newRoot, cluster_1_deploy)))

    # 6 call 'voltadmin shutdown --wait' on the original cluster
    step += 1
    writeCommands(files[getKey(survivor)],
                  'Step %d: wait for XDCR stream to drain' % step,
                  'voltadmin pause --wait -H %s:%d' % (survivor.hostname, survivor.adminport))

    # 7 initialize a new VoltDB root path on the nodes being shutdown ( may not need all of them)
    step += 1
    initNodes = 0
    for hostInfo in surviveSet:
        initNodes += 1
        writeCommands(files[getKey(hostInfo)],
                      'Step %d: initialize original cluster with new voltdb path' % step,
                      '%s init --dir=%s --config=%s' % (os.path.join(opts.newKit, 'bin/voltdb'),
                                                        opts.newRoot,
                                                        os.path.join(opts.newRoot, cluster_2_deploy)))
        if initNodes == halfNodes:
            break

    # 8 kill the original cluster
    step += 1
    writeCommands(files[getKey(survivor)],
                  'Step %d: shutdown the original cluster' % step,
                  'voltadmin shutdown -H %s:%d' % (survivor.hostname, survivor.adminport))

    # 9 rejoin the nodes being shutdown recently to the new cluster
    step += 1
    rejoinNodes = 0
    for hostInfo in surviveSet:
        rejoinNodes += 1
        file = files[getKey(hostInfo)]
        writeCommands(file,
                      'Step %d: rejoin the node to new cluster' % step,
                      '%s start --dir=%s -H %s' % (os.path.join(opts.newKit, 'bin/voltdb'),
                                                   opts.newRoot,
                                                   ','.join(leadersString)))
        if rejoinNodes == halfNodes:
            break

    # cleanup
    for key, file in files.items():
        file.close()
    if opts.newNode is not None:
        newNodeF.close()
    print 'Upgrade plan generated successfully in current directory. You might modify those per-node commands to fit your own need.'

# Choose half of nodes (lower bound) in the cluster which can be killed without violating k-safety
def pickNodeToKill(hosts, kfactor, expectation):
    victims = []
    survivors = []
    if kfactor >= expectation:
        for hostId, hostInfo in hosts.hosts_by_id.items():
            if len(victims) < expectation:
                victims.append(hostInfo)
            else:
                survivors.append(hostInfo)
        return victims, survivors

    # partition group case
    partitionGroup = defaultdict(list)
    for hostId, hostInfo in hosts.hosts_by_id.items():
        partitions = hostInfo['partitiongroup']
        partitionGroup[partitions].append(hostInfo)
    for hostInfos in partitionGroup.values():
        count = 0
        while count < kfactor:
            victims.append(hostInfos.pop())
            count += 1
        survivors.extend(hostInfos)
        if len(victims) == expectation:
            break;
    return victims, survivors

def getKey(host):
    return host.ipaddress + str(host.internalport) + host.hostname

def getHostnameOrIp(host):
    if host.hostname is None:
        return host.ipaddress
    else:
        return host.hostname

def writeHeader(file):
    delimiter = '=' * 40
    file.write(delimiter + '\n')
    file.write(file.name);
    file.write('\n')
    file.write(delimiter)
    file.write('\n')

def writeCommands(file, subject, command):
    delimiter = '*' * 5
    file.write(delimiter + subject + delimiter)
    file.write('\n')
    if command is not None:
        file.write(command)
        file.write('\n\n')

def updateDeployment(host, greatestRemoteClusterId, drSource):
    if greatestRemoteClusterId is None or greatestRemoteClusterId > 127:
        clusterId = 1  # start from 1
    else:
        clusterId = greatestRemoteClusterId + 1

    et = ElementTree.parse(host.deployment)
    dr = et.getroot().find('./dr')
    if dr is None:
        # append an empty DR tag and change it later
        dr = ElementTree.Element('dr')
        et.getroot().append(dr)

    # update DR tag
    dr.attrib['id'] = str(clusterId)
    dr.attrib['role'] = 'xdcr'

    # find DR connection source
    connection = dr.find('./connection')
    if connection is None:
        connection = ElementTree.Element('connection')
        dr.append(connection)

    # update DR connection source
    connection.attrib['enabled'] = 'true'
    connection.attrib['source'] = drSource
    return et, clusterId
