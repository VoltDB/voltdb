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
    bundles = VOLT.AdminBundle(),
    description = "Generate a checklist before performing in-service upgrade.",
    arguments=(
            VOLT.PathArgument('newKit', 'path to new VoltDB kit directory', absolute=True, optional=False),
            VOLT.PathArgument('newRoot', 'path to the parent of new VoltDB root directory', absolute=True, optional=False),
            VOLT.StringArgument('newNode', 'hostname or IP of the extra node', optional=True)
    )
)

def plan_upgrade(runner):
    
    hosts, kfactor = basicCheck(runner)
    
    # first check the existence of root path on all the existing nodes
    # 1. how to know the nodes information?
    # 2. how to check path on every node? ( a new system pro0cedure? ) 
    # 3. how to check the new root is valid?
    # 4. does the directory contain .initialized file? If it has, then this is a initialized root.
    # runner.call_proc('@UpgradeCheck',[VOLT.FastSerializer.VOLTTYPE_STRING], [runner.opts.newKit, runner.opts.newRoot])
    
    # assume both newKit and newRoot exist
    
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
                     
    # newRoot should contains a voltdbroot directory
    if not os.path.isdir(runner.opts.newRoot):
        runner.abort("New root directory is not initialized. Please noted that NEWROOT should be pointed to the parent of new VoltDB root directory.")
    
    generateCommands(runner.opts,
                     hosts, 
                     os.path.isfile(os.path.join(runner.opts.newRoot, 'voltdbroot', '.initialized')), 
                     kfactor)

def basicCheck(runner):
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['OVERVIEW'])
    
     # Convert @SystemInformation results to objects.
    hosts = Hosts(runner.abort)
    for tuple in response.table(0).tuples():
        hosts.update(tuple[0], tuple[1], tuple[2])
    
    numberOfNodes = len(hosts.hosts_by_id)
    if numberOfNodes % 2 == 1 and runner.opts.newNode is None:
        runner.abort("The cluster has odd number of nodes, plan_upgrade needs an extra node to generate the instructions")
        
    # get current version and root directory from an arbitrary node
    host = hosts.hosts_by_id.itervalues().next();
    currentVersion = host['version']
    currentVoltDBRoot = host['voltdbroot']
    currentDeployment = host['deployment']
    xmlroot = ElementTree.parse(currentDeployment).getroot()
    cluster = xmlroot.find("./cluster");
    if cluster is None:
        runner.abort("Couldn't find cluster tag in current deployment file")
    kfactor_tag = cluster.get('kfactor')
    if kfactor_tag is None:
        kfactor = 0
    else:
        kfactor = int(kfactor_tag)
        
    pd = xmlroot.find("./partition-detection")
    if pd is None:
        runner.abort("Couldn't find partition-detection setting in current deployment file")
    
    # sanity check, in case of nodes number or K-factor is less than required
    # K = 0, abort with error message
    if kfactor == 0:
        runner.abort("Current cluster doesn't have duplicate partitions to perform in-service upgrade. K-factor: %d" % kfactor)
    
    # N = 1, abort with error message
    if numberOfNodes == 1: 
        runner.abort("Current cluster doesn't have enough node to perform in-service upgrade, at least two nodes are required")
        
    return hosts, kfactor

def generateCommands(opts, hosts, initialized, kfactor):
    
    
    toKill = chooseNodeToKill(hosts, kfactor)
    
    for host in hosts.hosts_by_id.values():
        print host.hostname
    
    #1 for the new cluster, initialize the new root path if not being initialized already
    
    #2 kill half of the cluster
    files = {}
    delimiter = '-' * 5
    for hostId,hostInfo in hosts.hosts_by_id.items():
        filename = "upgradePlan-%s:%s-%s.txt" % (hostInfo.ipaddress, hostInfo.internalport, hostInfo.hostname)
        file = open(filename, 'w+')
        files[hostId] = file
        if hostId in toKill:
            file.write(delimiter + 'stop node' + delimiter)
            file.write('\n')
            if (hostInfo.hostname is None):
                file.write('voltadmin stop %s:%d' % (hostInfo.ipaddress, hostInfo.internalport))
            else:
                file.write('voltadmin stop %s:%d' % (hostInfo.hostname, hostInfo.internalport))
            file.write('\n')
    
    #3 start the new cluster ( need the license path, may need hostname or ip from the extra node )
    missing = len(hosts.hosts_by_id) - len(toKill)
    if opts.newNode is not None:
        missing -= 1
    for hostId,hostInfo in hosts.hosts_by_id.items():
        if hostId in toKill:
            file = files[hostId]
            file.write('\n')
            file.write(delimiter + 'start new cluster' + delimiter)
            file.write('\n')
            hostsString = []
            for hostId, hostInfo in hosts.hosts_by_id.items():
                if hostId in toKill:
                    hostname = hostInfo.hostname
                    if hostname is None:
                        hostsString.append(hostInfo.ipaddress + ':' + str(hostInfo.internalport))
                    else:
                        hostsString.append(hostname + ':' + str(hostInfo.internalport))
            
            if opts.newNode is not None:
                hostsString.append(opts.newNode)
            
            file.write("%s start --dir=%s -H %s --missing=%d" % (os.path.join(opts.newKit, 'voltdb'), opts.newRoot, ','.join(hostsString), missing))
            file.write('\n')
    
    #4 set up XDCR replication between two clusters (how to generate command for it? call @UAC?)
    
    #5 call 'voltadmin shutdown --wait' on the original cluster
    
    #6 initialize a new VoltDB root path on the nodes being shutdown ( may not need all of them)
    
    #7 rejoin the nodes being shutdown recently to the new cluster
    
    #8 turn on XDCR (optional), to avoid annoying 'Couldn't connect to host XX' message 

# Choose half of nodes (lower bound) in the cluster which can be killed without violating k-safety
def chooseNodeToKill(hosts, kfactor):
    victims = []
    expectation = len(hosts.hosts_by_id) / 2;
    if kfactor >= expectation:
        for hostId, hostInfo in hosts.hosts_by_id.items():
            victims.append(hostId)
            if len(victims) == expectation:
                break;
        return victims
    
    partitionGroup = defaultdict(list)
    for hostId, hostInfo in hosts.hosts_by_id.items():
        partitions = hostInfo['partitiongroup']
        partitionGroup[partitions].append(hostId)
    while len(victims) != expectation:
        for hostIds in partitionGroup.values():
            if len(hostIds) > kfactor:
                victims.append(hostIds.pop())
            if len(victims) == expectation:
                break;
    return victims
