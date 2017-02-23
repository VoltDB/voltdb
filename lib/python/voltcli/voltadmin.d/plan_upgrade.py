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
        
    print 'Pre-upgrade check is passed.'
    
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
    if numberOfNodes == 1: 
        runner.abort("Current cluster doesn't have enough node to perform in-service upgrade, at least two nodes are required")
        
    return hosts, kfactor

def generateCommands(opts, hosts, initialized, kfactor):
    halfNodes = len(hosts.hosts_by_id) / 2
    (killSet, surviveSet) = pickNodeToKill(hosts, kfactor, halfNodes)
    
    # find a survivor node and a killSet node
    survivor = surviveSet[0]
    victim = killSet[0]
    
    step = 1
    #1 kill half of the cluster
    files = {}
    for hostId,hostInfo in hosts.hosts_by_id.items():
        file = open("upgradePlan-%s:%s-%s.txt" % (hostInfo.ipaddress, hostInfo.internalport, hostInfo.hostname), 'w+')
        files[getKey(hostInfo)] = file
        writeHeader(file)
        if hostInfo in killSet:
            writeCommands(file,
                          'Step %d: stop node' % step,
                          'voltadmin stop -H %s:%d %s:%d' % (survivor.hostname, survivor.adminport, getHostnameOrIp(hostInfo), hostInfo.internalport))
    
    #2 for the new cluster, initialize the new root path if not being initialized already
    if not initialized:
        step += 1
        for hostInfo in killSet:
            writeCommands(files[getKey(hostInfo)],
                          'Step %d: initialize new cluster' % step,
                          '%s init --dir=%s --config=%s' % (os.path.join(opts.newKit, 'bin/voltdb'), opts.newRoot, victim.deployment))
    if opts.newNode is not None:
        if step == 1:
            step += 1
        newNodeF = open("upgradePlan-%s.txt" % (opts.newNode), 'w+')
        writeHeader(newNodeF)
        writeCommands(newNodeF,
                      'Step %d: initialize new cluster' % step,
                      '%s init --dir=%s --config=%s' % (os.path.join(opts.newKit, 'bin/voltdb'), opts.newRoot, victim.deployment))
            
    #3 start the new cluster ( need the license path, may need hostname or ip from the extra node )
    missing = len(surviveSet)
    if opts.newNode is not None:
        missing -= 1
    
    leadersString = []
    for hostInfo in killSet:
        leadersString.append(getHostnameOrIp(hostInfo) + ':' + str(hostInfo.internalport))
    if opts.newNode is not None:
        leadersString.append(opts.newNode)
        
    step += 1
    for hostInfo in killSet:
        writeCommands(files[getKey(hostInfo)],
                      'Step %d: start new cluster' % step,
                      "%s start --dir=%s -H %s --missing=%d" % (os.path.join(opts.newKit, 'bin/voltdb'), opts.newRoot, ','.join(leadersString), missing))
    if opts.newNode is not None:
        writeCommands(newNodeF,
                      'Step %d: start new cluster' % step,
                      "%s start --dir=%s -H %s --missing=%d" % (os.path.join(opts.newKit, 'bin/voltdb'), opts.newRoot, ','.join(leadersString), missing))
    
    #4 set up XDCR replication between two clusters (how to generate command for it? call @UAC?)
    # update old cluster's deployment file
    step += 1
    et = addDrTagToDeployment(survivor, 
                              '1', 
                              getHostnameOrIp(victim) + ':' + str(victim.drport))
    xmlFilename = "deployment-%s:%s-%s.xml" % (survivor.ipaddress, survivor.internalport, survivor.hostname)
    et.write(xmlFilename)
    writeCommands(file, 
                  'Step %d: turn XDCR on in the original cluster' % step, 
                  'voltadmin update -H %s:%d %s' % (survivor.hostname, survivor.adminport, xmlFilename))
    
    #5 update new cluster's deployment file
    step += 1
    et = addDrTagToDeployment(victim, 
                              '2', 
                              getHostnameOrIp(survivor) + ':' + str(survivor.drport))
    xmlFilename = "deployment-%s:%s-%s.xml" % (victim.ipaddress, victim.internalport, victim.hostname)
    et.write(xmlFilename)
    writeCommands(file, 
                  'Step %d: turn XDCR on' % step, 
                  'voltadmin update -H %s:%d %s' % (victim.hostname, victim.adminport, xmlFilename))
    
    #6 call 'voltadmin shutdown --wait' on the original cluster
    step += 1
    writeCommands(files[getKey(survivor)],
                  'Step %d: wait for XDCR stream to drain' % step,
                  'voltadmin pause --wait -H %s:%d' % (survivor.hostname, survivor.adminport))
            
    #7 initialize a new VoltDB root path on the nodes being shutdown ( may not need all of them)
    step += 1
    initNodes = 0
    for hostInfo in surviveSet:
        initNodes += 1
        writeCommands(files[getKey(hostInfo)],
                      'Step %d: initialize original cluster with new voltdb path' % step,
                      '%s init --dir=%s --config=%s' % (os.path.join(opts.newKit, 'bin/voltdb'), opts.newRoot, victim.deployment))
        if initNodes == halfNodes:
            break
    
    #8 kill the original cluster
    step += 1
    writeCommands(files[getKey(survivor)],
                  'Step %d: shutdown the original cluster' % step,
                  'voltadmin shutdown -H %s:%d' % (survivor.hostname, survivor.adminport))
    
    #9 rejoin the nodes being shutdown recently to the new cluster
    step += 1
    rejoinNodes = 0
    for hostInfo in surviveSet:
        rejoinNodes += 1
        file = files[getKey(hostInfo)]
        writeCommands(file, 
                      'Step %d: rejoin the node to new cluster' % step, 
                      '%s start --dir=%s -H %s' % (os.path.join(opts.newKit, 'voltdb'), opts.newRoot, ','.join(leadersString)))
        if rejoinNodes == halfNodes:
            break
    
    #10 turn off XDCR (optional), to avoid annoying 'Couldn't connect to host XX' message 
    
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
    while len(victims) != expectation:
        for hostInfos in partitionGroup.values():
            if len(hostInfos) > kfactor:
                victims.append(hostInfos.pop())
            if len(victims) == expectation:
                break;
    survivors.extend(hostInfos)
    return victims, survivors

def getKey(host):
    return host.ipaddress + str(host.internalport)

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
    file.write(command)
    file.write('\n\n')

def addDrTagToDeployment(host, drId, drSource):
    et = ElementTree.parse(host.deployment)
    dr = et.getroot().find('./dr')
    if dr is None:
        drTag = appendDRTag(et.getroot(), drId)
        appendConnectionTag(drTag, drSource)
    else:
        connection = dr.find('./connection')
        if connection is None:
            appendConnectionTag(drTag, drSource)
        else:
            appendDRSource(connection, drSource)
    return et

def appendDRSource(connectionTag, drSource):
    connectionTag.attrib['source'] = drSource + ',' + connectionTag.attrib['source']
    connectionTag.attrib['enabled'] = 'true'

def appendConnectionTag(drTag, drSource):
    connectionTag = ET.Element('connection')
    connectionTag.attrib['source'] = drSource
    connectionTag.attrib['enabled'] = 'true'
    drTag.append(connectionTag)
    
def appendDRTag(root, drId):
    drTag = ET.Element('dr')
    drTag.attrib['id'] = drId
    root.append(drTag)
    return drTag