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
import os.path

# TODO: change to actual release version when in-service upgrade is released
RELEASE_MAJOR_VERSION = 7
RELEASE_MINOR_VERSION = 0

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = "Generate a checklist before performing in-service upgrade.",
    arguments=(
            VOLT.PathArgument('new_kit', 'path to new VoltDB kit directory', absolute=True, optional=False),
            VOLT.PathArgument('new_root', 'path to new VoltDB root directory', absolute=True, optional=False)
    )
)

def plan_upgrade(runner):
    
    clusterSize = basicCheck(runner)
    
    # first check the existence of root path on all the existing nodes
    # 1. how to know the nodes information?
    # 2. how to check path on every node? ( a new system pro0cedure? ) 
    # 3. how to check the new root is valid?
    # 4. does the directory contain .initialized file? If it has, then this is a initialized root.
    # runner.call_proc('@UpgradeCheck',[VOLT.FastSerializer.VOLTTYPE_STRING], [runner.opts.new_kit, runner.opts.new_root])
    
    # assume both new_kit and new_root exist
    
    # verify the version of new kit is above the feature release version (e.g. 7.3)
    try:
        versionF = open(os.path.join(runner.opts.new_kit, 'version.txt'), 'r')
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
    
    # verify that the new root is initialized
    initialized = False
    if os.path.isfile(os.path.join(runner.opts.new_root, 'voltdbroot', '.initialized')) :
        initialized = True
        
    # Need a new node?
    needNewNode = False
    if clusterSize % 2 == 1:
        needNewNode = True
    
    generateCommands(initialized, needNewNode)

def basicCheck(runner):
    # Know about the current cluster
    # 1. existing number of nodes
    # 2. k-factor
    # 3. partition detection on/off
    # 4. current root
    # 5. current version
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['OVERVIEW'])
    
     # Convert @SystemInformation results to objects.
    hosts = Hosts(runner.abort)
    for tuple in response.table(0).tuples():
        hosts.update(tuple[0], tuple[1], tuple[2])
    
    numberOfNodes = len(hosts.hosts_by_id)
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
    
    pd_tag = pd.get('enabled') # can be True, False or None
    if pd_tag is None:
        pd_enabled = True # true by default
    elif pd_tag == 'True':
        pd_enabled = True
    else:
        pd_enabled = False
        
    
    print "Size of Cluster: %d" % numberOfNodes
    print "Current version: %s" % currentVersion
    print "Current VoltDB Root: %s" % currentVoltDBRoot
    print "K-factor: %d" % int(kfactor)
    print "Is partition-detection enabled? %s" % pd_enabled
    
    # sanity check, in case of nodes number or K-factor is less than required
    # K = 0, abort with error message
    if kfactor == 0:
        runner.abort("Current cluster doesn't have duplicate partitions to perform in-service upgrade. K-factor: %d" % kfactor)
    
    # N = 1, abort with error message
    if numberOfNodes == 1: 
        runner.abort("Current cluster doesn't have enough node to perform in-service upgrade, at least two nodes are required")
        
    # N = 2, don't recommend, must turn partition detection off
    if numberOfNodes == 2 and pd_enabled:
        runner.abort("In two-nodes cluster, you can't preform in-service upgrade when partition detection is enabled ")
    
    return numberOfNodes

def generateCommands(initialized, needNewNode):
    file = open('upgradePlan.txt', 'w+')
    
    #1 for the new cluster, initialize the new root path if not being initialized already
    
    #2 based on the information from @SystemInformation, choose half of the nodes to kill (how to choose?)
    
    #3 start the new cluster ( need the license path, may need hostname or ip from the extra node )
    
    #4 set up XDCR replication between two clusters (how to generate command for it? call @UAC?)
    
    #5 call 'voltadmin shutdown --wait' on the original cluster
    
    #6 initialize a new VoltDB root path on the nodes being shutdown ( may not need all of them)
    
    #7 rejoin the nodes being shutdown recently to the new cluster
    
    #8 turn on XDCR (optional), to avoid annoying 'Couldn't connect to host XX' message 
        