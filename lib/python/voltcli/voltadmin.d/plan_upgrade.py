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

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = "Generate a checklist before performing in-service upgrade.",
    arguments=(
            VOLT.PathArgument('new_kit', 'path to new VoltDB kit directory', absolute=True, optional=False),
            VOLT.PathArgument('new_root', 'path to new VoltDB root directory', absolute=True, optional=False)
    )
)

def plan_upgrade(runner):
    
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
    
    # first check the existence of root path on all the existing nodes
    # 1. how to know the nodes information?
    # 2. how to check path on every node? ( a new system procedure? ) 
    # 3. how to check the new root is valid?
    # 4. does the directory contain .initialized file? If it has, then this is a initialized root.
    runner.call_proc('@UpgradeCheck')
    
    # verify the version of new kit is above the feature release version (e.g. 7.3)
    # 1. check kit path existence (maybe also check the sub-directory structure?)
    # 2. read version.txt to get the version
    # 3. print it out
    
    # should I also check the md5sum of the kit ( feel like an overkill )
    
    # check the need of new node 
    # 1. if # of nodes is odd, then a new node is needed (do we need that information?)
    # 2. Suggest run the generated command list on new node first
    # 3. Do we use the host name of the new node?
