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
from voltcli import checkstats
from xml.etree import ElementTree
from collections import defaultdict
from urllib2 import Request, urlopen, URLError
import base64
import os
import sys
import subprocess

@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description="Generate a checklist before performing online upgrade.",
    arguments=(
            VOLT.PathArgument('newKit', 'path to new VoltDB kit directory', absolute=True, optional=False),
            VOLT.PathArgument('newRoot', 'path to the parent of new VoltDB root directory', absolute=True, optional=False),
            VOLT.StringArgument('newNode', 'hostname[:PORT] or IP[:PORT] of the extra node. (default PORT=3021)', optional=True)
    ),
    hideverb=True
)

def plan_upgrade(runner):
    hosts, kfactor, clusterIds = basicCheck(runner)

    generateCommands(runner,
                     hosts,
                     kfactor,
                     clusterIds)

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
    if fullClusterSize % 2 == 0 and runner.opts.newNode is not None:
        runner.abort("For even-numbered cluster, 2 parameters are expected (received 3).")

    if runner.opts.newNode is not None:
        result = checkNewNode(runner.opts.newNode)
        if result is False:
            runner.abort("Failed to resolve host {}.".format(runner.opts.newNode))

    host = hosts.hosts_by_id.itervalues().next();
    currentVersion = host.version

    # get k-factor from @SystemInformation
    response = runner.call_proc('@SystemInformation',
            [VOLT.FastSerializer.VOLTTYPE_STRING],
            ['DEPLOYMENT'])
    for tuple in response.table(0).tuples():
        if tuple[0] == 'kfactor':
            kfactor = tuple[1]
            break

    # sanity check, in case of nodes number or K-factor is less than required
    # K = 0, abort with error message
    if kfactor == 0:
        runner.abort("Current cluster doesn't have duplicate partitions to perform online upgrade. K-factor: %d" % kfactor)

    # N = 1, abort with error message
    if fullClusterSize == 1:
        runner.abort("Current cluster doesn't have enough node to perform online upgrade, at least two nodes are required")

    response = checkstats.get_stats(runner, "DRROLE")
    clusterIds = []
    for tuple in response.table(0).tuples():
        remote_cluster_id = tuple[2]
        if remote_cluster_id != -1:
            clusterIds.append(remote_cluster_id)
    if len(clusterIds) == 127:
        runner.abort("Failed to generate upgrade plan: number of connected cluster reaches the maximum limit (127).")

    # Check the existence of voltdb root path and new kit on all the existing nodes
    response = runner.call_proc('@CheckUpgradePlanNT',
                                [VOLT.FastSerializer.VOLTTYPE_STRING, VOLT.FastSerializer.VOLTTYPE_STRING],
                                [runner.opts.newKit, runner.opts.newRoot])
    error = False
    warnings = ""
    for tuple in response.table(0).tuples():
        hostId = tuple[0]
        result = tuple[1]
        warning = tuple[2]
        if result != 'Success':
            error = True
            host = hosts.hosts_by_id[hostId]
            if host is None:
                runner.abort('@CheckUpgradePlanNT returns a host id ' + hostId + " that doesn't belong to the cluster.")
            runner.error('Check failed on host ' + getHostnameOrIp(host) + " with the cause: " + result)
        if warning is not None:
            host = hosts.hosts_by_id[hostId]
            if host is None:
                runner.abort('@CheckUpgradePlanNT returns a host id ' + hostId + " that doesn't belong to the cluster.")
            warnings += 'On host ' + getHostnameOrIp(host) + ': \n' + warning + '\n'

    if error:
        runner.abort("Failed to pass pre-upgrade check. Abort. ")
    if warnings != "":
        runner.warning(warnings[:-1])  # get rid of last '\n'

    print '[1/4] Passed new VoltDB kit version check.'
    print '[2/4] Passed new VoltDB root path existence check.'

    return hosts, kfactor, clusterIds

def generateCommands(runner, hosts, kfactor, clusterIds):
    hostcount = len(hosts.hosts_by_id)
    (killSet, surviveSet) = pickNodeToKill(hosts, kfactor, hostcount / 2)

    # 0 generate deployment file
    step = 0
    origin_cluster_deploy = "deployment_for_current_version.xml"
    new_cluster_deploy = "deployment_for_new_version.xml"
    files, newNodeF = generateDeploymentFile(runner, hosts, surviveSet, killSet,
                                             clusterIds, origin_cluster_deploy,
                                             new_cluster_deploy, step)
    printout = '[3/4] Generated deployment file: '
    if os.path.isfile(origin_cluster_deploy):
        printout += origin_cluster_deploy
    if os.path.isfile(new_cluster_deploy):
        printout += " " + new_cluster_deploy
    print printout

    # 1 kill half of the cluster
    step += 1
    generateStopNodeCommand(hosts, surviveSet[0], killSet, files, step)

    # 2 for the new cluster, initialize the new root path
    step += 1
    generateInitNewClusterCommand(runner.opts, killSet, files, new_cluster_deploy, newNodeF, step)

    # 3 start the new cluster
    step += 1
    leadersString = generateStartNewClusterCommand(runner.opts, killSet, hostcount, files, newNodeF, step)

    # 4 load schema into the new cluster
    step += 1
    writeCommands(files[getKey(killSet[0])], 'Step %d: load schema' % step, '#instruction# load schema into the new-version cluster')

    # 5 only for upgrading stand-alone cluster, set up XDCR replication between two clusters
    # update old cluster's deployment file
    if len(clusterIds) == 0:
        step += 1
        generateTurnOnXDCRCommand(runner.opts, surviveSet[0], files, origin_cluster_deploy, step)

     # 6 call 'voltadmin shutdown --wait' on the original cluster
    step += 1
    generatePauseCommand(surviveSet[0], files, step)

    # 7 kill the original cluster
    step += 1
    generateShutdownOriginClusterCommand(surviveSet[0], files, step)

    # 8 run DR RESET on one node of the new cluster.
    step += 1
    generateDRResetCommand(runner, surviveSet[0], files, step)

    # 9 initialize a new VoltDB root path on the nodes being shutdown ( may not need all of them)
    step += 1
    generateInitOldClusterCommmand(runner.opts, surviveSet, files, new_cluster_deploy, hostcount / 2, step)

    # 10 rejoin the nodes being shutdown recently to the new cluster
    step += 1
    generateNodeRejoinCommand(runner.opts, surviveSet, leadersString, files, hostcount / 2, step)

    cleanup(runner.opts, files, newNodeF)
    print '[4/4] Generated online upgrade plan: upgrade-plan.txt'

def generateDeploymentFile(runner, hosts, surviveSet, killSet, clusterIds, origin_cluster_deploy, new_cluster_deploy, step):
    files = dict()
    oldDeploymentHasAbsPath = False
    newDeploymentHasAbsPath = False

    # get deployment file from the original cluster
    xmlString = getCurrentDeploymentFile(runner, surviveSet[0])

    if len(clusterIds) == 0:
        # If this is a stand-alone cluster, generate a deployment file with a XDCR connection source
        drId, oldDeploymentHasAbsPath = createDeploymentForOriginalCluster(runner,
                                                                           xmlString,
                                                                           getHostnameOrIp(killSet[0]) + ':' + str(killSet[0].drport),
                                                                           origin_cluster_deploy)
        newDeploymentHasAbsPath = createDeploymentForNewCluster(runner,
                                                                xmlString,
                                                                [drId],
                                                                getHostnameOrIp(surviveSet[0]) + ':' + str(surviveSet[0].drport),
                                                                new_cluster_deploy)
    else:
        newDeploymentHasAbsPath = createDeploymentForNewCluster(runner,
                                                                xmlString,
                                                                clusterIds,
                                                                None,
                                                                new_cluster_deploy)

    if oldDeploymentHasAbsPath or newDeploymentHasAbsPath:
        warningForDeploy = "Warn: Absolute paths in generated deployment file are commented out to avoid accidentally damage to your original cluster's artifact. Please review the file before use.\n"

    # generate instructions to copy deployment file to individual node
    for hostId, hostInfo in hosts.hosts_by_id.items():
        file = open("upgradePlan-%s:%s-%s.txt" % (hostInfo.ipaddress, hostInfo.internalport, hostInfo.hostname), 'w+')
        writeHeader(file)
        files[getKey(hostInfo)] = file
        if hostInfo in killSet:
            writeCommands(file,
                          'Step %d: copy deployment file' % step,
                          '%s#instruction# copy %s to %s' % (warningForDeploy, new_cluster_deploy, runner.opts.newRoot))
        if hostInfo in surviveSet:
            if len(clusterIds) == 0:
                # for stand-alone cluster
                writeCommands(file,
                              'Step %d: copy deployment file' % step,
                              '%s#instruction# copy %s and %s to %s' % (warningForDeploy, origin_cluster_deploy, new_cluster_deploy, runner.opts.newRoot))
            else:
                # for multi-cluster
                writeCommands(file,
                              'Step %d: copy deployment file' % step,
                              '%s#instruction# copy %s to %s' % (warningForDeploy, new_cluster_deploy, runner.opts.newRoot))
    newNodeF = None
    if runner.opts.newNode is not None:
        newNodeF = open("upgradePlan-%s.txt" % (runner.opts.newNode), 'w+')
        writeHeader(newNodeF)
        writeCommands(newNodeF,
                      'Step %d: copy deployment file' % step,
                      '#instruction# copy %s to %s' % (new_cluster_deploy, runner.opts.newRoot))
    return files, newNodeF

def generateStopNodeCommand(hosts, survivor, killSet, files, step):
    for hostId, hostInfo in hosts.hosts_by_id.items():
        if hostInfo in killSet:
            writeCommands(files[getKey(hostInfo)],
                          'Step %d: stop node' % step,
                          'voltadmin stop -H %s:%d %s:%d' % (survivor.hostname,
                                                             survivor.adminport,
                                                             getHostnameOrIp(hostInfo),
                                                             hostInfo.internalport))

def generateInitNewClusterCommand(opts, killSet, files, new_cluster_deploy, newNodeF, step):
    for hostInfo in killSet:
        writeCommands(files[getKey(hostInfo)],
                      'Step %d: initialize new cluster' % step,
                      '%s init --dir=%s --config=%s --force' % (os.path.join(opts.newKit, 'bin/voltdb'),
                                                                opts.newRoot,
                                                                os.path.join(opts.newRoot, new_cluster_deploy)))
    if opts.newNode is not None:
        writeCommands(newNodeF,
                      'Step %d: initialize new cluster' % step,
                      '%s init --dir=%s --config=%s --force' % (os.path.join(opts.newKit, 'bin/voltdb'),
                                                                opts.newRoot,
                                                                os.path.join(opts.newRoot, new_cluster_deploy)))

def generateStartNewClusterCommand(opts, killSet, hostcount, files, newNodeF, step):
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
                                                                      hostcount,
                                                                      hostcount / 2))


    if opts.newNode is not None:
        writeCommands(newNodeF,
                      'Step %d: start new cluster' % step,
                      "%s start --dir=%s -H %s -c %d --missing=%d" % (os.path.join(opts.newKit, 'bin/voltdb'),
                                                                      opts.newRoot,
                                                                      ','.join(leadersString),
                                                                      hostcount,
                                                                      hostcount / 2))
    return leadersString

def generateTurnOnXDCRCommand(opts, survivor, files, origin_cluster_deploy, step):
    writeCommands(files[getKey(survivor)],
              'Step %d: turn XDCR on in the original cluster' % step,
              'voltadmin update -H %s:%d %s' % (survivor.hostname,
                                                survivor.adminport,
                                                os.path.join(opts.newRoot, origin_cluster_deploy)))

def generatePauseCommand(survivor, files, step):
   writeCommands(files[getKey(survivor)],
                  'Step %d: wait for XDCR stream to drain' % step,
                  'voltadmin pause --wait -H %s:%d' % (survivor.hostname, survivor.adminport))

def generateShutdownOriginClusterCommand(survivor, files, step):
    writeCommands(files[getKey(survivor)],
                  'Step %d: shutdown the original cluster' % step,
                  'voltadmin shutdown -H %s:%d' % (survivor.hostname, survivor.adminport))

def generateDRResetCommand(runner, survivor, files, step):
    remoteTopo = dict()
    # Find remote covering host through drconsumer stats, one for each remote cluster
    response = checkstats.get_stats(runner, "DRCONSUMER")
    for tuple in response.table(1).tuples():
        remote_cluster_id = tuple[4]
        covering_host = tuple[7]
        last_applied_ts = tuple[9]
        if covering_host != '':
            if remote_cluster_id not in remoteTopo:
                remoteTopo[remote_cluster_id] = covering_host

    # assume remote cluster use the same admin port
    command = ""
    for clusterId, covering_host in remoteTopo.items():
        command += 'voltadmin dr reset --cluster=%s -H %s:%d --force\n' % (survivor.clusterid, covering_host.split(":")[0], survivor.adminport)
    writeCommands(files[getKey(survivor)],
                  'Step %d: run dr reset command to tell other clusters to stop generating binary logs for the origin cluster' % step,
                  command)


def generateInitOldClusterCommmand(opts, surviveSet, files, new_cluster_deploy, halfNodes, step):
    initNodes = 0
    for hostInfo in surviveSet:
        initNodes += 1
        writeCommands(files[getKey(hostInfo)],
                      'Step %d: initialize original cluster with new voltdb path' % step,
                      '%s init --dir=%s --config=%s --force' % (os.path.join(opts.newKit, 'bin/voltdb'),
                                                                opts.newRoot,
                                                                os.path.join(opts.newRoot, new_cluster_deploy)))
        if initNodes == halfNodes:
            break

def generateNodeRejoinCommand(opts, surviveSet, leadersString, files, halfNodes, step):
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

def cleanup(opts, files, newNodeF):
    upgradePlan = open("upgrade-plan.txt", 'w+')
    for key, file in files.items():
        file.seek(0)
        upgradePlan.write(file.read() + '\n')
        file.close()
        os.remove(file.name)

    if opts.newNode is not None:
        newNodeF.seek(0)
        upgradePlan.write(newNodeF.read() + '\n')
        newNodeF.close()
        os.remove(newNodeF.name)

    upgradePlan.close()

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

def getCurrentDeploymentFile(runner, host):
    # get deployment file through rest API
    url = 'http://' + getHostnameOrIp(host) + ':' + str(host.httpport) + '/deployment/download/'
    request = Request(url)
    base64string = base64.b64encode('%s:%s' % (runner.opts.username, runner.opts.password))
    request.add_header("Authorization", "Basic %s" % base64string)
    try:
        response = urlopen(request)
    except URLError, e:
        runner.abort("Failed to get deployment file from %s " % (getHostnameOrIp(host)))

    return response.read()

# Only stand-alone cluster needs it
def createDeploymentForOriginalCluster(runner, xmlString, drSource, origin_cluster_deploy):
    et = ElementTree.ElementTree(ElementTree.fromstring(xmlString))
    hasAbsPath = checkAbsPaths(runner, et.getroot(), origin_cluster_deploy)

    dr = et.getroot().find('./dr')
    if dr is None:
        runner.abort("This cluster doesn't have a DR tag in its deployment file, hence we can't generate online upgrade plan for it. " +
                     "In order to add DR tag to the deployment file, users are required to shutdown the database.")

    if 'id' not in dr.attrib:
        clusterId = 0;  # by default clusterId is 0
    else :
        clusterId = int(dr.attrib['id']);

    connection = dr.find('./connection')
    if connection is None:
        connection = ElementTree.Element('connection')
        dr.append(connection)

    if 'source' in connection.attrib:
        connection.attrib['source'] += ',' + drSource
    else:
        connection.attrib['source'] = drSource
    connection.attrib['enabled'] = 'true'

    prettyprint(et.getroot())
    et.write(origin_cluster_deploy)
    return clusterId, hasAbsPath;

# Both stand-alone and multisite cluster need it
def createDeploymentForNewCluster(runner, xmlString, clusterIds, drSource, new_cluster_deploy):
    et = ElementTree.ElementTree(ElementTree.fromstring(xmlString))
    hasAbsPath = checkAbsPaths(runner, et.getroot(), new_cluster_deploy)

    # Prefer not to use 0 as the cluster Id
    for clusterId in range(1, 127):
        if clusterId not in clusterIds:
            break

    # since we check the existence of DR tag when generating deployment file for origin cluster, it's safe to skip it here
    dr = et.getroot().find('./dr')
    dr.attrib['id'] = str(clusterId)
    connection = dr.find('./connection')

    if connection is None:
        connection = ElementTree.Element('connection')
        dr.append(connection)

    connection.attrib['enabled'] = 'true'
    if drSource is not None:
        # for stand-alone cluster
        if 'source' in connection.attrib:
            connection.attrib['source'] += ',' + drSource
        else:
            connection.attrib['source'] = drSource
    else:
        # for multi-cluster
        if 'source' not in connection.attrib:
            # Connect to a random covering host
            response = checkstats.get_stats(runner, "DRCONSUMER")
            for tuple in response.table(1).tuples():
                covering_host = tuple[7]
                if covering_host != '':
                    connection.attrib['source'] = covering_host + ":" + host.drport  # assume it use the same DR port
                    break;
            if 'source' not in connection.attrib:
                runner.abort("Failed to generate the deployment file, no remote host found")
        else:
            # Don't change the source if it exists
            pass

    prettyprint(et.getroot())
    et.write(new_cluster_deploy)
    return hasAbsPath

def commentAbsPath(root, pathname, comment):
    path = root.find(pathname)
    if os.path.isabs(path.attrib['path']):
        commentedPath = ElementTree.Comment(ElementTree.tostring(path))
        root.append(comment)
        root.append(commentedPath)
        root.remove(path)
        return True
    return False

# Check if the path is an absolute path, if true then comment the path in case of overwriting important artifact accidentally
def checkAbsPaths(runner, root, filename):
    ret = False;
    comment = ElementTree.Comment("ERROR: PLEASE EDIT THE PATH BEFORE USE!")
    paths = root.find('./paths')
    ret |= commentAbsPath(paths, 'voltdbroot', comment)
    ret |= commentAbsPath(paths, 'snapshots', comment)
    ret |= commentAbsPath(paths, 'exportoverflow', comment)
    ret |= commentAbsPath(paths, 'droverflow', comment)
    ret |= commentAbsPath(paths, 'commandlog', comment)
    ret |= commentAbsPath(paths, 'commandlogsnapshot', comment)

    # If any of the path is an absolute path, print an warning to console
    if ret:
        runner.warning(filename + " contains absolute paths. To avoid overwriting artifacts of the original cluster, please review the file before use.")

    return ret

# package ElementTree has a problem of not indent sub-element correctly, thus this function is used to beautify output xml.
def prettyprint(elem, level=0):
    tabspace = "    "
    i = "\n" + level * tabspace
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            prettyprint(elem, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def checkNewNode(hostname):
    try:
        output = subprocess.check_output("ping -c 1 " + hostname, shell=False)
    except Exception, e:
        return False

    return True
