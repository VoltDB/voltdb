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
from voltcli import checkstats
from voltcli.checkstats import StatisticsProcedureException
from voltcli.hostinfo import Hosts

RELEASE_MAJOR_VERSION = 7
RELEASE_MINOR_VERSION = 2

def reset(runner):
    result = runner.call_proc('@ResetDR', [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_TINYINT],
                              [runner.opts.clusterId, runner.opts.forcing * 1, runner.opts.resetAll * 1]).table(0)
    status = result.tuple(0).column_integer(0)
    message = result.tuple(0).column_string(1)
    if status == 0:
        runner.info(message)
    else:
        runner.error(message)
        sys.exit(1)

def drop(runner):
    result = runner.call_proc('@ResetDR', [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_TINYINT],
                              [-1, runner.opts.forcing * 1, -1]).table(0)
    status = result.tuple(0).column_integer(0)
    message = result.tuple(0).column_string(1)
    if status == 0:
        runner.info(message)
    else:
        runner.error(message)
        sys.exit(1)

    # post check for drop
    actionMessage = 'Not all connected clusters report received reset. You may continue monitoring remaining consumer clusters with @Statistics'
    try:
        if status == 0:
            checkstats.check_no_dr_consumer(runner)
            runner.info('All connected clusters have been successfully reset. Safe to shutdown...')
        else:
            if not checkstats.check_no_dr_consumer(runner, False):
                runner.warn('This cluster still has dr consumer(s). ' +
                            'This may be due to an unfinished dr drop cluster command. Please check those cluster(s) and do reset manually.')
    except StatisticsProcedureException as proex:
        runner.info('The previous command has timed out and stopped waiting... The cluster is in dropping phase.')
        runner.error(proex.message)
        if proex.isTimeout:
            runner.info(actionMessage)
        sys.exit(proex.exitCode)
    except (KeyboardInterrupt, SystemExit):
        runner.info('The previous command has stopped waiting... The cluster is in dropping phase.')
        runner.abort(actionMessage)

    sys.exit(status)

@VOLT.Multi_Command(
    bundles = VOLT.AdminBundle(),
    description = 'DR control command.',
    options = (
            VOLT.BooleanOption('-f', '--force', 'forcing', 'bypass precheck', default = False),
            VOLT.IntegerOption('-c', '--cluster', 'clusterId', 'remote-cluster-ID', default = -1),
            VOLT.BooleanOption('-a', '--all', 'resetAll', 'reset all connected cluster(s)', default = False),
            VOLT.IntegerOption('-t', '--timeout', 'timeout', 'The timeout value in seconds if @Statistics is not progressing.', default = 0),
    ),
    modifiers = (
            VOLT.Modifier('reset', reset, 'reset one/all remote dr cluster(s).'),
            VOLT.Modifier('drop', drop, 'gracefully drop current cluster off the mesh, inform all its connected cluster(s) to reset it.'),
    )
)

def dr(runner):
    if runner.opts.forcing and runner.opts.resetAll:
        runner.abort_with_help('You cannot specify both --force and --all options.')
    if runner.opts.clusterId >= 0 and runner.opts.resetAll:
        runner.abort_with_help('You cannot specify both --cluster and --all options.')
    if runner.opts.modifier == "drop" and (runner.opts.forcing or runner.opts.clusterId >=0 or runner.opts.resetAll):
        runner.abort_with_help('You cannot specify either --force, --cluster or --all for drop.')
    if runner.opts.timeout < 0:
        runner.abort_with_help('The timeout value must be non-negative seconds.')
    if runner.opts.clusterId < -1 or runner.opts.clusterId > 127:
        runner.abort_with_help('The specified cluster ID must be in the range of 0 to 127.')
    if runner.opts.clusterId >= 0 and runner.opts.clusterId == getOwnClusterId(runner):
        runner.abort_with_help('You cannot specify the current cluster ID in the DR RESET command. You must specify the ID of a different cluster.')
    if runner.opts.clusterId >= 0 and runner.opts.clusterId not in getRemoteClusterIds(runner):
        runner.abort_with_help('The specified cluster ID ' + str(runner.opts.clusterId) + ' is not recognized as a member of the current DR environment.')

    runner.go()

def getOwnClusterId(runner):
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['OVERVIEW'])

    # Convert @SystemInformation results to objects.
    hosts = Hosts(runner.abort)
    for tuple in response.table(0).tuples():
        hosts.update(tuple[0], tuple[1], tuple[2])

    # get current version and root directory from an arbitrary node
    host = next(iter(hosts.hosts_by_id.values()))

    # ClusterId in @SystemInformation is added in v7.2, so must check the version of target cluster to make it work properly.
    version = host.version
    versionStr = version.split('.')
    majorVersion = int(versionStr[0])
    minorVersion = int(versionStr[1])
    if majorVersion < RELEASE_MAJOR_VERSION or (majorVersion == RELEASE_MAJOR_VERSION and minorVersion < RELEASE_MINOR_VERSION):
        return -1

    return int(host.clusterid)

def getRemoteClusterIds(runner):
    response = checkstats.get_stats(runner, "DRROLE")
    clusterIds = []
    for tuple in response.table(0).tuples():
        remote_cluster_id = tuple[2]
        if remote_cluster_id != -1:
            clusterIds.append(remote_cluster_id)
    return clusterIds
