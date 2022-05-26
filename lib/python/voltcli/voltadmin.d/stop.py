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

# Stop a node. Written to easily support multiple, but configured for
# a single host for now.

from voltcli.hostinfo import Host
from voltcli.hostinfo import Hosts
from voltcli import checkstats
from voltcli.checkstats import StatisticsProcedureException
from voltcli import utility
import sys

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Stop one host of a running VoltDB cluster.',
    options=(
        VOLT.BooleanOption('-f', '--force', 'forcing', 'immediate shutdown', default=False),
        VOLT.IntegerOption('-t', '--timeout', 'timeout', 'The timeout value in seconds if @Statistics is not progressing.', default=120),
        ),
    arguments = (
        VOLT.StringArgument('target_host', 'the target hostname[:port] or address[:port]. (default port=3021)'),
    ),
)

def stop(runner):

    # Exec @SystemInformation to find out about the cluster.
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['OVERVIEW'])

    # Convert @SystemInformation results to objects.
    hosts = Hosts(runner.abort)
    for tuple in response.table(0).tuples():
        hosts.update(tuple[0], tuple[1], tuple[2])

    # Connect to an arbitrary host that isn't being stopped.
    defaultport = 3021
    min_hosts = 1
    max_hosts = 1
    target_host = utility.parse_hosts(runner.opts.target_host, min_hosts, max_hosts, defaultport)[0]
    (thost, chost) = hosts.get_target_and_connection_host(target_host.host, target_host.port)
    if thost is None:
        runner.abort('Host not found in cluster: %s:%d' % (target_host.host, target_host.port))
    if chost is None:
        runner.abort('The entire cluster is being stopped, use "shutdown" instead.')

    if runner.opts.username:
        user_info = ', user: %s' % runner.opts.username
    else:
        user_info = ''
    runner.info('Connecting to %s:%d%s (%s) to issue "stop" command' %
                (chost.get_admininterface(), chost.adminport, user_info, chost.hostname))
    runner.voltdb_connect(chost.get_admininterface(), chost.adminport,
                          runner.opts.username, runner.opts.password,
                          runner.opts.ssl_config, runner.opts.kerberos)

    if not runner.opts.forcing:
        stateMessage = 'The node shutdown process has stopped.'
        actionMessage = 'You may shutdown the node with the "voltadmin stop --force" command.'
        try:
            runner.info('Preparing for stopping node.')
            resp = runner.call_proc('@PrepareStopNode',
                                    [VOLT.FastSerializer.VOLTTYPE_INTEGER],
                                    [thost.id],
                                    check_status=False)
            if resp.status() != 1:
                runner.abort('The preparation for node shutdown failed with status: %s' % resp.response.statusString)
            # monitor partition leader migration
            runner.info('Completing partition leader migration away from host %d: %s' % (thost.id, thost.hostname))
            checkstats.check_partition_leaders_on_host(runner,thost.id)
            runner.info('All partition leaders have been migrated.')
            # monitor export master transfer, but don't fail on timeout: target may have been
            # disabled, preventing transfer. In that case it's ok to proceed with the stop
            try:
                runner.info('Completing export master transfer away from host %d: %s' % (thost.id, thost.hostname))
                checkstats.check_export_mastership_on_host(runner,thost.id)
                runner.info('All export masters have been transferred')
            except StatisticsProcedureException as proex:
                if not proex.isTimeout:
                    raise
                runner.info(proex.message)
                runner.info('This may be caused by an export target either disabled or removed from the configuration. No action is required; the stop node process will proceed.')
        except StatisticsProcedureException as proex:
             runner.info(stateMessage)
             runner.error(proex.message)
             if proex.isTimeout:
                 runner.info(actionMessage)
             sys.exit(proex.exitCode)
        except (KeyboardInterrupt, SystemExit):
            runner.info(stateMessage)
            runner.abort(actionMessage)
    # Stop the requested host using exec @StopNode HOST_ID
    runner.info('Stopping host %d: %s:%s' % (thost.id, thost.hostname, thost.internalport))
    if not runner.opts.dryrun:
        response = runner.call_proc('@StopNode',
                                    [VOLT.FastSerializer.VOLTTYPE_INTEGER],
                                    [thost.id],
                                    check_status=False)
        print(response)
        if response.status() != 1:  # not SUCCESS
            sys.exit(1)
