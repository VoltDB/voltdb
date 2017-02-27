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

# Stop a node. Written to easily support multiple, but configured for
# a single host for now.

from voltcli.hostinfo import Host
from voltcli.hostinfo import Hosts
from voltcli import utility

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Stop one host of a running VoltDB cluster.',
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
                          runner.opts.username, runner.opts.password)

    # Stop the requested host using exec @StopNode HOST_ID
    runner.info('Stopping host %d: %s:%s' % (thost.id, thost.hostname, thost.internalport))
    if not runner.opts.dryrun:
        response = runner.call_proc('@StopNode',
                                    [VOLT.FastSerializer.VOLTTYPE_INTEGER],
                                    [thost.id],
                                    check_status=False)
        print response
