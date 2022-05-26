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
import time
import signal
from voltcli import checkstats
from voltcli import utility
from voltcli.hostinfo import Hosts

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Dumping stacktrace for one or all hosts of a running VoltDB Cluster.',
    arguments =(
            VOLT.StringArgument('target_host', 'the target hostname[:port] or address[:port]. (default all hosts)', optional=True)
    ),
)

def jstack(runner):
    # take the jstack using exec @JStack HOST_ID
    if runner.opts.target_host is None:
        runner.info('Taking jstack of all hosts.')
        hsId = -1
    else:
        runner.info('Taking jstack of host: %s' % (runner.opts.target_host))
        hsId = findTargetHsId(runner)

    if not runner.opts.dryrun:
        response = runner.call_proc('@JStack',
                                    [VOLT.FastSerializer.VOLTTYPE_INTEGER],
                                    [hsId])
        print(response)
        if response.status() != 1:  # not SUCCESS
            sys.exit(1)

def findTargetHsId(runner):
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
    return thost.id
