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
import re
from voltcli.hostinfo import Hosts

RELEASE_MAJOR_VERSION = 9
RELEASE_MINOR_VERSION = 1

# elastic remove procedure call option
# Need to be update once ElasticRemoveNT.java add/remove/change option coding
class Option:
    TEST = 0
    START = 1
    STATUS = 2
    RESTART = 3
    UPDATE = 4

def hostIdsToNames(hostId, hosts):
    host = hosts.hosts_by_id.get(int(hostId))
    return host.hostname if host else 'UNAVAILABLE'

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Elastic resizing cluster command.',
    options = (
            VOLT.StringListOption(None, '--ignore', 'skip_requirements',
                                  '''Conditions that can be ignored when resizing the cluster:
                                  disabled_export -- ignore pending export data for targets that are disabled''',
                                  default = ''),
            VOLT.IntegerOption(None, '--delay', 'shutdown_delay', 'Delay the shutdown of the hosts which are being removed. '
                               + 'This needs to be specified if topics are being used. Unit is minutes.', default=-1),
            VOLT.StringOption(None, '--test', 'opt', 'Check the feasibility of current resizing plan.', action='store_const', const=Option.TEST, default=Option.START),
            VOLT.StringOption(None, '--restart', 'opt', 'Restart the previous failed resizing operation.', action='store_const', const=Option.RESTART),
            VOLT.StringOption(None, '--status', 'opt', 'Check the resizing progress.', action='store_const', const=Option.STATUS),
            VOLT.StringOption(None, '--update', 'opt', 'Update the options for the current resizing operation.', action='store_const', const=Option.UPDATE),
    ),
)
def resize(runner):
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['OVERVIEW'])

    # Convert @SystemInformation results to objects.
    hosts = Hosts(runner.abort)
    for tuple in response.table(0).tuples():
        hosts.update(*tuple)

    # get current version and root directory from an arbitrary node
    host = next(iter(hosts.hosts_by_id.values()))

    # check the version of target cluster to make it work properly.
    version = host.version
    versionStr = version.split('.')
    majorVersion = int(versionStr[0])
    minorVersion = int(versionStr[1])
    if "license" not in host:
        runner.abort('Elastic resize only available for enterprise edition.')

    if majorVersion < RELEASE_MAJOR_VERSION or (majorVersion == RELEASE_MAJOR_VERSION and minorVersion < RELEASE_MINOR_VERSION):
        runner.abort('The version of targeting cluster is ' + version + ' which is lower than version ' + str(RELEASE_MAJOR_VERSION) + '.' + str(RELEASE_MINOR_VERSION) +' for supporting elastic resize.' )

    # Convert shutdown delay input of minutes to millis
    shutdown_delay = runner.opts.shutdown_delay * 60000 if runner.opts.shutdown_delay > 0 else -1
    option = runner.opts.opt
    result = runner.call_proc('@ElasticRemoveNT', [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_STRING, VOLT.FastSerializer.VOLTTYPE_STRING, VOLT.FastSerializer.VOLTTYPE_BIGINT],
                              [option, '', ','.join(runner.opts.skip_requirements), shutdown_delay]).table(0)
    status = result.tuple(0).column_integer(0)
    message = result.tuple(0).column_string(1)
    if option in (Option.TEST, Option.START) and "host ids:" in message:
        host_names = ', '.join([hostIdsToNames(id, hosts) for id in re.search('host ids: \[(.+?)\]', message).group(1).split(',')])
        if option == Option.TEST:
            message = "Hosts will be removed: [" + host_names + "], " + message
        elif option == Option.START:
            message = "Starting cluster resize: Removing hosts: [" + host_names + "], " + message
    if status == 0:
        runner.info(message)
    else:
        runner.error(message)
        sys.exit(1)
