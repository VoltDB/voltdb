# This file is part of VoltDB.
# Copyright (C) 2008-2019 VoltDB Inc.
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

def test(runner):
    procedureCaller(runner, Option.TEST)

def start(runner):
    procedureCaller(runner, Option.START)

def restart(runner):
    procedureCaller(runner, Option.RESTART)

def status(runner):
    procedureCaller(runner, Option.STATUS)

def update(runner):
    procedureCaller(runner, Option.UPDATE)

def procedureCaller(runner, type):
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['OVERVIEW'])

    # Convert @SystemInformation results to objects.
    hosts = Hosts(runner.abort)
    for tuple in response.table(0).tuples():
        hosts.update(tuple[0], tuple[1], tuple[2])

    # get current version and root directory from an arbitrary node
    host = hosts.hosts_by_id.itervalues().next()

    # check the version of target cluster to make it work properly.
    version = host.version
    versionStr = version.split('.')
    majorVersion = int(versionStr[0])
    minorVersion = int(versionStr[1])
    if majorVersion < RELEASE_MAJOR_VERSION or (majorVersion == RELEASE_MAJOR_VERSION and minorVersion < RELEASE_MINOR_VERSION):
        runner.abort('The version of targeting cluster is ' + version + ' which is lower than version ' + str(RELEASE_MAJOR_VERSION) + '.' + str(RELEASE_MINOR_VERSION) +' for supporting elastic resize.' )

    result = runner.call_proc('@ElasticRemoveNT', [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_STRING, VOLT.FastSerializer.VOLTTYPE_STRING],
                              [type, '', ','.join(runner.opts.skip_requirements)]).table(0)
    status = result.tuple(0).column_integer(0)
    message = result.tuple(0).column_string(1)
    if status == 0:
        runner.info(message)
    else:
        runner.error(message)
        sys.exit(1)

@VOLT.Multi_Command(
    bundles = VOLT.AdminBundle(),
    description = 'Elastic resizing cluster command.',
    options = (
            VOLT.StringListOption(None, '--ignore', 'skip_requirements',
                                  '''requirements to skip when start resizing:
                                  disabled_export - Bypass checking disabled export targets. ''',
                                  default = ''),
    ),
    modifiers = (
            VOLT.Modifier('test', test, 'Check the feasibility of current resizing plan.'),
            VOLT.Modifier('start', start, 'Start the elastically resizing.'),
            VOLT.Modifier('restart', restart, 'Restart the previous failed resizing operation.'),
            VOLT.Modifier('status', status, 'Check the resizing progress.'),
            VOLT.Modifier('update', update, 'Update the options for the current resizing operation.'),
    )
)

def resize(runner):
    runner.go()