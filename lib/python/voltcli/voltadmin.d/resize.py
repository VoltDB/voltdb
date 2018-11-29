# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.
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

RELEASE_MAJOR_VERSION = 8
RELEASE_MINOR_VERSION = 5

def test(runner):
    result = runner.call_proc('@ElasticRemoveNT', [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_STRING],
                              [0, runner.opts.hostOrPartition]).table(0)
    status = result.tuple(0).column_integer(0)
    message = result.tuple(0).column_string(1)
    if status == 0:
        runner.info(message)
    else:
        runner.error(message)
        sys.exit(1)

def start(runner):
    result = runner.call_proc('@ElasticRemoveNT', [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_STRING],
                              [1, runner.opts.hostOrPartition]).table(0)
    status = result.tuple(0).column_integer(0)
    message = result.tuple(0).column_string(1)
    if status == 0:
        runner.info(message)
    else:
        runner.error(message)
        sys.exit(1)

def status(runner):
    result = runner.call_proc('@ElasticRemoveNT', [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_STRING],
                              [2, runner.opts.hostOrPartition]).table(0)
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
            VOLT.BooleanOption('-f', '--force', 'forcing', 'bypass precheck', default = False),
            VOLT.IntegerOption('-t', '--timeout', 'timeout', 'The timeout value in seconds if @Statistics is not progressing.', default = 0),
            VOLT.StringOption(None, '--hostOrPartition', 'hostOrPartition', 'Specified hosts or partitions to be removed.', default = ''), # TODO: use StringListOptition if hostOrPartition format changes
    ),
    modifiers = (
            VOLT.Modifier('test', test, 'Check the feasibility of current resizing plan.'),
            VOLT.Modifier('start', start, 'Start the elastically resizing.'),
            VOLT.Modifier('status', status, 'Check the resizing progress.'),
    )
)

def resize(runner):
    runner.go()