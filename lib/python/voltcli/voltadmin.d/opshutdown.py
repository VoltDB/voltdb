# This file is part of VoltDB.
# Copyright (C) 2008-2020 VoltDB Inc.
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
from voltcli.checkstats import StatisticsProcedureException
from voltcli import utility
from voltcli.hostinfo import Hosts

######################
# Temporary command to exercise the new @OpShutdown sysproc.
# No intention to ship this as-is.
######################


@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description='*EXPERIMENTAL* shut down the running VoltDB cluster.',
    options=(
        VOLT.BooleanOption('-f', '--force', 'forcing', 'immediate shutdown', default=False),
        VOLT.BooleanOption('-s', '--save', 'save', 'snapshot database contents', default=False),
        VOLT.BooleanOption('-c', '--cancel', 'cancel', 'cancel a shutdown', default=False),
        VOLT.IntegerOption('-t', '--timeout', 'timeout', 'progress timeout value in seconds', default=120),
        VOLT.IntegerOption('-w', '--waitlimit', 'waitlimit', 'overall wait limit in seconds', default=600),
        VOLT.BooleanOption('-x', '--xforce', 'xforce', 'force shutdown after timeout/waitlimit', default=False),
    )
)

def opshutdown(runner):
    if runner.opts.forcing and runner.opts.save:
       runner.abort_with_help('You cannot specify both --force and --save options.')
    if runner.opts.cancel and runner.opts.save:
       runner.abort_with_help('You cannot specify both --cancel and --save options.')
    if runner.opts.cancel and runner.opts.forcing:
       runner.abort_with_help('You cannot specify both --cancel and --force options.')
    if runner.opts.timeout <= 0:
        runner.abort_with_help('The timeout value must be more than zero seconds.')

    # Cancel
    if runner.opts.cancel:
        runner.info('Cancelling cluster shutdown ...')
        response = runner.call_proc('@CancelShutdown', [], [])
        print response
        return

    # Force: call @Shutdown directly
    if runner.opts.forcing:
        response = runner.call_proc('@Shutdown', columns, shutdown_params, check_status=False)
        print response
        return

    # Exec @SystemInformation to find out about the cluster.
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['OVERVIEW'])

    # Convert @SystemInformation results to objects.
    hosts = Hosts(runner.abort)
    for tuple in response.table(0).tuples():
        hosts.update(*tuple)
    host = hosts.hosts_by_id.itervalues().next()

    # Not forcing
    try:
        runner.info('Shutting down cluster...')
        shutdown_options = 0
        if runner.opts.save:
            shutdown_options |= 1
        if runner.opts.xforce:
            shutdown_options |= 2
        response = runner.call_proc('@OpShutdown',
                                    [VOLT.FastSerializer.VOLTTYPE_INTEGER, VOLT.FastSerializer.VOLTTYPE_INTEGER, VOLT.FastSerializer.VOLTTYPE_INTEGER],
                                    [shutdown_options, runner.opts.timeout, runner.opts.waitlimit],
                                check_status=False)
        print response # Expect 'connection broken'
    except (KeyboardInterrupt, SystemExit):
            runner.abort('Interrupted, but cluster may still be shutting down')
