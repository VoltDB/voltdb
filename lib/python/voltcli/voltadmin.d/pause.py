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
from voltcli import checkstats
from voltcli.checkstats import StatisticsProcedureException

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Pause the VoltDB cluster and switch it to admin mode.',
    options = (
        VOLT.BooleanOption('-w', '--wait', 'waiting',
                           'wait for all DR and Export transactions to be externally processed',
                           default = False),
        VOLT.IntegerOption('-t', '--timeout', 'timeout', 'The timeout value in seconds if @Statistics is not progressing.', default = 120),
    )
)
def pause(runner):
    if runner.opts.timeout <= 0:
        runner.abort_with_help('The timeout value must be more than zero seconds.')

    #Check the STATUS column. runner.call_proc() detects and aborts on errors.
    status = runner.call_proc('@Pause', [], []).table(0).tuple(0).column_integer(0)
    if status != 0:
        runner.abort('The cluster has failed to pause with status: %d' % status)
    runner.info('The cluster is paused.')
    if runner.opts.waiting:
        status = runner.call_proc('@Quiesce', [], []).table(0).tuple(0).column_integer(0)
        if status != 0:
            runner.abort('The cluster has failed to quiesce with status: %d' % status)
        runner.info('The cluster is quiesced.')
        actionMessage = 'Transactions may not be completely drained. You may continue monitoring the outstanding transactions with @Statistics'
        try:
            checkstats.check_exporter(runner)
            status = runner.call_proc('@Quiesce', [], []).table(0).tuple(0).column_integer(0)
            if status != 0:
                runner.abort('The cluster has failed to quiesce with status: %d' % status)
            checkstats.check_dr_producer(runner)
        except StatisticsProcedureException as proex:
            runner.info('The previous command has timed out and stopped waiting... The cluster is in a paused state.')
            runner.error(proex.message)
            if proex.isTimeout:
                runner.info(actionMessage)
            sys.exit(proex.exitCode)
        except (KeyboardInterrupt, SystemExit):
            runner.info('The previous command has stopped waiting... The cluster is in a paused state.')
            runner.abort(actionMessage)
