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
from voltcli.checkstats import StatisticsProcedureException
from voltcli import utility
from voltcli.hostinfo import Hosts


@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description='Shutdown the running VoltDB cluster.',
    options=(
        VOLT.BooleanOption('-f', '--force', 'forcing', 'immediate shutdown', default=False),
        VOLT.BooleanOption('-s', '--save', 'save', 'snapshot database contents', default=False),
        VOLT.BooleanOption('-c', '--cancel', 'cancel', 'cancel a shutdown', default=False),
        VOLT.IntegerOption('-t', '--timeout', 'timeout', 'The timeout value in seconds if @Statistics is not progressing.', default=120),
    )
)

def shutdown(runner):
    if runner.opts.forcing and runner.opts.save:
       runner.abort_with_help('You cannot specify both --force and --save options.')
    if runner.opts.cancel and runner.opts.save:
       runner.abort_with_help('You cannot specify both --cancel and --save options.')
    if runner.opts.cancel and runner.opts.forcing:
       runner.abort_with_help('You cannot specify both --cancel and --force options.')
    if runner.opts.timeout <= 0:
        runner.abort_with_help('The timeout value must be more than zero seconds.')
    shutdown_params = []
    columns = []
    zk_pause_txnid = 0

    if runner.opts.cancel:
        runner.info('Canceling cluster shutdown ...')
        response = runner.call_proc('@CancelShutdown', [], [])
        if response.status() != 1:
            runner.abort('Cancel shutdown failed with status: %d' % resp.response.statusString)
        else:
            runner.info('Shutdown canceled.')
    else:
        runner.info('Cluster shutdown in progress.')
        if not runner.opts.forcing:
            response = runner.call_proc('@SystemInformation',
                                    [VOLT.FastSerializer.VOLTTYPE_STRING],
                                    ['OVERVIEW'])

            # Convert @SystemInformation results to objects.
            hosts = Hosts(runner.abort)
            for tuple in response.table(0).tuples():
                hosts.update(*tuple)
            host = next(iter(hosts.hosts_by_id.values()))

            if host.get('clustersafety') == "REDUCED":
                runner.info('Since cluster is in reduced k safety mode, taking a final snapshot before shutdown.')
                runner.opts.save = True

            stateMessage = 'The cluster shutdown process has stopped. The cluster is still in a paused state.'
            actionMessage = 'You may shutdown the cluster with the "voltadmin shutdown --force" command, '\
                            + 'continue to wait with "voltadmin shutdown",\n'\
                            + 'or cancel the shutdown with the "voltadmin shutdown --cancel" command'
            try:
                runner.info('Preparing for shutdown...')
                resp = runner.call_proc('@PrepareShutdown', [], [])
                if resp.status() != 1:
                    runner.abort('The preparation for shutdown failed with status: %d' % resp.response.statusString)
                zk_pause_txnid = resp.table(0).tuple(0).column_integer(0)
                runner.info('The cluster is paused prior to shutdown.')

                runner.info('Writing out all queued export data...')
                status = runner.call_proc('@Quiesce', [], []).table(0).tuple(0).column_integer(0)
                if status != 0:
                    runner.abort('The cluster has failed to be quiesce with status: %d' % status)

                checkstats.check_clients(runner)
                checkstats.check_importer(runner)

                # Checking command log regardless of whether we're community or enterprise
                checkstats.check_command_log(runner)
                runner.info('If running Enterprise Edition, all transactions have been made durable.')

                if runner.opts.save:
                   actionMessage = 'You may shutdown the cluster with the "voltadmin shutdown --force" command, '\
                            + 'continue to wait with "voltadmin shutdown --save",\n'\
                            + 'or cancel the shutdown with the "voltadmin shutdown --cancel" command.'
                   columns = [VOLT.FastSerializer.VOLTTYPE_BIGINT]
                   shutdown_params = [zk_pause_txnid]
                   # save option, check more stats
                   checkstats.check_dr_consumer(runner)
                   runner.info('Starting resolution of external commitments...')
                   checkstats.check_exporter(runner)
                   status = runner.call_proc('@Quiesce', [], []).table(0).tuple(0).column_integer(0)
                   if status != 0:
                       runner.abort('The cluster has failed to quiesce with status: %d' % status)
                   checkstats.check_dr_producer(runner)
                   runner.info('Saving a final snapshot, The cluster will shutdown after the snapshot is finished...')
                else:
                    checkstats.check_exporter(runner)
                    runner.info('Shutting down the cluster...')
            except StatisticsProcedureException as proex:
                 runner.info(stateMessage)
                 runner.error(proex.message)
                 if proex.isTimeout:
                     runner.info(actionMessage)
                 sys.exit(proex.exitCode)
            except (KeyboardInterrupt, SystemExit):
                runner.info(stateMessage)
                runner.abort(actionMessage)
        response = runner.call_proc('@Shutdown', columns, shutdown_params, check_status=False)
        print(response)

