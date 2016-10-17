# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
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
import time
import signal
from voltcli import checkstats

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Shutdown the running VoltDB cluster.',
    options = (
        VOLT.BooleanOption('-f', '--force', 'forcing', 'immediate shutdown', default = False),
        VOLT.BooleanOption('-s', '--save', 'save', 'snapshot database contents', default = False),
    )
)
def shutdown(runner):
    if runner.opts.forcing and runner.opts.save:
       runner.abort_with_help('You cannot specify both --force and --save options.')
    shutdown_params = []
    columns = []
    zk_pause_txnid = 0
    runner.info('Cluster shutdown in progress.')
    if not runner.opts.forcing:
        try:
            runner.info('Preparing for shutdown')
            resp = runner.call_proc('@PrepareShutdown', [], [])
            if resp.status() != 1:
                runner.abort('The preparation for shutdown failed with status: %d' % resp.response.statusString)
            zk_pause_txnid = resp.table(0).tuple(0).column_integer(0)
            runner.info('The cluster is paused prior to shutdown.')
            runner.info('Writing out all queued export data')
            status = runner.call_proc('@Quiesce', [], []).table(0).tuple(0).column_integer(0)
            if status <> 0:
                runner.abort('The cluster has failed to be quiesce with status: %d' % status)
            runner.info('Completing outstanding export and DR transactions...')
            checkstats.check_export_dr(runner)
            runner.info('Completing outstanding client transactions.')
            checkstats.check_clients(runner)
            runner.info('Completing outstanding importer requests.')
            checkstats.check_importer(runner)
            runner.info('Cluster is ready for shutdown')
            if runner.opts.save:
               columns = [VOLT.FastSerializer.VOLTTYPE_BIGINT]
               shutdown_params =  [zk_pause_txnid]
        except (KeyboardInterrupt, SystemExit):
            runner.info('The cluster shutdown process has stopped. The cluster is still in a paused state.')
            runner.abort('You may shutdown the cluster with the "voltadmin shutdown --force" command, or continue to wait with "voltadmin shutdown".')
    response = runner.call_proc('@Shutdown', columns, shutdown_params, check_status = False)
    print response
