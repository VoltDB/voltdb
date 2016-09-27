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
from voltcli import checkstats

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Shutdown the running VoltDB cluster.',
    options = (
        VOLT.BooleanOption('-f', '--force', 'forcing', 'immediate shutdown', default = False),
    )
)
def shutdown(runner):
    if runner.opts.forcing==False:
        runner.info('Preparing for shutdown')
        status = runner.call_proc('@PrepareShutdown', [], []).table(0).tuple(0).column_integer(0)
        if status <> 0:
            runner.error('The preparation for shutdown failed with status: %d' % status)
            return
        runner.info('The cluster is paused prior to shutdown.')
        runner.info('Writing out all queued export data')
        status = runner.call_proc('@Quiesce', [], []).table(0).tuple(0).column_integer(0)
        if status <> 0:
            runner.error('The cluster has failed to be quiesce with status: %d' % status)
            return
        runner.info('Completing outstanding export and DR transactions...')
        checkstats.check_export_dr(runner)
        runner.info('Completing outstanding client transactions.')
        checkstats.check_clients(runner)
        runner.info('Completing outstanding importer requests.')
        checkstats.check_importer(runner)
        runner.info('Cluster is ready for shutdown')
    runner.info('Cluster shutdown in progress.')
    response = runner.call_proc('@Shutdown', [], [], check_status = False)
    print response

