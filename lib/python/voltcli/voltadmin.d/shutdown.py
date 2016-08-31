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
    description = 'Shut down the running VoltDB cluster.',
    options = (
        VOLT.BooleanOption('-f', '--force', 'forcing', 'flush transactions', default = False),
    )
)
def shutdown(runner):

    if runner.opts.forcing==False:
        status = runner.call_proc('@PrepareShutdown', [], []).table(0).tuple(0).column_integer(0)
        if status <> 0:
            runner.error('The cluster has failed to prepare for shutdown with status: %d' % status)
            return
        runner.info('Cluster is preparing for shutdown.')

        status = runner.call_proc('@Quiesce', [], []).table(0).tuple(0).column_integer(0)
        if status <> 0:
            runner.error('The cluster has failed to quiesce with status: %d' % status)
            return
        runner.info('The cluster is quiesced.')

        runner.info('Completing export and DR transactions...')
        checkstats.check_export_dr(runner)

        runner.info('Completing pending client transactions.')
        check_clients(runner)

    runner.info('Cluster shutdown in progress.')
    response = runner.call_proc('@Shutdown', [], [], check_status = False)
    print response

def check_clients(runner):
     while True:
        resp = checkstats.get_stats(runner, 'LIVECLIENTS')
        data = resp.table(0)
        trans = 0
        bytes = 0
        msgs  = 0
        for r in data.tuples():
            bytes += r[6]
            msgs += r[7]
            trans += r[8]
        runner.info('Outstanding transactions=' + str(trans) + ', buffer queued=' + str(bytes) + ', response messages=' + str(msgs))
        if trans == 0 and bytes == 0 and msgs == 0:
            return
        time.sleep(1)
