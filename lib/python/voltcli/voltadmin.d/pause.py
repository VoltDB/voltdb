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
    description = 'Pause the VoltDB cluster and switch it to admin mode.',
    options = (
        VOLT.BooleanOption('-w', '--wait', 'waiting',
                           'wait for all DR and Export transactions to be externally processed',
                           default = False)
    )
)
def pause(runner):
    # Check the STATUS column. runner.call_proc() detects and aborts on errors.
    status = runner.call_proc('@Pause', [], []).table(0).tuple(0).column_integer(0)
    if status <> 0:
        runner.error('The cluster has failed to pause with status: %d' % status)
        return
    runner.info('The cluster is paused.')
    if runner.opts.waiting:
        status = runner.call_proc('@Quiesce', [], []).table(0).tuple(0).column_integer(0)
        if status <> 0:
            runner.error('The cluster has failed to quiesce with status: %d' % status)
            return
        runner.info('The cluster is quiesced.')
        #check the dr stats
        checkstats.check_export_dr(runner)
