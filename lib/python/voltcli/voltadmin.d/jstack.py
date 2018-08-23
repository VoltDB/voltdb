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
import time
import signal
from voltcli import checkstats
from voltcli import utility

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Print out stacktrace using jstack for all clusters.',
    options = (
        VOLT.StringOption(None, '--hsIds', 'hsIds',
                              'Specify the host you want to dump stack trace.',
                              default = '0')
    ),
)

def jstack(runner):
    if not runner.opts.hsIds:
        print "Must provide site IDs for dumping stack traces."
        return
    json_opts = ['{hsIds: "%s"}' % (runner.opts.hsIds)]
    runner.verbose_info('@JStack "%s"' % json_opts)
    columns = [VOLT.FastSerializer.VOLTTYPE_STRING]
    print 'voltadmin: JStack command has been executed. Check the server logs for results.'
    response = runner.call_proc('@JStack', columns, json_opts)