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
    description = 'Dumping stacktrace for one or all hosts of a running VoltDB Cluster.',
    options = (
        VOLT.IntegerOption(None, '--hsId', 'hsId',
                              'Specify the hostId you want to dump, or -1 for all hosts.',
                              default = '-1')
    ),
)

def jstack(runner):
    if  (runner.opts.hsId < -1):
        runner.abort_with_help("Must provide valid host id for dumping stack traces.")

    # take the jstack using exec @JStack HOST_ID
    if runner.opts.hsId < 0:
        runner.info('Taking jstack of all hosts.')
    else:
        runner.info('Taking jstack of host %d: ' % (runner.opts.hsId))

    if not runner.opts.dryrun:
        response = runner.call_proc('@JStack',
                                    [VOLT.FastSerializer.VOLTTYPE_INTEGER],
                                    [runner.opts.hsId])
        print response
        if response.status() != 1:  # not SUCCESS
            sys.exit(1)