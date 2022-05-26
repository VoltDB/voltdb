# This file is part of VoltDB.
# Copyright (C) 2022 Volt Active Data Inc.
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

import os
from voltcli import utility

@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description='Log a message in the log of each node in the cluster.',
    arguments=(
        VOLT.StringArgument(
            'message',
            'The message to log',
            min_count=1, max_count=1),
    ),
)

# get proper error message from the procedure call
def note(runner):
    message = runner.opts.message
    print(message)
    response = runner.call_proc('@Note', [VOLT.FastSerializer.VOLTTYPE_STRING], [message])
    if response.status() != 1:
        runner.abort('Failed to log the message. Response status: %d' % response.response.statusString)
