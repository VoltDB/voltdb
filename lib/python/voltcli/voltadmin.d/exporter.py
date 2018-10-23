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

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Pause, skip or resume an export stream.',
    arguments = (
        VOLT.StringArgument('stream', 'The name of stream'),
        VOLT.StringArgument('target', 'The name of targte for the stream'),
        VOLT.StringArgument('operation', 'The operation, skip, pause, or resume')
    )
)
def exporter(runner):
    columns = [VOLT.FastSerializer.VOLTTYPE_STRING, VOLT.FastSerializer.VOLTTYPE_STRING, VOLT.FastSerializer.VOLTTYPE_STRING]
    params = [runner.opts.stream, runner.opts.target, runner.opts.operation]
    status = runner.call_proc('@ExportControl', columns, params)

