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

def release(runner):
    json_opts = ['{source:"%s",targets:%s,command:"release"}' % (runner.opts.source, runner.opts.targets)]
    response = runner.call_proc('@ExportControl', [VOLT.FastSerializer.VOLTTYPE_STRING], json_opts)
    print response.table(0).format_table(caption = 'Snapshot Restore Results')

@VOLT.Multi_Command(
    bundles = VOLT.AdminBundle(),
    description = 'Export control command.',
    options = (
            VOLT.StringOption('s', '--source', 'source', 'The stream source', default = None),
            VOLT.StringListOption('t', '--target', 'targets', 'The export target on the stream', default = None)
    ),
    modifiers = (
            VOLT.Modifier('release', release, 'move past gaps in the export stream.')
    )
)

def export(runner):
     runner.go()

