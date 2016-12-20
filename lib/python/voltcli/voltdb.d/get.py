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

import sys, os, subprocess

voltdbroot_help = ('Specifies the root directory for the database. The default '
                   'is voltdbroot under the current working directory.')
@VOLT.Command(
    description = 'Get voltdb configuration settings.',
    options = (
        VOLT.StringOption('-D', '--dir', 'directory_spec', voltdbroot_help, default = "voltdbroot"),
        VOLT.StringOption('-o', '--out', 'output', 'File to save configuration obtained.', default=""),
    ),
    arguments = (
        VOLT.StringArgument('deployment', 'Get deployment configuration.', default = 'deployment')
    )
)

def get(runner):
    runner.args.extend(['get', 'deployment', 'getvoltdbroot', runner.opts.directory_spec, 'file', runner.opts.output])
    runner.java_execute('org.voltdb.VoltDB', None, *runner.args)
