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

# Main Java Class
VoltDB = 'org.voltdb.VoltDB'

@VOLT.Command(
    options = (
        VOLT.StringOption('-C', '--config', 'configfile',
                         'specify the location of the deployment file',
                          default = None),
        VOLT.StringOption('-D', '--dir', 'directory_spec',
                          'Specifies the root directory for the database. The default is voltdbroot under the current working directory.',
                          default = None),
        VOLT.BooleanOption('-f', '--force', 'force',
                           'Initialize a new, empty database. Any previous session will be overwritten.'),
        VOLT.StringOption('-s', '--schema', 'schema',
                           'Specifies a file containing the data definition (as SQL statements) to be loaded when starting the database.'),
        VOLT.StringOption('-j', '--classes', 'classes_jarfile',
                          'Specifies a .jar file containing classes used to declare stored procedures. The classes are loaded automatically from a saved copy when the database starts.')
    ),
    description = 'Initializes a new, empty database.'
)

def init(runner):
    runner.args.extend(['initialize'])
    if runner.opts.configfile:
        runner.args.extend(['deployment', runner.opts.configfile])
    if runner.opts.directory_spec:
        runner.args.extend(['voltdbroot', runner.opts.directory_spec])
    if runner.opts.force:
        runner.args.extend(['force'])
    if runner.opts.schema:
        runner.args.extend(['schema', runner.opts.schema])
    if runner.opts.classes_jarfile:
        runner.args.extend(['classes', runner.opts.classes_jarfile])

    args = runner.args
    runner.java_execute(VoltDB, None, *args)
