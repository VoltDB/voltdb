# This file is part of VoltDB.
# Copyright (C) 2008-2019 VoltDB Inc.
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
from voltcli import utility

dir_spec_help = ('Specifies the root directory for the database. The default is the current working directory.')
license_help = ('Specifies the location of the license file. The default is the voltdb directory.')
get_resource_help = ('Supported configuration resources for get command are \'classes\', \'deployment\', \'schema\', and \'license\'.\r\n'
                     '           classes    - gets procedure classes of current node\n'
                     '           deployment - gets deployment configuration of current node\n'
                     '           schema     - gets schema of current node\n'
                     '           license     - gets license of current node\n')
output_help = ('Specifies the path and file name for the output file. Defaults are: '
               'Deployment - deployment.xml;\r\n'
               'Schema - schema.sql;\r\n'
               'Classes - procedures.jar'
               'License - license.xml')

@VOLT.Command(
    description = 'Write the selected database resource (deployment or schema) to a file.',
    options = (
        VOLT.StringOption('-o', '--output', 'output', output_help, default=None),
        VOLT.StringOption('-D', '--dir', 'directory_spec', dir_spec_help, default=None),
        VOLT.BooleanOption('-f', '--force', 'force', 'Overwrites an existing file.'),
        VOLT.StringOption('-l', '--license', 'license', license_help, default=None)
    ),
    arguments = (
        VOLT.StringArgument('resource', get_resource_help, default=None),
    )
)

def get(runner):
    runner.args.extend(['get'])

    if runner.opts.resource in ('deployment', 'schema', 'classes', 'license'):
        runner.args.extend([runner.opts.resource])
    else:
        utility.abort('Invalid argument \'%s\' for Get command. Valid arguments are deployment, schema, classes and license' % runner.opts.resource)

    if runner.opts.output:
        runner.args.extend(['file', runner.opts.output])
    if runner.opts.directory_spec:
        runner.args.extend(['getvoltdbroot', runner.opts.directory_spec])
    if runner.opts.force:
        runner.args.extend(['forceget'])
    if runner.opts.license:
        runner.args.extend(['license', runner.opts.license])

    runner.java_execute('org.voltdb.VoltDB', None, *runner.args)