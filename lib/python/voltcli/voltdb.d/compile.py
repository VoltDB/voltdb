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

import os
from voltcli import utility

# Main Java class.
VoltCompiler = 'org.voltdb.compiler.VoltCompiler'

# Command meta-data.
@VOLT.Command(
    # Descriptions for help screen.
    description  = 'Compile schema and stored procedures to build an application catalog.',
    description2 = 'At least one DDL file is required unless a project file is provided.',

    # Command line options.
    options = (
        VOLT.StringOption('-c', '--classpath', 'classpath',
                          'additional colon-separated Java CLASSPATH directories'),
        VOLT.StringOption('-o', '--output', 'catalog',
                          'the output application catalog jar file',
                          default = 'catalog.jar'),
        VOLT.StringOption('-p', '--project', 'project',
                          'the project file, e.g. project.xml (deprecated)')
    ),

    # Command line arguments.
    arguments = (
        VOLT.PathArgument('ddl', 'DDL file(s)', exists = True, min_count = 0, max_count = None)
    )
)

# Command implementation.
def compile(runner):

    # Check that there's something to compile.
    if not runner.opts.project and not runner.opts.ddl:
        runner.abort_with_help('Either project or DDL files must be specified.')

    # Explicit extensions allow VoltCompiler.main() to discriminate between the
    # new and old style argument lists, i.e. with and without a project file.
    # Checking here enables better error messages.
    if not runner.opts.catalog.lower().endswith('.jar'):
        runner.abort('Output catalog file "%s" does not have a ".jar" extension.'
                            % runner.opts.catalog)
    if runner.opts.project and not runner.opts.project.lower().endswith('.xml'):
        runner.abort('Project file "%s" does not have a ".xml" extension.'
                            % runner.opts.project)

    # Verbose argument display.
    if runner.is_verbose():
        params = ['Output catalog file: %s' % runner.opts.catalog]
        if runner.opts.project:
            params.append('Project file: %s' % runner.opts.project)
        if runner.opts.ddl:
            params.append('DDL files:')
            params.append(runner.opts.ddl)
        runner.verbose_info('Compilation parameters:', params)

    # Build the positional and keyword argument lists and invoke the compiler
    args = []
    if runner.opts.project:
        args.append(runner.opts.project)
    args.append(runner.opts.catalog)
    if runner.opts.ddl:
        args.extend(runner.opts.ddl)
    # Add procedures to classpath
    cpath = 'procedures'
    if runner.opts.classpath:
       cpath = 'procedures:' + runner.opts.classpath
    kwargs = dict(classpath = cpath)
    runner.java_execute(VoltCompiler, None, *args, **kwargs)
