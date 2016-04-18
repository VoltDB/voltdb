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

@VOLT.Command(
    bundles = VOLT.ConnectionBundle(),
    description  = 'Run the interactive SQL interpreter.',
    description2 = 'Optional arguments are executed as non-interactive queries.',
    options = [
        VOLT.EnumOption(None, '--format', 'format',
                        'output format', 'fixed', 'csv', 'tab',
                        default = 'fixed'),
        VOLT.BooleanOption(None, '--clean', 'clean',
                           'clean output with no headings or counts',
                           default = False),
        VOLT.BooleanOption(None, '--exception-stacks', 'exception_stacks',
                           'display exception stack traces',
                           default = False),
    ],
    arguments = [
        VOLT.StringArgument('query', min_count = 0, max_count = None,
                            help = 'One or more queries to execute non-interactively.')
    ],
)
def sql(runner):
    args = []
    if runner.opts.host:
        if runner.opts.host.host:
            args.append('--servers=%s' % runner.opts.host.host)
        if runner.opts.host.port:
            args.append('--port=%d'    % runner.opts.host.port)
    if runner.opts.username:
        args.append('--user=%s'     % runner.opts.username)
        args.append('--password=%s' % runner.opts.password)
    for query in runner.opts.query:
        args.append('--query=%s' % query)
    if runner.opts.format:
        args.append('--output-format=%s' % runner.opts.format.lower())
    if runner.opts.clean:
        args.append('--output-skip-metadata')
    if runner.opts.exception_stacks:
        args.append('--debug')
    runner.java_execute('org.voltdb.utils.SQLCommand', None, *args)
