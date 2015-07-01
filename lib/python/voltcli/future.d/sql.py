# This file is part of VoltDB.

# Copyright (C) 2008-2015 VoltDB Inc.
#
# This file contains original code and/or modifications of original code.
# Any modifications made by VoltDB Inc. are licensed under the following
# terms and conditions:
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

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
