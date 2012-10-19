# This file is part of VoltDB.

# Copyright (C) 2008-2012 VoltDB Inc.
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

import sys
from voltcli import utility

def show_config(runner, *args):
    if not args:
        # All labels.
        for (key, value) in runner.config.query_pairs():
            sys.stdout.write('%s=%s\n' % (key, value))
    else:
        # Specific keys requested.
        for filter in args:
            n = 0
            for (key, value) in runner.config.query_pairs(filter = filter):
                sys.stdout.write('%s=%s\n' % (key, d[key]))
                n += 1
            if n == 0:
                sys.stdout.write('%s *not found*\n' % filter)

targets = dict(
    config = show_config,
)

@VOLT.Command(description = 'Display various types of information.',
              usage = 'TARGET [ARGUMENT ...]',
              description2 = '''
Targets:

    Display all or specific configuration key/value pairs.

        show config [KEY ...]
''')
def show(runner):
    if not runner.args:
        utility.error('No target specified for "show".')
        runner.help()
    else:
        target = runner.args[0].lower()
        tgtargs = runner.args[1:]
        if target in targets:
            targets[target](runner, *tgtargs)
        else:
            utility.error('Invalid target "%s" specified.' % target)
            runner.help()
