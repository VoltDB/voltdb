# This file is part of VoltDB.
# Copyright (C) 2008-2013 VoltDB Inc.
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
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import sys
import os
import mysqlutil
from voltcli import utility
from voltcli import environment


def get_config(runner, *keys):
    class O(object):
        pass
    o = O()
    missing = [];
    for name in keys:
        key = 'mysql.%s' % name
        value = runner.config.get(key)
        if value:
            setattr(o, name, value)
        else:
            missing.append(key)
    if missing:
        utility.abort('Configuration properties are missing from "%s".' % runner.config.path,
                      'Use the "config" and "show" subcommands to work with the configuration.',
                      'Missing properties:', missing)
    return o


@VOLT.Command(
    description='VoltDB quick start from a live MySQL database.',
    description2='''
Run from a project directory where new files can be generated.
Edit %s to specify database parameters and generation options.
Use the "config" and "show" sub-commands to update and view the configuration.
''' % environment.config_name,
)
def mysql(runner):
    config = get_config(runner, 'uri', 'partition_table')
    mysqlutil.generate_schema(config.uri, config.partition_table)


@VOLT.Command(description = 'Configure project settings.',
              arguments = (
                  VOLT.StringArgument('keyvalue', 'KEY=VALUE assignment',
                                      min_count = 1, max_count = None),))
def config(runner):
    bad = []
    for arg in runner.opts.keyvalue:
        if arg.find('=') == -1:
            bad.append(arg)
    if bad:
        runner.abort('Bad arguments (must be KEY=VALUE format):', bad)
    for arg in runner.opts.keyvalue:
        key, value = [s.strip() for s in arg.split('=', 1)]
        # Default to 'volt.' if simple name is given.
        if key.find('.') == -1:
            key = 'volt.%s' % key
        runner.config.set_permanent(key, value)
        runner.info('Configuration: %s=%s' % (key, value))


@VOLT.Command(
    description = 'Display project configuration settings.',
    description2 = 'Displays all settings if no keys are specified.',
    arguments = (
        VOLT.StringArgument('arg', 'configuration key(s)', min_count = 0, max_count = None)
    ),
)
def show(runner):
    if not runner.opts.arg:
        # All labels.
        for (key, value) in runner.config.query_pairs():
            sys.stdout.write('%s=%s\n' % (key, value))
    else:
        # Specific keys requested.
        for filter in runner.opts.arg:
            n = 0
            for (key, value) in runner.config.query_pairs(filter = filter):
                sys.stdout.write('%s=%s\n' % (key, d[key]))
                n += 1
            if n == 0:
                sys.stdout.write('%s *not found*\n' % filter)
