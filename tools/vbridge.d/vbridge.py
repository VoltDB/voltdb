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


class ConfigProperty:
    def __init__(self, description, default=None):
        self.description = description
        self.default = default

class G:
    config_key = 'vbridge'
    config_properties = dict(
        connection_string = ConfigProperty('database connection string'),
        ddl_file          = ConfigProperty('generated DDL file name', default='ddl.sql'),
        partition_table   = ConfigProperty('table to use for partitioning analysis'),
        source_type       = ConfigProperty('source database type, e.g. "mysql', default='mysql'),
    )


def config_key(name):
    """
    Generate a configuration property key from a property name.
    """
    return '.'.join([G.config_key, name])


def config_help(samples=None):
    if samples:
        paren = ' (using actual property name)'
    else:
        paren = ''
        samples.append('name')
    set_samples = []
    for name in samples:
        set_samples.append('   %s config set %s=%s'
                                % (environment.command_name, name, '%s_VALUE' % name.upper()))
    format_dict = dict(
        command=environment.command_name,
        config=environment.config_name,
        set_samples='\n'.join(set_samples),
        paren=paren,
    )
    return '''\
Use the "config" verb to modify and view properties as follows.

To set a property%(paren)s:
%(set_samples)s

To display one, many, or all properties:
   %(command)s config get name
   %(command)s config get name1 name2 ...
   %(command)s config get

To get "config" command help:
   %(command)s help config

You can also edit "%(config)s" directly in a text editor.''' % format_dict


def get_config(runner, reset = False):
    """
    Utility function to look for and validate a set of configuration properties.
    """
    class O(object):
        pass
    o = O()
    missing = []
    defaults = []
    for name in sorted(G.config_properties.keys()):
        config_property = G.config_properties[name]
        key = config_key(name)
        value = runner.config.get(key)
        if not value or reset:
            if config_property.default is None:
                missing.append(name)
                runner.config.set_permanent(key, '')
            else:
                defaults.append(name)
                value = G.config_properties[name].default
                runner.config.set_permanent(key, value)
        else:
            # Use an existing config value.
            setattr(o, name, value)
    samples = []
    if not reset and missing:
        if len(missing) > 1:
            plural = 's'
        else:
            plural = ''
        print('\nThe following setting%s must be configured before proceeding:\n' % plural)
        table = [(name, G.config_properties[name].description) for name in missing]
        print utility.format_table(table, headings=['PROPERTY', 'DESCRIPTION'],
                                          indent=3, separator='  ')
        samples.extend(missing)
        o = None
    if defaults:
        if len(defaults) > 1:
            plural = 's were'
        else:
            plural = ' was'
        print '\nThe following setting default%s applied and saved permanently:\n' % plural
        print utility.format_table(
                    [(name, G.config_properties[name].default) for name in defaults],
                    indent=3, separator='  ', headings=['PROPERTY', 'VALUE'])
    if reset:
        o = None
    elif o is None:
        print '\n%s' % config_help(samples=samples)
    return o


@VOLT.Command(
    description='Port a live database to a starter VoltDB project.',
    description2='''
Run from a project directory where new files can be generated.
Use "config" sub-commands to set and get configuration properties.
''',
    options = (
        VOLT.BooleanOption('-O', '--overwrite', 'overwrite',
                           'overwrite existing files', default = False),
    ),
)
def port(runner):
    config = get_config(runner)
    if config is None:
        sys.exit(1)
    source_type = config.source_type.lower()
    if source_type != 'mysql':
        utility.abort('Unsupported source type "%s".' % source_type, 'Only "mysql" is valid.')
    if os.path.exists(config.ddl_file) and not runner.opts.overwrite:
        utility.abort('"%s" exists, delete the file or use the -O or --overwrite options.'
                            % config.ddl_file)
    generated_files = []
    output_stream = utility.File(config.ddl_file, 'w')
    output_stream.open()
    try:
        mysqlutil.generate_schema(config.connection_string, config.partition_table, output_stream)
        generated_files.append(output_stream.path)
    finally:
        output_stream.close()
    utility.info('Project files were successfully generated.',
                 'A thorough examination of their contents is recommended.',
                 generated_files)


def run_config_get(runner):
    """
    Implementation of "config get" sub-command."
    """
    if not runner.opts.arg:
        # All labels.
        for (key, value) in runner.config.query_pairs():
            sys.stdout.write('%s=%s\n' % (key, value))
    else:
        # Specific keys requested.
        for filter in runner.opts.arg:
            n = 0
            for (key, value) in runner.config.query_pairs(filter = config_key(filter)):
                sys.stdout.write('%s=%s\n' % (key, value))
                n += 1
            if n == 0:
                sys.stdout.write('%s *not found*\n' % filter)


def run_config_set(runner):
    """
    Implementation of "config set" sub-command.
    """
    bad = []
    for arg in runner.opts.arg:
        if arg.find('=') == -1:
            bad.append(arg)
    if bad:
        runner.abort('Bad arguments (must be KEY=VALUE format):', bad)
    for arg in runner.opts.arg:
        key, value = [s.strip() for s in arg.split('=', 1)]
        if key.find('.') == -1:
            key = config_key(key)
        runner.config.set_permanent(key, value)
        print 'set %s=%s' % (key, value)


def run_config_reset(runner):
    """
    Implementation of "config reset" sub-command.
    """
    utility.info('Clearing configuration settings...')
    # Perform the reset.
    get_config(runner, reset=True)
    # Display the help.
    get_config(runner)


@VOLT.Multi_Command(
    description  = 'Manipulate and view configuration properties.',
    modifiers = [
        VOLT.Modifier('get', run_config_get,
                      'Show one or more configuration properties.',
                      arg_name = 'KEY'),
        VOLT.Modifier('reset', run_config_reset,
                      'Reset configuration properties to default values.'),
        VOLT.Modifier('set', run_config_set,
                      'Set one or more configuration properties (use KEY=VALUE format).',
                      arg_name = 'KEY_VALUE'),
    ]
)
def config(runner):
    runner.go()
