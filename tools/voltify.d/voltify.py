# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
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

"""
Main voltify command module.
"""

import sys
import os
import mysqlutil
from voltcli import utility
from voltcli import environment

#===============================================================================
class ConfigProperty:
#===============================================================================
    def __init__(self, description, default=None):
        self.description = description
        self.default = default

#===============================================================================
class Global:
#===============================================================================
    tool_name = 'voltify'
    config_key = tool_name
    config_properties = dict(
        connection_string = ConfigProperty('database connection string'),
        ddl_file          = ConfigProperty('generated DDL file name', default='ddl.sql'),
        deployment_file   = ConfigProperty('generated deployment file name', default='deployment.xml'),
        package           = ConfigProperty('package/application name', default='voltapp'),
        run_script        = ConfigProperty('generated run script', default='run.sh'),
        partition_table   = ConfigProperty('table to use for partitioning analysis', default='AUTO'),
        source_type       = ConfigProperty('source database type, e.g. "mysql"', default='mysql'),
        host_count        = ConfigProperty('number of hosts', default='1'),
        sites_per_host    = ConfigProperty('number of sites per host', default='2'),
        kfactor           = ConfigProperty('K factor redundancy', default='0'),
    )

#===============================================================================
def config_key(name):
#===============================================================================
    """
    Generate a configuration property key from a property name.
    """
    return '.'.join([Global.config_key, name])

#===============================================================================
def config_help(samples=None):
#===============================================================================
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

#===============================================================================
class Configuration(dict):
#===============================================================================
    """
    Dictionary that also allows attribute-based access.
    """
    def __getattr__(self, name):
        return self[name]
    def __setattr__(self, name, value):
        self[name] = value

#===============================================================================
def get_config(runner, reset = False):
#===============================================================================
    """
    Utility function to look for and validate a set of configuration properties.
    """
    config = Configuration()
    missing = []
    defaults = []
    msgblocks = []
    for name in sorted(Global.config_properties.keys()):
        config_property = Global.config_properties[name]
        key = config_key(name)
        value = runner.config.get(key)
        if not value or reset:
            if config_property.default is None:
                missing.append(name)
                runner.config.set_permanent(key, '')
            else:
                defaults.append(name)
                value = Global.config_properties[name].default
                runner.config.set_permanent(key, value)
                setattr(config, name, value)
        else:
            # Use an existing config value.
            config[name] = value
    samples = []
    if not reset and missing:
        table = [(name, Global.config_properties[name].description) for name in missing]
        msgblocks.append([
            'The following settings must be configured before proceeding:',
            '',
            utility.format_table(table, headings=['PROPERTY', 'DESCRIPTION'],
                                        indent=3, separator='  ')
        ])
        samples.extend(missing)
        config = None
    if defaults:
        msgblocks.append([
            'The following setting defaults were applied and saved permanently:',
            '',
            utility.format_table(
                [(name, Global.config_properties[name].default) for name in defaults],
                indent=3, separator='  ', headings=['PROPERTY', 'VALUE'])
        ])
    if reset:
        config = None
    elif config is None:
        msgblocks.append([config_help(samples=samples)])
    if msgblocks:
        for msgblock in msgblocks:
            print ''
            for msg in msgblock:
                print msg
        print ''
    return config

#===============================================================================
class FileGenerator(utility.FileGenerator):
#===============================================================================

    def __init__(self, runner, config):
        self.runner = runner
        self.config = config
        if config.source_type != 'mysql':
            utility.abort('Unsupported source type "%s".' % config.source_type,
                          'Only "mysql" is valid.')
        output_files = [config.ddl_file, config.deployment_file, config.run_script]
        overwrites = [p for p in output_files if os.path.exists(p)]
        if overwrites and not runner.opts.overwrite:
            utility.abort('Output files exist, delete or use the overwrite option.', overwrites)
        utility.FileGenerator.__init__(self, self, **config)

    def generate_readme(self):
        self.from_template('%s-README.txt' % Global.tool_name)

    def generate_ddl(self):
        def generate_schema(output_stream):
            schema = mysqlutil.generate_schema(self.config.connection_string,
                                               self.config.partition_table,
                                               output_stream)
            table_list = ',\n'.join(['        "%s"' % name for name in schema.table_names])
            self.add_symbols(table_list=table_list)
        self.custom(self.config.ddl_file, generate_schema)

    def generate_deployment(self):
        self.from_template('deployment.xml', self.config.deployment_file)

    def generate_run_script(self):
        self.from_template('run.sh', self.config.run_script, permissions=0755)

    def generate_sample_client(self):
        self.from_template('Client.java', 'src/com/%s/Client.java' % self.config.package)

    #TODO: Still need to create a useful stored procedure sample.
    #def generate_sample_procedure(self):
    #    self.from_template('Counts.java', 'procedures/com/%s/Counts.java' % self.config.package)

    def generate_all(self):
        self.generate_readme()
        self.generate_deployment()
        self.generate_run_script()
        self.generate_ddl()
        self.generate_sample_client()
        #self.generate_sample_procedure()
        utility.info('Project files were generated successfully.',
                     'Please examine the following files thoroughly before using.',
                     self.generated)

    def find_resource(self, path):
        return self.runner.find_resource(os.path.join('template', path), required=True)

#===============================================================================
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
#===============================================================================
    config = get_config(runner)
    if config is None:
        sys.exit(1)
    generator = FileGenerator(runner, config)
    generator.generate_all()

#===============================================================================
def run_config_get(runner):
#===============================================================================
    """
    Implementation of "config get" sub-command."
    """
    if not runner.opts.arg:
        # All labels.
        for (key, value) in runner.config.query_pairs():
            sys.stdout.write('%s=%s\n' % (key, value))
    else:
        # Specific keys requested.
        for arg in runner.opts.arg:
            n = 0
            for (key, value) in runner.config.query_pairs(filter=config_key(arg)):
                sys.stdout.write('%s=%s\n' % (key, value))
                n += 1
            if n == 0:
                sys.stdout.write('%s *not found*\n' % arg)

#===============================================================================
def run_config_set(runner):
#===============================================================================
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

#===============================================================================
def run_config_reset(runner):
#===============================================================================
    """
    Implementation of "config reset" sub-command.
    """
    utility.info('Clearing configuration settings...')
    # Perform the reset.
    get_config(runner, reset=True)
    # Display the help.
    get_config(runner)

#===============================================================================
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
#===============================================================================
    runner.go()
