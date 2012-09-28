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

__author__ = 'scooper'

import sys
import os
import copy
import optparse

import vcli_meta
import vcli_cli
import vcli_env
import vcli_util

#### Verb runner class

class VerbRunner(object):

    def __init__(self, name, options, config, args, primary, secondary, project_path):
        """
        VerbRunner constructor.
        """
        self.name         = name
        self.options      = options
        self.config       = config
        self.args         = args
        self.primary      = primary
        self.secondary    = secondary
        self.project_path = project_path
        self.classpath    = ':'.join(vcli_env.classpath)
        # Extend the classpath if volt.classpath is configured.
        classpath_ext = self.config.get('volt.classpath')
        if classpath_ext:
            self.classpath += ':'.join((self.classpath, classpath_ext))

    def run(self, name, *args):
        """
        Run a CLI command with arguments.
        """
        runner = VerbRunner(name,
                            self.options,
                            self.config,
                            args,
                            self.primary,
                            self.secondary,
                            self.project_path)
        for verb in vcli_meta.verbs:
            if verb.name == name:
                break
        else:
            self.abort('Verb "%s" (being called by "%s") was not found.' % (name, self.name))
        verb.execute(runner)

    def shell(self, *args):
        """
        Run a shell command.
        """
        vcli_util.run_cmd(*args)

    def java(self, java_class, java_opts_override, *args):
        """
        Run a Java command line with option overrides.
        """
        java_args = [vcli_env.java]
        java_opts = vcli_util.merge_java_options(vcli_env.java_opts, java_opts_override)
        java_args.extend(java_opts)
        java_args.append('-Dlog4j.configuration=file://%s' % os.environ['LOG4J_CONFIG_PATH'])
        java_args.append('-Djava.library.path="%s"' % os.environ['VOLTDB_VOLTDB'])
        java_args.extend(('-classpath', self.classpath))
        java_args.append(java_class)
        for arg in args:
            if arg is not None:
                java_args.append(arg)
        return vcli_util.run_cmd(*java_args)

    def java_compile(self, outdir, *srcfiles):
        """
        Compile Java source using javac.
        """
        vcli_util.run_cmd('javac', '-target', '1.6', '-source', '1.6',
                          '-classpath', self.classpath, '-d', outdir, *srcfiles)

    def abort(self, *msgs):
        """
        Display errors (optional) and abort execution.
        """
        vcli_util.error('Fatal error in "%s" command.' % self.name, *msgs)
        self.help()
        vcli_util.abort()

    def help(self, *args):
        """
        Display help for command.
        """
        for name in args:
            for verb in vcli_meta.verbs:
                if verb.name == name:
                    print ''
                    parser = optparse.OptionParser(
                            description = verb.metadata.description,
                            usage = '%%prog %s %s' % (verb.name, verb.metadata.usage))
                    parser.print_help()
                    print ''
                    if verb.metadata.description2:
                        print verb.metadata.description2.strip()
                    break
            else:
                vcli_util.error('Verb "%s" was not found.' % verb.name)

    def usage(self, *args):
        """
        Display usage screen.
        """
        print ''
        self.primary.print_help()
        print ''

def main(version, description):
    """
    Called by main script to run using command line arguments.
    """
    # Initialize the environment
    vcli_env.initialize()

    # Set the version number
    vcli_meta.version = version

    # Run the command
    run_command(description, *sys.argv[1:])

class VoltConfig(vcli_util.PersistentConfig):
    """
    Persistent configuration adds volt-specific messages to generic base class.
    """

    def __init__(self, permanent_path, local_path):
        vcli_util.PersistentConfig.__init__(self, 'INI', permanent_path, local_path)

    def get_required(self, key):
        value = self.get(key)
        if value is None:
            vcli_util.abort('Configuration parameter "%s.%s" was not found.' % (path, name),
                            'Set parameters using the "config" command, for example:',
                            ['%s config %s.%s=VALUE' % (vcli_meta.bin_name, path, name)])
        return value

def run_command(description, *cmdargs):
    """
    Run a command after parsing the command line arguments provided.
    """
    # Determine important paths
    project_path   = os.path.join(os.getcwd(), 'project.xml')
    permanent_path = os.path.join(os.getcwd(), 'volt.cfg')
    local_path     = os.path.join(os.getcwd(), 'volt_local.cfg')

    # Load the configuration and state
    config = VoltConfig(permanent_path, local_path)

    # Parse the command line.
    aliases = {}
    alias_dict = config.query(filter = 'alias.')
    for key in alias_dict:
        name = key.split('.', 2)[1]
        aliases[name] = alias_dict[key]
    processor = vcli_cli.VoltCLICommandProcessor(vcli_meta.verbs,
                                                 vcli_meta.cli.options,
                                                 aliases,
                                                 vcli_meta.cli.usage,
                                                 '\n'.join((
                                                     description,
                                                     vcli_meta.cli.description)),
                                                 '%%prog version %s' % vcli_meta.version)
    verb, options, args, primary, secondary = processor.parse(cmdargs)

    # Run the command.
    runner = VerbRunner(verb.name, options, config, args, primary, secondary, project_path)
    verb.execute(runner)
