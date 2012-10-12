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

import VOLT
import vcli_cli
import vcli_env
import vcli_util

# Standard CLI options.
base_cli_spec = VOLT.CLISpec(
    description = '''\
Specific actions are provided by subcommands.  Run "%prog help SUBCOMMAND" to
display full usage for a subcommand, including its options and arguments.  Note
that some subcommands are fully handled in Java.  For these subcommands help is
usually available by running the subcommand with no additional arguments or
followed by the -h option.
''',
    usage = '%prog [OPTIONS] COMMAND [ARGUMENTS ...]',
    options_spec = (
        VOLT.CLIOption('-d', '--debug', action = 'store_true', dest = 'debug',
                       help = 'display debug messages'),
        VOLT.CLIOption('-n', '--dry-run', action = 'store_true', dest = 'dryrun',
                       help = 'dry run displays actions without executing them'),
        VOLT.CLIOption('-p', '--pause', action = 'store_true', dest = 'pause',
                       help = 'pause before significant actions'),
        VOLT.CLIOption('-v', '--verbose', action = 'store_true', dest = 'verbose',
                       help = 'display verbose messages, including external command lines'),
    )
)

#### ToolRunner class - invokes other tools using <tool>.command(args...) syntax.

class ToolCallable(object):
    def __init__(self, runner, tool, command):
        self.runner  = runner
        self.tool    = tool
        self.command = command
    def __call__(self, *args):
        self.runner.shell(self.tool, self.command, *args)

class ToolRunner(object):
    def __init__(self, runner, tool):
        self.runner = runner
        self.tool   = tool
    def __getattr__(self, command):
        return ToolCallable(self.runner, self.tool, command)

#### Verb runner class

class VerbRunner(object):

    def __init__(self, parsed_command, verbspace, config, project_path, version):
        """
        VerbRunner constructor.
        """
        self.name         = parsed_command.verb.name
        self.verbspace    = verbspace
        self.config       = config
        self.parser       = parsed_command.outer_parser
        self.opts         = parsed_command.inner_opts
        self.args         = parsed_command.inner_args
        self.project_path = project_path
        self.classpath    = ':'.join(vcli_env.classpath)
        self.go_default   = None
        # Extend the classpath if volt.classpath is configured.
        classpath_ext = self.config.get('volt.classpath')
        if classpath_ext:
            self.classpath += ':'.join((self.classpath, classpath_ext))
        # Allows invoking volt or voltadmin tools via runner.<tool>.command(args...).
        self.volt      = ToolRunner(self, 'volt')
        self.voltadmin = ToolRunner(self, 'voltadmin')

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

    def get_catalog(self):
        """
        Get the catalog path from the configuration.
        """
        return self.config.get_required('volt.catalog')

    def mkdir(self, dir):
        """
        Create a directory recursively.
        """
        self.shell('mkdir', '-p', dir)

    def catalog_exists(self):
        """
        Test if catalog file exists.
        """
        return os.path.exists(self.get_catalog())

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
            for verb in self.verbspace.verbs:
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
        self.parser.print_help()
        print ''

    def set_go_default(self, go_default):
        """
        Called by Verb to set the default go action.
        """
        self.go_default = go_default

    def go(self, verb, *args):
        """
        Default go action provided by Verb object.
        """
        if self.go_default is None:
            vcli_util.abort('Verb "%s" (class %s) does not provide a default go action.'
                                % (verb.name, verb.__class__.__name__))
        else:
            self.go_default(self, *args)

class VerbSpace(object):
    """
    Manages a collection of Verb objects that support a particular CLI interface.
    """

    def __init__(self, command_path):
        self.verbs = []
        self.search_dirs = [os.path.dirname(__file__)]
        self.command_path = os.path.realpath(command_path)
        self.command_dir, self.command_name = os.path.split(self.command_path)
        self.verbs_subdir = '%s.d' % self.command_name
        if self.command_dir not in self.search_dirs:
            self.search_dirs.append(self.command_dir)

    # @Command decorator
    # Decorator invocation must have an argument list, even if empty.
    def decorator_Command(self, *args, **kwargs):
        def inner_decorator(function):
            def wrapper(*args, **kwargs):
                function(*args, **kwargs)
            self.verbs.append(VOLT.FunctionVerb(function.__name__, wrapper, *args, **kwargs))
            return wrapper
        return inner_decorator

    # @Java_Command decorator
    # Decorator invocation must have an argument list, even if empty.
    def decorator_Java_Command(self, java_class, *args, **kwargs):
        def inner_decorator(function):
            def wrapper(*args, **kwargs):
                function(*args, **kwargs)
            # Automatically set passthrough to True.
            kwargs['passthrough'] = True
            # Add extra message to description.
            extra_help = '(use --help for full usage)'
            if 'description' in kwargs:
                kwargs['description'] += ' %s' % extra_help
            else:
                kwargs['description'] = extra_help
            self.verbs.append(VOLT.JavaFunctionVerb(function.__name__, java_class, wrapper, *args, **kwargs))
            return wrapper
        return inner_decorator

    def initialize(self):
        # Verbs support the possible actions.
        # Look for Verb subclasses in the .d subdirectory (relative to this file and to
        # the working directory).
        self.verbs.extend(vcli_util.find_and_load_subclasses(VOLT.Verb,
                                                             self.search_dirs,
                                                             self.verbs_subdir,
                                                             VOLT         = VOLT,
                                                             Command      = self.decorator_Command,
                                                             Java_Command = self.decorator_Java_Command))
        self.verbs.sort(cmp = lambda x, y: cmp(x.name, y.name))

def main(version, description):
    """
    Called by main script to run using command line arguments.
    """
    # Initialize the environment
    vcli_env.initialize(version)

    # Pull in the verb space based on the running script name.
    verbspace = VerbSpace(sys.argv[0])
    verbspace.initialize()

    # Run the command
    run_command(verbspace, description, version, *sys.argv[1:])

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
                            ['%s config %s.%s=VALUE' % (vcli_env.bin_name, path, name)])
        return value

def run_command(verbspace, description, version, *cmdargs):
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
    processor = vcli_cli.VoltCLICommandProcessor(verbspace.verbs,
                                                 base_cli_spec.options_spec,
                                                 aliases,
                                                 base_cli_spec.usage,
                                                 '\n'.join((description,
                                                            base_cli_spec.description)),
                                                 '%%prog version %s' % version)
    parsed_command = processor.parse(cmdargs)

    # Initialize utility function options according to parsed options.
    vcli_util.set_debug(parsed_command.outer_opts.debug)
    vcli_util.set_dryrun(parsed_command.outer_opts.dryrun)

    # Run the command.
    runner = VerbRunner(parsed_command, verbspace, config, project_path, version)
    parsed_command.verb.execute(runner)
