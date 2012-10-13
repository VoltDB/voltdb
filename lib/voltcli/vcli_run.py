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
import inspect

import vcli_cli
import vcli_env
import vcli_util

#===============================================================================
# Global data
#===============================================================================

# Standard CLI options.
base_cli_spec = vcli_cli.CLISpec(
    description = '''\
Specific actions are provided by subcommands.  Run "%prog help SUBCOMMAND" to
display full usage for a subcommand, including its options and arguments.  Note
that some subcommands are fully handled in Java.  For these subcommands help is
usually available by running the subcommand with no additional arguments or
followed by the -h option.
''',
    usage = '%prog [OPTIONS] COMMAND [ARGUMENTS ...]',
    options_spec = (
        vcli_cli.CLIOption('-d', '--debug', action = 'store_true', dest = 'debug',
                       help = 'display debug messages'),
        vcli_cli.CLIOption('-n', '--dry-run', action = 'store_true', dest = 'dryrun',
                       help = 'dry run displays actions without executing them'),
        vcli_cli.CLIOption('-p', '--pause', action = 'store_true', dest = 'pause',
                       help = 'pause before significant actions'),
        vcli_cli.CLIOption('-v', '--verbose', action = 'store_true', dest = 'verbose',
                       help = 'display verbose messages, including external command lines'),
    )
)

# Internal command names that get added to the VOLT namespace of user scripts.
internal_commands = ['volt', 'voltadmin']

#===============================================================================
# ToolRunner and ToolCallable classes
#
# Supports running internal tools from user scripts through the VOLT namespace.
#   Syntax: VOLT.<tool>.command(args...)
#===============================================================================

class ToolCallable(object):
    def __init__(self, tool_runner, verb_name):
        self.tool_runner = tool_runner
        self.verb_name   = verb_name
    def __call__(self, *cmdargs):
        args = [self.verb_name] + list(cmdargs)
        run_command(self.tool_runner.verbspace, self.tool_runner.config, *args)

class ToolRunner(object):
    def __init__(self, verbspace, config):
        self.verbspace = verbspace
        self.config    = config
    def __getattr__(self, verb_name):
        return ToolCallable(self, verb_name)

#===============================================================================
class JavaRunner(object):
#===============================================================================
    """
    Executes or compiles Java programs.
    """

    def __init__(self, classpath):
        self.classpath = classpath

    def execute(self, java_class, java_opts_override, *args):
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

    def compile(self, outdir, *srcfiles):
        """
        Compile Java source using javac.
        """
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        vcli_util.run_cmd('javac', '-target', '1.6', '-source', '1.6',
                          '-classpath', self.classpath, '-d', outdir, *srcfiles)

#===============================================================================
class VerbRunner(object):
#===============================================================================

    def __init__(self, command, verbspace, config):
        """
        VerbRunner constructor.
        """
        # Unpack the command object for use by command implementations.
        self.verb   = command.verb
        self.opts   = command.inner_opts
        self.args   = command.inner_args
        self.parser = command.inner_parser
        # The verbspace supports running nested commands.
        self.verbspace    = verbspace
        self.config       = config
        self.go_default   = None
        self.project_path = os.path.join(os.getcwd(), 'project.xml')

    def shell(self, *args):
        """
        Run a shell command.
        """
        vcli_util.run_cmd(*args)

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
        vcli_util.error('Fatal error in "%s" command.' % self.verb.name, *msgs)
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

    def execute(self):
        self.verb.execute(self)

#===============================================================================
class VerbDecorators(object):
#===============================================================================
    """
    Provides decorators used by command implementations to declare commands.
    """

    def __init__(self, verbs):
        self.verbs = verbs

    # @VOLT.Command decorator
    # Decorator invocation must have an argument list, even if empty.
    def Command(self, *args, **kwargs):
        def inner_decorator(function):
            def wrapper(*args, **kwargs):
                function(*args, **kwargs)
            self.verbs.append(vcli_cli.FunctionVerb(function.__name__, wrapper, *args, **kwargs))
            return wrapper
        return inner_decorator

    # @VOLT.Java_Command decorator
    # Decorator invocation must have an argument list, even if empty.
    def Java_Command(self, java_class, *args, **kwargs):
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
            self.verbs.append(vcli_cli.JavaFunctionVerb(function.__name__, java_class, wrapper, *args, **kwargs))
            return wrapper
        return inner_decorator

#===============================================================================
class VerbSpace(object):
#===============================================================================
    """
    Manages a collection of Verb objects that support a particular CLI interface.
    """
    def __init__(self, name, version, description, VOLT, verbs):
        self.name        = name
        self.version     = version
        self.description = description
        self.VOLT        = VOLT
        self.verbs       = verbs
        self.verbs.sort(cmp = lambda x, y: cmp(x.name, y.name))

#===============================================================================
class VOLT(object):
#===============================================================================
    """
    The VOLT namespace provided to dynamically loaded verb scripts.
    """
    def __init__(self, verb_decorators, java_runner):
        # Add all verb_decorators methods not starting with '_' as members.
        for name, function in inspect.getmembers(verb_decorators, inspect.ismethod):
            if not name.startswith('_'):
                setattr(self, name, function)
        self.CLIOption = vcli_cli.CLIOption
        self.java = java_runner

#===============================================================================
def main(version, description):
#===============================================================================
    """
    Called by running script to execute command with command line arguments.
    """
    # Initialize the environment
    vcli_env.initialize(version)

    # Load the configuration and state
    permanent_path = os.path.join(os.getcwd(), 'volt.cfg')
    local_path     = os.path.join(os.getcwd(), 'volt_local.cfg')
    config = VoltConfig(permanent_path, local_path)

    # Search for modules based on both this file's and the calling script's location.
    command_dir, command_name = os.path.split(os.path.realpath(sys.argv[0]))
    verbspace = load_verbspace(command_name, command_dir, config, version, description)

    # Make internal commands available to user commands via the VOLT namespace.
    if command_name not in internal_commands:
        for internal_command in internal_commands:
            internal_verbspace = load_verbspace(internal_command, None, config, version,
                                                'Internal "%s" command' % internal_command)
            tool_runner = ToolRunner(internal_verbspace, config)
            setattr(verbspace.VOLT, internal_command, tool_runner)

    # Run the command
    run_command(verbspace, config, *sys.argv[1:])

#===============================================================================
def load_verbspace(command_name, command_dir, config, version, description):
#===============================================================================
    """
    Build a verb space by searching for source files with verbs based on both
    this file's and, if provided, the calling script's location.
    """
    search_dirs = [os.path.dirname(__file__)]
    verbs_subdir = '%s.d' % command_name
    if command_dir is not None and command_dir not in search_dirs:
        search_dirs.append(command_dir)

    # Build the Java classpath and create a Java runner for VOLT.java.
    classpath = ':'.join(vcli_env.classpath)
    classpath_ext = config.get('volt.classpath')
    if classpath_ext:
        classpath = ':'.join((classpath, classpath_ext))
    java_runner = JavaRunner(classpath)

    # Build the VOLT namespace to provide the limited set of classes, functions
    # and decorators needed by command implementations.
    verbs = []
    verb_decorators = VerbDecorators(verbs)
    namespace_VOLT = VOLT(verb_decorators, java_runner)

    # Build the verbspace by executing modules found based on the calling
    # script location and the location of this module. The executed modules
    # have decorator calls that populate the verbs list.
    vcli_util.search_and_execute(search_dirs, verbs_subdir, VOLT = namespace_VOLT)
    return VerbSpace(command_name, version, description, namespace_VOLT, verbs)

#===============================================================================
class VoltConfig(vcli_util.PersistentConfig):
#===============================================================================
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

#===============================================================================
def run_command(verbspace, config, *cmdargs):
#===============================================================================
    """
    Run a command after parsing the command line arguments provided.
    """
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
                                                 '\n'.join((verbspace.description,
                                                            base_cli_spec.description)),
                                                 '%%prog version %s' % verbspace.version)
    command = processor.parse(cmdargs)

    # Initialize utility function options according to parsed options.
    vcli_util.set_debug(command.outer_opts.debug)
    vcli_util.set_dryrun(command.outer_opts.dryrun)

    # Run the command.
    runner = VerbRunner(command, verbspace, config)
    runner.execute()
