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
    cli_options = (
        vcli_cli.CLIBoolean('-d', '--debug', 'debug',
                            'display debug messages'),
        vcli_cli.CLIBoolean('-n', '--dry-run', 'dryrun',
                            'display actions without executing them'),
        vcli_cli.CLIBoolean('-p', '--pause', 'pause',
                            'pause before significant actions'),
        vcli_cli.CLIBoolean('-v', '--verbose', 'verbose',
                            'display verbose messages, including external command lines'),
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
class Verb(object):
#===============================================================================
    """
    Base class for verb implementations that provide the available sub-commands.
    """
    def __init__(self, name, **kwargs):
        self.name     = name
        self.cli_spec = vcli_cli.CLISpec(**kwargs)
        vcli_util.debug(str(self))
    def execute(self, runner):
        vcli_util.abort('%s "%s" object does not implement the required execute() method.'
                            % (self.__class__.__name__, self.name))
    def __cmp__(self, other):
        return cmp(self.name, other.name)
    def __str__(self):
        return '%s: %s\n%s' % (self.__class__.__name__, self.name, self.cli_spec)

#===============================================================================
class FunctionVerb(Verb):
#===============================================================================
    """
    Verb that wraps a function. Used by @VOLT.Command decorator.
    """
    def __init__(self, name, function, **kwargs):
        Verb.__init__(self, name, **kwargs)
        self.function = function
    def execute(self, runner):
        self.function(runner)

#===============================================================================
class JavaFunctionVerb(Verb):
#===============================================================================
    """
    Verb that wraps a function implemented in Java. Used by @VOLT.Java_Command
    decorator.
    """
    def __init__(self, name, java_class, function, **kwargs):
        Verb.__init__(self, name, **kwargs)
        self.java_class         = java_class
        self.function           = function
        self.java_opts_override = kwargs.get('java_opts_override', None)
    def execute(self, runner):
        runner.set_go_default(self.go)
        self.function(runner)
    def go(self, runner, *args):
        java_args = list(args) + list(runner.args)
        runner.java(self.java_class, self.java_opts_override, *java_args)

#===============================================================================
class HelpVerb(Verb):
#===============================================================================
    """
    Verb to provide standard help. Used by @VOLT.Help decorator.
    """
    def __init__(self, name, function, **kwargs_in):
        kwargs = copy.copy(kwargs_in)
        if 'description' not in kwargs:
            kwargs['description'] = 'Display command help'
        if 'usage' not in kwargs:
            kwargs['usage'] = '[COMMAND ...]'
        Verb.__init__(self, name, **kwargs)
        self.function = function
    def execute(self, runner):
        runner.set_go_default(self.go)
        self.function(runner)
    def go(self, runner, *args):
        runner.help(*runner.args)

#===============================================================================
class VerbRunner(object):
#===============================================================================

    def __init__(self, command, verbspace, config, cli_processor):
        """
        VerbRunner constructor.
        """
        # Unpack the command object for use by command implementations.
        self.verb   = command.verb
        self.opts   = command.opts
        self.args   = command.args
        self.parser = command.parser
        # The verbspace supports running nested commands.
        self.verbspace     = verbspace
        self.config        = config
        self.cli_processor = cli_processor
        self.go_default    = None
        self.project_path  = os.path.join(os.getcwd(), 'project.xml')

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
        if args:
            for name in args:
                for verb_name in self.verbspace.verb_names:
                    if verb_name == name:
                        verb = self.verbspace.verbs[name]
                        parser = self.cli_processor.create_verb_parser(verb)
                        sys.stdout.write('\n')
                        parser.print_help()
                        sys.stdout.write('\n')
                        if verb.cli_spec.description2:
                            sys.stdout.write('%s\n' % verb.cli_spec.description2.strip())
                        break
                else:
                    vcli_util.error('Verb "%s" was not found.' % verb.name)
        else:
            self.usage()

    def usage(self, *args):
        """
        Display usage screen.
        """
        sys.stdout.write('\n')
        self.cli_processor.print_help()
        sys.stdout.write('\n')

    def set_go_default(self, go_default):
        """
        Called by Verb to set the default go action.
        """
        self.go_default = go_default

    def go(self, *args):
        """
        Default go action provided by Verb object.
        """
        if self.go_default is None:
            vcli_util.abort('Verb "%s" (class %s) does not provide a default go action.'
                                % (self.verb.name, self.verb.__class__.__name__))
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
        """
        Constructor. The verbs argument is the dictionary to populate with
        discovered Verb objects. It maps verb name to Verb object.
        """
        self.verbs = verbs

    def Command(self, *args, **kwargs):
        """
        @VOLT.Command decorator
        Decorator invocation must have an argument list, even if empty.
        """
        def inner_decorator(function):
            def wrapper(*args, **kwargs):
                function(*args, **kwargs)
            verb = FunctionVerb(function.__name__, wrapper, *args, **kwargs)
            self.verbs[verb.name] = verb
            return wrapper
        return inner_decorator

    def Java_Command(self, java_class, *args, **kwargs):
        """
        @VOLT.Java_Command decorator
        Decorator invocation must have an argument list, even if empty.
        """
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
            verb = JavaFunctionVerb(function.__name__, java_class, wrapper, *args, **kwargs)
            self.verbs[verb.name] = verb
            return wrapper
        return inner_decorator

    def Help(self, *args, **kwargs):
        """
        @VOLT.Help decorator
        Decorator invocation must have an argument list, even if empty.
        """
        def inner_decorator(function):
            def wrapper(*args, **kwargs):
                function(*args, **kwargs)
            verb = HelpVerb(function.__name__, wrapper, *args, **kwargs)
            self.verbs[verb.name] = verb
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
        self.verb_names  = self.verbs.keys()
        self.verb_names.sort()

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
        self.CLIOption  = vcli_cli.CLIOption
        self.CLIBoolean = vcli_cli.CLIBoolean
        self.java       = java_runner

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

    # Build the VOLT namespace with the specific set of classes, functions and
    # decorators we make available to command implementations.
    verbs = {}
    verb_decorators = VerbDecorators(verbs)
    namespace_VOLT = VOLT(verb_decorators, java_runner)

    # Build the verbspace by executing modules found based on the calling
    # script location and the location of this module. The executed modules
    # have decorator calls that populate the verbs dictionary.
    vcli_util.search_and_execute(search_dirs, verbs_subdir, VOLT = namespace_VOLT)

    # Add the standard help verb if it wasn't supplied.
    if 'help' not in verbs:
        def default_help(runner):
            runner.go()
        verbs['help'] = HelpVerb('help', default_help)

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
    processor = vcli_cli.VoltCLICommandProcessor(verbspace.verbs,
                                                 base_cli_spec.cli_options,
                                                 base_cli_spec.usage,
                                                 '\n'.join((verbspace.description,
                                                            base_cli_spec.description)),
                                                 '%%prog version %s' % verbspace.version)
    command = processor.parse(cmdargs)

    # Initialize utility function options according to parsed options.
    vcli_util.set_debug(command.outer_opts.debug)
    vcli_util.set_dryrun(command.outer_opts.dryrun)

    # Run the command.
    runner = VerbRunner(command, verbspace, config, processor)
    runner.execute()

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

    # Pre-scan for debug and dry-run options so that early code can display
    # debug messages and prevent execution appropriately.
    preproc = vcli_cli.VoltCLICommandPreprocessor(base_cli_spec.cli_options)
    preproc.preprocess(sys.argv[1:])
    vcli_util.set_debug(preproc.get_option('-d', '--debug') == True)
    vcli_util.set_dryrun(preproc.get_option('-n', '--dry-run') == True)

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
