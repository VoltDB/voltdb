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
import inspect

import voltdbclient
from verbs import *
from voltcli import cli
from voltcli import environment
from voltcli import utility

#===============================================================================
# Global data
#===============================================================================

# Standard CLI options.
base_cli_spec = cli.CLISpec(
    description = '''\
Specific actions are provided by verbs.  Run "%prog help VERB" to display full
usage for a verb, including its options and arguments.  Note that some verbs
are fully handled in Java.  For these verbs help is usually available by
running the verb with no additional arguments or followed by the -h option.
''',
    usage = '%prog [OPTIONS] COMMAND [ARGUMENTS ...]',
    cli_options = (
        cli.CLIBoolean('-d', '--debug', 'debug',
                       'display debug messages'),
        cli.CLIBoolean('-n', '--dry-run', 'dryrun',
                       'display actions without executing them'),
        cli.CLIBoolean('-p', '--pause', 'pause',
                       'pause before significant actions'),
        cli.CLIBoolean('-v', '--verbose', 'verbose',
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
    Execute or compile Java programs.
    """

    def __init__(self, classpath):
        self.classpath = classpath

    def execute(self, java_class, java_opts_override, *args):
        """
        Run a Java command line with option overrides.
        """
        java_args = [environment.java]
        java_opts = utility.merge_java_options(environment.java_opts, java_opts_override)
        java_args.extend(java_opts)
        java_args.append('-Dlog4j.configuration=file://%s' % os.environ['LOG4J_CONFIG_PATH'])
        java_args.append('-Djava.library.path="%s"' % os.environ['VOLTDB_VOLTDB'])
        java_args.extend(('-classpath', self.classpath))
        java_args.append(java_class)
        for arg in args:
            if arg is not None:
                java_args.append(arg)
        return utility.run_cmd(*java_args)

    def compile(self, outdir, *srcfiles):
        """
        Compile Java source using javac.
        """
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        utility.run_cmd('javac', '-target', '1.6', '-source', '1.6',
                          '-classpath', self.classpath, '-d', outdir, *srcfiles)

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
        # Build the Java classpath and create a Java runner.
        classpath = ':'.join(environment.classpath)
        classpath_ext = config.get('volt.classpath')
        if classpath_ext:
            classpath = ':'.join((classpath, classpath_ext))
        if hasattr(self.verb, 'classpath') and self.verb.classpath:
            classpath = ':'.join((self.verb.classpath, classpath))
        self.java = JavaRunner(classpath)
        self.call = ToolRunner(self.verbspace, self.config)

    def shell(self, *args):
        """
        Run a shell command.
        """
        utility.run_cmd(*args)

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
        utility.error('Fatal error in "%s" command.' % self.verb.name, *msgs)
        self.help()
        utility.abort()

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
                    utility.error('Verb "%s" was not found.' % verb.name)
        else:
            self.usage()

    def package(self, output_dir_in, force):
        """
        Create python-runnable package/zip file.
        """
        if output_dir_in is None:
            output_dir = ''
        else:
            output_dir = output_dir_in
        output_path = os.path.join(output_dir, '%s.zip' % environment.command_name)
        utility.info('Creating Python-runnable zip file: %s' % output_path)
        if os.path.exists(output_path) and not force:
            if utility.choose('Overwrite "%s"?' % output_path, 'yes', 'no') == 'n':
                utility.abort()
        zipper = utility.Zipper('\.pyc$')
        zipper.open(output_path)
        try:
            # Generate the __main__.py module for automatic execution from the zip file.
            main_script = ('''\
import sys
from voltcli import runner
runner.main('%(name)s', '', '%(version)s', '%(description)s', *sys.argv[1:])'''
                    % self.verbspace.__dict__)
            zipper.add_string(main_script, '__main__.py')
            # Recursively package lib/python as lib in the zip file.
            zipper.add_directory(environment.volt_python, '')
        finally:
            zipper.close()

    def usage(self):
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
            utility.abort('Verb "%s" (class %s) does not provide a default go action.'
                                % (self.verb.name, self.verb.__class__.__name__))
        else:
            self.go_default(self, *args)

    def execute(self):
        self.verb.execute(self)

#===============================================================================
class VOLT(object):
#===============================================================================
    """
    The VOLT namespace provided to dynamically loaded verb scripts.
    """
    def __init__(self, verb_decorators):
        # Add all verb_decorators methods not starting with '_' as members.
        for name, function in inspect.getmembers(verb_decorators, inspect.ismethod):
            if not name.startswith('_'):
                setattr(self, name, function)
        # For declaring options in command decorators.
        self.CLIBoolean = cli.CLIBoolean
        self.CLIValue   = cli.CLIValue
        # Expose voltdbclient symbols for Volt client commands.
        self.VoltProcedure  = voltdbclient.VoltProcedure
        self.VoltResponse   = voltdbclient.VoltResponse
        self.VoltException  = voltdbclient.VoltException
        self.VoltTable      = voltdbclient.VoltTable
        self.VoltColumn     = voltdbclient.VoltColumn
        self.FastSerializer = voltdbclient.FastSerializer
        # As a convenience expose the utility module so that commands don't
        # need to import it.
        self.utility = utility

#===============================================================================
def load_verbspace(command_name, command_dir, config, version, description):
#===============================================================================
    """
    Build a verb space by searching for source files with verbs in this source
    file's directory, the calling script location (if provided), and the
    working directory.
    """
    utility.debug('Loading verbspace for "%s" from "%s"...' % (command_name, command_dir))
    scan_base_dirs = [os.path.dirname(__file__)]
    verbs_subdir = '%s.d' % command_name
    if command_dir is not None and command_dir not in scan_base_dirs:
        scan_base_dirs.append(command_dir)
    cwd = os.getcwd()
    if cwd not in scan_base_dirs:
        scan_base_dirs.append(cwd)

    # Build the VOLT namespace with the specific set of classes, functions and
    # decorators we make available to command implementations.
    verbs = {}
    verb_decorators = VerbDecorators(verbs)
    namespace_VOLT = VOLT(verb_decorators)

    # Build the verbspace by executing modules found based on the calling
    # script location and the location of this module. The executed modules
    # have decorator calls that populate the verbs dictionary.
    finder = utility.PythonSourceFinder()
    for scan_dir in scan_base_dirs:
        finder.add_path(os.path.join(scan_dir, verbs_subdir))
    # If running from a zip package add resource locations.
    pkg_dir = sys.modules['__main__'].__file__.split('/', 1)[0]
    if pkg_dir.endswith('.zip'):
        finder.add_resource('__main__', os.path.join('voltcli', verbs_subdir))
    finder.search_and_execute(VOLT = namespace_VOLT)

    # Add standard verbs if they aren't supplied.
    def default_func(runner):
        runner.go()
    for verb_name, verb_cls in (('help', HelpVerb), ('package', PackageVerb)):
        if verb_name not in verbs:
            verbs[verb_name] = verb_cls(verb_name, default_func)

    return VerbSpace(command_name, version, description, namespace_VOLT, verbs)

#===============================================================================
class VoltConfig(utility.PersistentConfig):
#===============================================================================
    """
    Volt-specific persistent configuration provides customized error messages.
    """
    def __init__(self, permanent_path, local_path):
        utility.PersistentConfig.__init__(self, 'INI', permanent_path, local_path)

    def get_required(self, key):
        value = self.get(key)
        if value is None:
            utility.abort('Configuration parameter "%s" was not found.' % key,
                            'Set parameters using the "config" command, for example:',
                            ['%s config %s=VALUE' % (environment.command_name, key)])
        return value

#===============================================================================
def run_command(verbspace, config, *cmdargs):
#===============================================================================
    """
    Run a command after parsing the command line arguments provided.
    """
    # Parse the command line.
    processor = cli.VoltCLICommandProcessor(verbspace.verbs,
                                            base_cli_spec.cli_options,
                                            base_cli_spec.usage,
                                            '\n'.join((verbspace.description,
                                                       base_cli_spec.description)),
                                            '%%prog version %s' % verbspace.version)
    command = processor.parse(cmdargs)

    # Initialize utility function options according to parsed options.
    utility.set_verbose(command.outer_opts.verbose)
    utility.set_debug(  command.outer_opts.debug)
    utility.set_dryrun( command.outer_opts.dryrun)

    # Run the command.
    runner = VerbRunner(command, verbspace, config, processor)
    runner.execute()

#===============================================================================
def main(command_name, command_dir, version, description, *args):
#===============================================================================
    """
    Called by running script to execute command with command line arguments.
    """
    try:
        # Pre-scan for verbose, debug, and dry-run options so that early code
        # can display verbose and debug messages, and obey dry-run.
        preproc = cli.VoltCLICommandPreprocessor(base_cli_spec.cli_options)
        preproc.preprocess(args)
        utility.set_verbose(preproc.get_option('-v', '--verbose') == True)
        utility.set_debug(  preproc.get_option('-d', '--debug'  ) == True)
        utility.set_dryrun( preproc.get_option('-n', '--dry-run') == True)

        # Load the configuration and state
        permanent_path = os.path.join(os.getcwd(), 'volt.cfg')
        local_path     = os.path.join(os.getcwd(), 'volt_local.cfg')
        config = VoltConfig(permanent_path, local_path)

        # Initialize the environment
        environment.initialize(command_name, command_dir, version)

        # Search for modules based on both this file's and the calling script's location.
        verbspace = load_verbspace(command_name, command_dir, config, version, description)

        # Make internal commands available to user commands via the VOLT namespace.
        if command_name not in internal_commands:
            for internal_command in internal_commands:
                internal_verbspace = load_verbspace(internal_command, None, config, version,
                                                    'Internal "%s" command' % internal_command)
                tool_runner = ToolRunner(internal_verbspace, config)
                setattr(verbspace.VOLT, internal_command, tool_runner)

        # Run the command
        run_command(verbspace, config, *args)

    except KeyboardInterrupt:
        sys.stderr.write('\n')
        utility.abort('break')
