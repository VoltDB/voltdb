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
usage for a verb, including its options and arguments.
''',
    usage = '%prog VERB [ OPTIONS ... ] [ ARGUMENTS ... ]',
    options = (
        cli.BooleanOption(None, '--debug', 'debug', None),
        cli.BooleanOption(None, '--pause', 'pause', None),
        cli.BooleanOption('-v', '--verbose', 'verbose',
                          'display verbose messages and external commands'),
    )
)

# Internal command names that get added to the VOLT namespace of user scripts.
internal_commands = ['voltdb', 'voltadmin']

# Written to the README file when the packaged executable is created.
compatibility_warning = '''\
The program package in the bin directory requires Python version 2.6 or greater.
It will crash with older Python versions that fail to detect and run compressed
Python executable packages.

The following two alternatives allow the tool to function on systems with older
Python versions, as long is it is version 2.4 or later.

1) Run the source script having the same name in the tools directory. For
   example:

      tools/%(name)s VERB [ OPTIONS ... ] [ ARGUMENTS ... ]

2) Pass the full command line including the executable package path as arguments
   to an explicit python version. For example:

      python2.6 bin/%(name)s VERB [ OPTIONS ... ] [ ARGUMENTS ... ]'''

# README file template.
readme_template = '''
=== %(name)s README ===

%(usage)s

-- WARNING --

%(warning)s
'''

#===============================================================================
class JavaRunner(object):
#===============================================================================
    """
    Execute or compile Java programs.
    """

    def __init__(self, verb, config, **kwargs):
        self.verb      = verb
        self.config    = config
        self.kwargs    = kwargs
        self.classpath = None

    def initialize(self):
        if self.classpath is None:
            # Build the Java classpath using environment variable, config file,
            # verb attribute, and kwargs.
            self.classpath = ':'.join(environment.classpath)
            classpath_ext = self.config.get('volt.classpath')
            if classpath_ext:
                self.classpath = ':'.join((self.classpath, classpath_ext))
            verb_classpath = getattr(self.verb, 'classpath', None)
            if verb_classpath:
                self.classpath = ':'.join((verb_classpath, self.classpath))
            if 'classpath' in self.kwargs:
                self.classpath = ':'.join((self.kwargs['classpath'], self.classpath))

    def execute(self, java_class, java_opts_override, *args, **kwargs):
        """
        Run a Java command line with option overrides.
        Supported keyword arguments:
            classpath           Java classpath.
            daemon              Run as background (daemon) process if True.
            daemon_name         Daemon name.
            daemon_description  Daemon description for messages.
            daemon_output       Output directory for PID files and stdout/error capture.
        """
        self.initialize()
        classpath = self.classpath
        kwargs_classpath = kwargs.get('classpath', None)
        if kwargs_classpath:
            classpath = ':'.join((kwargs_classpath, classpath))
        java_args = [environment.java]
        java_opts = utility.merge_java_options(environment.java_opts, java_opts_override)
        java_args.extend(java_opts)
        java_args.append('-Dlog4j.configuration=file://%s' % os.environ['LOG4J_CONFIG_PATH'])
        java_args.append('-Djava.library.path="%s"' % os.environ['VOLTDB_VOLTDB'])
        java_args.extend(('-classpath', classpath))
        java_args.append(java_class)
        for arg in args:
            if arg is not None:
                java_args.append(arg)
        daemonizer = utility.kwargs_get(kwargs, 'daemonizer')
        if daemonizer:
            # Does not return if successful.
            daemonizer.start_daemon(*java_args)
        else:
            return utility.run_cmd(*java_args)

    def compile(self, outdir, *srcfiles):
        """
        Compile Java source using javac.
        """
        self.initialize()
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        utility.run_cmd('javac', '-target', '1.7', '-source', '1.7',
                          '-classpath', self.classpath, '-d', outdir, *srcfiles)

#===============================================================================
class VerbRunner(object):
#===============================================================================

    def __init__(self, command, verbspace, internal_verbspaces, config, **kwargs):
        """
        VerbRunner constructor.
        """
        # Unpack the command object for use by command implementations.
        self.verb   = command.verb
        self.opts   = command.opts
        self.args   = command.args
        self.parser = command.parser
        # The verbspace supports running nested commands.
        self.verbspace    = verbspace
        self.config       = config
        self.default_func = None
        # The internal verbspaces are just used for packaging other verbspaces.
        self.internal_verbspaces = internal_verbspaces
        # Create a Java runner.
        self.java = JavaRunner(self.verb, self.config, **kwargs)
        # Populated for Volt client verbs.
        self.client = None

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

    def is_dryrun(self):
        """
        Return True if dry-run is enabled.
        """
        return utility.is_dryrun()

    def is_verbose(self):
        """
        Return True if verbose messages are enabled.
        """
        return utility.is_verbose()

    def is_debug(self):
        """
        Return True if debug messages are enabled.
        """
        return utility.is_debug()

    def info(self, *msgs):
        """
        Display INFO level messages.
        """
        utility.info(*msgs)

    def verbose_info(self, *msgs):
        """
        Display verbose INFO level messages if enabled.
        """
        utility.verbose_info(*msgs)

    def debug(self, *msgs):
        """
        Display DEBUG level message(s) if debug is enabled.
        """
        utility.debug(*msgs)

    def warning(self, *msgs):
        """
        Display WARNING level messages.
        """
        utility.warning(*msgs)

    def error(self, *msgs):
        """
        Display ERROR level messages.
        """
        utility.error(*msgs)

    def abort(self, *msgs):
        """
        Display errors (optional) and abort execution.
        """
        self._abort(False, *msgs)

    def abort_with_help(self, *msgs):
        """
        Display errors and help and abort execution.
        """
        self._abort(True, *msgs)

    def help(self, *args, **kwargs):
        """
        Display help for command.
        """
        # The only valid keyword argument is 'all' for now.
        context = '%s.help()' % self.__class__.__name__
        all = utility.kwargs_get_boolean(kwargs, 'all', default = False)
        if all:
            for verb_name in self.verbspace.verb_names:
                verb_spec = self.verbspace.verbs[verb_name].cli_spec
                if not verb_spec.baseverb and not verb_spec.hideverb:
                    sys.stdout.write('\n===== Verb: %s =====\n\n' % verb_name)
                    self._print_verb_help(verb_name)
            for verb_name in self.verbspace.verb_names:
                verb_spec = self.verbspace.verbs[verb_name].cli_spec
                if verb_spec.baseverb and not verb_spec.hideverb:
                    sys.stdout.write('\n===== Common Verb: %s =====\n\n' % verb_name)
                    self._print_verb_help(verb_name)
        else:
            if args:
                for name in args:
                    for verb_name in self.verbspace.verb_names:
                        if verb_name == name.lower():
                            sys.stdout.write('\n')
                            self._print_verb_help(verb_name)
                            break
                    else:
                        utility.error('Verb "%s" was not found.' % name)
                        self.usage()
            else:
                self.usage()

    def package(self, output_dir_in, force, *args):
        """
        Create python-runnable package/zip file.
        """
        if output_dir_in is None:
            output_dir = ''
        else:
            output_dir = output_dir_in
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
        if args:
            # Package other verbspaces.
            for name in args:
                if name not in self.internal_verbspaces:
                    utility.abort('Unknown base command "%s" specified for packaging.' % name)
                verbspace = self.internal_verbspaces[name]
                self._create_package(output_dir, verbspace.name, verbspace.version,
                                     verbspace.description, force)
        else:
            # Package the active verbspace.
            self._create_package(output_dir, self.verbspace.name, self.verbspace.version,
                                 self.verbspace.description, force)
        # Warn for Python version < 2.6.
        compat_msg = compatibility_warning % dict(name = self.verbspace.name)
        if sys.version_info[0] == 2 and sys.version_info[1] < 6:
            utility.warning(compat_msg)
        # Generate README.<tool> file.
        readme_path = os.path.join(output_dir, 'README.%s' % self.verbspace.name)
        readme_file = utility.File(readme_path, mode = 'w')
        readme_file.open()
        try:
            readme_file.write(readme_template % dict(name    = self.verbspace.name,
                                                     usage   = self.get_usage(),
                                                     warning = compat_msg))
        finally:
            readme_file.close()

    def usage(self):
        """
        Display usage screen.
        """
        parser = VoltCLIParser(self.verbspace)
        sys.stdout.write('\n')
        parser.print_help()
        sys.stdout.write('\n')

    def get_usage(self):
        """
        Get usage string.
        """
        return VoltCLIParser(self.verbspace).get_usage_string()

    def verb_usage(self):
        """
        Display usage for active verb.
        """
        if self.verb:
            sys.stdout.write('\n')
            self._print_verb_help(self.verb.name)
            sys.stdout.write('\n')

    def set_default_func(self, default_func):
        """
        Called by a Verb object to set the default function.
        """
        self.default_func = default_func

    def go(self):
        """
        Invoke the default function provided by a Verb object.
        """
        if self.default_func is None:
            utility.abort('Verb "%s" (class %s) does not provide a default go() function.'
                                % (self.verb.name, self.verb.__class__.__name__))
        else:
            self.default_func(self)

    def execute(self):
        """
        Execute the verb function.
        """
        self.verb.execute(self)

    def call(self, *args, **kwargs):
        """
        Call a verbspace verb with arguments.
        """
        if not args:
            utility.abort('No arguments were passed to VerbRunner.call().')
        if args[0].find('.') == -1:
            self._run_command(self.verbspace, *args, **kwargs)
        else:
            verbspace_name, verb_name = args[0].split('.', 1)
            verb_name = verb_name.lower()
            if verbspace_name not in self.internal_verbspaces:
                utility.abort('Unknown name passed to VerbRunner.call(): %s' % verbspace_name)
            verbspace = self.internal_verbspaces[verbspace_name]
            if verb_name not in verbspace.verb_names:
                utility.abort('Unknown verb passed to VerbRunner.call(): %s' % args[0],
                              'Available verbs in "%s":' % verbspace_name,
                              verbspace.verb_names)
            args2 = [verb_name] + list(args[1:])
            self._run_command(verbspace, *args2, **kwargs)

    def call_proc(self, sysproc_name, types, args, check_status=True, timeout=None):
        if self.client is None:
            utility.abort('Command is not set up as a client.',
                          'Add an appropriate admin or client bundle to @VOLT.Command().')
        utility.verbose_info('Call procedure: %s%s' % (sysproc_name, tuple(args)))
        proc = voltdbclient.VoltProcedure(self.client, sysproc_name, types)
        response = proc.call(params=args, timeout=timeout)
        if check_status and response.status != 1:
            utility.abort('"%s" procedure call failed.' % sysproc_name, (response,))
        utility.verbose_info(response)
        return utility.VoltResponseWrapper(response)

    def java_execute(self, java_class, java_opts_override, *args, **kwargs):
        """
        Execute a Java program.
        """
        if utility.kwargs_get_boolean(kwargs, 'daemon', default=False):
            kwargs['daemonizer'] = self._get_daemonizer(**kwargs)
        self.java.execute(java_class, java_opts_override, *args, **kwargs)

    def setup_daemon_kwargs(self, kwargs, name=None, description=None, output=None):
        """
        Initialize daemon keyword arguments.
        """
        daemon_name = utility.daemon_file_name(
            base_name=name,
            host=getattr(self.opts, 'host', None),
            instance=getattr(self.opts, 'instance', None))
        # Default daemon output directory to the state directory, which is
        # frequently set to ~/.<command_name>.
        daemon_output = output
        if daemon_output is None:
            daemon_output = utility.get_state_directory()
        # Provide a generic description if one wasn't provided.
        daemon_description = description
        if daemon_description is None:
            daemon_description = "server"
        kwargs['daemon'] = True
        kwargs['daemon_name'] = daemon_name
        kwargs['daemon_description'] = daemon_description
        kwargs['daemon_output'] = daemon_output

    def create_daemonizer(self, name=None, description=None, output=None):
        kwargs = {}
        self.setup_daemon_kwargs(kwargs, name=name, description=description, output=output)
        return self._get_daemonizer(**kwargs)

    def find_resource(self, name, required=False):
        """
        Find a resource file.
        """
        if self.verbspace.scan_dirs:
            for scan_dir in self.verbspace.scan_dirs:
                path = os.path.join(scan_dir, name)
                if os.path.exists(path):
                    return path
        if required:
            utility.abort('Resource file "%s" is missing.' % name)
        return None

    def _print_verb_help(self, verb_name):
        # Internal method to display help for a verb
        verb = self.verbspace.verbs[verb_name]
        parser = VoltCLIParser(self.verbspace)
        parser.initialize_verb(verb_name)
        parser.print_help()
        sys.stdout.write('\n')

    def _create_package(self, output_dir, name, version, description, force):
        # Internal method to create a runnable Python package.
        output_path = os.path.join(output_dir, name)
        utility.info('Creating compressed executable Python program: %s' % output_path)
        zipper = utility.Zipper(excludes = ['[.]pyc$'])
        zipper.open(output_path, force = force, preamble = '#!/usr/bin/env python\n')
        try:
            # Generate the __main__.py module for automatic execution from the zip file.
            standalone = str(environment.standalone)
            main_script = ('''\
import sys
from voltcli import runner
runner.main('%(name)s', '', '%(version)s', '%(description)s',
            package = True, standalone = %(standalone)s, *sys.argv[1:])'''
                    % locals())
            zipper.add_file_from_string(main_script, '__main__.py')
            # Recursively package lib/python as lib in the zip file.
            zipper.add_directory(environment.volt_python, '')
            # Add <verbspace-name>.d subdirectory if found under the command directory.
            command_d = os.path.join(environment.command_dir, '%s.d' % name)
            if os.path.isdir(command_d):
                dst = os.path.join('voltcli', os.path.basename(command_d))
                zipper.add_directory(command_d, dst)
        finally:
            zipper.close(make_executable = True)

    def _run_command(self, verbspace, *args, **kwargs):
        # Internal method to run a command.
        parser = VoltCLIParser(verbspace)
        command = parser.parse(*args)
        runner = VerbRunner(command, verbspace, self.internal_verbspaces, self.config, **kwargs)
        runner.execute()

    def _abort(self, show_help, *msgs):
        if self.verb:
            utility.error('Fatal error in "%s" command.' % self.verb.name, *msgs)
            if show_help:
                self.verb_usage()
        else:
            utility.error(*msgs)
            if show_help:
                self.help()
        sys.exit(1)

    def _get_daemonizer(self, **kwargs):
        """
        Scan keyword arguments for daemon-related options (and strip them
        out). Return a daemonizer.
        """
        name = utility.kwargs_get_string(kwargs, 'daemon_name', default=environment.command_name)
        description = utility.kwargs_get_string(kwargs, 'daemon_description', default=False)
        output = utility.kwargs_get_string(kwargs, 'daemon_output', default=environment.command_dir)
        return utility.Daemonizer(name, description, output=output)

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
        # Expose all BaseOption and BaseArgument derivatives used for declaring
        # options and arguments in command decorators.
        for name, cls in inspect.getmembers(cli, inspect.isclass):
            if issubclass(cls, cli.BaseOption) or issubclass(cls, cli.BaseArgument):
                setattr(self, name, cls)
        # Expose specific useful voltdbclient symbols for Volt client commands.
        self.VoltProcedure  = voltdbclient.VoltProcedure
        self.VoltResponse   = voltdbclient.VoltResponse
        self.VoltException  = voltdbclient.VoltException
        self.VoltTable      = voltdbclient.VoltTable
        self.VoltColumn     = voltdbclient.VoltColumn
        self.FastSerializer = voltdbclient.FastSerializer
        # For declaring multi-command verbs like "show".
        self.Modifier = Modifier
        # Bundles
        self.ConnectionBundle = ConnectionBundle
        self.ClientBundle     = ClientBundle
        self.AdminBundle      = AdminBundle
        self.ServerBundle     = ServerBundle
        # As a convenience expose the utility module so that commands don't
        # need to import it.
        self.utility = utility

#===============================================================================
def load_verbspace(command_name, command_dir, config, version, description, package):
#===============================================================================
    """
    Build a verb space by searching for source files with verbs in this source
    file's directory, the calling script location (if provided), and the
    working directory.
    """
    utility.debug('Loading verbspace for "%s" version "%s" from "%s"...'
                        % (command_name, version, command_dir))
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
    scan_dirs = [os.path.join(d, verbs_subdir) for d in scan_base_dirs]
    for scan_dir in scan_dirs:
        finder.add_path(scan_dir)
    # If running from a zip package add resource locations.
    if package:
        finder.add_resource('__main__', os.path.join('voltcli', verbs_subdir))
    finder.search_and_execute(VOLT = namespace_VOLT)

    # Add standard verbs if they aren't supplied.
    def default_func(runner):
        runner.go()
    for verb_name, verb_cls in (('help', HelpVerb), ('package', PackageVerb)):
        if verb_name not in verbs:
            verbs[verb_name] = verb_cls(verb_name, default_func)

    return VerbSpace(command_name, version, description, namespace_VOLT, scan_dirs, verbs)

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
class VoltCLIParser(cli.CLIParser):
#===============================================================================
    def __init__(self, verbspace):
        """
        VoltCLIParser constructor.
        """
        cli.CLIParser.__init__(self, environment.command_name,
                                     verbspace.verbs,
                                     base_cli_spec.options,
                                     base_cli_spec.usage,
                                     '\n'.join((verbspace.description,
                                                base_cli_spec.description)),
                                     '%%prog version %s' % verbspace.version)

#===============================================================================
def run_command(verbspace, internal_verbspaces, config, *args, **kwargs):
#===============================================================================
    """
    Run a command after parsing the command line arguments provided.
    """
    # Parse the command line.
    parser = VoltCLIParser(verbspace)
    command = parser.parse(*args)

    # Initialize utility function options according to parsed options.
    utility.set_verbose(command.opts.verbose)
    utility.set_debug(  command.opts.debug)
    if hasattr(command.opts, 'dryrun'):
        utility.set_dryrun( command.opts.dryrun)

    # Run the command. Pass along kwargs. This allows verbs calling other verbs
    # to add keyword arguments like "classpath".
    runner = VerbRunner(command, verbspace, internal_verbspaces, config, **kwargs)
    runner.execute()

#===============================================================================
def main(command_name, command_dir, version, description, *args, **kwargs):
#===============================================================================
    """
    Called by running script to execute command with command line arguments.
    """
    # The "package" keyword flags when running from a package zip __main__.py.
    package = utility.kwargs_get_boolean(kwargs, 'package', default=False)
    # The "standalone" keyword allows environment.py to skip the library search.
    standalone = utility.kwargs_get_boolean(kwargs, 'standalone', default=False)
    # The "state_directory" keyword overrides ~/.<command_name> as the
    # directory used for runtime state files.
    state_directory = utility.kwargs_get_string(kwargs, 'state_directory', default=None)
    try:
        # Pre-scan for verbose, debug, and dry-run options so that early code
        # can display verbose and debug messages, and obey dry-run.
        opts = cli.preprocess_options(base_cli_spec.options, args)
        utility.set_verbose(opts.verbose)
        utility.set_debug(opts.debug)

        # Load the configuration and state
        permanent_path = os.path.join(os.getcwd(), environment.config_name)
        local_path     = os.path.join(os.getcwd(), environment.config_name_local)
        config = VoltConfig(permanent_path, local_path)

        # Initialize the environment
        environment.initialize(standalone, command_name, command_dir, version)

        # Initialize the state directory (for runtime state files).
        if state_directory is None:
            state_directory = '~/.%s' % environment.command_name
        state_directory = os.path.expandvars(os.path.expanduser(state_directory))
        utility.set_state_directory(state_directory)

        # Search for modules based on both this file's and the calling script's location.
        verbspace = load_verbspace(command_name, command_dir, config, version,
                                   description, package)

        # Make internal commands available to user commands via runner.verbspace().
        internal_verbspaces = {}
        if command_name not in internal_commands:
            for internal_command in internal_commands:
                internal_verbspace = load_verbspace(internal_command, None, config, version,
                                                    'Internal "%s" command' % internal_command,
                                                    package)
                internal_verbspaces[internal_command] = internal_verbspace

        # Run the command
        run_command(verbspace, internal_verbspaces, config, *args)

    except KeyboardInterrupt:
        sys.stderr.write('\n')
        utility.abort('break')
