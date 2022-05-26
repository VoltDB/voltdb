# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

__author__ = 'scooper'

import sys

import voltdbclient
from voltcli import cli
from voltcli import environment
from voltcli import utility
from voltcli import checkconfig

#===============================================================================
class BaseVerb(object):
#===============================================================================
    """
    Base class for verb implementations. Used by the @Volt.Command decorator.
    """
    def __init__(self, name, **kwargs):
        self.name = name
        self.classpath = utility.kwargs_get_string(kwargs, 'classpath', default = None)
        self.cli_spec = cli.CLISpec(**kwargs)
        self.dirty_opts = False
        self.dirty_args = False
        self.command_arguments = utility.flatten_to_list(kwargs.get('command_arguments', None))
        self.log4j_default = utility.flatten_to_list(kwargs.get('log4j_default', None))
        utility.debug(str(self))

    def execute(self, runner):
        utility.abort('%s "%s" object does not implement the required execute() method.'
                            % (self.__class__.__name__, self.name))

    def add_options(self, *args):
        """
        Add options if not already present as an option or argument.
        """
        for o in args:
            dest_name = o.get_dest()
            if self.cli_spec.find_option(dest_name):
                utility.debug('Not adding "%s" option more than once.' % dest_name)
            else:
                self.cli_spec.add_to_list('options', o)
                self.dirty_opts = True

    def add_arguments(self, *args):
        self.cli_spec.add_to_list('arguments', *args)
        self.dirty_args = True

    def get_attr(self, name, default = None):
        return self.cli_spec.get_attr(name)

    def pop_attr(self, name, default = None):
        return self.cli_spec.pop_attr(name)

    def merge_java_options(self, name, *options):
        return self.cli_spec.merge_java_options(name, *options)

    def set_defaults(self, **kwargs):
        return self.cli_spec.set_defaults(**kwargs)

    def __cmp__(self, other):
        return cmp(self.name, other.name)

    def __str__(self):
        return '%s: %s\n%s' % (self.__class__.__name__, self.name, self.cli_spec)

    def get_option_count(self):
        if not self.cli_spec.options:
            return 0
        return len(self.cli_spec.options)

    def get_argument_count(self):
        if not self.cli_spec.arguments:
            return 0
        return len(self.cli_spec.arguments)

    def iter_options(self):
        if self.cli_spec.options:
            self._check_options()
            for o in self.cli_spec.options:
                yield o

    def iter_arguments(self):
        if self.cli_spec.arguments:
            self._check_arguments()
            for a in self.cli_spec.arguments:
                yield a

    def _check_options(self):
        if self.dirty_opts:
            self.cli_spec.options.sort()
            self.dirty_opts = False


    def _check_arguments(self):
        if self.dirty_args:
            # Use a local function to sanity check an argument's min/max counts,
            # with an additional check applied to arguments other than the last
            # one since they cannot repeat or be missing.
            def check_argument(cli_spec_arg, is_last):
                if cli_spec_arg.min_count < 0 or cli_spec_arg.max_count < 0:
                    utility.abort('%s argument (%s) has a negative min or max count declared.'
                                        % (self.name, self.cli_spec_arg.name))
                if cli_spec_arg.min_count == 0 and cli_spec_arg.max_count == 0:
                    utility.abort('%s argument (%s) has zero min and max counts declared.'
                                        % (self.name, self.cli_spec_arg.name))
                if not is_last and (cli_spec_arg.min_count != 1 or cli_spec_arg.max_count != 1):
                    utility.abort('%s argument (%s) is not the last argument, '
                                  'but has min/max counts declared.'
                                        % (self.name, self.cli_spec_arg.name))
            nargs = len(self.cli_spec.arguments)
            if nargs > 1:
                # Check all arguments except the last.
                for i in range(nargs-1):
                    check_argument(self.cli_spec.arguments[i], False)
                # Check the last argument.
                check_argument(self.cli_spec.arguments[-1], True)
            self.dirty_args = False

#===============================================================================
class CommandVerb(BaseVerb):
#===============================================================================
    """
    Verb that wraps a command function. Used by the @VOLT.Command decorator.
    """
    def __init__(self, name, function, **kwargs):
        BaseVerb.__init__(self, name, **kwargs)
        self.function = function
        self.bundles = utility.kwargs_get_list(kwargs, 'bundles')
        # Allow the bundles to adjust options.
        for bundle in self.bundles:
            bundle.initialize(self)
        self.add_options(cli.BooleanOption(None, '--dry-run', 'dryrun', None))

    def execute(self, runner):
        # Start the bundles, e.g. to create client a connection.
        for bundle in self.bundles:
            bundle.start(self, runner)
        try:
            # Set up the go() method for use as the default implementation.
            runner.set_default_func(self.go)
            # Execute the verb function.
            self.function(runner)
        finally:
            # Stop the bundles in reverse order.
            for i in range(len(self.bundles)-1, -1, -1):
                self.bundles[i].stop(self, runner)

    def go(self, runner):
        gofound = False
        for bundle in self.bundles:
            if hasattr(bundle, 'go'):
                bundle.go(self, runner)
                gofound = True
        if not gofound:
            utility.abort('go() method is not implemented by any bundle or %s.'
                                % self.__class__.__name__)

#===============================================================================
class HelpVerb(CommandVerb):
#===============================================================================
    """
    Verb to provide standard help. Used by the @VOLT.Help decorator.
    """
    def __init__(self, name, function, **kwargs):
        CommandVerb.__init__(self, name, function, **kwargs)
        self.set_defaults(description = 'Display general or verb-specific help.', baseverb = True)
        self.add_options(
            cli.BooleanOption('-a', '--all', 'all',
                              'display all available help, including verb usage'))
        self.add_arguments(
            cli.StringArgument('verb', 'verb name', min_count = 0, max_count = None))

    def go(self, runner):
        runner.help(all = runner.opts.all, *runner.opts.verb)

#===============================================================================
class PackageVerb(CommandVerb):
#===============================================================================
    """
    Verb to create a runnable Python package. Used by @VOLT.Package decorator.
    """
    def __init__(self, name, function, **kwargs):
        CommandVerb.__init__(self, name, function, **kwargs)
        self.set_defaults(description  = 'Create a runnable Python program package.',
                          baseverb     = True,
                          hideverb     = True,
                          description2 = '''
The optional NAME argument(s) allow package generation for base commands other
than the current one. If no NAME is provided the current base command is
packaged.''')
        self.add_options(
            cli.BooleanOption('-f', '--force', 'force',
                              'overwrite existing file without asking',
                              default = False),
            cli.StringOption('-o', '--output_dir', 'output_dir',
                             'specify the output directory (defaults to the working directory)'))
        self.add_arguments(
            cli.StringArgument('name', 'base command name', min_count = 0, max_count = None))

    def go(self, runner):
        runner.package(runner.opts.output_dir, runner.opts.force, *runner.opts.name)

#===============================================================================
class Modifier(object):
#===============================================================================
    """
    Class for declaring multi-command modifiers.
    """
    def __init__(self, name, function, description, arg_name = ''):
        self.name = name
        self.description = description
        self.function = function
        self.arg_name = arg_name.upper()

#===============================================================================
class MultiVerb(CommandVerb):
#===============================================================================
    """
    Verb to create multi-commands with modifiers and optional arguments.
    """
    def __init__(self, name, function, **kwargs):
        CommandVerb.__init__(self, name, function, **kwargs)
        self.modifiers = utility.kwargs_get_list(kwargs, 'modifiers', default = [])
        if not self.modifiers:
            utility.abort('Multi-command "%s" must provide a "modifiers" list.' % self.name)
        valid_modifiers = '|'.join([mod.name for mod in self.modifiers])
        has_args = 0
        rows = []
        for mod in self.modifiers:
            if mod.arg_name:
                usage = '%s %s [ %s ... ]' % (self.name, mod.name, mod.arg_name)
                has_args += 1
            else:
                usage = '%s %s' % (self.name, mod.name)
            rows.append((usage, mod.description))
        caption = '"%s" Command Variations' % self.name
        other_info = utility.format_table(rows, caption = caption, separator = '  ')
        self.set_defaults(other_info = other_info.strip())
        args = [
            cli.StringArgument('modifier',
                               'command modifier (valid modifiers: %s)' % valid_modifiers)]
        if has_args > 0:
            if has_args == len(self.modifiers):
                arg_desc = 'optional arguments(s)'
            else:
                arg_desc = 'optional arguments(s) (where applicable)'
            args.append(cli.StringArgument('arg', arg_desc, min_count = 0, max_count = None))
        self.add_arguments(*args)

    def go(self, runner):
        mod_name = runner.opts.modifier.lower()
        for mod in self.modifiers:
            if mod.name == mod_name:
                mod.function(runner)
                break
        else:
            utility.error('Invalid "%s" modifier "%s". Valid modifiers are listed below:'
                                % (self.name, mod_name),
                          [mod.name for mod in self.modifiers])
            runner.help(self.name)

#===============================================================================
class VerbDecorators(object):
#===============================================================================
    """
    Provide decorators used by command implementations to declare commands.
    NB: All decorators assume they are being called.  E.g. @VOLT.Command() is
    valid, but @VOLT.Command is not, even though Python won't catch the latter
    as a compile-time error.
    """

    def __init__(self, verbs):
        """
        Constructor. The verbs argument is the dictionary to populate with
        discovered Verb objects. It maps verb name to Verb object.
        """
        self.verbs = verbs

    def _add_verb(self, verb):
        if verb.name in self.verbs:
            utility.abort('Verb "%s" is declared more than once.' % verb.name)
        self.verbs[verb.name] = verb

    def _get_decorator(self, verb_class, *args, **kwargs):
        def inner_decorator(function):
            def wrapper(*args, **kwargs):
                function(*args, **kwargs)
            verb = verb_class(function.__name__, wrapper, *args, **kwargs)
            self._add_verb(verb)
            return wrapper
        return inner_decorator

    def Command(self, *args, **kwargs):
        """
        @VOLT.Command decorator for declaring general-purpose commands.
        """
        return self._get_decorator(CommandVerb, *args, **kwargs)

    def Help(self, *args, **kwargs):
        """
        @VOLT.Help decorator for declaring help commands.
        """
        return self._get_decorator(HelpVerb, *args, **kwargs)

    def Package(self, *args, **kwargs):
        """
        @VOLT.Package decorator for declaring commands for CLI packaging.
        """
        return self._get_decorator(PackageVerb, *args, **kwargs)

    def Multi_Command(self, *args, **kwargs):
        """
        @VOLT.Multi decorator for declaring "<verb> <tag>" commands.
        """
        return self._get_decorator(MultiVerb, *args, **kwargs)

#===============================================================================
class VerbSpace(object):
#===============================================================================
    """
    Manages a collection of Verb objects that support a particular CLI interface.
    """
    def __init__(self, name, version, description, VOLT, scan_dirs, verbs, pro_version):
        self.name        = name
        self.version     = version
        self.pro_version       = pro_version
        self.description = description.strip()
        self.VOLT        = VOLT
        self.scan_dirs   = scan_dirs
        self.verbs       = verbs
        self.verb_names  = list(self.verbs.keys())
        self.verb_names.sort()

#===============================================================================
class JavaBundle(object):
#===============================================================================
    """
    Verb that wraps a function that calls into a Java class. Used by
    the @VOLT.Java decorator.
    """
    def __init__(self, java_class):
        self.java_class = java_class

    def initialize(self, verb):
        verb.add_options(
           cli.StringOption(None, '--client', 'clientport', 'specify the client port as [ipaddress:]port-number'),
           cli.StringOption(None, '--internal', 'internalport', 'specify the internal port as [ipaddress:]port-number used to communicate between cluster nodes'),
           cli.StringOption(None, '--zookeeper', 'zkport', 'specify the zookeeper port as [ipaddress:]port-number'),
           cli.StringOption(None, '--drpublic', 'drpublic', 'Specifies the interface (ipaddress[:port-number]) advertised to consumer clusters as the DR interface, used in hosted environments where internal interfaces are not reachable'),
           cli.StringOption(None, '--replication', 'replicationport', 'specify the replication port as [ipaddress:]port-number (1st of 3 sequential ports)'),
           cli.StringOption(None, '--admin', 'adminport', 'specify the admin port as [ipaddress:]port-number'),
           cli.StringOption(None, '--http', 'httpport', 'specify the http port as [ipaddress:]port-number'),
           cli.StringOption(None, '--status', 'statusport', None), # hidden: 'specify the port used for status monitoring via http, as [ipaddress:]port-number'
           cli.StringOption(None, '--internalinterface', 'internalinterface', 'specify the network interface to use for internal communication, such as the internal and zookeeper ports'),
           cli.StringOption(None, '--externalinterface', 'externalinterface', 'specify the network interface to use for external ports, such as the admin and client ports'),
           cli.StringOption(None, '--publicinterface', 'publicinterface', 'For hosted or cloud environments with non-public interfaces, this argument specifies a publicly-accessible alias for reaching the server. Particularly useful for remote access to the VoltDB Management Center.'),
           cli.StringOption(None, '--topicspublic', 'topicspublic', 'Specifies the interface (ipaddress[:port-number]) advertised to subscribers as the Topics interface, used in hosted environments where internal interfaces are not reachable'),
           cli.StringOption(None, '--topicsport', 'topicsport', 'specify the topics port as [ipaddress:]port-number'))


    def start(self, verb, runner):
        verb.add_options(cli.StringOption(None, '--credentials', "credentials", "contains username and password information"))
        pass

    def go(self, verb, runner):
        self.run_java(runner, verb, *runner.args)

    def stop(self, verb, runner):
        pass

    def run_java(self, verb, runner, *args, **kw):
        opts_override = verb.get_attr('java_opts_override', default = [])
        runner.java_execute(self.java_class, opts_override, *args, **kw)

#===============================================================================
class ServerBundle(JavaBundle):
#===============================================================================
    """
    Bundle class to run org.voltdb.VoltDB process.
    As of V11.0, only used by 'voltdb start' command.
    """
    def __init__(self, subcommand,
                 safemode_available=False,
                 supports_daemon=False,
                 daemon_name=None,
                 daemon_description=None,
                 daemon_output=None,
                 supports_multiple_daemons=False,
                 check_environment_config=False,
                 supports_paused=False):
        JavaBundle.__init__(self, 'org.voltdb.VoltDB')
        self.subcommand = subcommand
        self.safemode_available = safemode_available
        self.supports_daemon = supports_daemon
        self.daemon_name = daemon_name
        self.daemon_description = daemon_description
        self.daemon_output = daemon_output
        self.supports_multiple_daemons = supports_multiple_daemons
        self.check_environment_config = check_environment_config
        self.supports_paused = supports_paused

    def initialize(self, verb):
        JavaBundle.initialize(self, verb)
        verb.add_options(
            cli.StringListOption(None, '--ignore', 'skip_requirements',
                             '''requirements to skip when start voltdb:
                 thp - Checking for Transparent Huge Pages (THP) has been disabled.  Use of THP can cause VoltDB to run out of memory. Do not disable this check on production systems.''',
                             default = None))
        verb.add_options(
            cli.StringOption('-g', '--placement-group', 'placementgroup',
                             'placement group',
                             default = '0'))
        # --safemode only used by recover server action (on start)
        if self.safemode_available:
            verb.add_options(cli.BooleanOption(None, '--safemode', 'safemode', None))
        if self.supports_daemon:
            verb.add_options(
                cli.BooleanOption('-B', '--background', 'daemon',
                                  'run the VoltDB server in the background (as a daemon process)'))
            if self.supports_multiple_daemons:
                # Keep the -I/--instance option hidden for now.
                verb.add_options(
                    cli.IntegerOption('-I', '--instance', 'instance',
                                  #'specify an instance number for multiple servers on the same host'))
                                  None))
        if self.supports_paused:
            verb.add_options(
                cli.BooleanOption('-p', '--pause', 'paused',
                                  'Start Database in paused mode.'))

    def go(self, verb, runner):
        if self.check_environment_config:
            incompatible_options = checkconfig.test_hard_requirements()
            for k,v in  list(incompatible_options.items()):
                state = v[0]
                if state == 'PASS' :
                    pass
                elif state == "WARN":
                    utility.warning(v[1])
                elif state == 'FAIL' :
                    if k in list(checkconfig.skippableRequirements.keys()) and runner.opts.skip_requirements and checkconfig.skippableRequirements[k] in runner.opts.skip_requirements:
                        utility.warning(v[1])
                    else:
                        utility.abort(v[1])
                else:
                    utility.error(v[1])
        final_args = [self.subcommand]
        if self.safemode_available and runner.opts.safemode:
            final_args.extend(['safemode'])
        if runner.opts.placementgroup:
            final_args.extend(['placementgroup', runner.opts.placementgroup])
        if runner.opts.clientport:
            final_args.extend(['port', runner.opts.clientport])
        if runner.opts.adminport:
            final_args.extend(['adminport', runner.opts.adminport])
        if runner.opts.httpport:
            final_args.extend(['httpport', runner.opts.httpport])
        if runner.opts.statusport:
            final_args.extend(['statusport', runner.opts.statusport])
        if runner.opts.internalinterface:
            final_args.extend(['internalinterface', runner.opts.internalinterface])
        if runner.opts.internalport:
            final_args.extend(['internalport', runner.opts.internalport])
        if runner.opts.replicationport:
            final_args.extend(['replicationport', runner.opts.replicationport])
        if runner.opts.zkport:
            final_args.extend(['zkport', runner.opts.zkport])
        if runner.opts.externalinterface:
            final_args.extend(['externalinterface', runner.opts.externalinterface])
        if runner.opts.publicinterface:
            final_args.extend(['publicinterface', runner.opts.publicinterface])
        if runner.opts.drpublic:
            final_args.extend(['drpublic', runner.opts.drpublic])
        if runner.opts.topicsport:
            final_args.extend(('topicsHostPort', runner.opts.topicsport))
        if runner.opts.topicspublic:
            final_args.extend(('topicspublic', runner.opts.topicspublic))
        if self.supports_paused and runner.opts.paused:
            final_args.extend(['paused'])
        if runner.args:
            final_args.extend(runner.args)
        kwargs = {}
        if self.supports_daemon and runner.opts.daemon:
            # Provide a default description if not specified.
            daemon_description = self.daemon_description
            if daemon_description is None:
                daemon_description = "VoltDB server"
            # Initialize all the daemon-related keyword arguments.
            runner.setup_daemon_kwargs(kwargs, name=self.daemon_name,
                                               description=daemon_description,
                                               output=self.daemon_output)
        else:
            # Replace the Python process.
            kwargs['exec'] = True
        self.run_java(verb, runner, *final_args, **kwargs)

    def stop(self, verb, runner):
        pass

#===============================================================================
class ConnectionBundle(object):
#===============================================================================
    """
    Bundle class to add host(s), port(s), user, and password connection
    options. Use by assigning an instance to the "bundles" keyword inside a
    decorator invocation.
    """
    def __init__(self, default_port = None, min_count = 1, max_count = 1):
        self.default_port = default_port
        self.min_count    = min_count
        self.max_count    = max_count

    def initialize(self, verb):
        verb.add_options(
            cli.HostOption('-H', '--host', 'host', 'connection',
                           default      = 'localhost',
                           min_count    = self.min_count,
                           max_count    = self.max_count,
                           default_port = self.default_port),
            cli.StringOption('-p', '--password', 'password', "the connection password"),
            cli.StringOption('-u', '--user', 'username', 'the connection user name'),
            cli.StringOption(None, '--credentials', 'credentials', 'the credentials file which contains user name and password'),
            cli.StringOption(None, '--ssl', 'ssl_config','''enable and config ssl''', default=None),
            cli.BooleanOption(None, '--kerberos', 'kerberos', '''enable kerberos'''))

    def start(self, verb, runner):
        pass

    def stop(self, verb, runner):
        pass

#===============================================================================
class BaseClientBundle(ConnectionBundle):
#===============================================================================
    """
    Bundle class to automatically create a client connection.  Use by
    assigning an instance to the "bundles" keyword inside a decorator
    invocation.
    """
    def __init__(self, default_port):
        ConnectionBundle.__init__(self, default_port = default_port, min_count = 1, max_count = 1)

    def start(self, verb, runner):
        runner.voltdb_connect(runner.opts.host.host,
                              runner.opts.host.port,
                              username=runner.opts.username,
                              password=runner.opts.password,
                              ssl_config=runner.opts.ssl_config,
                              kerberos=runner.opts.kerberos)

    def stop(self, verb, runner):
        runner.voltdb_disconnect()

#===============================================================================
class ClientBundle(BaseClientBundle):
#===============================================================================
    """
    Bundle class to automatically create an non-admin client connection.  Use
    by assigning an instance to the "bundles" keyword inside a decorator
    invocation.
    """
    def __init__(self, **kwargs):
        BaseClientBundle.__init__(self, 21212, **kwargs)

#===============================================================================
class AdminBundle(BaseClientBundle):
#===============================================================================
    """
    Bundle class to automatically create an admin client connection.  Use by
    assigning an instance to the "bundles" keyword inside a decorator
    invocation.
    """
    def __init__(self, **kwargs):
        BaseClientBundle.__init__(self, 21211, **kwargs)
