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

from voltdbclient import *
from voltcli import cli
from voltcli import environment
from voltcli import utility

#===============================================================================
class BaseVerb(object):
#===============================================================================
    """
    Base class for verb implementations. Used by the @Volt.Command decorator.
    """
    def __init__(self, name, **kwargs):
        self.name = name
        self.classpath = utility.kwargs_get(kwargs, 'classpath', default = None)
        self.cli_spec = cli.CLISpec(**kwargs)
        self.dirty_opts = False
        self.dirty_args = False
        self.command_arguments = utility.flatten_to_list(kwargs.get('command_arguments', None))
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

    def get_options(self, required_only = False):
        return (o for o in self.iter_options(required_only = required_only))

    def get_arguments(self):
        return (o for o in self.iter_arguments())

    def get_option_count(self):
        if not self.cli_spec.options:
            return 0
        return len(self.cli_spec.options)

    def get_argument_count(self):
        if not self.cli_spec.arguments:
            return 0
        return len(self.cli_spec.arguments)

    def iter_options(self, required_only = False):
        if self.cli_spec.options:
            if self.dirty_opts:
                self.cli_spec.options.sort()
                self.dirty_opts = False
            for o in self.cli_spec.options:
                if not required_only or (o.required and getattr(opts, o.kwargs['dest']) is None):
                    yield o

    def iter_arguments(self):
        if self.cli_spec.arguments:
            if self.dirty_args:
                self.cli_spec.arguments.sort()
                self.dirty_args = False
            for o in self.cli_spec.arguments:
                yield o

#===============================================================================
class CommandVerb(BaseVerb):
#===============================================================================
    """
    Verb that wraps a command function. Used by the @VOLT.Command decorator.
    """
    def __init__(self, name, function, **kwargs):
        BaseVerb.__init__(self, name, **kwargs)
        self.function = function

    def execute(self, runner):
        runner.set_default_func(self.go)
        self.function(runner)

    def go(self, runner, *args):
        utility.abort('The default go() method is not implemented by %s.'
                            % self.__class__.__name__)

#===============================================================================
class JavaVerb(CommandVerb):
#===============================================================================
    """
    Verb that wraps a function that calls into a Java class. Used by
    the @VOLT.Java decorator.
    """
    def __init__(self, name, function, java_class, **kwargs):
        CommandVerb.__init__(self, name, function, **kwargs)
        self.java_class = java_class
        self.add_options(
           cli.IntegerOption('-D', '--debugport', 'debugport',
                             'enable remote debugging on the specified port'),
           cli.BooleanOption('-n', '--dry-run', 'dryrun',
                             'display actions without executing them'))

    def go(self, runner, *args):
        final_args = list(args) + list(runner.args)
        self.run_java(runner, *final_args)

    def run_java(self, runner, *args):
        opts_override = self.get_attr('java_opts_override', default = [])
        if runner.opts.debugport:
            kw = {'debugport': runner.opts.debugport}
        else:
            kw = {}
        runner.java.execute(self.java_class, opts_override, *args, **kw)

#===============================================================================
class ServerVerb(JavaVerb):
#===============================================================================
    """
    Verb that wraps a function that calls into a Java class to run a VoltDB
    server process. Used by the @VOLT.Server decorator.
    """
    def __init__(self, name, function, server_subcommand, **kwargs):
        JavaVerb.__init__(self, name, function, 'org.voltdb.VoltDB', **kwargs)
        self.server_subcommand = server_subcommand
        # Add common server-ish options.
        self.add_options(
            cli.StringOption('-d', '--deployment', 'deployment',
                             'the deployment configuration file path',
                             default = 'deployment.xml'),
            cli.StringOption('-H', '--host', 'host',
                             'the coordinating host as HOST[:PORT]',
                             default = 'localhost'),
            cli.StringOption('-l', '--license', 'license',
                             'the license file path'))
        self.add_arguments(
            cli.StringArgument('catalog',
                               'the application catalog jar file path'))
        # Add appropriate server-ish Java options.
        self.merge_java_options('java_opts_override',
                '-server',
                '-XX:+HeapDumpOnOutOfMemoryError',
                '-XX:HeapDumpPath=/tmp',
                '-XX:-ReduceInitialCardMarks')

    def go(self, runner, *args):
        final_args = [self.server_subcommand]
        catalog = runner.opts.catalog
        if not catalog:
            catalog = runner.config.get('volt.catalog')
        if catalog is None:
            utility.abort('A catalog path is required.')
        final_args.extend(['catalog', catalog])
        if args:
            final_args.extend(args)
        if runner.opts.deployment:
            final_args.extend(['deployment', runner.opts.deployment])
        if runner.opts.host:
            host = utility.parse_hosts(runner.opts.host, min_hosts = 1, max_hosts = 1)[0]
            final_args.extend(['host', host.host])
            if host.port is not None:
                final_args.extend(['port', host.port])
        if runner.opts.license:
            final_args.extend(['license', runner.opts.license])
        java_args = list(args) + list(runner.args)
        self.run_java(runner, *final_args)

#===============================================================================
class ClientVerb(CommandVerb):
#===============================================================================
    """
    Verb that wraps a function which uses the client API to call stored
    procedures, etc..
    """
    def __init__(self, name, function, default_port, **kwargs):
        CommandVerb.__init__(self, name, function, **kwargs)
        self.default_port = default_port
        self.add_options(
            cli.StringOption('-H', '--host', 'host',
                             'the target host as HOST[:PORT]',
                             default = 'localhost'),
            cli.StringOption('-p', '--password', 'password',
                             "the user's connection password",
                             default = ''),
            cli.StringOption('-u', '--user', 'username',
                             'the connection user name',
                             default = ''))

    def execute(self, runner):
        host = utility.parse_hosts(runner.opts.host, min_hosts = 1, max_hosts = 1,
                                   default_port = self.default_port)[0]
        # Connect the client.
        try:
            runner.client = FastSerializer(host.host, host.port,
                                           username = runner.opts.username,
                                           password = runner.opts.password)
        except Exception, e:
            utility.abort('Client connection failed.', e)
        try:
            # Execute the verb.
            CommandVerb.execute(self, runner)
        finally:
            # Disconnect the client.
            runner.client.close()

#===============================================================================
class HelpVerb(CommandVerb):
#===============================================================================
    """
    Verb to provide standard help. Used by the @VOLT.Help decorator.
    """
    def __init__(self, name, function, **kwargs):
        CommandVerb.__init__(self, name, function, **kwargs)
        self.set_defaults(description = 'Display general or verb-specific help.',
                          usage       = '[VERB ...]',
                          baseverb    = True)
        self.add_options(
            cli.BooleanOption('-a', '--all', 'all',
                              'display all available help, including verb usage'))

    def go(self, runner, *args):
        runner.help(all = runner.opts.all, *runner.args)

#===============================================================================
class PackageVerb(CommandVerb):
#===============================================================================
    """
    Verb to create a runnable Python package. Used by @VOLT.Package decorator.
    """
    def __init__(self, name, function, **kwargs):
        CommandVerb.__init__(self, name, function, **kwargs)
        self.set_defaults(description  = 'Create a runnable Python program package.',
                          usage        = '[NAME ...]',
                          baseverb     = True,
                          hideverb     = True,
                          description2 = '''\
The optional NAME argument(s) allow package generation for base commands other
than the current one. If no NAME is provided the current base command is
packaged.''')
        self.add_options(
            cli.BooleanOption('-f', '--force', 'force',
                              'overwrite existing file without asking',
                              default = False),
            cli.StringOption('-o', '--output_dir', 'output_dir',
                             'specify the output directory (defaults to the working directory)'))

    def go(self, runner, *args):
        runner.package(runner.opts.output_dir, runner.opts.force, *args)

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

    def _get_java_decorator(self, verb_class, java_class, *args, **kwargs):
        # Add extra message to description.
        extra_help = '(use --help for full usage)'
        if 'description' in kwargs:
            kwargs['description'] += ' %s' % extra_help
        else:
            kwargs['description'] = extra_help
        return self._get_decorator(verb_class, java_class, *args, **kwargs)

    def Command(self, *args, **kwargs):
        """
        @VOLT.Command decorator for declaring general-purpose commands.
        """
        return self._get_decorator(CommandVerb, *args, **kwargs)

    def Java(self, java_class, *args, **kwargs):
        """
        @VOLT.Java decorator for declaring commands that call Java classes.
        """
        return self._get_java_decorator(JavaVerb, java_class, *args, **kwargs)

    def Server(self, server_subcommand, *args, **kwargs):
        """
        @VOLT.Server decorator for declaring commands that call Java classes to
        run a VoltDB server process.
        """
        return self._get_decorator(ServerVerb, server_subcommand, *args, **kwargs)

    def Client(self, *args, **kwargs):
        """
        @VOLT.Client decorator for declaring commands that run as VoltDB
        clients.
        """
        return self._get_decorator(ClientVerb, 21212, *args, **kwargs)

    def Admin_Client(self, *args, **kwargs):
        """
        @VOLT.Admin_Client decorator for declaring commands that run as
        administrative VoltDB clients.
        """
        return self._get_decorator(ClientVerb, 21211, *args, **kwargs)

    def Help(self, *args, **kwargs):
        """
        @VOLT.Help decorator for declaring help commands.
        """
        return self._get_decorator(HelpVerb, *args, **kwargs)

    def Package(self, *args, **kwargs):
        """
        @VOLT.Package decorator for declarin commands for CLI packaging.
        """
        return self._get_decorator(PackageVerb, *args, **kwargs)

#===============================================================================
class VerbSpace(object):
#===============================================================================
    """
    Manages a collection of Verb objects that support a particular CLI interface.
    """
    def __init__(self, name, version, description, VOLT, verbs):
        self.name        = name
        self.version     = version
        self.description = description.strip()
        self.VOLT        = VOLT
        self.verbs       = verbs
        self.verb_names  = self.verbs.keys()
        self.verb_names.sort()

