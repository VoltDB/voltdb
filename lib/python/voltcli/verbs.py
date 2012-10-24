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

import copy

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
        # The baseverb flag is used for common verbs like package and help so
        # that they can be kept separate from application-specific verbs.
        self.classpath = utility.kwargs_get(kwargs, 'classpath', default = None)
        self.cli_spec = cli.CLISpec(**kwargs)
        utility.debug(str(self))

    def execute(self, runner):
        utility.abort('%s "%s" object does not implement the required execute() method.'
                            % (self.__class__.__name__, self.name))

    def add_cli_options(self, *cli_options):
        self.cli_spec.add_cli_options(*cli_options)

    def get_option(self, name, default = None):
        return self.cli_spec.get_option(name)

    def pop_option(self, name, default = None):
        return self.cli_spec.pop_option(name)

    def merge_java_options(self, name, *options):
        return self.cli_spec.merge_java_options(name, *options)

    def set_defaults(self, **kwargs):
        return self.cli_spec.set_defaults(**kwargs)

    def __cmp__(self, other):
        return cmp(self.name, other.name)

    def __str__(self):
        return '%s: %s\n%s' % (self.__class__.__name__, self.name, self.cli_spec)

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
        runner.set_go_default(self.go)
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
        self.add_cli_options(
               cli.IntegerOption('-R', '--remotedebug', 'remotedebug',
                                 'enable remote debugging on specified port, e.g. 9090'))

    def go(self, runner, *args):
        self.run_java(runner, list(args) + list(runner.args))

    def run_java(self, runner, *args):
        opts_override = self.get_option('java_opts_override', default = [])
        if runner.opts.remotedebug:
            kw = {'remotedebug': runner.opts.remotedebug}
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
        self.add_cli_options(
                cli.StringOption('-c', '--catalog', 'catalog',
                                 'the application catalog jar file path',
                                 required = True),
                cli.StringOption('-d', '--deployment', 'deployment',
                                 'the deployment configuration file path',
                                 default = 'deployment.xml'),
                cli.StringOption('-H', '--host', 'host',
                                 'the coordinating host as HOST[:PORT]',
                                 default = 'localhost'),
                cli.StringOption('-l', '--license', 'license',
                                 'the license file path'))
        # Add appropriate server-ish Java options.
        self.merge_java_options('java_opts_override',
                'server',
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
        self.add_cli_options(
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
        print 'HelpVerb: %s' % self.cli_spec.description
        self.set_defaults(description = 'Display command help.',
                          usage       = '[COMMAND ...]',
                          baseverb    = True)
        self.add_cli_options(
            cli.BooleanOption('-a', '--all', 'all',
                              'display all available help, including verb usage'))

    def go(self, runner, *args):
        runner.help(all = runner.opts.all, *runner.args)

#===============================================================================
class PackageVerb(CommandVerb):
#===============================================================================
    """
    Verb to create a Python-runnable package. Used by @VOLT.Package decorator.
    """
    def __init__(self, name, function, **kwargs):
        CommandVerb.__init__(self, name, function, **kwargs)
        kwargs = copy.copy(kwargs)
        self.set_defaults(description  = 'Create a Python-runnable program package.',
                          usage        = '[NAME ...]',
                          baseverb     = True,
                          description2 = '''\
The optional NAME argument(s) allow package generation for base commands other
than the current one. If no NAME is provided the current base command is
packaged.''')
        self.add_cli_options(
            cli.BooleanOption('-f', '--force', 'force',
                              'overwrite existing file without asking', default = False),
            cli.StringOption('-o', '--output_dir', 'outdir',
                             'specify output directory (defaults to working directory)'))

    def go(self, runner, *args):
        runner.package(runner.opts.outdir, runner.opts.force, *args)

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
        # Automatically set passthrough to True.
        kwargs['passthrough'] = True
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

