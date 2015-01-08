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
import optparse
import shlex
import copy

from voltcli import utility

# Volt CLI command processor

# Individual option variables are added by the option parser. They are available
# externally as module attributes.

#===============================================================================
class BaseOption(object):
#===============================================================================
    """
    General CLI option specification (uses optparse keywords for now).
    """
    def __init__(self, short_opt, long_opt, dest, help_msg, **kwargs):
        self.short_opt = short_opt
        self.long_opt  = long_opt
        self.kwargs    = kwargs
        self.kwargs['dest'] = dest
        # A help message of None makes it a hidden option.
        if help_msg is not None:
            self.kwargs['help'] = help_msg
            if 'default' in self.kwargs:
                if utility.is_string(kwargs['default']):
                    self.kwargs['help'] += ' (default="%s")' % self.kwargs['default']
                else:
                    self.kwargs['help'] += ' (default=%s)' % self.kwargs['default']
        else:
            self.kwargs['help'] = optparse.SUPPRESS_HELP

    def get_option_names(self):
        return [a for a in (self.short_opt, self.long_opt) if a is not None]

    def get_dest(self):
        if 'dest' not in self.kwargs:
            utility.abort('%s must specify a "dest" property.' % self.__class__.__name__)
        return self.kwargs['dest']

    def get_default(self):
        return self.kwargs.get('default', None)

    def postprocess_value(self, value):
        # Hook for massaging the option instance value. Default to NOP.
        return value

    def __str__(self):
        return '%s(%s/%s %s)' % (self.__class__.__name__,
                                 self.short_opt, self.long_opt, self.kwargs)

    def __cmp__(self, other):
        # Sort options by lowercase letter or word, depending on which is available.
        if self.short_opt:
            if other.short_opt:
                return cmp(self.short_opt.lower(), other.short_opt.lower())
            return 1
        if other.short_opt:
            return -1
        if self.long_opt:
            if other.long_opt:
                return cmp(self.long_opt.lower(), other.long_opt.lower())
            return 1
        if other.long_opt:
            return -1
        return 0

    def has_value(self):
        return (not 'action' in self.kwargs or self.kwargs['action'] == 'store')

#===============================================================================
class BooleanOption(BaseOption):
#===============================================================================
    """
    Boolean CLI option.
    """
    def __init__(self, short_opt, long_opt, dest, help_msg, **kwargs):
        BaseOption.__init__(self, short_opt, long_opt, dest, help_msg,
                           action = 'store_true', **kwargs)

#===============================================================================
class StringOption(BaseOption):
#===============================================================================
    """
    CLI string value option.
    """
    def __init__(self, short_opt, long_opt, dest, help_msg, **kwargs):
        BaseOption.__init__(self, short_opt, long_opt, dest, help_msg, **kwargs)

#===============================================================================
class IntegerOption(BaseOption):
#===============================================================================
    """
    Integer CLI option.
    """
    def __init__(self, short_opt, long_opt, dest, help_msg, **kwargs):
        BaseOption.__init__(self, short_opt, long_opt, dest, help_msg, **kwargs)
    def postprocess_value(self, value):
        if type(value) is not int:
            try:
                converted = int(value.strip())
            except ValueError:
                utility.abort('Bad "%s" integer value: %s' % (self.get_dest().upper(), value))
            return converted
        return value

#===============================================================================
class StringListOption(StringOption):
#===============================================================================
    """
    CLI comma-separated string list option.
    """
    def __init__(self, short_opt, long_opt, dest, help_msg, **kwargs):
        StringOption.__init__(self, short_opt, long_opt, dest, help_msg, **kwargs)
    def postprocess_value(self, value):
        return [v.strip() for v in value.split(',')]

#===============================================================================
class IntegerListOption(StringOption):
#===============================================================================
    """
    CLI comma-separated integer list option.
    """
    def __init__(self, short_opt, long_opt, dest, help_msg, **kwargs):
        StringOption.__init__(self, short_opt, long_opt, dest, help_msg, **kwargs)
    def postprocess_value(self, value):
        bad = []
        converted = []
        for v in value.split(','):
            try:
                converted.append(int(v.strip()))
            except ValueError:
                bad.append(v.strip())
        if bad:
            utility.abort('Bad "%s" integer list value(s):' % self.get_dest().upper(), bad)
        return converted

#===============================================================================
class EnumOption(StringOption):
#===============================================================================
    """
    Enumeration option for selecting from a list of possible symbols.
    """
    def __init__(self, short_opt, long_opt, dest, help_pfx, *values, **kwargs):
        if not values or len(values) <= 1:
            utility.abort('EnumOption "%s" must specify multiple valid values.' % dest)
        self.values = values
        help_msg = '%s [%s]' % (help_pfx, '|'.join(self.values))
        StringOption.__init__(self, short_opt, long_opt, dest, help_msg, **kwargs)
    def postprocess_value(self, value):
        if value not in self.values:
            utility.abort('EnumOption "%s" value "%s" is not one of the following:'
                              % (self.get_dest(), value), self.values)
        return value

#===============================================================================
class HostOption(StringOption):
#===============================================================================
    """
    Comma-separated HOST[:PORT] list option.
    """
    def __init__(self, short_opt, long_opt, dest, name, **kwargs):
        self.min_count    = utility.kwargs_get_integer(kwargs, 'min_count', default = 1)
        self.max_count    = utility.kwargs_get_integer(kwargs, 'max_count', default = 1)
        self.default_port = utility.kwargs_get_integer(kwargs, 'default_port', default = 21212)
        if self.max_count == 1:
            help_msg = 'the %s HOST[:PORT]' % name
        else:
            help_msg = 'the comma-separated %s HOST[:PORT] list' % name
        if self.default_port:
            help_msg += ' (default port=%d)' % self.default_port
        StringOption.__init__(self, short_opt, long_opt, dest, help_msg, **kwargs)
    def postprocess_value(self, value):
        hosts = utility.parse_hosts(value,
                                    min_hosts = self.min_count,
                                    max_hosts = self.max_count,
                                    default_port = self.default_port)
        if self.max_count == 1:
            return hosts[0]
        return hosts

#===============================================================================
class ArgumentException(Exception):
#===============================================================================
    pass

#===============================================================================
class BaseArgument(object):
#===============================================================================
    def __init__(self, name, help, **kwargs):
        self.name      = name
        self.help      = help
        self.min_count = kwargs.get('min_count', 1)
        self.max_count = kwargs.get('max_count', 1)
        # A max_count value of None is interpreted as infinity.
        if self.max_count is None:
            self.max_count = sys.maxint
    def get(self, value):
        utility.abort('BaseArgument subclass must implement a get(value) method: %s'
                            % self.__class__.__name__)

#===============================================================================
class StringArgument(BaseArgument):
#===============================================================================
    def __init__(self, name, help, **kwargs):
        BaseArgument.__init__(self, name, help, **kwargs)
    def get(self, value):
        return str(value)

#===============================================================================
class IntegerArgument(BaseArgument):
#===============================================================================
    def __init__(self, name, help, **kwargs):
        BaseArgument.__init__(self, name, help, **kwargs)
    def get(self, value):
        try:
            return int(value)
        except ValueError, e:
            raise ArgumentException('%s value is not a valid integer: %s'
                                        % (self.name.upper(), str(value)))

#===============================================================================
class PathArgument(StringArgument):
#===============================================================================
    def __init__(self, name, help, **kwargs):
        # For now the only intelligence is to check for absolute paths when required.
        # TODO: Add options to check for directories, files, attributes, etc..
        self.absolute = utility.kwargs_get_boolean(kwargs, 'absolute', default = False)
        self.exists   = utility.kwargs_get_boolean(kwargs, 'exists', default = False)
        requirements = []
        help2 = ''
        if self.absolute:
            requirements.append('absolute path')
        if self.exists:
            requirements.append('must exist')
        if requirements:
            help2 = ' (%s)' % ', '.join(requirements)
        StringArgument.__init__(self, name, help + help2, **kwargs)
    def get(self, value):
        svalue = str(value)
        if self.absolute and not svalue.startswith('/'):
            raise ArgumentException('%s path is not absolute: %s' % (self.name.upper(), svalue))
        if self.exists and not os.path.exists(svalue):
            raise ArgumentException('%s path does not exist: %s' % (self.name.upper(), svalue))
        return svalue

#===============================================================================
class ParsedCommand(object):
#===============================================================================
    """
    Holds the result of parsing a CLI command.
    """
    def __init__(self, parser, opts, args, verb):
        self.opts   = opts
        self.args   = args
        self.parser = parser
        self.verb   = verb

    def __str__(self):
        return 'ParsedCommand: %s %s %s' % (self.verb.name, self.opts, self.args)

#===============================================================================
class ExtendedHelpOptionParser(optparse.OptionParser):
#===============================================================================
    '''
    Extends OptionParser in order to support extended help.
    '''
    def __init__(self, *args, **kwargs):
        self.format_epilog_called = False
        optparse.OptionParser.__init__(self, *args, **kwargs)

    def format_epilog(self, formatter):
        """
        OptionParser hook that allows us to append verb descriptions to the
        help message.
        """
        self.format_epilog_called = True
        return self.on_format_epilog()

    def print_help(self):
        """
        Override OptionParser.print_help() to work around Python 2.4 optparse
        not supporting format_epilog().
        """
        self.format_epilog_called = False
        optparse.OptionParser.print_help(self)
        if not self.format_epilog_called:
            sys.stdout.write(self.on_format_epilog())

    def on_format_epilog(self):
        utility.abort('ExtendedHelpOptionParser subclass must override on_format_epilog(): %s'
                            % self.__class__.__name__)

#===============================================================================
class CLIParser(ExtendedHelpOptionParser):
#===============================================================================
    """
    Command/sub-command (verb) argument and option parsing and validation.
    """

    def __init__(self, prog, verbs, base_options, usage, description, version):
        """
        Command line processor constructor.
        """
        self.prog         = prog
        self.verb         = None
        self.verbs        = verbs
        self.verb_names   = verbs.keys()
        self.base_options = base_options
        self.verb_names.sort()
        self.base_options.sort()
        optparse.OptionParser.__init__(self,
            prog        = prog,
            description = description,
            usage       = usage,
            version     = version)

    def add_base_options(self):
        """
        Add the base options.
        """
        for option in self.base_options:
            self.add_option(*option.get_option_names(), **option.kwargs)

    def add_verb_options(self, verb):
        """
        Add options for verb command line.
        """
        for option in verb.iter_options():
            try:
                self.add_option(*option.get_option_names(), **option.kwargs)
            except Exception, e:
                utility.abort('Exception initializing options for verb "%s".' % verb.name, e)

    def process_verb_options(self, verb, opts):
        """
        Validate the verb options and post-process the values.
        """
        max_width = 0
        missing = []
        # Post-process the option values, e.g. convert strings to lists as needed.
        for o in verb.iter_options():
            dest = o.get_dest()
            value = getattr(opts, dest)
            if not value is None:
                setattr(opts, dest, o.postprocess_value(value))

    def process_verb_arguments(self, verb, verb_args, verb_opts):
        """
        Validate the verb arguments. Check that required arguments are present
        and populate verb_opts attributes with scalar values or lists (for
        trailing arguments with max_count > 1).
        """
        # Add fixed arguments passed in through the decorator to the verb object.
        args = copy.copy(verb_args) + verb.command_arguments
        # Set attributes for required arguments.
        missing = []
        exceptions = []
        iarg = 0
        nargs = verb.get_argument_count()
        for arg in verb.iter_arguments():
            # It's missing if we've exhausted all the arguments before
            # exhausting all the argument specs, unless it's the last argument
            # spec and it's optional.
            if iarg > len(args) or (iarg == len(args) and arg.min_count > 0):
                missing.append((arg.name, arg.help))
            else:
                value = None
                # The last argument can have repeated arguments. If more than
                # one are allowed the values are put into a list.
                if iarg == nargs - 1 and arg.max_count > 1:
                    if len(args) - iarg < arg.min_count:
                        utility.abort('A minimum of %d %s arguments are required.'
                                            % (arg.min_count, arg.name.upper()))
                    if len(args) - iarg > arg.max_count:
                        utility.abort('A maximum of %d %s arguments are allowed.'
                                            % (arg.max_count, arg.name.upper()))
                    # Pass through argument class get() for validation, conversion, etc..
                    # Skip bad values and report on them at the end.
                    value = []
                    for v in args[iarg:]:
                        try:
                            value.append(arg.get(v))
                        except ArgumentException, e:
                            exceptions.append(e)
                    iarg = len(args)
                elif len(args) > 0:
                    # All other arguments are treated as scalars.
                    # Pass through argument class get() for validation, conversion, etc..
                    try:
                        value = arg.get(args[iarg])
                    except ArgumentException, e:
                        exceptions.append(e)
                    iarg += 1
                if value is not None or arg.min_count == 0:
                    setattr(verb_opts, arg.name, value)
        # Run the gauntlet of error disclosure. Abort and display usage as appropriate.
        had_errors = 0
        show_usage = False
        if exceptions:
            msg = 'Argument value %s:' % utility.pluralize('error', len(exceptions))
            utility.error(msg, [str(e) for e in exceptions])
            had_errors += 1
        if iarg < len(args):
            self._abort('Extra arguments were provided:', args[iarg:])
            had_errors += 1
            show_usage = True
        if missing:
            fmt = '%%-%ds  %%s' % max([len(o) for (o, h) in missing])
            msg = 'Missing required %s:' % utility.pluralize('argument', len(missing))
            utility.error(msg, [fmt % (o.upper(), h) for (o, h) in missing])
            had_errors += 1
            show_usage = True
        if had_errors > 0:
            if show_usage:
                self._abort()
            sys.exit(1)

    def initialize_verb(self, verb_name):
        """
        Initialize command line options for a specific verb.
        """
        # Add the base options that are applicable to all verbs.
        self.add_base_options()

        # See if we know about the verb.
        if verb_name.startswith('-'):
            self._abort('The first argument must be a verb, not an option.')
        if verb_name not in self.verbs:
            self._abort('Unknown verb: %s' % verb_name)
        self.verb = self.verbs[verb_name]

        # Change the messaging from generic to verb-specific.
        self.set_usage(self._get_verb_usage(self.verb, brief=False))
        self.set_description(self.verb.cli_spec.get_attr('description', 'No description provided'))

        # Parse the command-specific options.
        self.add_verb_options(self.verb)

    def parse(self, *cmdargs):
        """
        Parse command line.
        """
        # Need something.
        if not cmdargs:
            self._abort('No verb was specified.')
        pre_opts = preprocess_options(self.base_options, cmdargs)

        # Support verb-less options like -h, --help and --version.
        if cmdargs[0].startswith('-') and (pre_opts.help or pre_opts.version):
            opts, args = self.parse_args(list(cmdargs))
            return ParsedCommand(self, opts, args, None)

        # Initialize options and arguments.
        self.initialize_verb(cmdargs[0])
        verb_cmdargs = list(cmdargs[1:])

        if self.verb.cli_spec.passthrough:
            # Provide all options and arguments without processing the options.
            # E.g. Java programs want to handle all the options without interference.
            verb_args = verb_cmdargs
            verb_opts = None
        else:
            # Parse the verb command line.
            verb_opts, verb_parsed_args = self.parse_args(verb_cmdargs)
            # Post-process options.
            self.process_verb_options(self.verb, verb_opts)
            # Post-process arguments.
            self.process_verb_arguments(self.verb, verb_parsed_args, verb_opts)
            # The arguments should all be attributes in verb_opts now.
            verb_args = []

        return ParsedCommand(self, verb_opts, verb_args, self.verb)

    def get_usage_string(self):
        """
        Get usage string.
        """
        # Swap stdout with UsageScraper pseudo-file object so that output is captured.
        # Necessary because optparse only sends help to stdout.
        class UsageScraper(object):
            def __init__(self):
                self.usage = []
            def write(self, s):
                self.usage.append(s)
        scraper = UsageScraper()
        stdout_save = sys.stdout
        try:
            sys.stdout = scraper
            self.print_help()
        finally:
            sys.stdout = stdout_saves
        return ''.join(scraper.usage)

    def on_format_epilog(self):
        if not self.verb:
            return self._format_verb_list()
        blocks = []
        if self.verb.get_argument_count() > 0:
            rows = [(get_argument_usage(a), a.help) for a in self.verb.iter_arguments()]
            blocks.append('\n'.join(['Arguments:', utility.format_table(rows, indent = 2)]))
        # other_info is used for the multi-verb variation list.
        other_info = self.verb.cli_spec.get_attr('other_info', None)
        if other_info:
            blocks.append(other_info.strip())
        # Automatically wrap description2 as a paragraph.
        description2 = self.verb.cli_spec.get_attr('description2', None)
        if description2:
            blocks.append(utility.paragraph(description2))
        return '\n%s' % '\n\n'.join(blocks)

    def _abort(self, *msgs):
        utility.error(*msgs)
        sys.stdout.write('\n')
        self.print_help()
        sys.stdout.write('\n')
        sys.exit(1)

    def _format_verb_list(self):
        rows1 = []
        rows2 = []
        for verb_name in self.verb_names:
            verb = self.verbs[verb_name]
            if not verb.cli_spec.hideverb:
                usage = self._get_verb_usage(verb, brief=True)
                if verb.cli_spec.baseverb:
                    rows2.append((usage, verb.cli_spec.description))
                else:
                    rows1.append((usage, verb.cli_spec.description))
        table1 = utility.format_table(rows1, caption = 'Verb Descriptions', separator = '  ')
        table2 = utility.format_table(rows2, caption = 'Common Verbs', separator = '  ')
        return '%s\n%s' % (table1, table2)

    def _iter_options(self, verb):
        options = []
        for option in self.base_options:
            yield option
        if verb:
            for option in verb.iter_options():
                yield option

    def _iter_visible_options(self, verb):
        for option in self._iter_options(verb):
            if option.kwargs.get('help', None) != optparse.SUPPRESS_HELP:
                yield option

    def _count_visible_options(self, verb):
        return len([o for o in self._iter_visible_options(verb)])

    def _get_verb_usage(self, verb, brief=False):
        """
        Provide the full usage string, including argument names, for a verb.
        """
        args = [get_argument_usage(a) for a in verb.iter_arguments()]
        usage = [self.prog, verb.name]
        if not brief:
            num_visible_options = self._count_visible_options(verb)
            if num_visible_options > 0:
                usage.append('[ OPTIONS ... ]')
        if verb.cli_spec.usage:
            usage.append(verb.cli_spec.usage)
        if args:
            usage.append(' '.join(args))
        return ' '.join(usage)

#===============================================================================
class CLISpec(object):
#===============================================================================
    def __init__(self, **kwargs):
        self._kwargs = kwargs
        # Make sure options and arguments are flat lists.
        if 'options' in self._kwargs:
            self._kwargs['options'] = utility.flatten_to_list(self._kwargs['options'])
        else:
            self._kwargs['options'] = []
        if 'arguments' in self._kwargs:
            self._kwargs['arguments'] = utility.flatten_to_list(self._kwargs['arguments'])
        else:
            self._kwargs['arguments'] = []

    def __getattr__(self, name):
        return self._kwargs.get(name, None)

    def __str__(self):
        s = 'CLISpec: [\n'
        keys = self._kwargs.keys()
        keys.sort()
        for key in keys:
            s += '   %s: %s\n' % (key, utility.to_display_string(self._kwargs[key]))
        s += ']'
        return s

    def add_to_list(self, name, *args):
        utility.kwargs_merge_list(self._kwargs, name, *args)

    def get_attr(self, name, default = None):
        return utility.kwargs_get(self._kwargs, name, default = default, remove = False)

    def pop_attr(self, name, default = None):
        return utility.kwargs_get(self._kwargs, name, default = default, remove = True)

    def merge_java_options(self, name, *options):
        utility.kwargs_merge_java_options(self._kwargs, name, options)

    def set_defaults(self, **kwargs):
        utility.kwargs_set_defaults(self._kwargs, **kwargs)

    def find_option(self, dest_name):
        for o in self._kwargs['options']:
            if o.get_dest() == dest_name:
                return o
        return None

    def find_argument(self, dest_name):
        for a in self._kwargs['arguments']:
            if a.name == dest_name:
                return a
        return None

#===============================================================================
def get_argument_usage(a):
#===============================================================================
    if a.max_count > 1:
        ellipsis = ' ...'
    else:
        ellipsis = ''
    if a.min_count == 0:
        fmt = '[ %s%s ]'
    else:
        fmt = '%s%s'
    return fmt % (a.name.upper(), ellipsis)

#===============================================================================
def preprocess_options(base_options, cmdargs):
#===============================================================================
    """
    Simplistically parses command line options to allow early option checking.
    Allows the parsing process to display debug messages.  Returns an object
    with attributes set for option values.
    """
    class OptionValues(object):
        pass
    option_values = OptionValues()
    # Create a base option dictionary indexed by short and long options.
    # Add the built-in optparse help and version options so that they can be
    # detected as stand-alone options.
    options = {}
    builtins = [BooleanOption('-h', '--help', 'help', ''),
                BooleanOption(None, '--version', 'version', '')]
    for opt in list(base_options) + builtins:
        setattr(option_values, opt.get_dest(), opt.get_default())
        if opt.short_opt:
            options[opt.short_opt] = opt
        if opt.long_opt:
            options[opt.long_opt] = opt
    # Walk through the options and arguments and set option values as attributes.
    iopt = 0
    while iopt < len(cmdargs):
        if cmdargs[iopt].startswith('-'):
            if cmdargs[iopt] in options:
                opt = options[cmdargs[iopt]]
                if opt.has_value():
                    # Option with argument
                    setattr(option_values, opt.get_dest(), cmdargs[iopt+1])
                    iopt += 1
                else:
                    # Boolean option
                    setattr(option_values, opt.get_dest(), True)
        iopt += 1
    return option_values
