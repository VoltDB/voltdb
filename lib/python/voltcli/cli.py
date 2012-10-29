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
        if 'required' in kwargs:
            self.required = kwargs.pop('required')
        else:
            self.required = False
        self.kwargs = kwargs
        self.kwargs['dest'] = dest
        self.kwargs['help'] = help_msg
        if 'default' in self.kwargs:
            if utility.is_string(kwargs['default']):
                self.kwargs['help'] += ' (default="%s")' % self.kwargs['default']
            else:
                self.kwargs['help'] += ' (default=%s)' % self.kwargs['default']
        if self.required:
            self.kwargs['help'] += ' (required)'

    def get_option_names(self):
        return [a for a in (self.short_opt, self.long_opt) if a is not None]

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
        BaseOption.__init__(self, short_opt, long_opt, dest, help_msg, type = 'string', **kwargs)

#===============================================================================
class IntegerOption(BaseOption):
#===============================================================================
    """
    Integer CLI option.
    """
    def __init__(self, short_opt, long_opt, dest, help_msg, **kwargs):
        BaseOption.__init__(self, short_opt, long_opt, dest, help_msg, type = 'int', **kwargs)

#===============================================================================
class BaseArgument(object):
#===============================================================================
    def __init__(self, name, help):
        self.name = name
        self.help = help
    def get(self):
        utility.abort('BaseArgument subclass must implement a get() method: %s'
                            % self.__class__.__name__)

#===============================================================================
class StringArgument(BaseArgument):
#===============================================================================
    def __init__(self, name, help):
        BaseArgument.__init__(self, name, help)
    def get(self, value):
        return str(value)

#===============================================================================
class IntegerArgument(BaseArgument):
#===============================================================================
    def __init__(self, name, help):
        BaseArgument.__init__(self, name, help)
    def get(self, value):
        try:
            return int(value)
        except ValueError, e:
            utility.abort('"%s" argument is not a valid integer: %s' % value)

#===============================================================================
class ParsedCommand(object):
#===============================================================================
    """
    Holds the result of parsing a CLI command.
    """
    def __init__(self, outer_parser, outer_opts, parser, opts, args, verb):
        self.outer_parser = outer_parser
        self.outer_opts   = outer_opts
        self.opts         = opts
        self.args         = args
        self.parser       = parser
        self.verb         = verb

    def __str__(self):
        return 'ParsedCommand: %s | %s %s %s' % (self.outer_opts, self.verb.name,
                                                 self.opts, self.args)

#===============================================================================
class VoltCLICommandPreprocessor(object):
#===============================================================================
    """
    This class knows how to iterate the outer options and find the verb.
    It can do simplistic parsing of the outer options to allow early option
    checking before the full command line is parsed. This allows the parsing
    process itself to display debug messages.
    """
    def __init__(self, options):
        """
        Command line preprocessor constructor. Initializes metadata.
        """
        self.options = options
        # Determine which options require arguments.
        self.arg_opts = set()
        for option in options:
            if (not 'action' in option.kwargs or option.kwargs['action'] == 'store'):
                for opt in option.args:
                    arg_opts.add(opt)
        # Clear the parsed data.
        self.clear()

    def clear(self):
        """
        Clear the parsed data.
        """
        self.cmdargs       = []
        self.iverb         = 0
        self.option_values = []
        self.outer_opts    = []
        self.verb_cmdargs  = []
        self.verb          = None

    def preprocess(self, cmdargs):
        """
        Preprocess the command line. Populate verb and option data.
        """
        self.clear()
        self.cmdargs = cmdargs
        iopt = 0
        while iopt < len(self.cmdargs):
            if not self.cmdargs[iopt].startswith('-'):
                # Found the verb - we're done.
                break
            if self.cmdargs[iopt] in self.arg_opts:
                # Option with argument
                self.option_values.append((self.cmdargs[iopt], self.cmdargs[iopt+1]))
                iopt += 2
            else:
                # Boolean option
                self.option_values.append((self.cmdargs[iopt], True))
                iopt += 1
        self.iverb = iopt
        self.outer_opts = list(self.cmdargs[:self.iverb])
        if iopt < len(cmdargs):
            self.verb = self.cmdargs[self.iverb].lower()
            self.verb_cmdargs = list(self.cmdargs[self.iverb+1:])

    def get_option(self, *names):
        """
        Get an option value or None if not set. Allow multiple option names.
        When multiple values exist the last one wins.
        """
        ret_value = None
        for name, value in self.option_values:
            if name in names:
                if value is not None:
                    ret_value = value
        return ret_value

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
class VerbOptionParser(ExtendedHelpOptionParser):
#===============================================================================
    """
    Option parser to extend the help displayed for verbs to include arguments.
    """
    def __init__(self, verb):
        self.verb = verb
        ExtendedHelpOptionParser.__init__(self, description = verb.cli_spec.description,
                                                usage       = get_verb_usage(verb))

    def on_format_epilog(self):
        if self.verb.get_argument_count() == 0:
            return ''
        rows = [(a.name.upper(), a.help) for a in self.verb.iter_arguments()]
        return '\nArguments:\n%s\n' % utility.format_table(rows, indent = 2)

#===============================================================================
class VoltCLICommandProcessor(ExtendedHelpOptionParser):
#===============================================================================
    """
    Bi-level argument/option parsing and validation class. Handles both global
    and verb-specific arguments and options.
    """

    def __init__(self, verbs, options, usage, description, version):
        """
        Command line processor constructor.
        """
        self.verbs      = verbs
        self.verb_names = verbs.keys()
        self.options    = options
        self.preproc    = VoltCLICommandPreprocessor(self.options)
        full_usage      = '%s\n' % usage
        self.verb_names.sort()
        if verbs:
            for verb_name in self.verb_names:
                verb = self.verbs[verb_name]
                if not verb.cli_spec.baseverb:
                    full_usage += '\n       %s' % get_verb_usage(verb)
            full_usage += '\n'
            for verb_name in self.verb_names:
                verb = self.verbs[verb_name]
                if verb.cli_spec.baseverb:
                    full_usage += '\n       %s' % get_verb_usage(verb)
        optparse.OptionParser.__init__(self,
            description = description,
            usage       = full_usage,
            version     = version)
        self.options.sort()
        for option in self.options:
            self.add_option(*option.get_option_names(), **option.kwargs)

    def create_verb_parser(self, verb):
        """
        Create CLI option parser for verb command line.
        """
        # Parse the command-specific options.
        parser = VerbOptionParser(verb)
        if self.preproc.verb:
            for option in verb.iter_options():
                try:
                    parser.add_option(*option.get_option_names(), **option.kwargs)
                except Exception, e:
                    utility.abort('Exception initializing options for verb "%s".'
                                        % verb.name, e)
        return parser

    def check_verb_options(self, verb, opts):
        """
        Validate the verb options, e.g. check that required options are present.
        """
        max_width = 0
        missing = []
        for o in verb.iter_options(required_only = True):
            missing_opt = ', '.join(o.get_option_names())
            max_width = max(len(missing_opt), max_width)
            missing.append((missing_opt, o.kwargs['help']))
        # Abort if required options are missing.
        check_missing_items('option', missing)

    def process_verb_arguments(self, verb, verb_args, verb_opts):
        """
        Validate the verb arguments. Check that required arguments are present.
        Set option values for required arguments and remove them from the
        argument list.
        """
        # Add fixed arguments passed in through the decorator to the verb object.
        args = copy.copy(verb_args) + verb.command_arguments
        # Set attributes for required arguments.
        missing = []
        iarg = 0
        for o in verb.iter_arguments():
            if iarg < len(args):
                setattr(verb_opts, o.name, args[iarg])
            else:
                missing.append((o.name, o.help))
            iarg += 1
        # Abort if arguments are missing.
        check_missing_items('argument', missing)
        # Return the argument list with the required arguments removed.
        return args[iarg:]

    def parse(self, cmdargs):
        """
        Parse command line.
        """
        # Separate the global options preceding the command from
        # command-specific options that follow it.
        # Allow no verb for help and version requests.
        self.preproc.preprocess(cmdargs)
        allow_no_verb = (self.preproc.get_option('-h', '--help', '--version') is not None)
        if self.preproc.verb is None and not allow_no_verb:
            self._abort('No verb was specified.')

        # See if we know about the verb.
        if self.preproc.verb is None:
            verb = None
        else:
            if self.preproc.verb not in self.verbs:
                self._abort('Unknown command: %s' % self.preproc.verb)
            verb = self.verbs[self.preproc.verb]

        # Parse the global options. args should be empty
        outer_opts, outer_args = optparse.OptionParser.parse_args(self, self.preproc.outer_opts)
        assert len(outer_args) == 0

        # Parse the command-specific options.
        verb_parser = self.create_verb_parser(verb)
        if self.preproc.verb:
            if verb.cli_spec.passthrough:
                # Provide all options and arguments without processing the options.
                # E.g. Java programs want to handle all the options without interference.
                verb_args = self.preproc.verb_cmdargs
                verb_opts = None
            else:
                # Parse the verb command line.
                verb_opts, verb_args = verb_parser.parse_args(self.preproc.verb_cmdargs)
                # Check for required options.
                self.check_verb_options(verb, verb_opts)
                # Post-process expected arguments.
                verb_args = self.process_verb_arguments(verb, verb_args, verb_opts)
        else:
            verb_opts = None
            args = []

        return ParsedCommand(self, outer_opts, verb_parser, verb_opts, verb_args, verb)

    def on_format_epilog(self):
        return self._format_verb_list()

    def _abort(self, *msgs):
        utility.error(*msgs)
        sys.stdout.write('\n')
        self.print_help()
        sys.stdout.write('\n\n')
        utility.abort()

    def _format_verb_list(self):
        rows1 = []
        rows2 = []
        for verb_name in self.verb_names:
            verb = self.verbs[verb_name]
            if verb.cli_spec.baseverb:
                rows2.append((verb.name, verb.cli_spec.description))
            else:
                rows1.append((verb.name, verb.cli_spec.description))
        table1 = utility.format_table(rows1, caption = 'Verb Descriptions')
        table2 = utility.format_table(rows2, caption = 'Common Verbs')
        return '\n%s\n\n%s' % (table1, table2)

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

#===============================================================================
def check_missing_items(type_name, missing_items):
#===============================================================================
    """
    Look at item list with (name, description) pairs and abort with a useful
    error message if the list isn't empty.
    """
    if missing_items:
        if len(missing_items) > 1:
            plural = 's'
        else:
            plural = ''
        fmt = '%%-%ds  %%s' % max([len(o) for (o, h) in missing_items])
        utility.abort('Missing required %s%s:' % (type_name, plural),
                      (fmt % (o.upper(), h) for (o, h) in missing_items))

#===============================================================================
def get_verb_usage(verb):
#===============================================================================
    """
    Provide the full usage string, including argument names, for a verb.
    """
    usage = '%%prog %s' % verb.name
    if verb.cli_spec.usage:
        usage += ' %s' % verb.cli_spec.usage
    for o in verb.iter_arguments():
        usage += ' %s' % o.name.upper()
    return usage
