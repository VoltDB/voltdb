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
class CLIOption(object):
#===============================================================================
    """
    General CLI option specification (uses optparse keywords for now).
    """
    def __init__(self, short_opt, long_opt, dest, help_msg, **kwargs):
        self.args   = (short_opt, long_opt)
        if 'required' in kwargs:
            self.required = kwargs.pop('required')
        else:
            self.required = False
        self.kwargs = kwargs
        self.kwargs['dest'] = dest
        self.kwargs['help'] = help_msg
        if 'default' in self.kwargs:
            self.kwargs['help'] += ' (default=%s)' % self.kwargs['default']
        if self.required:
            self.kwargs['help'] += ' (required)'
    def __str__(self):
        return '%s(%s %s)' % (self.__class__.__name__, self.args, self.kwargs)

#===============================================================================
class CLIBoolean(CLIOption):
#===============================================================================
    """
    Boolean CLI option.
    """
    def __init__(self, short_opt, long_opt, dest, help_msg, **kwargs):
        CLIOption.__init__(self, short_opt, long_opt, dest, help_msg,
                           action = 'store_true', **kwargs)

#===============================================================================
class CLIValue(CLIOption):
#===============================================================================
    """
    CLI string value option.
    """
    def __init__(self, short_opt, long_opt, dest, help_msg, **kwargs):
        CLIOption.__init__(self, short_opt, long_opt, dest, help_msg, type = 'string', **kwargs)

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
    def __init__(self, cli_options):
        """
        Command line preprocessor constructor. Initializes metadata.
        """
        self.cli_options = cli_options
        # Determine which options require arguments.
        self.arg_opts = set()
        for cli_option in cli_options:
            if (not 'action' in cli_option.kwargs or cli_option.kwargs['action'] == 'store'):
                for opt in cli_option.args:
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
            self.verb = self.cmdargs[self.iverb]
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
class VoltCLICommandProcessor(optparse.OptionParser):
#===============================================================================
    '''
    Bi-level argument/option parsing and validation class. Handles both global
    and verb-specific arguments and options.
    '''

    def __init__(self, verbs, cli_options, usage, description, version):
        """
        Command line processor constructor.
        """
        self.verbs       = verbs
        self.verb_names  = verbs.keys()
        self.cli_options = cli_options
        self.preproc     = VoltCLICommandPreprocessor(self.cli_options)
        full_usage       = '%s\n' % usage
        self.verb_names.sort()
        if verbs:
            for verb_name in self.verb_names:
                verb = self.verbs[verb_name]
                full_usage += '\n       %%prog %s %s' % (verb.name, verb.cli_spec.usage)
        optparse.OptionParser.__init__(self,
            description = description,
            usage       = full_usage,
            version     = version)
        for cli_option in self.cli_options:
            self.add_option(*cli_option.args, **cli_option.kwargs)

    def create_verb_parser(self, verb):
        """
        Create CLI option parser for verb command line.
        """
        # Parse the command-specific options.
        parser = optparse.OptionParser(description = verb.cli_spec.description,
                                       usage = '%%prog %s %s' % (verb.name, verb.cli_spec.usage))
        if self.preproc.verb:
            if verb.cli_spec.cli_options:
                for cli_option in verb.cli_spec.cli_options:
                    try:
                        parser.add_option(*cli_option.args, **cli_option.kwargs)
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
        if self.preproc.verb:
            if verb.cli_spec.cli_options:
                for cli_option in verb.cli_spec.cli_options:
                    if cli_option.required and getattr(opts, cli_option.kwargs['dest']) is None:
                        missing_opt = ', '.join(cli_option.args)
                        max_width = max(len(missing_opt), max_width)
                        missing.append((missing_opt, cli_option.kwargs['help']))
        if missing:
            if len(missing) > 1:
                plural = 's'
            else:
                plural = ''
            fmt = '%%-%ds  %%s' % max_width
            utility.abort('Missing required option%s:' % plural,
                          (fmt % (o, h) for (o, h) in missing))

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
        else:
            verb_opts = None
            args = []

        return ParsedCommand(self, outer_opts, verb_parser, verb_opts, verb_args, verb)

    def format_epilog(self, formatter):
        """
        OptionParser hook that allows us to append verb descriptions to the
        help message.
        """
        rows = []
        for verb_name in self.verb_names:
            verb = self.verbs[verb_name]
            rows.append((verb.name, verb.cli_spec.description))
        return '\n%s' % utility.format_table("Verb Descriptions", None, rows)

    def _abort(self, *msgs):
        utility.error(*msgs)
        sys.stdout.write('\n')
        self.print_help()
        sys.stdout.write('\n\n')
        utility.abort()

#===============================================================================
class CLISpec(object):
#===============================================================================
    def __init__(self, **kwargs):
        self._kwargs = kwargs
        # Provide a default description
        if 'description' not in self._kwargs:
            self._kwargs['description'] = '(description missing)'
        # Make sure usage is a string, even if empty.
        if 'usage' not in self._kwargs:
            self._kwargs['usage'] = ''
        # Make sure cli_options is a flat list.
        if 'cli_options' in self._kwargs:
            self._kwargs['cli_options'] = utility.flatten_to_list(self._kwargs['cli_options'])
        else:
            self._kwargs['cli_options'] = []
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
