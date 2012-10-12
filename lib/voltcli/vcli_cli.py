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

import VOLT
import vcli_util

# Volt CLI command processor

# Individual option variables are added by the option parser. They are available
# externally as module attributes.

class ParsedCommand(object):
    def __init__(self, outer_parser, outer_opts, inner_parser, inner_opts, inner_args, verb):
        self.outer_parser = outer_parser
        self.outer_opts   = outer_opts
        self.inner_opts   = inner_opts
        self.inner_args   = inner_args
        self.inner_parser = inner_parser
        self.verb         = verb

class VoltCLICommandProcessor(optparse.OptionParser):
    '''
    Bi-level argument/option parsing and validation class. Handles both global
    and verb-specific arguments and options.
    '''

    def __init__(self, verbs, options_spec, aliases, usage, description, version):
        self.verbs        = verbs
        self.options_spec = options_spec
        self.aliases      = aliases
        full_usage        = '%s\n' % usage
        for verb in verbs:
            full_usage += '\n       %%prog %s %s' % (verb.name, verb.metadata.usage)
        optparse.OptionParser.__init__(self,
            description = description,
            usage       = full_usage,
            version     = version)
        for option_spec in self.options_spec:
            self.add_option(*option_spec.args, **option_spec.kwargs)

    def parse(self, cmdargs):

        # Separate the global options preceding the command from
        # command-specific options that follow it.
        iverb = 0
        allow_no_command = False
        while iverb < len(cmdargs):
            # Skip options and any associated option arguments.
            if cmdargs[iverb].startswith('-'):
                # Special detection of help and version options allows a command
                # line with no arguments to pass for those options.
                if cmdargs[iverb] in ('-h', '--help', '--version'):
                    allow_no_command = True
                for option_spec in self.options_spec:
                    if cmdargs[iverb] in option_spec.args:
                        # Skip the argument of an option that takes one
                        if (not 'action' in option_spec.kwargs or option_spec.kwargs['action'] == 'store'):
                            iverb += 1
            else:
                # Found the command.
                break
            iverb += 1
        if iverb == len(cmdargs) and not allow_no_command:
            self._abort('Missing command.')

        # Parse the global options. args should be empty
        outer_opts, outer_args = optparse.OptionParser.parse_args(self, list(cmdargs[:iverb]))
        assert len(outer_args) == 0

        # Resolve an alias (not recursive).
        verb_name = cmdargs[iverb].lower()
        if verb_name in self.aliases:
            # Use shell-style splitting to get mapped verb and arguments.
            alias_tokens = shlex.split(self.aliases[verb_name])
            if len(alias_tokens) == 0:
                self._abort('Missing alias definition for "%s"' % verb_name)
            # The first token is the mapped verb
            verb_name = alias_tokens[0]
            # Prepend the remaing arguments to the command line arguments.
            if len(alias_tokens) > 1:
                outer_args = alias_tokens[1:] + outer_args

        # See if we know about the verb.
        verb = None
        for verb_chk in self.verbs:
            if verb_chk.name == verb_name:
                verb = verb_chk
                break
        else:
            self._abort('Unknown command: %s' % verb_name)

        # Parse the command-specific options.
        inner_parser = optparse.OptionParser(
                description = verb.metadata.description,
                usage = '%%prog %s %s' % (verb.name, verb.metadata.usage))
        if iverb < len(cmdargs):
            if verb.metadata.options_spec:
                for option_spec in verb.metadata.options_spec:
                    inner_parser.add_option(*option_spec.args, **option_spec.kwargs)
            if verb.metadata.passthrough:
                # Provide all options and arguments without processing the options.
                # E.g. Java programs want to handle all the options without interference.
                inner_args = cmdargs[iverb+1:]
                inner_opts = None
            else:
                # Parse the inner command line.
                inner_opts, inner_args = inner_parser.parse_args(list(cmdargs[iverb+1:]))
        else:
            inner_opts = None
            inner_args = []

        return ParsedCommand(self, outer_opts, inner_parser, inner_opts, inner_args, verb)

    def _abort(self, *msgs):
        vcli_util.error(*msgs)
        sys.stdout.write('\n')
        self.print_help()
        sys.stdout.write('\n')
        sys.stderr.write('\n')
        vcli_util.abort()

    def format_epilog(self, formatter):
        rows = ((verb.name, verb.metadata.description) for verb in self.verbs)
        return '\n%s' % vcli_util.format_table("Subcommand Descriptions", None, rows)
