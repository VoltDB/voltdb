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

# Use the internal utility module that has no circular dependencies.
import _util

# Set during option processing
debug = False

# Volt CLI option parser

# Individual option variables are added by the option parser. They are available
# externally as module attributes.

class VoltCLIOptionParser(optparse.OptionParser):
    '''
    Bi-level argument/option parsing and validation class. Handles both global
    and verb-specific arguments and options.
    '''

    def __init__(self, verbs, options, usage, description, version):
        self.verbs   = verbs
        self.options = options
        full_usage = '%s\n' % usage
        for verb in verbs:
            full_usage += '\n       %s' % verb.metadata.usage
        optparse.OptionParser.__init__(self,
            description = description,
            usage       = full_usage,
            version     = version)
        for cli_option in options:
            self.add_option(*cli_option.args, **cli_option.kwargs)

    def parse(self, cmdargs):

        # Separate the global options preceding the command from
        # command-specific options that follow it.
        iverb = 0
        while iverb < len(cmdargs):
            # Skip options and any associated option arguments.
            if cmdargs[iverb].startswith('-'):
                for opt in self.options:
                    if cmdargs[iverb] in opt.args:
                        # Skip the argument of an option that takes one
                        if (not 'action' in opt.kwargs or opt.kwargs['action'] == 'store'):
                            iverb += 1
            else:
                # Found the command.
                break
            iverb += 1

        # Parse the global options. args should be empty
        opts, args = optparse.OptionParser.parse_args(self, list(cmdargs[:iverb]))
        assert len(args) == 0

        # Set all the options as module attributes.
        for option in self.options:
            name = option.kwargs['dest']
            setattr(sys.modules[__name__], name, getattr(opts, name))

        if iverb == len(cmdargs):
            self._abort('Missing command.')
        verb_name = cmdargs[iverb].lower()
        verb = None
        for verb_chk in self.verbs:
            if verb_chk.name == verb_name:
                verb = verb_chk
                break
        else:
            self._abort('Unknown command: %s' % verb_name)

        # Parse the command-specific options.
        verb_parser = optparse.OptionParser(description = verb.metadata.description,
                                            usage = verb.metadata.usage)
        if iverb < len(cmdargs):
            if verb.metadata.options:
                for opt in verb.metadata.options:
                    verb_parser.add_option(*opt.args, **opt.kwargs)
            options, args = verb_parser.parse_args(list(cmdargs[iverb+1:]))
        else:
            options = None
            args = []

        return verb, options, args, self, verb_parser

    def _abort(self, *msgs):
        _util.error(*msgs)
        sys.stdout.write('\n')
        self.print_help()
        sys.stdout.write('\n')
        sys.stderr.write('\n')
        _util.abort()

    def format_epilog(self, formatter):
        rows = ((verb.name, verb.metadata.description) for verb in self.verbs)
        return '\n%s' % _util.format_table("Commands", None, rows)
