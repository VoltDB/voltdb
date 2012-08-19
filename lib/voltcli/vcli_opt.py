# This file is part of VoltDB.

# Copyright (C) 2008-2011 VoltDB Inc.
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

import vcli_meta

# Volt CLI option parser

# Individual option variables are added by the option parser. They are available
# externally as module attributes.

class VoltCLIOptionParser(optparse.OptionParser):
    '''
    Bi-level argument/option parsing and validation class. Handles both global
    and verb-specific arguments and options.
    '''

    def __init__(self):
        usage = '%s\n' % vcli_meta.cli.usage
        for verb in vcli_meta.verbs:
            usage += '\n       %s' % verb.metadata.usage
        optparse.OptionParser.__init__(self,
            description = vcli_meta.cli.description,
            usage       = usage,
            version     = vcli_meta.version_string)
        for cli_option in vcli_meta.cli.options:
            self.add_option(*cli_option.args, **cli_option.kwargs)

    def parse(self):

        # Separate the global options preceding the command from
        # command-specific options that follow it.
        iverb = 1
        while iverb < len(sys.argv):
            # Skip options and any associated option arguments.
            if sys.argv[iverb].startswith('-'):
                for opt in vcli_meta.cli.options:
                    if sys.argv[iverb] in opt.args:
                        # Skip the argument of an option that takes one
                        if (not 'action' in opt.kwargs or opt.kwargs['action'] == 'store'):
                            iverb += 1
            else:
                # Found the command.
                break
            iverb += 1

        # Parse the global options. args should be empty
        opts, args = optparse.OptionParser.parse_args(self, sys.argv[1:iverb])
        assert len(args) == 0

        # Set all the options as module attributes.
        for option in vcli_meta.cli.options:
            name = option.kwargs['dest']
            setattr(sys.modules[__name__], name, getattr(opts, name))

        if iverb == len(sys.argv):
            self._abort('Missing command.')
        verb_name = sys.argv[iverb].lower()
        verb = None
        for verb_chk in vcli_meta.verbs:
            if verb_chk.name == verb_name:
                verb = verb_chk
                break
        else:
            self._abort('Unknown command: %s' % verb_name)

        # Parse the command-specific options.
        verb_parser = optparse.OptionParser(description = verb.metadata.description,
            usage = verb.metadata.usage)
        if iverb + 1 < len(sys.argv):
            if verb.metadata.options:
                for opt in verb.metadata.options:
                    verb_parser.add_option(*opt.args, **opt.kwargs)
            options, args = verb_parser.parse_args(sys.argv[iverb+1:])
        else:
            options = None
            args = []

        return verb, options, args, verb_parser

    def _abort(self, *msgs):
        _util.error(*msgs)
        sys.stderr.write('\n')
        self.print_help()
        sys.stderr.write('\n')
        _util.abort()

    def format_epilog(self, formatter):
        max_verb_width = 0
        for verb in vcli_meta.verbs:
            max_verb_width = max(max_verb_width, len(verb.name))
        fmt = '%%-%ds  %%s' % max_verb_width
        return '''
COMMAND is one of the following:

%s
''' % '\n'.join([fmt % (verb.name, verb.metadata.description) for verb in vcli_meta.verbs])

def parse_cli():
    parser = VoltCLIOptionParser()
    return parser.parse()